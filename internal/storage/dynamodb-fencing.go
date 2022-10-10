/*
 *    Copyright 2022 scailio GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package storage

import (
	"context"
	"errors"
	"math/big"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	error2 "github.com/scailio-oss/dlock/error"
)

type DynamoDBWithFencing struct {
	dynamoDbClient *dynamodb.Client
	tableName      string
	timeout        time.Duration
}

// Creates a new DB implemnentation using a DynamoDB backend. It uses the given dynamoDB table name and adds the given
// timeout to all calls to dynamoDB.
func NewDynamoDbWithFencing(dynamoDbClient *dynamodb.Client, tableName string, timeout time.Duration) FencingDB {
	return &DynamoDBWithFencing{
		dynamoDbClient: dynamoDbClient,
		tableName:      tableName,
		timeout:        timeout,
	}
}

func (d *DynamoDBWithFencing) InsertNewLock(ctx context.Context, lockId string, ownerName string, leaseUntil time.Time, stealLockUntil time.Time) (*StolenLockInfo, *big.Int, error) {
	untilStr := strconv.FormatInt(leaseUntil.UnixMilli(), 10)
	stealUntilStr := strconv.FormatInt(stealLockUntil.UnixMilli(), 10)

	key := map[string]types.AttributeValue{
		pkFieldName: &types.AttributeValueMemberS{Value: lockId},
	}

	dynamoCtx, cancelGet := context.WithTimeout(ctx, d.timeout)
	defer cancelGet()
	getItemRes, err := d.dynamoDbClient.GetItem(dynamoCtx, &dynamodb.GetItemInput{
		Key:            key,
		TableName:      aws.String(d.tableName),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, nil, err
	}

	pkExists := expression.AttributeExists(expression.Name(pkFieldName))

	one := big.NewInt(1)
	fencingVal := one
	if getItemRes.Item[fencingTokenFieldName] != nil {
		// previous fencing token available

		oldUntilStr := getItemRes.Item[untilFieldName].(*types.AttributeValueMemberN).Value
		oldUntilInt, _ := strconv.ParseInt(oldUntilStr, 10, 64)

		if oldUntilInt >= stealLockUntil.UnixMilli() {
			return nil, nil, &error2.LockTakenError{}
		}

		oldFencingValBytes := (getItemRes.Item[fencingTokenFieldName].(*types.AttributeValueMemberB)).Value
		fencingVal = fencingVal.SetBytes(oldFencingValBytes)
		fencingVal = fencingVal.Add(fencingVal, one)

		// only replace with new fencing value if nothing changed in the DB in the meantime
		pkExists = expression.And(
			expression.And(
				expression.And(
					pkExists,
					expression.Equal(
						expression.Name(untilFieldName),
						expression.Value(&types.AttributeValueMemberN{Value: oldUntilStr}),
					),
				),
				expression.Equal(
					expression.Name(lockOwnerFieldName),
					expression.Value(&types.AttributeValueMemberS{Value: getItemRes.Item[lockOwnerFieldName].(*types.AttributeValueMemberS).Value}),
				),
			),
			expression.Equal(
				expression.Name(fencingTokenFieldName),
				expression.Value(&types.AttributeValueMemberB{Value: oldFencingValBytes}),
			),
		)
	} else {
		// no previous fencing token available

		// Note that this will fail as well if there is another insert concurrently which wins.
		pkExists = expression.And(
			pkExists,
			expression.LessThanEqual(
				expression.Name(untilFieldName),
				expression.Value(&types.AttributeValueMemberN{Value: stealUntilStr}),
			),
		)
	}

	cond := expression.Or(
		expression.AttributeNotExists(expression.Name(pkFieldName)),
		pkExists,
	)

	expr, _ := expression.NewBuilder().WithCondition(cond).Build()

	itm := map[string]types.AttributeValue{
		pkFieldName:           &types.AttributeValueMemberS{Value: lockId},
		lockOwnerFieldName:    &types.AttributeValueMemberS{Value: ownerName},
		untilFieldName:        &types.AttributeValueMemberN{Value: untilStr},
		fencingTokenFieldName: &types.AttributeValueMemberB{Value: fencingVal.Bytes()},
	}

	dynamoCtx, cancelPut := context.WithTimeout(ctx, d.timeout)
	defer cancelPut()
	out, err := d.dynamoDbClient.PutItem(dynamoCtx, &dynamodb.PutItemInput{
		Item:                      itm,
		TableName:                 aws.String(d.tableName),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ReturnValues:              types.ReturnValueAllOld,
	})

	if err != nil {
		var conditionalCheckFailedException *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckFailedException) {
			return nil, nil, &error2.LockTakenError{Cause: err}
		}
		return nil, nil, err
	}

	if a, ok := out.Attributes[untilFieldName]; ok {
		oldUntil := a.(*types.AttributeValueMemberN)
		if oldUntil.Value != untilStr {
			// We stole a lock that expired.
			from := ownerName // if we did not receive an owner, it was not changed, i.e. it was not chaned by the query
			if a, ok := out.Attributes[lockOwnerFieldName]; ok {
				fromAttr := a.(*types.AttributeValueMemberS)
				from = fromAttr.Value
			}

			i, _ := strconv.ParseInt(oldUntil.Value, 10, 64)
			oldUntilTime := time.UnixMilli(i)

			return &StolenLockInfo{
				OwnerName:   from,
				LockedUntil: oldUntilTime,
			}, fencingVal, nil
		}
	}

	return nil, fencingVal, nil
}

func (d *DynamoDBWithFencing) ReleaseLock(ctx context.Context, lockId string, leaseUntil time.Time, ownerName string) error {
	newUntil := time.Unix(1, 0)
	// We must not remove the lock, but rather reduce its lease time back in the history, so it will be "stolen" by new
	// acquirers. This is needed, so we do not lose the fencing token.
	return d.UpdateUntil(ctx, lockId, leaseUntil, newUntil, ownerName)
}

func (d *DynamoDBWithFencing) UpdateUntil(ctx context.Context, lockId string, oldUntil time.Time, newUntil time.Time, ownerName string) error {
	oldUntilStr := strconv.FormatInt(oldUntil.UnixMilli(), 10)
	newUntilStr := strconv.FormatInt(newUntil.UnixMilli(), 10)

	key := map[string]types.AttributeValue{
		pkFieldName: &types.AttributeValueMemberS{Value: lockId},
	}

	cond := expression.And(
		expression.AttributeExists(expression.Name(pkFieldName)),
		expression.And(
			expression.Equal(
				expression.Name(lockOwnerFieldName),
				expression.Value(&types.AttributeValueMemberS{Value: ownerName})),
			expression.Equal(
				expression.Name(untilFieldName),
				expression.Value(&types.AttributeValueMemberN{Value: oldUntilStr}))))

	update := expression.Set(
		expression.Name(lockOwnerFieldName),
		expression.Value(&types.AttributeValueMemberS{Value: ownerName}),
	)
	update = update.Set(
		expression.Name(untilFieldName),
		expression.Value(&types.AttributeValueMemberN{Value: newUntilStr}),
	)

	expr, _ := expression.NewBuilder().WithCondition(cond).WithUpdate(update).Build()

	dynamoCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	// Use "Update" instead of "put" to not overwrite the fencing token
	_, err := d.dynamoDbClient.UpdateItem(dynamoCtx, &dynamodb.UpdateItemInput{
		Key:                       key,
		TableName:                 aws.String(d.tableName),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
	})

	return err
}
