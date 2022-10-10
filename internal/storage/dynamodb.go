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
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	error2 "github.com/scailio-oss/dlock/error"
)

type DynamoDB struct {
	dynamoDbClient *dynamodb.Client
	tableName      string
	timeout        time.Duration
}

// Creates a new DB implemnentation using a DynamoDB backend. It uses the given dynamoDB table name and adds the given
// timeout to all calls to dynamoDB.
func NewDynamoDb(dynamoDbClient *dynamodb.Client, tableName string, timeout time.Duration) DB {
	return &DynamoDB{
		dynamoDbClient: dynamoDbClient,
		tableName:      tableName,
		timeout:        timeout,
	}
}

func (d *DynamoDB) InsertNewLock(ctx context.Context, lockId string, ownerName string, leaseUntil time.Time, stealLockUntil time.Time) (*StolenLockInfo, error) {
	untilStr := strconv.FormatInt(leaseUntil.UnixMilli(), 10)
	stealUntilStr := strconv.FormatInt(stealLockUntil.UnixMilli(), 10)

	itm := map[string]types.AttributeValue{
		pkFieldName:        &types.AttributeValueMemberS{Value: lockId},
		lockOwnerFieldName: &types.AttributeValueMemberS{Value: ownerName},
		untilFieldName:     &types.AttributeValueMemberN{Value: untilStr},
	}

	cond := expression.Or(
		expression.AttributeNotExists(expression.Name(pkFieldName)),
		expression.And(
			expression.AttributeExists(expression.Name(pkFieldName)),
			expression.LessThanEqual(
				expression.Name(untilFieldName),
				expression.Value(&types.AttributeValueMemberN{Value: stealUntilStr}))))

	expr, _ := expression.NewBuilder().WithCondition(cond).Build()

	dynamoCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
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
			return nil, &error2.LockTakenError{Cause: err}
		}
		return nil, err
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
			}, nil
		}
	}

	return nil, nil
}

func (d *DynamoDB) RemoveLock(ctx context.Context, lockId string, leaseUntil time.Time, ownerName string) error {
	key := map[string]types.AttributeValue{
		pkFieldName: &types.AttributeValueMemberS{Value: lockId},
	}

	untilStr := strconv.FormatInt(leaseUntil.UnixMilli(), 10)

	cond := expression.And(
		expression.AttributeExists(expression.Name(pkFieldName)),
		expression.And(
			expression.Equal(
				expression.Name(lockOwnerFieldName),
				expression.Value(&types.AttributeValueMemberS{Value: ownerName})),
			expression.Equal(
				expression.Name(untilFieldName),
				expression.Value(&types.AttributeValueMemberN{Value: untilStr}))))

	expr, _ := expression.NewBuilder().WithCondition(cond).Build()

	dynamoCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	_, err := d.dynamoDbClient.DeleteItem(dynamoCtx, &dynamodb.DeleteItemInput{
		Key:                       key,
		TableName:                 aws.String(d.tableName),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	return err
}

func (d *DynamoDB) UpdateUntil(ctx context.Context, lockId string, oldUntil time.Time, newUntil time.Time, ownerName string) error {
	oldUntilStr := strconv.FormatInt(oldUntil.UnixMilli(), 10)
	newUntilStr := strconv.FormatInt(newUntil.UnixMilli(), 10)

	itm := map[string]types.AttributeValue{
		pkFieldName:        &types.AttributeValueMemberS{Value: lockId},
		lockOwnerFieldName: &types.AttributeValueMemberS{Value: ownerName},
		untilFieldName:     &types.AttributeValueMemberN{Value: newUntilStr},
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

	expr, _ := expression.NewBuilder().WithCondition(cond).Build()

	dynamoCtx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()
	_, err := d.dynamoDbClient.PutItem(dynamoCtx, &dynamodb.PutItemInput{
		Item:                      itm,
		TableName:                 aws.String(d.tableName),
		ConditionExpression:       expr.Condition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})

	return err
}
