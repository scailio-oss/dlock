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

package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	"github.com/scailio-oss/dlock/factory"
)

func main() {
	awsConfig := aws.Config{} // Whatever you need to create the config
	dynamoDbClient := dynamodb.NewFromConfig(awsConfig)

	// Ensure this is unique for this program instance. E.g. use AWS RequestId in Lambda.
	ownerName := strconv.FormatUint(rand.Uint64(), 16)

	locker := factory.NewLocker(dynamoDbClient, ownerName,
		// This locker locks objects of type 'streets in NYC'
		factory.WithLockIdPrefix("nyc-street-"),
		factory.WithLease(10*time.Second),
		factory.WithHeartbeat(2*time.Second),
		factory.WithMaxClockSkew(10*time.Second),
		factory.WithWarnAfter(9*time.Second),
		factory.WithDynamoDbTimeout(1*time.Second),
	)
	defer locker.Close()

	// Try to acquire a lock on 'wallstreet'
	lock, err := locker.TryLock(context.Background(), "wallstreet")
	if err != nil {
		fmt.Printf("Could not lock: %v\n", err)
		return
	}

	// TODO do things exclusively on object 'wallstreet'

	select {
	case <-lock.WarnChan():
		// Will fire after 9 seconds after the last successful heartbeat
		fmt.Printf("WARNING, the lock is about to expire\n")
	default:
	}

	lock.Unlock(context.Background())
}
