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

package dlock

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/benbjohnson/clock"

	"github.com/scailio-oss/dlock/internal/lock"
	internallocker "github.com/scailio-oss/dlock/internal/locker"
	internallogger "github.com/scailio-oss/dlock/internal/logger"
	"github.com/scailio-oss/dlock/internal/storage"
	"github.com/scailio-oss/dlock/locker"
	"github.com/scailio-oss/dlock/logger"
)

const defaultTableName = "dlock"
const defaultLease = 1 * time.Minute
const defaultHeartbeat = 15 * time.Second
const defaultWarnAfter = 50 * time.Second
const defaultMaxClockSkew = 10 * time.Second
const defaultDynamoDbTimeout = 1 * time.Second

// NewLocker creates a new Locker based on DynamoDB.
// ownerName: unique name identifying this Locker instance - this information will be written into the DynamoDB
// options: Additional, optional options.
func NewLocker(dynamodbClient *dynamodb.Client, ownerName string, options ...LockerOption) locker.Locker {
	params := &LockerParams{}
	for _, opt := range options {
		opt(params)
	}

	if params.logger == nil {
		params.logger = internallogger.Default()
	}
	if params.tableName == "" {
		params.tableName = defaultTableName
	}
	if params.lease == 0 {
		params.lease = defaultLease
	}
	if params.heartbeat == 0 {
		params.heartbeat = defaultHeartbeat
	}
	if params.warnAfter == 0 {
		params.warnAfter = defaultWarnAfter
	}
	if params.maxClockSkew == 0 {
		params.maxClockSkew = defaultMaxClockSkew
	}
	if params.dynamoDbTimeout == 0 {
		params.dynamoDbTimeout = defaultDynamoDbTimeout
	}
	// lockIdPrefix is by default "" already

	clk := clock.New()

	db := storage.NewDynamoDb(dynamodbClient, params.tableName, params.dynamoDbTimeout)
	var wcm lock.WarnChanManager
	if params.warnDisabled {
		wcm = lock.NewNoopWarnChanManager()
	} else {
		wcm = lock.NewWarnChanManager(params.logger, clk)
	}

	return internallocker.New(db, clk, params.logger, ownerName, params.lease, params.heartbeat,
		params.warnAfter, params.lockIdPrefix, params.maxClockSkew, wcm)
}

type LockerParams struct {
	logger          logger.Logger
	tableName       string
	lease           time.Duration
	heartbeat       time.Duration
	warnAfter       time.Duration
	warnDisabled    bool
	lockIdPrefix    string
	maxClockSkew    time.Duration
	dynamoDbTimeout time.Duration
}

type LockerOption func(params *LockerParams)

// Use the given Logger instead of a default one
func WithLogger(logger logger.Logger) LockerOption {
	return func(params *LockerParams) {
		params.logger = logger
	}
}

// Use the given DynamoDB table name instead of the default defaultTableName
func WithTableName(tableName string) LockerOption {
	return func(params *LockerParams) {
		params.tableName = tableName
	}
}

// Use the given Lease time instead of the default defaultLease.
// The lease time is the duration after which a lock will timeout automatically and other owners can steal a lock (after
// an additional wait for the MaxClockSkew).
// The Locker runs a heartbeat loop which auto-refreshes all locks regularly, extending their lifetime to the leasetime
// as long as this locker is running and as long as we have a connection to DynamoDB.
// The lease time and heartbeat time should be chosen in a way that multiple heartbeats are sent during a normal lease
// time, which allows single heartbeats to fail e.g. due to temporary connection issues, but the lock not being lost
// immediately.
// Note that Locker will store a fixed internal leaseUntilTime for each lock, i.e. a timestamp that identifies when the
// lock will timeout if it is not refreshed with heartbeats. The guarantees that the Locks give, require that system
// clocks are synchronized well, see WithMaxClockSkew.
func WithLease(lease time.Duration) LockerOption {
	return func(params *LockerParams) {
		params.lease = lease
	}
}

// Use the given heartbeat duration instead of the default defaultHeartbeat.
// See WithLease.
// Also, at heartbeat time, internal cleanup of Locker datastructures is executed, if it did not happen before.
func WithHeartbeat(heartbeat time.Duration) LockerOption {
	return func(params *LockerParams) {
		params.heartbeat = heartbeat
	}
}

// Use the given warn time instead of the default defaultWarnAfter.
// A lock that has been acquired has a WarnChan that will receive a message after this amount of time has passed since
// acquiring the lock or the last successful heartbeat. This allows the program to stop working on resources that are
// locked by the distributed locks, since after the lock actually expired, there is no way to guarantee exclusive access
// to those resources anymore.
//
// Example: leaseTime = 1 min, heartbeat = 15 sec, warnAfter = 50 sec
// After the lock is acquired, the heartbeats start extending the lifetime of the lock, each setting the internal
// leaseUntilTime of the lock to currentTime + lease duration. If all heartbeats succeed, there will be no warning
// issued. If heartbeats start failing, e.g. due to a network connection issue to DynamoDB, then 50 sec after the last
// update to the internal leaseUntilTime, the lock will send a message on the WarnChan.
//
// See also WithWarnDisabled
func WithWarnAfter(warnAfter time.Duration) LockerOption {
	return func(params *LockerParams) {
		params.warnAfter = warnAfter
	}
}

// Disables warnChans fully. The corresponding methods will return nil.
//
// See also WithWarnAfter.
func WithWarnDisabled() LockerOption {
	return func(params *LockerParams) {
		params.warnDisabled = true
	}
}

// Use this prefix to all lockIds that are used within TryLock.
// Since a single Locker should lock only items of the same type, this prefix can be used to re-use the same DynamoDB
// table for different Lockers locking different kinds of objects.
func WithLockIdPrefix(lockIdPrefix string) LockerOption {
	return func(params *LockerParams) {
		params.lockIdPrefix = lockIdPrefix
	}
}

// Use this maximum clock skew instead of the default defaultMaxClockSkew.
// Locker relies on well-synchronized system clocks, which in reality is very hard to achieve. To compensate, this
// parameter specifies an upper bound of the time difference of the system clocks of all participanting systems, i.e.
// all systems that execute a Locker with the same LockIdPrefix on the same dynamoDB table.
// The Locker will not steal an existing lock for leaseUntilTime+maxClockSkew.
func WithMaxClockSkew(maxClockSkew time.Duration) LockerOption {
	return func(params *LockerParams) {
		params.maxClockSkew = maxClockSkew
	}
}

// Use this timeout for dynamoDb calls instead of the default.
// Locker will call dynamoDb both in methods directly triggered by the user, but also in goroutines (e.g. heartbeat).
// This timeout will be used for the remote calls.
func WithDynamoDbTimeout(dynamoDbTimeout time.Duration) LockerOption {
	return func(params *LockerParams) {
		params.dynamoDbTimeout = dynamoDbTimeout
	}
}
