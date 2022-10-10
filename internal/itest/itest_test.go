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

//go:build itest

// The tests in this "integration test" package start up a lock dynamodb via Docker and use that with a Locker.
//
// Requirements:
// - Docker installed locally.
// - run tests via `go test -tags itest ./...`
//
// The itests focus is the glue layer between Locker and DynamoDB, namely the storage implementation.
package itest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/scailio-oss/dlock"
)

func TestSimpleLockUnlockLock(t *testing.T) {
	// GIVEN
	dynamoDbClient, shutdown := startDynamoDb()
	defer shutdown()
	locker := dlock.NewLocker(dynamoDbClient, "itest")
	defer locker.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "simple")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l.IsExpired(), "Expected correct isExpired state of lock")
	l.Unlock(context.Background())

	l2, err := locker.TryLock(context.Background(), "simple")

	// THEN
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l2.IsExpired(), "Expected correct isExpired state of lock")
}

func TestSimpleLockLock(t *testing.T) {
	// GIVEN
	dynamoDbClient, shutdown := startDynamoDb()
	defer shutdown()
	locker := dlock.NewLocker(dynamoDbClient, "itest")
	defer locker.Close()
	locker2 := dlock.NewLocker(dynamoDbClient, "itest2")
	defer locker2.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "locklock")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l.IsExpired(), "Expected correct isExpired state of lock")

	l2, err := locker.TryLock(context.Background(), "locklock")

	// THEN
	assert.Error(t, err, "Expected no error when locking")
	assert.Nil(t, l2, "Expected to have gotton no lock")
}

func TestLockWithHeartbeats(t *testing.T) {
	// GIVEN
	dynamoDbClient, shutdown := startDynamoDb()
	defer shutdown()

	opts := []dlock.LockerOption{
		dlock.WithLease(1 * time.Second),
		dlock.WithHeartbeat(200 * time.Millisecond),
	}

	locker := dlock.NewLocker(dynamoDbClient, "itest", opts...)
	defer locker.Close()
	locker2 := dlock.NewLocker(dynamoDbClient, "itest2", opts...)
	defer locker2.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "heartbeat")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l.IsExpired(), "Expected correct isExpired state of lock")

	// If no heartbeats succeeded, then l should have expired after 2 seconds
	time.Sleep(2 * time.Second)

	// THEN
	assert.False(t, l.IsExpired(), "Expected that lock one is still valid")

	// WHEN
	// try to lock from other locker
	_, err = locker2.TryLock(context.Background(), "heartbeat")

	// THEN
	assert.Error(t, err, "Expected to have received an error, since the first lock is still alive in the DB")
}

func TestLockSteal(t *testing.T) {
	// GIVEN
	dynamoDbClient, shutdown := startDynamoDb()
	defer shutdown()

	opts := []dlock.LockerOption{
		dlock.WithLease(1 * time.Second),
		dlock.WithHeartbeat(10 * time.Second), // high, so no heartbeat will occur
		dlock.WithMaxClockSkew(500 * time.Millisecond),
	}

	locker := dlock.NewLocker(dynamoDbClient, "itest", opts...)
	defer locker.Close()
	locker2 := dlock.NewLocker(dynamoDbClient, "itest2", opts...)
	defer locker2.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "steal")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l.IsExpired(), "Expected correct isExpired state of lock")

	// Wait lease time  + a bit, but not as long as maxClockSkew
	time.Sleep(1100 * time.Millisecond)

	// THEN
	assert.True(t, l.IsExpired(), "Expected that lock one is valid no more")

	// WHEN
	// try to lock before maxClockSkew is done
	l2, err := locker2.TryLock(context.Background(), "steal")

	// THEN
	assert.Error(t, err, "Expected to have received an error, since maxClockSkew is not yet done")
	assert.Nil(t, l2, "Expected no new lock received yet")

	// WHEN
	// Wait until maxclockSkew is done
	time.Sleep(500 * time.Millisecond)
	// try to lock from other locker
	l2, err = locker2.TryLock(context.Background(), "steal")

	// THEN
	assert.NoError(t, err, "Expected to have no error when trying to steal the lock")
	assert.False(t, l2.IsExpired(), "Expected new lock is not expired")
}

func TestLockFenceSimple(t *testing.T) {
	// GIVEN
	dynamoDbClient, shutdown := startDynamoDb()
	defer shutdown()

	opts := []dlock.LockerOption{
		dlock.WithLease(10 * time.Second),
		dlock.WithHeartbeat(10 * time.Second), // high, so no heartbeat will occur
		dlock.WithMaxClockSkew(500 * time.Millisecond),
		dlock.WithFencingEnabled(true),
	}

	locker := dlock.NewLocker(dynamoDbClient, "itest", opts...)
	defer locker.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "fencesimple")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l.IsExpired(), "Expected correct isExpired state of lock")

	// THEN
	assert.NotNil(t, l.FencingToken(), "Expected valid fencing token")
	fencingToken1 := *l.FencingToken()

	// WHEN
	l.Unlock(context.Background())
	l2, err := locker.TryLock(context.Background(), "fencesimple")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l2.IsExpired(), "Expected correct isExpired state of lock")

	// THEN
	assert.NotNil(t, l2.FencingToken(), "Expected valid fencing token")
	fencingToken2 := *l2.FencingToken()

	assert.NotEqualf(t, fencingToken1, fencingToken2, "Expected both fencingTokens to differ")
	assert.Equal(t, uint64(1), fencingToken1.Uint64(), "Expected correct fencing token 1 value ")
	assert.Equal(t, uint64(2), fencingToken2.Uint64(), "Expected correct fencing token 2 value ")
}

func TestLockFenceSteal(t *testing.T) {
	// GIVEN
	dynamoDbClient, shutdown := startDynamoDb()
	defer shutdown()

	opts := []dlock.LockerOption{
		dlock.WithLease(1 * time.Second),
		dlock.WithHeartbeat(10 * time.Second), // high, so no heartbeat will occur
		dlock.WithMaxClockSkew(500 * time.Millisecond),
		dlock.WithFencingEnabled(true),
	}

	locker := dlock.NewLocker(dynamoDbClient, "itest", opts...)
	defer locker.Close()
	locker2 := dlock.NewLocker(dynamoDbClient, "itest2", opts...)
	defer locker2.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "fencesteal")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l.IsExpired(), "Expected correct isExpired state of lock")

	assert.NotNil(t, l.FencingToken(), "Expected valid fencing token")
	fencingToken1 := *l.FencingToken()

	// Wait lease time  + a bit, but not as long as maxClockSkew
	time.Sleep(1100 * time.Millisecond)

	// THEN
	assert.True(t, l.IsExpired(), "Expected that lock one is valid no more")

	// WHEN
	// try to lock before maxClockSkew is done
	l2, err := locker2.TryLock(context.Background(), "fencesteal")

	// THEN
	assert.Error(t, err, "Expected to have received an error, since maxClockSkew is not yet done")
	assert.Nil(t, l2, "Expected no new lock received yet")

	// WHEN
	// Wait until maxclockSkew is done
	time.Sleep(500 * time.Millisecond)
	// try to lock from other locker
	l2, err = locker2.TryLock(context.Background(), "fencesteal")

	// THEN
	assert.NotNil(t, l2.FencingToken(), "Expected valid fencing token")
	fencingToken2 := *l2.FencingToken()
	assert.NoError(t, err, "Expected to have no error when trying to steal the lock")
	assert.False(t, l2.IsExpired(), "Expected new lock is not expired")

	assert.NotEqualf(t, fencingToken1, fencingToken2, "Expected both fencingTokens to differ")
}

func TestLockWithFencingAndHeartbeats(t *testing.T) {
	// GIVEN
	dynamoDbClient, shutdown := startDynamoDb()
	defer shutdown()

	opts := []dlock.LockerOption{
		dlock.WithLease(1 * time.Second),
		dlock.WithHeartbeat(200 * time.Millisecond),
		dlock.WithFencingEnabled(true),
	}

	locker := dlock.NewLocker(dynamoDbClient, "itest", opts...)
	defer locker.Close()
	locker2 := dlock.NewLocker(dynamoDbClient, "itest2", opts...)
	defer locker2.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "heartbeat")
	assert.NoError(t, err, "Expected no error when locking")
	assert.False(t, l.IsExpired(), "Expected correct isExpired state of lock")

	// If no heartbeats succeeded, then l should have expired after 2 seconds
	time.Sleep(2 * time.Second)

	// THEN
	assert.False(t, l.IsExpired(), "Expected that lock one is still valid")

	// WHEN
	// try to lock from other locker
	_, err = locker2.TryLock(context.Background(), "heartbeat")

	// THEN
	assert.Error(t, err, "Expected to have received an error, since the first lock is still alive in the DB")
}
