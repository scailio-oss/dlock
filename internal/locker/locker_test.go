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

package locker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	error2 "github.com/scailio-oss/dlock/error"
	"github.com/scailio-oss/dlock/internal/lock"
	"github.com/scailio-oss/dlock/internal/locker/test"
	"github.com/scailio-oss/dlock/internal/logger"
)

const (
	ownerName    = "test"
	lockIdPrefix = "prefix-"
)

var step = 1 * time.Second
var t0 = time.Unix(0, 0)

const timeoutDuration = 1 * time.Second

type lockerSetupData struct {
	lease        time.Duration
	heartbeat    time.Duration
	warnAfter    time.Duration
	maxClockSkew time.Duration
	clk          *clock.Mock
	db           *test.MockDb
}

func lockerSetup(data lockerSetupData) (*lockerImpl, *test.MockDb, *clock.Mock) {
	db := test.NewMockDb()
	clk := clock.NewMock()

	if data.clk != nil {
		clk = data.clk
	}

	if data.db != nil {
		db = data.db
	}

	res := New(db, clk, logger.Default(), ownerName, data.lease, data.heartbeat, data.warnAfter, lockIdPrefix,
		data.maxClockSkew, lock.NewWarnChanManager(logger.Default(), clk))
	return res.(*lockerImpl), db, clk
}

func TestLockUnlock(t *testing.T) {
	// GIVEN
	locker, db, _ := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    1 * step,
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	// WHEN
	l, err := locker.TryLock(context.Background(), "l1")

	// THEN
	assert.NoError(t, err, "Expected no error on tryLock")
	assert.NotNil(t, l, "Expected to have received a lock")
	_, ok := db.Locks[lockIdPrefix+"l1"]
	assert.True(t, ok, "Expected DB to have entry for lock")

	// WHEN
	l.Unlock(context.Background())

	// THEN
	assert.True(t, l.IsExpired(), "Expected lock to be expired")
	_, ok = db.Locks[lockIdPrefix+"l1"]
	assert.False(t, ok, "Expected DB to NOT have entry for lock")
}

func TestUnlockOnClose(t *testing.T) {
	// GIVEN
	locker, db, _ := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    2 * step,
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})

	// WHEN
	l, _ := locker.TryLock(context.Background(), "l1")
	locker.Close()

	// THEN
	assert.True(t, l.IsExpired(), "Expected lock to be expired")
	_, ok := db.Locks[lockIdPrefix+"l1"]
	assert.False(t, ok, "Expected DB to NOT have entry for lock")
}

func TestHeartbeats(t *testing.T) {
	// GIVEN
	locker, db, clk := lockerSetup(lockerSetupData{
		lease:        3 * step,
		heartbeat:    1 * step,
		warnAfter:    100 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	// WHEN
	l, _ := locker.TryLock(context.Background(), "l1")
	initialLeaseUntil := db.Locks[lockIdPrefix+"l1"].LeaseUntil

	clk.Add(step)
	runtime.Gosched()
	clk.Add(step)
	runtime.Gosched()
	clk.Add(step)
	runtime.Gosched()

	// THEN
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()
	for {
		select {
		case <-timeout.C:
			db.Mu.Lock()
			newLeaseUntil := db.Locks[lockIdPrefix+"l1"].LeaseUntil
			db.Mu.Unlock()
			fmt.Printf("Newest least in DB: %v\n", newLeaseUntil)
			assert.Fail(t, "Expected that the lock is replaced in DB")
			return
		default:
		}

		db.Mu.Lock()
		newLeaseUntil := db.Locks[lockIdPrefix+"l1"].LeaseUntil
		db.Mu.Unlock()

		if newLeaseUntil.Equal(t0.Add(6 * step)) {
			// sucess!
			break
		}
	}

	assert.Equal(t, t0.Add(3*step), initialLeaseUntil, "initialLeaseUntil correct")
	assert.False(t, l.IsExpired(), "Expected correct isExpired on lock")
}

func TestWarnChan(t *testing.T) {
	// GIVEN
	locker, _, clk := lockerSetup(lockerSetupData{
		lease:        4 * step,
		heartbeat:    4 * step,
		warnAfter:    2 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	// WHEN
	l, _ := locker.TryLock(context.Background(), "warnchan")
	select {
	case <-l.WarnChan():
		assert.Fail(t, "Should not yet have been able to select from warnChan")
	default:
	}

	log.Println("About to increase time")
	clk.Add(3 * step)
	log.Println("Time increased")

	// THEN
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()
	select {
	case _, ok := <-l.WarnChan():
		assert.True(t, ok, "Expected to have been warned in the warnchan")
	case <-timeout.C:
		assert.Fail(t, "Timeout waiting to select from warnChan")
	}
}

func TestStealAfterClockSkew(t *testing.T) {
	// GIVEN
	locker, db, clk := lockerSetup(lockerSetupData{
		lease:        3 * step,
		heartbeat:    20 * step, // ignore heartbeats here
		warnAfter:    1 * step,
		maxClockSkew: 2 * step,
	})
	defer locker.Close()
	locker2, _, _ := lockerSetup(lockerSetupData{
		lease:        3 * step,
		heartbeat:    20 * step, // ignore heartbeats here
		warnAfter:    1 * step,
		maxClockSkew: 2 * step,
		clk:          clk,
		db:           db,
	})
	defer locker2.Close()

	// WHEN
	l, _ := locker.TryLock(context.Background(), "l1")

	clk.Add(4 * step) // expired, but within clock skew
	runtime.Gosched()

	// Note: Lock still in DB, since we did not yet unlock it!
	assert.False(t, db.Removed[lockIdPrefix+"l1"], "Expected that entry in DB was not removed")

	_, err := locker2.TryLock(context.Background(), "l1")

	// THEN
	assert.Error(t, err, "Expected to not be able to lock yet, since clockSkewTime is not done")
	_, ok := err.(*error2.LockTakenError)
	assert.True(t, ok, "Expected error to be a lockTakenError")

	// WHEN
	clk.Add(2 * step)
	runtime.Gosched()

	// Note: Lock still in DB, since we did not yet unlock it!
	assert.False(t, db.Removed[lockIdPrefix+"l1"], "Expected that entry in DB was not removed")

	l2, err := locker2.TryLock(context.Background(), "l1")

	// THEN
	assert.NoError(t, err, "Expected no error on third lock")
	assert.True(t, l.IsExpired(), "Expected first lock to have expired")
	assert.False(t, l2.IsExpired(), "Expected second lock to have stolen from the first")
}

func TestUpdateErrors(t *testing.T) {
	// GIVEN
	locker, db, clk := lockerSetup(lockerSetupData{
		lease:        2 * step,
		heartbeat:    20 * step, // ignore heartbeats
		warnAfter:    20 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	db.UpdateResponse[lockIdPrefix+"l1"] = errors.New("testerror")

	// WHEN
	l, _ := locker.TryLock(context.Background(), "l1")
	clk.Add(step)

	// THEN
	assert.False(t, l.IsExpired(), "Expected correct isExpired after blocking a few heartbeats")

	// WHEN
	clk.Add(2 * step)

	// THEN
	assert.True(t, l.IsExpired(), "Expected lock is expired")
}

func TestInsertError(t *testing.T) {
	// GIVEN
	locker, db, _ := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    1 * step,
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	db.InsertResponse[lockIdPrefix+"l1"] = &test.MockInsertResponse{
		Stolen:   nil,
		Err:      errors.New("testerror"),
		DoInsert: false,
	}

	// WHEN
	_, err := locker.TryLock(context.Background(), "l1")

	// THEN
	assert.Error(t, err, "Expected error on tryLock")
}

func TestRemoveError(t *testing.T) {
	// GIVEN
	locker, db, _ := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    1 * step,
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	db.RemoveResponse[lockIdPrefix+"l1"] = errors.New("testerror")

	// WHEN
	l, _ := locker.TryLock(context.Background(), "l1")

	l.Unlock(context.Background())

	// THEN
	assert.True(t, l.IsExpired(), "Expected isExpired although unlokc failed in DB")
}

func TestExpiredDuringHeartbeat(t *testing.T) {
	// GIVEN
	locker, db, clk := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    2 * step,
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	// WHEN
	l, _ := locker.TryLock(context.Background(), "l1")

	clk.Add(2 * step) // -> t2

	// expect: heartbeat loop runs, identifies that the lock is timed out, executes an unlock, which in turn removes the
	// lock from the DB.

	// THEN
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()
	for {
		select {
		case <-timeout.C:
			assert.Fail(t, "Expected that the lock is deleted from DB")
			return
		default:
		}

		db.Mu.Lock()
		r := db.Removed[lockIdPrefix+"l1"]
		db.Mu.Unlock()

		if r {
			// Item was removed from DB, success!
			assert.True(t, l.IsExpired(), "Expected lock shows that it is expired.")
			return
		}
	}

}

func TestUnlockSameLockTwice(t *testing.T) {
	// GIVEN
	locker, _, _ := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    2 * step,
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	// WHEN
	l, _ := locker.TryLock(context.Background(), "l1")

	l.Unlock(context.Background())
	l.Unlock(context.Background())
}

func TestMultipleLocksSameLockIdUnlockCleanup(t *testing.T) {
	// GIVEN
	locker, db, clk := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    3 * step, // ignore heartbeat
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	// WHEN
	l1, _ := locker.TryLock(context.Background(), "l1")
	clk.Add(2 * step) // lock timeout, though no heartbeat ran, therefore no cleanup is done

	insertCallCountBefore := db.InsertCallCount
	_, err := locker.TryLock(context.Background(), "l1")
	insertCallCountAfter := db.InsertCallCount

	// THEN
	assert.Error(t, err, "Expected error before being able to re-acquire the same lockId")
	assert.Equal(t, insertCallCountBefore, insertCallCountAfter, "Expected that we did not try to acquire the"+
		"lock in the DB, but the Locker itself prevented us.") // otherwise we might mess up with the ownerName etc.

	// WHEN
	l1.Unlock(context.Background()) // unlock triggers cleanup
	_, err = locker.TryLock(context.Background(), "l1")

	// THEN
	assert.NoError(t, err, "Expected error after first lock was cleaned up")
}

func TestMultipleLocksSameLockIdHeartbeatCleanup(t *testing.T) {
	// GIVEN
	locker, _, clk := lockerSetup(lockerSetupData{
		lease:        1 * step,
		heartbeat:    3 * step,
		warnAfter:    1 * step,
		maxClockSkew: 0,
	})
	defer locker.Close()

	// WHEN
	//goland:noinspection GoUnhandledErrorResult
	locker.TryLock(context.Background(), "l1")
	clk.Add(2 * step) // lock timeout, though no heartbeat ran, therefore no cleanup is done

	_, err := locker.TryLock(context.Background(), "l1")

	// THEN
	assert.Error(t, err, "Expected error before being able to re-acquire the same lockId")

	// WHEN
	clk.Add(step) // heartbeat triggered, which triggers cleanup

	// THEN
	timeout := time.NewTimer(timeoutDuration)
	defer timeout.Stop()
	for {
		select {
		case <-timeout.C:
			fmt.Printf("Last error: %v\n", err)
			assert.Fail(t, "Could not acquire lock after timeout, although it should be cleaned up")
			return
		default:
		}
		_, err = locker.TryLock(context.Background(), "l1")
		if err == nil {
			return // success, no error anymore!
		}

		time.Sleep(1 * time.Millisecond)
	}
}
