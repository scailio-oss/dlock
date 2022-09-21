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
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	error2 "github.com/scailio-oss/dlock/error"
	"github.com/scailio-oss/dlock/internal/lock"
	"github.com/scailio-oss/dlock/internal/storage"
	lock2 "github.com/scailio-oss/dlock/lock"
	"github.com/scailio-oss/dlock/locker"
	"github.com/scailio-oss/dlock/logger"
)

type lockerImpl struct {
	logger               logger.Logger
	ownerName            string
	db                   storage.DB
	clock                clock.Clock
	warnChanManager      lock.WarnChanManager
	leaseDuration        time.Duration
	heartbeatDuration    time.Duration
	warnAfterDuration    time.Duration
	lockIdPrefix         string
	maxClockSkewDuration time.Duration

	activeLocks   map[string]lock.Lock
	activeLocksMu sync.Mutex // sync access to activeLocks

	closeChan chan struct{}
	closeOnce sync.Once // used to close closeChan

	closed   bool
	closedMu sync.RWMutex // sync access to closed
}

// Create a new Locker. WarnChanManager will be closed when the Locker is closed. Other params: See factory.NewLocker.
func New(db storage.DB, clock clock.Clock, logger logger.Logger, ownerName string, lease time.Duration, heartbeat time.Duration,
	warnAfter time.Duration, lockIdPrefix string, maxClockSkew time.Duration, warnChanManager lock.WarnChanManager) locker.Locker {
	l := &lockerImpl{
		logger:               logger,
		ownerName:            ownerName,
		db:                   db,
		clock:                clock,
		warnChanManager:      warnChanManager,
		leaseDuration:        lease,
		heartbeatDuration:    heartbeat,
		warnAfterDuration:    warnAfter,
		lockIdPrefix:         lockIdPrefix,
		maxClockSkewDuration: maxClockSkew,

		closeChan:   make(chan struct{}),
		activeLocks: map[string]lock.Lock{},
	}

	startupCompleteChan := make(chan struct{})
	go l.heartbeatLoop(startupCompleteChan)
	// Wait until the goroutine actually started. In unit tests we saw that the main goroutine might starve the heartbeatLoop
	// from starting up, in that case messing with all timings. Instead of using runtime.Gosched(), we chose to explicitly
	// block until we know the goroutine has started, since Gosched cannot guarantee this.
	<-startupCompleteChan

	return l
}

func (l *lockerImpl) TryLock(ctx context.Context, lockId string) (lock2.Lock, error) {
	// lock this mu during TryLock to not start closing while acquiring this lock.
	l.closedMu.RLock()
	defer l.closedMu.RUnlock()

	if l.closed {
		return nil, errors.New("locker closed")
	}

	if l.lockIdPrefix != "" {
		lockId = l.lockIdPrefix + lockId
	}

	now := l.clock.Now()

	until := now.Add(l.leaseDuration)
	stealUntil := now.Add(-l.maxClockSkewDuration)

	// Add uninitialized lock object into activeLocks already, before going to the database. This prevents us from
	// creating two Lock objects for the same lockId simultaneously in one Locker.
	// It could happen, that the existing lock is expired, though still in the activeLocks map, e.g. if heartbeats are
	// too long. In such a case, if we'd support simply creating a new Lock object for the same lockId, then the expired
	// lock item could execute an Unlock for the not-expired lock - since our internal data structures do not handle that
	// case. Therefore, simply block this case here, since it can be expected of the caller to have control over all Lock
	// objects - and if a very long heartbeat should be used, then the caller must call Unlock on the expired lock object
	// to cleanup.
	res := lock.New(l.logger, l.clock, l.warnChanManager, l.unlock)
	l.activeLocksMu.Lock()
	if _, ok := l.activeLocks[lockId]; ok {
		l.activeLocksMu.Unlock()
		return nil, &error2.LockTakenError{Cause: errors.New("Same Locker has acquired the lease of this lockId already.")}
	}
	l.activeLocks[lockId] = res
	l.activeLocksMu.Unlock()

	stolen, err := l.db.InsertNewLock(ctx, lockId, l.ownerName, until, stealUntil)
	if err != nil {
		l.logger.Error(ctx, "Could not acquire lock (lockId)", lockId)

		// cleanup activeLocks again
		l.activeLocksMu.Lock()
		defer l.activeLocksMu.Unlock()
		delete(l.activeLocks, lockId)

		return nil, err
	}

	if stolen != nil {
		l.logger.Warn(ctx, "Stole lock successfully. Continuing. (lockId/oldOwner/oldUntil)", lockId, stolen.OwnerName,
			stolen.LockedUntil)
	} else {
		l.logger.Info(ctx, "Acquired lock (lockId/until)", lockId, until)
	}

	res.Update(lockId, until, now.Add(l.warnAfterDuration), false)

	return res, nil
}

// Unlock the given lock. When calling this, the caller MUST NOT have lock.LockForUpdate!
func (l *lockerImpl) unlock(ctx context.Context, lock lock.Lock) {
	lock.LockForUpdate()
	defer lock.UnlockForUpdate()

	if lock.Unlocked() {
		return
	}

	err := l.db.RemoveLock(ctx, lock.LockId(), lock.LeaseUntil(), l.ownerName)

	if err != nil {
		l.logger.Warn(ctx, "Error while unlocking lock, ignoring (lockId/until)", lock.LockId(), lock.LeaseUntil(), err)
		// Note: ignoring error, since in a distributed system you can't figure out what the results of unlock not working
		// might be (imagine network issues to dynamoDB). Calling code must assume it does not have the lock anymore anyway.
	} else {
		l.logger.Info(ctx, "Unlocked successfully (lockId/until)", lock.LockId(), lock.LeaseUntil())
	}

	lock.Update(lock.LockId(), time.Unix(0, 0), time.Unix(0, 0), true)

	l.activeLocksMu.Lock()
	defer l.activeLocksMu.Unlock()
	delete(l.activeLocks, lock.LockId())
}

// try to execute a heartbeat for the goiven lock, extending the lease time to the max again. Calling this method is no
// guarantee that the lock afterwards is "isExpired, therefore double-check!
func (l *lockerImpl) heartbeat(ctx context.Context, lock lock.Lock) {
	l.logger.Debug(ctx, "Heartbeating lock (lockId)", lock.LockId())
	if lock.IsExpired() {
		l.logger.Warn(ctx, "Identified timed out lock at heartbeat. Calling unlock for cleanup. (lockId,until)",
			lock.LockId(), lock.LeaseUntil())

		l.unlock(ctx, lock)
		return
	}

	if !lock.TryLockForUpdate() {
		l.logger.Warn(context.Background(), "Skipping heartbeat since an update is active (lockId,until)",
			lock.LockId(), lock.LeaseUntil())
		return
	}
	var unlockOnce sync.Once
	defer func() { unlockOnce.Do(func() { lock.UnlockForUpdate() }) }()

	now := l.clock.Now()
	newUntil := now.Add(l.leaseDuration)

	err := l.db.UpdateUntil(ctx, lock.LockId(), lock.LeaseUntil(), newUntil, l.ownerName)

	if err != nil {
		l.logger.Error(ctx, "Heartbeat failed on lock (lockId/oldUntil)", lock.LockId(), lock.LeaseUntil())

		// Note: It might be the case that the values were actually changed in the DB, but on the "return path" from the DB
		// to our service there was an error, and we never received the ACK of the DB. In such an unlikely case, the
		// heartbeat might actually have succeeded, though we can't know about this here. So err on the side of caution and
		// not update the lock itself - which might lead to us thinking that we don't have the lock anymore, although we
		// actually have in the DB. Since the look in the DB will time out though, after some time we can re-acquire the
		// lock.

	} else {
		// update successful in database
		if !lock.Update(lock.LockId(), newUntil, now.Add(l.warnAfterDuration), false) {
			// The lock identified itself as expired already and might've returned that to the system on an isExpired call.
			// This can happen, if the update to dynamoDB took a bit and the check at the beginning of the method was still
			// !isExpired.

			// The db call succeeded, i.e. we effectively still have the lock. Though we want to prevent a case where
			// lock.IsExpired returned true and returns false later again, therefore we unlock again here and bring the
			// database to our known state.

			l.logger.Warn(ctx, "Race during heartbeat. Since lock.IsExpired might've returned true already, unlocking "+
				"the lock in DB (lockId)", lock.LockId())

			// we must UnlockForUpdate before calling unlock!
			unlockOnce.Do(func() { lock.UnlockForUpdate() })
			l.unlock(ctx, lock)
		}
	}
}

// Regularly executes heartbeats on all locks.
// The chan passed in will be closed just before entering the worker loop.
func (l *lockerImpl) heartbeatLoop(startupCompleteChan chan struct{}) {
	ticker := l.clock.Ticker(l.heartbeatDuration)
	defer ticker.Stop()
	l.logger.Debug(context.Background(), "Starting heartbeat loop")
	close(startupCompleteChan)
	for {
		select {
		case <-l.closeChan:
			l.logger.Debug(context.Background(), "Shutting down heartbeat loop")
			return
		case <-ticker.C:
			l.logger.Debug(context.Background(), "Triggering heartbeat for all active locks")
			l.executeForAllActiveLocksConcurrently(func(lock lock.Lock) {
				l.heartbeat(context.Background(), lock)
			})
		}
	}
}

// Executes a goroutine for each of the currently active locks. The goroutine calls the given function. This function
// returns as soon as all worker functions completed.
func (l *lockerImpl) executeForAllActiveLocksConcurrently(fn func(lock lock.Lock)) {
	var group sync.WaitGroup

	l.activeLocksMu.Lock()
	group.Add(len(l.activeLocks))
	for _, v := range l.activeLocks {
		lck := v
		go func() {
			fn(lck)
			group.Done()
		}()
	}
	l.activeLocksMu.Unlock()

	group.Wait()
}

func (l *lockerImpl) Close() {

	l.closedMu.Lock()
	l.closed = true
	l.closedMu.Unlock()

	l.warnChanManager.Close()

	// now: nothing can create new locks anymore. Release all current locks.
	l.executeForAllActiveLocksConcurrently(func(lock lock.Lock) {
		lock.Unlock(context.Background())
	})

	l.closeOnce.Do(func() {
		close(l.closeChan)
	})
}
