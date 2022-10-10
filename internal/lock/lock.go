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

package lock

import (
	"context"
	"math/big"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/scailio-oss/dlock/lock"
	"github.com/scailio-oss/dlock/logger"
)

// Function that executes an Unlock and calls lock.Update afterwards
type UnlockFn func(context.Context, Lock)

// Internal interface for a Lock.
// A lock internally has a LockId, a current LeaseUntil and a state Unlocked (which can be try even before LeaseUntil,
// e.g. if Unlock was called and successful). Also it receives the warnTime at which it should issue a message on the WarnChan.
// Use Update to update the internal state
// Additionally a Lock provides a "lock for update" which can be used externally by functions which need exclusivity
// when preparing a call to Update.
type Lock interface {
	lock.Lock

	// Update the internal state of the lock, setting the values. Returns true on success and false if this lock was
	// already expired with the old values. In the latter case, the new values are not applied and it cannot be guaranteed
	// that IsExpired has not yet returned false.
	// This method must be called at least once, after the lock expired in order for the lock to cleanup internal
	// resources correctly.
	Update(lockId string, leaseUntil time.Time, warnTime time.Time, unlocked bool) bool

	// Try to lock the "lock for update"
	TryLockForUpdate() bool
	// Lock the "lock for update", blocking
	LockForUpdate()
	// Unlock the "lock for update"
	UnlockForUpdate()

	// The current lockId
	LockId() string
	// The current lease is valid until
	LeaseUntil() time.Time
	// The current state of the unlocked flag
	Unlocked() bool

	// SetFencingToken sets the fencing token
	SetFencingToken(token *big.Int)
}

// Create a new lock. Call Update to initialize the object.
func New(logger logger.Logger, clock clock.Clock, warnChanManager WarnChanManager, unlockFn UnlockFn) Lock {
	res := &lockImpl{
		unlockFn:        unlockFn,
		clock:           clock,
		warnChanManager: warnChanManager,
		logger:          logger,
	}
	return res
}

type lockImpl struct {
	internalMu      sync.Mutex // Serialize access to internal fields
	logger          logger.Logger
	unlockFn        UnlockFn
	clock           clock.Clock
	warnChanManager WarnChanManager
	initialized     bool
	warnChan        <-chan string

	lockId     string
	leaseUntil time.Time
	unlocked   bool

	fencingToken *big.Int

	updateMu sync.Mutex // Mutex used externally to prepare calls to Update.
}

func (l *lockImpl) IsExpired() bool {
	l.internalMu.Lock()
	defer l.internalMu.Unlock()

	return l.isExpiredInternal()
}

func (l *lockImpl) isExpiredInternal() bool {
	now := l.clock.Now()
	return l.initialized && (l.unlocked || now.After(l.leaseUntil))
}

func (l *lockImpl) Unlock(ctx context.Context) {
	l.internalMu.Lock()
	if l.unlocked { // fail fast
		l.internalMu.Unlock()
		return
	}
	fn := l.unlockFn
	l.internalMu.Unlock()

	fn(ctx, l)
}

func (l *lockImpl) WarnChan() <-chan string {
	return l.warnChan
}

func (l *lockImpl) Update(lockId string, leaseUntil time.Time, warnTime time.Time, unlocked bool) bool {
	l.internalMu.Lock()

	if l.isExpiredInternal() {
		l.internalMu.Unlock()
		return false
	}

	l.leaseUntil = leaseUntil
	l.unlocked = unlocked
	l.lockId = lockId
	l.internalMu.Unlock()

	// Call warnchan manager outside of lock
	var newWarnChan <-chan string
	if !l.isExpiredInternal() {
		newWarnChan = l.warnChanManager.Register(lockId, warnTime)
	} else {
		l.warnChanManager.Unregister(lockId)
	}

	l.internalMu.Lock()
	defer l.internalMu.Unlock()

	if newWarnChan != nil {
		l.warnChan = newWarnChan
	}
	l.initialized = true

	return true
}

func (l *lockImpl) LockForUpdate() {
	l.updateMu.Lock()
}

func (l *lockImpl) TryLockForUpdate() bool {
	return l.updateMu.TryLock()
}

func (l *lockImpl) UnlockForUpdate() {
	l.updateMu.Unlock()
}

func (l *lockImpl) LockId() string {
	l.internalMu.Lock()
	defer l.internalMu.Unlock()

	return l.lockId
}

func (l *lockImpl) LeaseUntil() time.Time {
	l.internalMu.Lock()
	defer l.internalMu.Unlock()

	return l.leaseUntil
}

func (l *lockImpl) Unlocked() bool {
	l.internalMu.Lock()
	defer l.internalMu.Unlock()

	return l.unlocked
}

func (l *lockImpl) SetFencingToken(token *big.Int) {
	l.internalMu.Lock()
	defer l.internalMu.Unlock()

	l.fencingToken = token
}

func (l *lockImpl) FencingToken() *big.Int {
	l.internalMu.Lock()
	defer l.internalMu.Unlock()

	if l.fencingToken == nil {
		return nil
	}
	token := big.Int{}
	token.Set(l.fencingToken)
	return &token
}
