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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/scailio-oss/dlock/internal/lock/test"
	"github.com/scailio-oss/dlock/internal/logger"
)

const lockId = "lockId"

type lockSetupParam struct {
	unlockFn UnlockFn
}

var defaultUnlockFn = func(ctx context.Context, l Lock) {
	l.Update(l.LockId(), l.LeaseUntil(), l.LeaseUntil(), true)
}

func lockSetup(param *lockSetupParam) (Lock, *clock.Mock, WarnChanManager) {
	clk := clock.NewMock()
	wcm := NewWarnChanManager(logger.Default(), clk) // TODO close warnchanmanager in tests
	l := New(logger.Default(), clk, wcm, param.unlockFn)
	return l, clk, wcm
}

func TestUnlockFnCalled(t *testing.T) {
	// GIVEN
	var unlockCalled bool
	unlockFn := func(ctx context.Context, l Lock) {
		defaultUnlockFn(ctx, l)
		unlockCalled = true
	}
	l, _, _ := lockSetup(&lockSetupParam{unlockFn: unlockFn})
	updated := l.Update(lockId, test.T1, test.T1, false)
	ctx := context.Background()

	// WHEN
	l.Unlock(ctx)

	// THEN
	assert.True(t, updated, "Expected to have successfully updated the lock")
	assert.True(t, l.IsExpired(), "Expected lock to be expired")
	assert.True(t, unlockCalled, "Expected correct state on unlockCalled")
}

func TestLocked(t *testing.T) {
	// GIVEN
	l, _, _ := lockSetup(&lockSetupParam{unlockFn: defaultUnlockFn})
	l.Update(lockId, test.T1, test.T1, false)

	// WHEN
	// nop

	// THEN
	assert.False(t, l.IsExpired(), "Expected correct isExpired")
}

func TestTimeoutAfterWait(t *testing.T) {
	// GIVEN
	var unlockCalled bool
	unlockFn := func(ctx context.Context, l Lock) {
		defaultUnlockFn(ctx, l)
		unlockCalled = true
	}
	l, clk, _ := lockSetup(&lockSetupParam{unlockFn: unlockFn})
	l.Update(lockId, test.T1, test.T1, false)

	// WHEN
	clk.Add(2 * test.Step) // -> t2

	// THEN
	assert.True(t, l.IsExpired(), "Expected correct isExpired")
	assert.False(t, unlockCalled, "Expected correct unlockCalled")
}

func TestWarnSimple(t *testing.T) {
	// GIVEN
	l, clk, _ := lockSetup(&lockSetupParam{unlockFn: defaultUnlockFn})
	l.Update(lockId, test.T3, test.T2, false)

	// THEN
	select {
	case <-l.WarnChan():
		assert.Fail(t, "Should not yet have been able to select from warnChan")
	default:
	}

	// WHEN
	clk.Add(2 * test.Step) // ->t2

	// THEN
	timeout := time.NewTimer(test.TimeoutDuration)
	defer timeout.Stop()
	select {
	case lId := <-l.WarnChan():
		assert.Equal(t, lockId, lId, "Expected to have received correct lockId from warnChan")
	case <-timeout.C:
		assert.Fail(t, "Should have been able to select from warnChan after waiting")
	}
}

func TestWarnWithHeartbeats(t *testing.T) {
	// GIVEN
	l, clk, _ := lockSetup(&lockSetupParam{unlockFn: defaultUnlockFn})
	l.Update(lockId, test.T3, test.T2, false)

	// THEN
	select {
	case <-l.WarnChan():
		assert.Fail(t, "Should not yet have been able to select from warnChan")
	default:
	}

	// WHEN
	clk.Add(test.Step) // move to t1
	times := []time.Time{test.T3, test.T4, test.T5}
	updatedAll := true
	for i := 0; i < len(times)-1; i++ {
		updatedAll = updatedAll &&
			l.Update(lockId, times[i+1], times[i], false)
		clk.Add(test.Step)
	}

	// THEN
	select {
	case <-l.WarnChan():
		assert.Fail(t, "Should not have been able to select from warnChan, since we extended")
	default:
	}

	assert.True(t, updatedAll, "Expected that all updates (heartbeats) were successful")
	assert.Equal(t, test.T5, l.LeaseUntil(), "Expected correct final lease until")
}

func TestWarnChanCloses(t *testing.T) {
	// GIVEN
	l, clk, _ := lockSetup(&lockSetupParam{unlockFn: defaultUnlockFn})
	l.Update(lockId, test.T3, test.T2, false)

	// WHEN
	l.Unlock(context.Background())
	clk.Add(3 * test.Step)

	// THEN
	timeout := time.NewTimer(test.TimeoutDuration)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		assert.Fail(t, "Timeout waiting for warnChan close")
	case _, ok := <-l.WarnChan():
		assert.False(t, ok, "Expected WarnChan to be closed without sending a time")
	}
}
