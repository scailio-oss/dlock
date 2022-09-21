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
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/scailio-oss/dlock/internal/lock/test"
	"github.com/scailio-oss/dlock/internal/logger"
)

const (
	warnChanLockId1 = "lock1"
	warnChanLockId2 = "lock2"
)

func warnChanSetup() (WarnChanManager, *clock.Mock) {
	clk := clock.NewMock()
	wcm := NewWarnChanManager(logger.Default(), clk)

	return wcm, clk
}

func TestNoMessageInitially(t *testing.T) {
	// GIVEN
	wcm, _ := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)

	// THEN
	assertNoMessage(t, ch)
}

func TestReadFromChannelAfterTime(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)

	clk.Add(test.Step)

	// THEN
	assertMessageAvailAsync(t, ch, warnChanLockId1)
}

func TestReregisterMessageAfterCotrrectTime(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T2)
	clk.Add(test.Step) // -> t1
	assertNoMessage(t, ch)

	wcm.Register(warnChanLockId1, test.T3)
	clk.Add(test.Step) // -> t2
	assertNoMessage(t, ch)

	wcm.Register(warnChanLockId1, test.T4)
	clk.Add(test.Step) // -> t3
	assertNoMessage(t, ch)

	wcm.Register(warnChanLockId1, test.T5)
	clk.Add(test.Step) // -> t4
	assertNoMessage(t, ch)

	clk.Add(test.Step)

	// THEN
	assertMessageAvailAsync(t, ch, warnChanLockId1)

}

func TestReregisterReturnsSameChan(t *testing.T) {
	// GIVEN
	wcm, _ := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)
	ch2 := wcm.Register(warnChanLockId1, test.T2)

	// THEN
	if ch != ch2 {
		assert.Fail(t, "Expected same chan to be returned on multiple register calls")
	}
}

func TestTwoRegistersFirstWarnedLast(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T2)
	ch2 := wcm.Register(warnChanLockId2, test.T1)

	clk.Add(test.Step) // t1

	// THEN
	assertNoMessage(t, ch)
	assertMessageAvailAsync(t, ch2, warnChanLockId2)

	// WHEN
	clk.Add(test.Step)

	// THEN
	assertMessageAvailAsync(t, ch, warnChanLockId1)
	assertNoMessage(t, ch2)
}

func TestTwoRegistersFirstWarnedFirst(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)
	ch2 := wcm.Register(warnChanLockId2, test.T2)

	clk.Add(test.Step) // t1

	// THEN
	assertMessageAvailAsync(t, ch, warnChanLockId1)
	assertNoMessage(t, ch2)

	// WHEN
	clk.Add(test.Step)

	// THEN
	assertNoMessage(t, ch)
	assertMessageAvailAsync(t, ch2, warnChanLockId2)
}

func TestTwoRegistersWarnedSameTime(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)
	ch2 := wcm.Register(warnChanLockId2, test.T1)

	clk.Add(test.Step) // t1

	// THEN
	assertMessageAvailAsync(t, ch, warnChanLockId1)
	assertMessageAvailAsync(t, ch2, warnChanLockId2)
}

func TestUnregisterClosesChan(t *testing.T) {
	// GIVEN
	wcm, _ := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)
	wcm.Unregister(warnChanLockId1)

	// THEN
	timeout := time.NewTimer(test.TimeoutDuration)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		assert.Fail(t, "Timeout waiting for message on channel")
	case _, ok := <-ch:
		assert.False(t, ok, "Expected that channel was closed")
	}
}

func TestUnregisterDoesNtoTryToSendMessage(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)
	wcm.Unregister(warnChanLockId1)

	clk.Add(2 * test.Step)

	// THEN

	// channel still closed
	timeout := time.NewTimer(test.TimeoutDuration)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		assert.Fail(t, "Timeout waiting for message on channel")
	case _, ok := <-ch:
		assert.False(t, ok, "Expected that channel was closed")
	}
}

func TestRegisterSecondAfterFirstWarned(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)
	clk.Add(2 * test.Step)
	assertMessageAvailAsync(t, ch, warnChanLockId1)

	ch2 := wcm.Register(warnChanLockId2, test.T3)
	assertNoMessage(t, ch2)

	clk.Add(test.Step)

	// THEN
	assertMessageAvailAsync(t, ch2, warnChanLockId2)
}

func TestReregisterAfterWarn(t *testing.T) {
	// GIVEN
	wcm, clk := warnChanSetup()
	defer wcm.Close()

	// WHEN
	ch := wcm.Register(warnChanLockId1, test.T1)

	clk.Add(test.Step)
	assertMessageAvailAsync(t, ch, warnChanLockId1)

	ch2 := wcm.Register(warnChanLockId1, test.T2)
	clk.Add(test.Step)

	// THEN
	assertMessageAvailAsync(t, ch, warnChanLockId1)
	if ch != ch2 {
		assert.Fail(t, "Expected same channel to be returned even after warn was fired initially")
	}
}

func assertMessageAvailAsync(t *testing.T, ch <-chan string, expectedMsg string) bool {
	timeout := time.NewTimer(test.TimeoutDuration)
	defer timeout.Stop()
	select {
	case <-timeout.C:
		assert.Fail(t, "Timeout waiting for message on channel")
		return false
	case msg := <-ch:
		assert.Equal(t, expectedMsg, msg, "Expected correct message on chan")
		return true
	}
}

func assertNoMessage(t *testing.T, ch <-chan string) bool {
	select {
	case <-ch:
		assert.Fail(t, "Expected to not be able to read from channel (yet)")
		return false
	default:
		return true
	}
}
