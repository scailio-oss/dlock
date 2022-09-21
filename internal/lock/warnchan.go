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
	"container/heap"
	"context"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	"github.com/scailio-oss/dlock/logger"
)

type lockInfo struct {
	// The time when we should warn for this lock, which was received most recently via Register.
	newestWarnTime time.Time
	// Channel which we returned on the first Register call for this lock. It is buffered with 1. Note that we do not need
	// to buffer >1 msg, since it would be the same message mulitple times anyway.
	ch     chan string
	lockId string

	// Index in the lockInfoHeap of this lockInfo
	heapIndex int
}

type lockInfoHeap []*lockInfo

var _ heap.Interface = &lockInfoHeap{}

// WarnChanManager is a central instance per Locker which manages all WarnChans of all Locks.
type WarnChanManager interface {
	// Register new warnTime for a lock. Returns a chan that will receive the lockId that reached the warnTime. Result is nil
	// after Close. The returned chan will be closed when the lock is unregistered, or, at the latest, on Close. Must call
	// Unregister for the lock later. Will always return the same channel instance for the same lock while it is not unregistered.
	// Will return nil always if warnChan is disabled.
	Register(lockId string, warnTime time.Time) <-chan string

	// Unregister the lock from the manager. Channel will be closed and no more messages will be sent.
	Unregister(lockId string)

	// Close the manager & free up resources, unregistering all remaining registered locks.
	Close()
}

type warnChanManagerImpl struct {
	// This is closed to trigger shutdown of warnLoop
	closeChan chan struct{}
	// write lock during shutdown, sync access to closed
	closeMu sync.RWMutex
	closed  bool
	// When warnLoop has finished shutting down, it closes this chan.
	closeFinishedChan chan struct{}

	logger logger.Logger
	clock  clock.Clock
	// send a message when the head of the heap changed. Valid until closed == true. The inner chan is closed when the work is done.
	newHeapHeadChan chan chan struct{}

	mu sync.Mutex // sync access to locks and locksHeap
	// lockId -> lockInfo for active locks
	locks map[string]*lockInfo
	// Heap of locks, ordered by newestWarnTime, earliest first.
	locksHeap *lockInfoHeap
}

func NewWarnChanManager(logger logger.Logger, clk clock.Clock) WarnChanManager {
	h := make(lockInfoHeap, 0)
	heap.Init(&h)
	res := &warnChanManagerImpl{
		locks:             map[string]*lockInfo{},
		locksHeap:         &h,
		closeChan:         make(chan struct{}),
		newHeapHeadChan:   make(chan chan struct{}),
		closeFinishedChan: make(chan struct{}),
		clock:             clk,
		logger:            logger,
	}

	startupCompleteChan := make(chan struct{})
	go res.warnLoop(startupCompleteChan)
	// Wait until the goroutine actually started. In unit tests we saw that the main goroutine might starve the warnLoop
	// from starting up, in that case messing with all timings. Instead of using runtime.Gosched(), we chose to explicitly
	// block until we know the goroutine has started, since Gosched cannot guarantee this.
	<-startupCompleteChan

	return res
}

// Register new warnTime for a lock. Returns a chan that will receive the lockId that reached the warnTime. Result is nil
// after Close. The returned chan will be closed when the lock is unregistered, or, at the latest, on Close.
func (w *warnChanManagerImpl) Register(lockId string, warnTime time.Time) <-chan string {
	// Take readlock on closeMu to ensure this does not run concurrently to Close
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()

	if w.closed {
		return nil
	}

	w.mu.Lock()
	newHeapHead := false

	li, ok := w.locks[lockId]
	if ok {
		// this is an update, not an insert. Though we might've popped it from the heap already!
		li.newestWarnTime = warnTime

		if li.heapIndex == -1 {
			// re-insert!
			heap.Push(w.locksHeap, li)
		} else {
			w.locksHeap.fix(li)
		}
		if w.locksHeap.head() == li {
			newHeapHead = true
		}
	} else {
		li = &lockInfo{
			lockId:         lockId,
			newestWarnTime: warnTime,
			ch:             make(chan string, 1), // buffer one, since if one is in the pipeline, we do not need to deliver another
		}
		heap.Push(w.locksHeap, li)
		if w.locksHeap.head() == li {
			newHeapHead = true
		}
		w.locks[lockId] = li
	}

	w.mu.Unlock()

	if newHeapHead {
		// inform newHeapHeadChan outside of lock!
		doneChan := make(chan struct{})
		w.newHeapHeadChan <- doneChan
		// block until the change has been processed by warnloop. This is required to ensure warnloop sets up the correct
		// wait duration, otherwise the logic in warnloop could run much later. For the unit tests, the internal clock might
		// proceed much further concurrently to warnloop setting up the timer - which would destroy the unit test.
		// Therefore: Block.
		<-doneChan
	}

	return li.ch
}

func (w *warnChanManagerImpl) Unregister(lockId string) {
	// Take readlock on closeMu to ensure this does not run concurrently to Close
	w.closeMu.RLock()
	defer w.closeMu.RUnlock()

	w.doUnregister(lockId)
}

func (w *warnChanManagerImpl) doUnregister(lockId string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	lockInfo, ok := w.locks[lockId]
	if !ok {
		return
	}

	delete(w.locks, lockId)
	close(lockInfo.ch)
	w.logger.Debug(context.Background(), "Unregistered lock (lockId)", lockId)
	// do not update the heap, instead just ignore the lockInfo in warnLoop
}

// Loops until closeChan is closed and wakes up once when the first element in locksHeap needs to be warned. Will
// schedule the next wakeup accordingly.
// The chan passed in will be closed just before entering the worker loop.
func (w *warnChanManagerImpl) warnLoop(startupCompleteChan chan struct{}) {
	var timer *clock.Timer
	var timerChan <-chan time.Time

	setupNextWarnAt := func(at time.Time) {
		now := w.clock.Now()
		warnIn := at.Sub(now)
		if warnIn <= 0 {
			// This happens primarily in the unit tests, where we might have increased the time already too far, so the
			// warnTime is in the past. Warn immediately. This should not happen as much in realworld scenarios.
			w.logger.Debug(context.Background(), "Updated warnLoop. Will warn immediately, since warnTime is in the past (now/warnIn)", now, warnIn)
			newChan := make(chan time.Time, 1)
			newChan <- now
			timerChan = newChan
			return
		}

		// Note: The mock timer for the unit tests internally re-calculates a fixed time.Time when to trigger the Timer. We
		// must prevent the clock proceeding between calculating warnIn and the Timer setting itself up - otherwise the
		// timer might fire too late.
		// Outside the unit-tests, this is not as bad, since at max a few millis or so have passed.
		timer = w.clock.Timer(warnIn)
		timerChan = timer.C
		w.logger.Debug(context.Background(), "Updated warnLoop (now/warnIn)", now, warnIn)
	}

	close(startupCompleteChan)

	for {
		select {
		case <-w.closeChan:
			if timer != nil {
				timer.Stop()
				timer = nil
			}
			timerChan = nil
			close(w.closeFinishedChan)
			return
		case doneChan := <-w.newHeapHeadChan:
			if timer != nil {
				timer.Stop()
				timer = nil
			}
			timerChan = nil

			w.mu.Lock()
			if h := w.locksHeap.head(); h != nil {
				setupNextWarnAt(h.newestWarnTime)
			}
			w.mu.Unlock()
			close(doneChan)
			continue
		case <-timerChan:
		}

		// Timer triggered, check if we need to send (a) warn(s). We iterate over the beginning of locksHeap and see for
		// which of the locksInfo objectd we need to send a message.

		if timer != nil {
			timer.Stop()
			timer = nil
		}
		timerChan = nil
		for {
			w.mu.Lock()
			lockInfo := w.locksHeap.head()
			if lockInfo == nil {
				// locks Heap is empty, nothing more to do.
				w.mu.Unlock()
				break
			}

			if w.clock.Now().Before(lockInfo.newestWarnTime) {
				// we reached an item that is not yet up for warning, we're done in this loop.
				w.mu.Unlock()
				break
			}

			heap.Pop(w.locksHeap)
			doWarn := true
			if l, ok := w.locks[lockInfo.lockId]; !ok || l != lockInfo {
				// lockInfo in queue is outdated: either it was removed from locks already or replaced with a different instance.
				doWarn = false
			}

			// If we need to warn, just be sure we have correct values and lockInfo object is not changed in the meantime
			li := *lockInfo
			w.mu.Unlock()

			if doWarn {
				w.logger.Debug(context.Background(), "Warning lock (lockId)", li.lockId)
				select {
				case li.ch <- li.lockId:
				default:
					// li.ch either closed or there's a message buffered already. Since the message would be the same now
					// (lockId), we can skip this addition.
				}
			}
		}

		// Setup next warn time if we have any
		w.mu.Lock()
		if h := w.locksHeap.head(); h != nil {
			setupNextWarnAt(h.newestWarnTime)
		}
		w.mu.Unlock()
	}
}

func (w *warnChanManagerImpl) Close() {
	w.closeMu.Lock()
	defer w.closeMu.Unlock()

	if w.closed {
		return
	}

	// shutdown warnloop
	close(w.closeChan)
	<-w.closeFinishedChan

	// Unregister all remaining locks
	for {
		w.mu.Lock()
		if len(w.locks) == 0 {
			w.mu.Unlock()
			break
		}

		lockIds := make([]string, 0, len(w.locks))
		for _, l := range w.locks {
			lockIds = append(lockIds, l.lockId)
		}
		w.mu.Unlock()

		for _, lockId := range lockIds {
			w.doUnregister(lockId)
		}
	}

	close(w.newHeapHeadChan)

	w.locksHeap = nil
	w.closed = true
}

func (l *lockInfoHeap) Len() int {
	return len((*l))
}

func (l *lockInfoHeap) Less(i, j int) bool {
	if (*l)[i].newestWarnTime.Equal((*l)[j].newestWarnTime) {
		// if [i].time == [j].time, use lockId as tiebreaker
		return (*l)[i].lockId < (*l)[j].lockId
	}

	return (*l)[i].newestWarnTime.Before((*l)[j].newestWarnTime)
}

func (l *lockInfoHeap) Swap(i, j int) {
	(*l)[i], (*l)[j] = (*l)[j], (*l)[i]
	(*l)[i].heapIndex = i
	(*l)[j].heapIndex = j
}

func (l *lockInfoHeap) Push(x any) {
	li := x.(*lockInfo)
	*l = append(*l, li)
	li.heapIndex = len(*l) - 1
}

func (l *lockInfoHeap) Pop() any {
	prev := *l
	newLen := len(prev) - 1
	popped := prev[newLen]
	popped.heapIndex = -1
	prev[newLen] = nil
	*l = prev[0:newLen]
	return popped
}

func (l *lockInfoHeap) fix(lock *lockInfo) {
	heap.Fix(l, lock.heapIndex)
}

// Returns the head of the heap or nil. The head of the heap is the item with the smallest newestWarnTime.
func (l *lockInfoHeap) head() *lockInfo {
	if len(*l) > 0 {
		return (*l)[0]
	}
	return nil
}

type noopWarnChanManager struct {
}

func NewNoopWarnChanManager() WarnChanManager {
	return &noopWarnChanManager{}
}

func (n *noopWarnChanManager) Register(_ string, _ time.Time) <-chan string {
	return nil
}

func (n *noopWarnChanManager) Unregister(_ string) {
}

func (n *noopWarnChanManager) Close() {
}
