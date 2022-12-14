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
)

// Lock is a distributed lock.
type Lock interface {
	// IsExpired retruns true if the lock is expired or it must be assumed that the lock is expired. Will never change
	// from true -> false.
	IsExpired() bool

	// Unlock this lock if it is still locked and cleanup internal resources. If using a heartbeat timeout that is higher
	// than the leasetime, this method might need to be called even after IsExpired == true to execute internal cleanup and
	// allow re-acquiring the same lockId using the same Locker instance. If the heartbeat time is smaller than the lease
	// time, cleanup happens automatically.
	Unlock(ctx context.Context)

	// WarnChan returns a channel that will publish the lockId when this lock is about to expire. This message will be
	// sent after the configured warnTime duration has elapsed since the acquiring the lock/the last successful heartbeat.
	// The channel will get closed when or after the lock is released.
	// WarnChan returns nil if warning is disabled.
	WarnChan() <-chan string

	// FencingToken returns the fencing token, if enabled when creating the Locker.
	//
	// A fencing token is guaranteed to be of larger value than all previously acquired locks for the given lockId
	// (monotonically increasing). It can be used for fencing support in downstream systems as described at
	// https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html#making-the-lock-safe-with-fencing.
	//
	// Using the fencing token, absolute exclusivity guarantees can be achieved, even in face of arbitrary process pauses.
	//
	// The token will never be negative and will never change for a given Lock.
	FencingToken() *big.Int
}
