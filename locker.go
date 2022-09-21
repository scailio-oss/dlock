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
	"context"
)

// Locker creates distributed locks for synchronizing access to objects of the same type.
// It uses an AWS DynamoDB table to synchronize locks. The table must be created and have a partition key of type String
// with the name "key".
type Locker interface {
	// TryLock tries to acquire a lock for one of the objects in scope of this locker, the object being identified by the
	// given ID.
	//
	// Example: If this locker is responsible for the locks of type "streets-in-new-york-city" then one of the objects that
	// can be locked is "wall-street".
	//
	// If the object is already locked by a different Locker (e.g. in a different process), a LockTakenError will be
	// returned. Other errors might be returned as well, in which case the caller must also assume to NOT have acquired
	// the lock.
	//
	// If this locker has currently locked the object with the given lockId, a LockTakenError will be returned. Unlock the
	// existing Lock first.
	//
	// TryLock will steal a lock that has a leaseUntilTime which is older than maxClockSkew, see factory.WithLease and
	// factory.WithMaxClockSkew.
	//
	// Stealing locks is safe, since Locker relies on the clocks of the systems of participating other instances of Locker
	// (which compete in locking the same object type) to be synchronized well. The Locker which acquired
	// the lock initially already assumes that it lost the lock, since its lease time passed, and it was not able to
	// refresh the lock in time.
	//
	// Stealing locks is not only safe, but actually required to allow automatic recovery of situations where systems of
	// participating other instances of Locker might've failed to release their locks, e.g. due to system crashes,
	// container preemptions, network errors etc.
	TryLock(ctx context.Context, lockId string) (Lock, error)

	// Close unlocks all currently held locks and frees up all resources. Must be called when the Locker is not needed
	// anymore.
	Close()
}
