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

package storage

import (
	"context"
	"time"
)

// Database layer providing serializable compare-and-set operations as required by the Locker.
//
// A lock has a unique identifying lockId and if it is locked a current ownerName and a timestamp until when it is
// leased by that owner. A lock can be stolen if the existing leaseTime is older than a specific time.
type DB interface {
	// Inserts a new lock with the given details iff none exists or the exisiting lock is older than stealLockUntil. In
	// the latter case the lock is stolen and details about the old lock are returned.
	// If the lock is taken by someone else and it is not being stolen, a dlock.LockTakenError is returned, but other
	// errors may be returned.
	InsertNewLock(ctx context.Context, lockId string, ownerName string, leaseUntil time.Time, stealLockUntil time.Time) (*StolenLockInfo, error)

	// Remove a lock iff its current leaseUntil and owner is as specified. If the lock is not removed or any other error
	// occurs, return an error.
	RemoveLock(ctx context.Context, lockId string, leaseUntil time.Time, ownerName string) error

	// Update the leaseUntil field of an existing lock iff its current leaseUntil is still oldUntil and the owner is as
	// specified. If the lock is not updated or any other error occurs, return an error.
	UpdateUntil(ctx context.Context, lockId string, oldUntil time.Time, newUntil time.Time, ownerName string) error
}

type StolenLockInfo struct {
	// The owner who previously owned the lock
	OwnerName string
	// The time until that owner owned the lock previously
	LockedUntil time.Time
}
