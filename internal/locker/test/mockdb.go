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

package test

import (
	"context"
	"errors"
	"sync"
	"time"

	error2 "github.com/scailio-oss/dlock/error"
	"github.com/scailio-oss/dlock/internal/storage"
)

type MockLock struct {
	Owner      string
	LeaseUntil time.Time
}

type MockInsertResponse struct {
	Stolen   *storage.StolenLockInfo
	Err      error
	DoInsert bool // should lock be inserted
}

func NewMockDb() *MockDb {
	return &MockDb{
		Locks:          map[string]*MockLock{},
		InsertResponse: map[string]*MockInsertResponse{},
		RemoveResponse: map[string]error{},
		UpdateResponse: map[string]error{},
		Removed:        map[string]bool{},
	}
}

type MockDb struct {
	Mu sync.Mutex

	// TODO keep fields internal, provide accessor methods which lock automatically.
	Locks           map[string]*MockLock
	InsertResponse  map[string]*MockInsertResponse
	RemoveResponse  map[string]error
	UpdateResponse  map[string]error
	Removed         map[string]bool
	InsertCallCount int
}

func (m *MockDb) InsertNewLock(_ context.Context, lockId string, ownerName string, leaseUntil time.Time, stealLockUntil time.Time) (*storage.StolenLockInfo, error) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	m.InsertCallCount++

	if r, ok := m.InsertResponse[lockId]; ok {
		if r.DoInsert {
			m.Locks[lockId] = &MockLock{
				Owner:      ownerName,
				LeaseUntil: leaseUntil,
			}
		}
		return r.Stolen, r.Err
	}

	if l, ok := m.Locks[lockId]; ok {
		if !l.LeaseUntil.Before(stealLockUntil) {
			return nil, &error2.LockTakenError{Cause: errors.New("still locked")}
		}
	}
	m.Locks[lockId] = &MockLock{
		Owner:      ownerName,
		LeaseUntil: leaseUntil,
	}
	return nil, nil
}

func (m *MockDb) RemoveLock(_ context.Context, lockId string, until time.Time, owner string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	l, ok := m.Locks[lockId]
	if !ok {
		return errors.New("lock does not exist")
	}
	if !(l.LeaseUntil.Equal(until) && l.Owner == owner) {
		return errors.New("Unlock information given is not valid")
	}

	m.Removed[lockId] = true
	delete(m.Locks, lockId)

	if r, ok := m.RemoveResponse[lockId]; ok {
		return r
	}

	return nil
}

func (m *MockDb) UpdateUntil(_ context.Context, lockId string, _ time.Time, newUntil time.Time, ownerName string) error {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	if r, ok := m.UpdateResponse[lockId]; ok {
		return r
	}
	m.Locks[lockId] = &MockLock{
		Owner:      ownerName,
		LeaseUntil: newUntil,
	}
	return nil
}
