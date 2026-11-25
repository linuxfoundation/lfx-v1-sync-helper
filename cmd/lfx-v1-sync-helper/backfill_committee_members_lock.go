// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// A live --backfill-committee-members run holds this lease for its whole
// duration. Resume treats every unrestored settings hold as abandoned, which
// is only true when no other run is still working on it: without the lease a
// second run would restore a committee's settings while the first is still
// re-emitting its members.
const memberBackfillLockKey = "backfill.member_settings.lock"

// Package vars so tests can shorten them.
var (
	memberBackfillLockHeartbeat = 30 * time.Second
	memberBackfillLockStale     = 3 * time.Minute
)

type memberBackfillLock struct {
	Owner       string    `json:"owner"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
}

// memberBackfillLease is a held lock. Lost is closed when the heartbeat can no
// longer prove ownership; the run must then stop starting new committees.
type memberBackfillLease struct {
	owner string
	Lost  <-chan struct{}
	stop  chan struct{}
	done  sync.WaitGroup
}

func newMemberBackfillLockOwner() string {
	host, _ := os.Hostname()
	suffix, err := randomHex8()
	if err != nil {
		suffix = fmt.Sprint(time.Now().UnixNano())
	}
	return host + "-" + suffix
}

// acquireMemberBackfillLock takes the lease, or takes over one whose holder
// stopped heartbeating more than memberBackfillLockStale ago.
func acquireMemberBackfillLock(ctx context.Context) (*memberBackfillLease, error) {
	owner := newMemberBackfillLockOwner()
	value, err := json.Marshal(memberBackfillLock{Owner: owner, HeartbeatAt: time.Now().UTC()})
	if err != nil {
		return nil, err
	}
	revision, err := mappingStore.Create(ctx, memberBackfillLockKey, value)
	if errors.Is(err, ErrKeyExists) {
		entry, getErr := mappingStore.Get(ctx, memberBackfillLockKey)
		if getErr != nil {
			return nil, fmt.Errorf("reading member backfill lock: %w", getErr)
		}
		var current memberBackfillLock
		if err := json.Unmarshal(entry.Value, &current); err != nil {
			return nil, fmt.Errorf("decoding member backfill lock: %w", err)
		}
		if age := time.Since(current.HeartbeatAt); age < memberBackfillLockStale {
			return nil, fmt.Errorf("another committee member backfill is running (owner %s, last heartbeat %s ago); wait for it, or for its lock to go stale after %s",
				current.Owner, age.Round(time.Second), memberBackfillLockStale)
		}
		logger.With("previous_owner", current.Owner, "last_heartbeat", current.HeartbeatAt).
			WarnContext(ctx, "taking over a stale committee member backfill lock")
		revision, err = mappingStore.Update(ctx, memberBackfillLockKey, value, entry.Revision)
	}
	if err != nil {
		return nil, fmt.Errorf("acquiring member backfill lock: %w", err)
	}

	lost := make(chan struct{})
	lease := &memberBackfillLease{owner: owner, Lost: lost, stop: make(chan struct{})}
	lease.done.Add(1)
	go lease.heartbeat(ctx, revision, lost)
	return lease, nil
}

// heartbeat refreshes the lock with a revision check. It gives up, closing
// lost, when another owner took the lock or when it could not refresh for
// half the stale window, before anyone else may take it over.
func (l *memberBackfillLease) heartbeat(ctx context.Context, revision uint64, lost chan<- struct{}) {
	defer l.done.Done()
	ticker := time.NewTicker(memberBackfillLockHeartbeat)
	defer ticker.Stop()
	lastSuccess := time.Now()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
		}
		value, _ := json.Marshal(memberBackfillLock{Owner: l.owner, HeartbeatAt: time.Now().UTC()})
		next, err := mappingStore.Update(context.WithoutCancel(ctx), memberBackfillLockKey, value, revision)
		switch {
		case err == nil:
			revision, lastSuccess = next, time.Now()
			continue
		case errors.Is(err, ErrRevisionMismatch):
			logger.ErrorContext(ctx, "committee member backfill lock was taken by another run")
		case time.Since(lastSuccess) < memberBackfillLockStale/2:
			logger.With(errKey, err).WarnContext(ctx, "failed to refresh committee member backfill lock; retrying")
			continue
		default:
			logger.With(errKey, err).ErrorContext(ctx, "could not refresh committee member backfill lock")
		}
		close(lost)
		return
	}
}

// Release stops the heartbeat and expires the lock if this run still owns it.
// It writes a zero heartbeat with a revision check instead of deleting the key:
// the MappingStore has no conditional delete, and a read-then-delete could
// remove a lock another run took over in between. An expired lock is taken
// over by the next run at once.
func (l *memberBackfillLease) Release(ctx context.Context) {
	close(l.stop)
	l.done.Wait()
	entry, err := mappingStore.Get(ctx, memberBackfillLockKey)
	if err != nil {
		return
	}
	var current memberBackfillLock
	if json.Unmarshal(entry.Value, &current) != nil || current.Owner != l.owner {
		return
	}
	value, err := json.Marshal(memberBackfillLock{Owner: l.owner})
	if err != nil {
		return
	}
	if _, err := mappingStore.Update(ctx, memberBackfillLockKey, value, entry.Revision); err != nil && !errors.Is(err, ErrRevisionMismatch) {
		logger.With(errKey, err).WarnContext(ctx, "failed to release committee member backfill lock; it goes stale on its own")
	}
}
