// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

// Distributed KV-backed locking for serialising concurrent read-modify-write
// operations on shared mapping state.
//
// The mappingLocker interface abstracts the underlying lock backend so it can
// be swapped (e.g. for Redis) without touching the callers.  The only current
// implementation is kvMappingLocker, which uses the NATS JetStream KV bucket:
//   - A lock is acquired by atomically creating a key (Create fails when the
//     key already exists).
//   - Locks older than the configured timeout are considered stale and are
//     forcibly reclaimed.
//   - The caller retries up to maxRetries times, sleeping retryInterval
//     between each attempt.
//
// Key naming convention: callers are responsible for constructing fully-qualified
// lock keys (including any namespace prefix) before passing them to acquire/release.
// For example: meetingMappingLockKeyPrefix + meetingID.
//
// TODO: When the handlers are migrated to the wrapper services, this locking
// mechanism should be revisited.  The wrappers have direct access to the
// database, so per-resource conditional updates or proper DB-level locking
// should replace this distributed KV lock.

import (
	"context"
	"strconv"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	// Lock key prefix and default settings for meeting-mapping operations.
	meetingMappingLockKeyPrefix = "v1_meeting_mapping_lock."
	// Lock key prefix for past-meeting-mapping operations.
	pastMeetingMappingLockKeyPrefix = "v1_past_meeting_mapping_lock."

	mappingLockTimeout       = 10 * time.Second
	mappingLockRetryInterval = 500 * time.Millisecond
	mappingLockRetryAttempts = 5
)

// mappingLocker is the interface for acquiring and releasing locks over shared
// mapping resources.  Implementations must be safe for concurrent use.
//
// This interface is intentionally backend-agnostic and can be satisfied by:
//   - A distributed lock backed by NATS JetStream KV (kvMappingLocker, current impl)
//   - A distributed lock backed by Redis (e.g. Redlock algorithm)
//   - A local in-process lock backed by Go sync.Mutex (useful for single-instance
//     deployments or testing)
//
// The key parameter is always a fully-qualified lock key including any
// namespace prefix (e.g. meetingMappingLockKeyPrefix + meetingID).
type mappingLocker interface {
	// acquire tries to acquire the lock for key.
	// Returns (acquired, waited) — waited is true if at least one retry was made.
	acquire(ctx context.Context, key string) (acquired bool, waited bool)
	// release frees the lock for key.
	release(ctx context.Context, key string) error
}

// lockerConfig holds the runtime configuration for a kvMappingLocker.
type lockerConfig struct {
	timeout       time.Duration
	retryInterval time.Duration
	maxRetries    int
}

// lockerOption is a functional option for configuring a kvMappingLocker.
type lockerOption func(*lockerConfig)

// withLockerOptionTimeout sets the duration after which an existing lock is considered
// stale and may be forcibly reclaimed.
func withLockerOptionTimeout(d time.Duration) lockerOption {
	return func(c *lockerConfig) { c.timeout = d }
}

// withLockerOptionRetryInterval sets the sleep duration between consecutive acquire attempts.
func withLockerOptionRetryInterval(d time.Duration) lockerOption {
	return func(c *lockerConfig) { c.retryInterval = d }
}

// withLockerOptionMaxRetries sets the maximum number of acquire attempts before giving up.
func withLockerOptionMaxRetries(n int) lockerOption {
	return func(c *lockerConfig) { c.maxRetries = n }
}

// kvMappingLocker is the NATS JetStream KV implementation of mappingLocker.
type kvMappingLocker struct {
	cfg lockerConfig
	kv  jetstream.KeyValue
}

// newKVMappingLocker creates a kvMappingLocker backed by the given KV bucket.
// Default settings match the meeting-mapping use case; override them via opts.
func newKVMappingLocker(kv jetstream.KeyValue, opts ...lockerOption) *kvMappingLocker {
	cfg := lockerConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &kvMappingLocker{cfg: cfg, kv: kv}
}

// acquire implements mappingLocker.
func (l *kvMappingLocker) acquire(ctx context.Context, key string) (bool, bool) {
	return acquireKVLock(ctx, l.kv, key, l.cfg.timeout, l.cfg.retryInterval, l.cfg.maxRetries)
}

// release implements mappingLocker.
func (l *kvMappingLocker) release(ctx context.Context, key string) error {
	return releaseKVLock(ctx, l.kv, key)
}

// acquireKVLock is the low-level distributed lock acquisition over a NATS
// JetStream KV bucket.
func acquireKVLock(ctx context.Context, kv jetstream.KeyValue, lockKey string, timeout, retryInterval time.Duration, maxRetries int) (bool, bool) {
	var waited bool

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lockValue := strconv.FormatInt(time.Now().Unix(), 10)

		// Atomic create: succeeds only if the key does not yet exist.
		if _, err := kv.Create(ctx, lockKey, []byte(lockValue)); err == nil {
			return true, waited
		}

		// The key already exists — check whether the lock is stale.
		if entry, getErr := kv.Get(ctx, lockKey); getErr == nil {
			if ts, parseErr := strconv.ParseInt(string(entry.Value()), 10, 64); parseErr == nil {
				if time.Since(time.Unix(ts, 0)) > timeout {
					// Stale lock: overwrite and claim it.
					if _, updateErr := kv.Put(ctx, lockKey, []byte(lockValue)); updateErr == nil {
						return true, waited
					}
				}
			}
		}

		if attempt < maxRetries {
			waited = true
			time.Sleep(retryInterval)
		}
	}

	return false, waited
}

// releaseKVLock deletes the lock entry from the given KV bucket.
func releaseKVLock(ctx context.Context, kv jetstream.KeyValue, lockKey string) error {
	return kv.Delete(ctx, lockKey)
}
