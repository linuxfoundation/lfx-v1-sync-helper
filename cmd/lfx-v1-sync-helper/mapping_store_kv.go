// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// The lfx-v1-sync-helper service.
package main

import (
	"context"
	"errors"

	"github.com/nats-io/nats.go/jetstream"
)

// kvMappingStore is a MappingStore adapter over a jetstream.KeyValue
// bucket. It exists so the legacy KV bucket is addressable through the
// same interface as the Postgres backend, letting main.go pick a
// backend at boot without any handler-level branching.
//
// The adapter is a straight-through wrapper: no revision or value
// translation happens because the KV surface already matches
// MappingStore's semantics. All that changes is error translation to
// the sentinel errors defined in mapping_store.go.
type kvMappingStore struct {
	kv jetstream.KeyValue
}

// newKVMappingStore constructs the adapter. kv must be a live bucket
// handle (jsContext.KeyValue result); nil is not accepted and will
// panic on first use.
func newKVMappingStore(kv jetstream.KeyValue) *kvMappingStore {
	return &kvMappingStore{kv: kv}
}

// Get returns the current entry for key. Translates
// jetstream.ErrKeyNotFound (and the rarely-surfaced ErrKeyDeleted) to
// ErrKeyNotFound so callers can keep using errors.Is on a single
// sentinel across backends.
func (s *kvMappingStore) Get(ctx context.Context, key string) (MappingEntry, error) {
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrKeyDeleted) {
			return MappingEntry{}, ErrKeyNotFound
		}
		return MappingEntry{}, err
	}
	return MappingEntry{Value: entry.Value(), Revision: entry.Revision()}, nil
}

// Put unconditionally writes value at key and returns the new revision.
func (s *kvMappingStore) Put(ctx context.Context, key string, value []byte) (uint64, error) {
	return s.kv.Put(ctx, key, value)
}

// Update writes value at key only when the current revision matches
// expectedRevision. Translates the JetStream "wrong last sequence" API
// error (code 10071) to ErrRevisionMismatch via the existing
// isRevisionMismatchError helper — that is the same detector used
// throughout the codebase for KV CAS mismatches. ErrKeyNotFound is
// also mapped to ErrRevisionMismatch to match jetstream.KeyValue.Update
// semantics — on that path callers cannot distinguish "no row" from
// "wrong rev" via KV either, so preserving that ambiguity keeps
// behaviour identical.
func (s *kvMappingStore) Update(ctx context.Context, key string, value []byte, expectedRevision uint64) (uint64, error) {
	rev, err := s.kv.Update(ctx, key, value, expectedRevision)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) || isRevisionMismatchError(err) {
			return 0, ErrRevisionMismatch
		}
		return 0, err
	}
	return rev, nil
}

// Create writes value at key only when the key does not already exist.
// Translates jetstream.ErrKeyExists to ErrKeyExists.
func (s *kvMappingStore) Create(ctx context.Context, key string, value []byte) (uint64, error) {
	rev, err := s.kv.Create(ctx, key, value)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyExists) {
			return 0, ErrKeyExists
		}
		return 0, err
	}
	return rev, nil
}

// Delete removes the row. Idempotent — deleting a non-existent key
// returns nil because jetstream.KeyValue.Delete is idempotent.
func (s *kvMappingStore) Delete(ctx context.Context, key string) error {
	if err := s.kv.Delete(ctx, key); err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return nil
		}
		return err
	}
	return nil
}
