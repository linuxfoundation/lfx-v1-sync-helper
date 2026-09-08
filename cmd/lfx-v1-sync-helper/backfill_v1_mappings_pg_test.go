// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"testing"
)

// TestMappingRecordCopySource_Iteration verifies that the pgx.CopyFromSource
// implementation walks the buffer exactly once and returns values in the
// column order declared in copyMappingRecordsToStaging.
func TestMappingRecordCopySource_Iteration(t *testing.T) {
	buf := []mappingRecord{
		{key: "project.sfid.a", value: "uid-a", tombstoned: false, seq: 10, deleted: false},
		{key: "project.uid.a", value: "", tombstoned: true, seq: 11, deleted: false},
		{key: "committee.sfid.z", value: "", tombstoned: false, seq: 12, deleted: true},
	}
	src := &mappingRecordCopySource{buf: buf}

	var rows [][]any
	for src.Next() {
		vs, err := src.Values()
		if err != nil {
			t.Fatalf("Values(): %v", err)
		}
		rows = append(rows, vs)
	}
	if err := src.Err(); err != nil {
		t.Fatalf("Err(): %v", err)
	}
	if len(rows) != len(buf) {
		t.Fatalf("got %d rows, want %d", len(rows), len(buf))
	}
	for i, r := range rows {
		if want := 5; len(r) != want {
			t.Fatalf("row %d: got %d values, want %d", i, len(r), want)
		}
		if r[0] != buf[i].key {
			t.Errorf("row %d col 0 (mapping_key): got %v, want %v", i, r[0], buf[i].key)
		}
		if r[1] != buf[i].value {
			t.Errorf("row %d col 1 (mapping_value): got %v, want %v", i, r[1], buf[i].value)
		}
		if r[2] != buf[i].tombstoned {
			t.Errorf("row %d col 2 (tombstoned): got %v, want %v", i, r[2], buf[i].tombstoned)
		}
		if r[3] != int64(buf[i].seq) {
			t.Errorf("row %d col 3 (seq): got %v, want %v", i, r[3], int64(buf[i].seq))
		}
		if r[4] != buf[i].deleted {
			t.Errorf("row %d col 4 (deleted): got %v, want %v", i, r[4], buf[i].deleted)
		}
	}

	// Iterator exhausted — subsequent Next() must return false without
	// advancing pos or panicking.
	if src.Next() {
		t.Fatal("Next() after exhaustion returned true")
	}
}

// TestMappingRecordCopySource_EmptyBuf verifies the zero-value / empty
// buffer path used by v1MappingsWriter's dry-run flush.
func TestMappingRecordCopySource_EmptyBuf(t *testing.T) {
	src := &mappingRecordCopySource{}
	if src.Next() {
		t.Fatal("Next() on empty buf returned true")
	}
	if err := src.Err(); err != nil {
		t.Fatalf("Err(): %v", err)
	}
}
