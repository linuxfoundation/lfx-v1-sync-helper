// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Diff holds the result of comparing two maps.
type Diff struct {
	OnlyInDynamo map[string]any    // keys present in DynamoDB item but not in OS v1_data
	OnlyInOS     map[string]any    // keys present in OS v1_data but not in DynamoDB item
	ValueDiffs   map[string]ValuePair // dotted key path → {DynamoDB, OS} values that differ
}

// ValuePair holds two differing values at the same path.
type ValuePair struct {
	DynamoDB any `json:"dynamodb"`
	OS       any `json:"opensearch"`
}

// IsClean returns true when there are no differences.
func (d *Diff) IsClean() bool {
	return len(d.OnlyInDynamo) == 0 && len(d.OnlyInOS) == 0 && len(d.ValueDiffs) == 0
}

// diffOpts controls optional normalization behavior during comparison.
type diffOpts struct {
	stripAuth0Prefix bool // treat "auth0|X" and "X" as equal strings
}

// DiffMaps compares a DynamoDB item against an OpenSearch v1_data map.
func DiffMaps(dynamo, osData map[string]any, opts diffOpts) *Diff {
	d := &Diff{
		OnlyInDynamo: make(map[string]any),
		OnlyInOS:     make(map[string]any),
		ValueDiffs:   make(map[string]ValuePair),
	}
	diffRecursive("", dynamo, osData, d, opts)
	return d
}

func diffRecursive(prefix string, dynamo, osData map[string]any, d *Diff, opts diffOpts) {
	key := func(k string) string {
		if prefix == "" {
			return k
		}
		return prefix + "." + k
	}

	for k, dv := range dynamo {
		ov, ok := osData[k]
		if !ok {
			if !isZeroValue(dv) {
				d.OnlyInDynamo[key(k)] = dv
			}
			continue
		}
		compareValues(key(k), dv, ov, d, opts)
	}

	for k, ov := range osData {
		if _, ok := dynamo[k]; !ok && !isZeroValue(ov) {
			d.OnlyInOS[key(k)] = ov
		}
	}
}

func compareValues(path string, dv, ov any, d *Diff, opts diffOpts) {
	// nil vs empty slice/map are equivalent — the absence of a field in DynamoDB
	// and an empty collection in OS represent the same semantic state.
	if dv == nil && isZeroValue(ov) {
		return
	}
	if ov == nil && isZeroValue(dv) {
		return
	}

	dv = normalize(dv, opts)
	ov = normalize(ov, opts)

	dm, dIsMap := toStringMap(dv)
	om, oIsMap := toStringMap(ov)

	if dIsMap && oIsMap {
		diffRecursive(path, dm, om, d, opts)
		return
	}

	ds, dIsSlice := toSlice(dv)
	os_, oIsSlice := toSlice(ov)

	if dIsSlice && oIsSlice {
		compareSlices(path, ds, os_, d, opts)
		return
	}

	if !reflect.DeepEqual(dv, ov) {
		d.ValueDiffs[path] = ValuePair{DynamoDB: dv, OS: ov}
	}
}

func compareSlices(path string, ds, os_ []any, d *Diff, opts diffOpts) {
	// Sort primitive slices before comparing so order differences are ignored.
	dSorted := sortedSlice(ds)
	oSorted := sortedSlice(os_)

	maxLen := len(dSorted)
	if len(oSorted) > maxLen {
		maxLen = len(oSorted)
	}

	for i := 0; i < maxLen; i++ {
		elemPath := fmt.Sprintf("%s[%d]", path, i)
		if i >= len(dSorted) {
			d.OnlyInOS[elemPath] = oSorted[i]
			continue
		}
		if i >= len(oSorted) {
			d.OnlyInDynamo[elemPath] = dSorted[i]
			continue
		}
		compareValues(elemPath, dSorted[i], oSorted[i], d, opts)
	}
}

// matchesIgnorePattern reports whether path matches pattern, where "[*]" in the
// pattern matches any array index like "[0]", "[1]", etc.
func matchesIgnorePattern(pattern, path string) bool {
	if pattern == path {
		return true
	}
	pi, pathi := 0, 0
	for pi < len(pattern) && pathi < len(path) {
		if pattern[pi] == '[' && pi+2 < len(pattern) && pattern[pi+1] == '*' && pattern[pi+2] == ']' {
			if path[pathi] != '[' {
				return false
			}
			end := pathi + 1
			for end < len(path) && path[end] != ']' {
				end++
			}
			if end >= len(path) {
				return false
			}
			pathi = end + 1
			pi += 3
		} else {
			if pattern[pi] != path[pathi] {
				return false
			}
			pi++
			pathi++
		}
	}
	return pi == len(pattern) && pathi == len(path)
}

// deleteMatchingKeys removes all keys in m that match any of the given patterns
// (which may contain "[*]" wildcards).
func deleteMatchingKeys[V any](m map[string]V, patterns []string) {
	for k := range m {
		for _, p := range patterns {
			if matchesIgnorePattern(p, k) {
				delete(m, k)
				break
			}
		}
	}
}

// isZeroValue returns true for values that represent an empty/zero state in OS
// (false, 0, "", nil, empty slice/map). OS often stores zero-value defaults that
// simply aren't present in DynamoDB, so these should not count as diffs.
func isZeroValue(v any) bool {
	if v == nil {
		return true
	}
	v = normalize(v, diffOpts{})
	switch x := v.(type) {
	case bool:
		return !x
	case float64:
		return x == 0
	case string:
		return x == ""
	case []any:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	}
	return false
}

// normalize converts numeric types to float64 and json.Number to float64
// so that DynamoDB numbers (stored as string-backed json.Number or int64)
// compare equal to OpenSearch numbers (stored as float64).
// When opts.stripAuth0Prefix is set, it also strips the "auth0|" provider
// prefix from strings so bare usernames match OS values like "auth0|username".
func normalize(v any, opts diffOpts) any {
	switch x := v.(type) {
	case json.Number:
		if f, err := x.Float64(); err == nil {
			return f
		}
		return x.String()
	case int:
		return float64(x)
	case int32:
		return float64(x)
	case int64:
		return float64(x)
	case float32:
		return float64(x)
	case string:
		if opts.stripAuth0Prefix {
			return strings.TrimPrefix(x, "auth0|")
		}
		return x
	}
	return v
}

func toStringMap(v any) (map[string]any, bool) {
	m, ok := v.(map[string]any)
	return m, ok
}

func toSlice(v any) ([]any, bool) {
	s, ok := v.([]any)
	return s, ok
}

// sortedSlice returns a copy of the slice sorted by string representation
// of each element. Only sorts when all elements are primitive (string/number/bool).
func sortedSlice(s []any) []any {
	cp := make([]any, len(s))
	copy(cp, s)

	allPrimitive := true
	for _, v := range cp {
		switch v.(type) {
		case string, float64, float32, int, int32, int64, bool, json.Number:
		default:
			allPrimitive = false
		}
	}
	if !allPrimitive {
		return cp
	}

	sort.Slice(cp, func(i, j int) bool {
		return fmt.Sprintf("%v", cp[i]) < fmt.Sprintf("%v", cp[j])
	})
	return cp
}
