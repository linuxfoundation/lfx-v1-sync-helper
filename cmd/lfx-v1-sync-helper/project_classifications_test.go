// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"reflect"
	"testing"

	projectservice "github.com/linuxfoundation/lfx-v2-project-service/api/project/v1/gen/project_service"
)

func TestProjectBasesEqualClassifications(t *testing.T) {
	base := &projectservice.ProjectBase{
		Industry: []string{"Energy & Climate"}, Technology: []string{"Blockchain"}, TechnicalActivityType: []string{"Open Data"},
	}
	for _, tt := range []struct {
		name string
		edit func(*projectservice.ProjectBase)
	}{
		{"industry-only", func(p *projectservice.ProjectBase) { p.Industry = []string{"Manufacturing"} }},
		{"technology-only", func(p *projectservice.ProjectBase) { p.Technology = []string{} }},
		{"activity-only", func(p *projectservice.ProjectBase) { p.TechnicalActivityType = []string{"Open Hardware"} }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			copyOfBase := *base
			tt.edit(&copyOfBase)
			if projectBasesEqual(base, &copyOfBase) {
				t.Error("classification-only change did not trigger a base update")
			}
		})
	}
	unchanged := *base
	if !projectBasesEqual(base, &unchanged) {
		t.Error("identical classification fields should not trigger a base update")
	}
}

func TestMapV1ProjectClassificationsToPayloads(t *testing.T) {
	previous := mappingStore
	store := newFakeMappingStore()
	mappingStore = store
	t.Cleanup(func() { mappingStore = previous })
	if _, err := store.Put(t.Context(), "project.sfid.test-parent-sfid", []byte("00000000-0000-4000-8000-000000000002")); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name     string
		industry any
		want     []string
	}{
		{"multiple values", "Energy & Climate;Manufacturing", []string{"Energy & Climate", "Manufacturing"}},
		{"explicit clear", "", []string{}},
		{"null clear", nil, []string{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]any{
				"name": "Test Project", "slug__c": "test-project", "parent_project__c": "test-parent-sfid",
				"industry__c": tt.industry, "technology__c": "Blockchain;Linux Kernel", "technical_activity_type__c": "Open Data",
			}
			created, err := mapV1DataToProjectCreatePayload(t.Context(), data)
			if err != nil {
				t.Fatal(err)
			}
			updated, err := mapV1DataToProjectUpdateBasePayload(t.Context(), "00000000-0000-4000-8000-000000000001", data)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(created.Industry, tt.want) || !reflect.DeepEqual(updated.Industry, tt.want) ||
				!reflect.DeepEqual(created.Technology, []string{"Blockchain", "Linux Kernel"}) ||
				!reflect.DeepEqual(updated.TechnicalActivityType, []string{"Open Data"}) {
				t.Errorf("create/update classifications = %#v/%#v, want industry %#v", created, updated, tt.want)
			}
		})
	}
}

func TestV1ProjectClassifications(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   any
		present bool
		want    []string
	}{
		{"multi-value", "Energy & Climate;Manufacturing", true, []string{"Energy & Climate", "Manufacturing"}},
		{"whitespace and empty tokens", "  Open Data ; ; Open Hardware ", true, []string{"Open Data", "Open Hardware"}},
		{"v1 None sentinel is clear", "None", true, []string{}},
		{"explicit empty", "", true, []string{}},
		{"null", nil, true, []string{}},
		{"missing", nil, false, nil},
		{"malformed", 42, true, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]any{}
			if tt.present {
				data["industry__c"] = tt.value
			}
			if got := v1ProjectClassifications(data, "industry__c"); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("classifications = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestV2ProjectClassifications(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   any
		present bool
		want    *[]string
	}{
		{"JSON array", []any{"Energy & Climate", "Manufacturing"}, true, slicePtr([]string{"Energy & Climate", "Manufacturing"})},
		{"typed array", []string{"Open Data"}, true, slicePtr([]string{"Open Data"})},
		{"explicit empty", []any{}, true, slicePtr([]string{})},
		{"null unbackfilled field is preserved", nil, true, nil},
		{"missing preserved", nil, false, nil},
		{"malformed list omitted", []any{"Energy", 42}, true, nil},
		{"malformed scalar omitted", "Energy", true, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			data := map[string]any{}
			if tt.present {
				data["industry"] = tt.value
			}
			got := v2ProjectClassifications(data, "industry")
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("classifications = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func slicePtr(values []string) *[]string { return &values }
