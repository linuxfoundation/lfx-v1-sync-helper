// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

// validate-v1-v2 scans ITX DynamoDB tables and checks that each record is
// present and matches the corresponding document in LFX V2 OpenSearch.
//
// Usage:
//
//	go run ./scripts/validate-v1-v2 --service meetings
//	go run ./scripts/validate-v1-v2 --table itx-poll --limit 5 --output json -v
//
// See scripts/validate-v1-v2/README.md for full documentation.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strings"
)

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func isUUID(s string) bool { return uuidRE.MatchString(s) }

const (
	defaultOSURL   = "http://localhost:9200"
	osIndex        = "resources"
	maxSampleDiffs = 10
)

// TableReport holds validation results for a single DynamoDB table.
type TableReport struct {
	Table         string       `json:"table"`
	Skipped       bool         `json:"skipped,omitempty"`
	SkipReason    string       `json:"skip_reason,omitempty"`
	Scanned       int          `json:"scanned"`
	Processed     int          `json:"processed"`
	MissingInOS   int          `json:"missing_in_os"`
	NotLatest     int          `json:"not_latest"`
	FieldMismatch int          `json:"field_mismatch"`
	Clean         int          `json:"clean"`
	SampleMissing []MissingRecord `json:"sample_missing,omitempty"`
	SampleDiffs   []RecordDiff   `json:"sample_diffs,omitempty"`
}

// MissingRecord holds the DynamoDB item for a record absent in OpenSearch.
type MissingRecord struct {
	ObjectID string         `json:"object_id"`
	Item     map[string]any `json:"item"`
}

// RecordDiff holds the diff for a single record.
type RecordDiff struct {
	ObjectID     string         `json:"object_id"`
	OnlyInDynamo map[string]any `json:"only_in_dynamo,omitempty"`
	OnlyInOS     map[string]any `json:"only_in_os,omitempty"`
	ValueDiffs   map[string]ValuePair `json:"value_diffs,omitempty"`
}

// Report is the full output structure.
type Report struct {
	Tables []TableReport `json:"tables"`
	Total  Summary       `json:"total"`
}

// Summary aggregates counts across all tables.
type Summary struct {
	Scanned       int `json:"scanned"`
	Processed     int `json:"processed"`
	MissingInOS   int `json:"missing_in_os"`
	NotLatest     int `json:"not_latest"`
	FieldMismatch int `json:"field_mismatch"`
	Clean         int `json:"clean"`
}

func main() {
	tableFlag := flag.String("table", "", "Comma-separated DynamoDB table name(s)")
	serviceFlag := flag.String("service", "", "Service group: meetings, mailing-lists, surveys, voting")
	projectFlag := flag.String("project", "", "Filter records by project UID")
	projectsFileFlag := flag.String("projects-file", "", "Path to project mapping file (v2uid=v1sfid per line)")
	limitFlag := flag.Int("limit", 0, "Max records to scan per table (0 = no limit)")
	outputFlag := flag.String("output", "text", "Output format: text or json")
	verboseFlag := flag.Bool("v", false, "Print per-record diffs")
	flag.BoolVar(verboseFlag, "verbose", false, "Print per-record diffs")
	flag.Parse()

	if *tableFlag == "" && *serviceFlag == "" {
		fmt.Fprintln(os.Stderr, "error: at least one of --table or --service is required")
		flag.Usage()
		os.Exit(1)
	}
	if *projectsFileFlag == "" {
		fmt.Fprintln(os.Stderr, "error: --projects-file is required (e.g. scripts/validate-v1-v2/projects-dev.txt)")
		os.Exit(1)
	}
	if err := loadProjectMapping(*projectsFileFlag); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	if *serviceFlag != "" && !validServices[*serviceFlag] {
		fmt.Fprintf(os.Stderr, "error: unknown service %q; valid options: meetings, mailing-lists, surveys, voting\n", *serviceFlag)
		os.Exit(1)
	}
	if *outputFlag != "text" && *outputFlag != "json" {
		fmt.Fprintf(os.Stderr, "error: --output must be text or json\n")
		os.Exit(1)
	}

	// Resolve the list of tables to process.
	tables := resolveTables(*tableFlag, *serviceFlag)

	osURL := envOr("OPENSEARCH_URL", defaultOSURL)

	ctx := context.Background()

	dynamo, err := NewDynamoClient(ctx)
	if err != nil {
		slog.Error("failed to create DynamoDB client", "error", err)
		os.Exit(1)
	}

	osClient, err := NewOSClient(osURL, osIndex)
	if err != nil {
		slog.Error("failed to create OpenSearch client", "error", err)
		os.Exit(1)
	}

	for _, tc := range tables {
		if tc.UseCommitteeProjectMapping {
			if err := loadAllowedSurveyIDs(ctx, dynamo); err != nil {
				slog.Error("failed to load committee-project mapping", "error", err)
				os.Exit(1)
			}
			break
		}
	}
	for _, tc := range tables {
		if tc.MeetingIDAttribute != "" {
			if err := loadAllowedMeetingIDs(ctx, dynamo); err != nil {
				slog.Error("failed to load allowed meeting IDs", "error", err)
				os.Exit(1)
			}
			break
		}
	}

	report := Report{}

	for _, tc := range tables {
		tr := processTable(ctx, tc, dynamo, osClient, *projectFlag, *limitFlag, *verboseFlag)
		report.Tables = append(report.Tables, tr)
		report.Total.Scanned += tr.Scanned
		report.Total.Processed += tr.Processed
		report.Total.MissingInOS += tr.MissingInOS
		report.Total.NotLatest += tr.NotLatest
		report.Total.FieldMismatch += tr.FieldMismatch
		report.Total.Clean += tr.Clean
	}

	if *outputFlag == "json" {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			slog.Error("failed to encode JSON report", "error", err)
			os.Exit(1)
		}
		return
	}

	printTextReport(report, *verboseFlag)
}

func processTable(
	ctx context.Context,
	tc TableConfig,
	dynamo *DynamoClient,
	osClient *OSClient,
	projectUID string,
	limit int,
	verbose bool,
) TableReport {
	tr := TableReport{Table: tc.Table}

	if tc.Skip {
		tr.Skipped = true
		tr.SkipReason = tc.SkipReason
		fmt.Printf("  SKIP  %s — %s\n", tc.Table, tc.SkipReason)
		return tr
	}

	fmt.Printf("  SCAN  %s (object_type=%s)\n", tc.Table, tc.V1ObjectType)

	// DynamoDB stores v1 SFIDs, so translate the v2 UID to its v1 SFID for the scan filter.
	scanProjectID := projectV2ToV1[projectUID]

	processed := 0
	scanned, err := dynamo.ScanTable(ctx, tc.Table, tc.ProjectAttribute, scanProjectID, limit,
		func(item map[string]any) error {
			if tc.ProjectAttribute != "" {
				if projVal, ok := item[tc.ProjectAttribute]; ok {
					if projID, ok := projVal.(string); ok && !allowedV1ProjectIDs[projID] {
						return nil
					}
				}
			}
			if tc.UseCommitteeProjectMapping {
				id, _ := item[tc.V1IDAttribute].(string)
				if !allowedSurveyIDs[id] {
					return nil
				}
			}
			if tc.MeetingIDAttribute != "" {
				meetingID, _ := item[tc.MeetingIDAttribute].(string)
				if !allowedMeetingIDs[meetingID] {
					return nil
				}
			}

			for _, rf := range tc.RequiredV1Fields {
				v, ok := item[rf]
				if !ok || isZeroValue(v) {
					return nil
				}
			}

			if tc.IDMustBeUUID {
				keyAttrs := tc.CompositeIDAttrs
				if len(keyAttrs) == 0 {
					keyAttrs = []string{tc.V1IDAttribute}
				}
				skip := false
				for _, attr := range keyAttrs {
					if v, ok := item[attr]; !ok || !isUUID(fmt.Sprintf("%v", v)) {
						skip = true
						break
					}
				}
				if skip {
					return nil
				}
			}

			processed++

			objectID, err := ObjectID(tc.V1ObjectType, tc.V1IDAttribute, tc.CompositeIDAttrs, item)
			if err != nil {
				slog.Warn("skipping item: could not derive object ID", "table", tc.Table, "error", err)
				return nil
			}

			doc, err := osClient.GetByID(ctx, objectID)
			if err != nil {
				slog.Warn("opensearch lookup failed", "id", objectID, "error", err)
				return nil
			}

			if !doc.Found {
				tr.MissingInOS++
				if len(tr.SampleMissing) < maxSampleDiffs {
					tr.SampleMissing = append(tr.SampleMissing, MissingRecord{
						ObjectID: objectID,
						Item:     item,
					})
				}
				if verbose {
					fmt.Printf("    MISSING %s\n", objectID)
				}
				return nil
			}

			if !doc.Latest {
				tr.NotLatest++
				if verbose {
					fmt.Printf("    NOT_LATEST %s\n", objectID)
				}
				return nil
			}

			diff := DiffMaps(item, doc.V1Data, diffOpts{stripAuth0Prefix: tc.StripAuth0Prefix})
			deleteMatchingKeys(diff.OnlyInOS, tc.IgnoreV2Fields)
			deleteMatchingKeys(diff.ValueDiffs, tc.IgnoreV2Fields)
			deleteMatchingKeys(diff.OnlyInDynamo, tc.IgnoreV1Fields)
			deleteMatchingKeys(diff.ValueDiffs, tc.IgnoreV1Fields)
			if diff.IsClean() {
				tr.Clean++
				return nil
			}

			tr.FieldMismatch++
			if verbose || len(tr.SampleDiffs) < maxSampleDiffs {
				rd := RecordDiff{
					ObjectID:     objectID,
					OnlyInDynamo: diff.OnlyInDynamo,
					OnlyInOS:     diff.OnlyInOS,
					ValueDiffs:   diff.ValueDiffs,
				}
				tr.SampleDiffs = append(tr.SampleDiffs, rd)
				if verbose {
					printRecordDiff(rd)
				}
			}
			return nil
		},
	)

	tr.Scanned = scanned
	tr.Processed = processed
	if err != nil {
		slog.Error("scan error", "table", tc.Table, "error", err)
	}

	fmt.Printf("        scanned=%d processed=%d missing=%d not_latest=%d mismatch=%d clean=%d\n",
		tr.Scanned, tr.Processed, tr.MissingInOS, tr.NotLatest, tr.FieldMismatch, tr.Clean)
	return tr
}

func resolveTables(tableFlag, serviceFlag string) []TableConfig {
	seen := map[string]bool{}
	var out []TableConfig

	add := func(tc TableConfig) {
		if !seen[tc.Table] {
			seen[tc.Table] = true
			out = append(out, tc)
		}
	}

	if tableFlag != "" {
		for _, name := range strings.Split(tableFlag, ",") {
			name = strings.TrimSpace(name)
			if name == "" {
				continue
			}
			tc, ok := tableByName(name)
			if !ok {
				// Unknown table — create a minimal config so the user sees an error.
				tc = TableConfig{
					Table:      name,
					Skip:       true,
					SkipReason: "table not found in known table list",
				}
			}
			add(tc)
		}
	}

	if serviceFlag != "" {
		for _, tc := range tablesForService(serviceFlag) {
			add(tc)
		}
	}

	return out
}

func printTextReport(r Report, verbose bool) {
	fmt.Println()
	fmt.Println("────────────────────────── SUMMARY ──────────────────────────")
	fmt.Printf("  Total scanned   : %d\n", r.Total.Scanned)
	fmt.Printf("  Total processed : %d\n", r.Total.Processed)
	fmt.Printf("  Missing in OS   : %d\n", r.Total.MissingInOS)
	fmt.Printf("  Not latest      : %d\n", r.Total.NotLatest)
	fmt.Printf("  Field mismatch  : %d\n", r.Total.FieldMismatch)
	fmt.Printf("  Clean           : %d\n", r.Total.Clean)
	fmt.Println()

	if !verbose {
		for _, tr := range r.Tables {
			if len(tr.SampleMissing) > 0 {
				fmt.Printf("  Sample missing for %s (first %d):\n", tr.Table, maxSampleDiffs)
				for _, mr := range tr.SampleMissing {
					fmt.Printf("    %s\n", mr.ObjectID)
				}
				fmt.Println()
			}
		}
		for _, tr := range r.Tables {
			if len(tr.SampleDiffs) == 0 {
				continue
			}
			fmt.Printf("  Sample diffs for %s (first %d):\n", tr.Table, maxSampleDiffs)
			for _, rd := range tr.SampleDiffs {
				printRecordDiff(rd)
			}
		}
	}
}

func printRecordDiff(rd RecordDiff) {
	fmt.Printf("    [%s]\n", rd.ObjectID)
	for k, v := range rd.OnlyInDynamo {
		fmt.Printf("      + dynamo only  %s = %v\n", k, v)
	}
	for k, v := range rd.OnlyInOS {
		fmt.Printf("      - os only      %s = %v\n", k, v)
	}
	for k, vp := range rd.ValueDiffs {
		fmt.Printf("      ~ changed      %s: dynamo=%v  os=%v\n", k, vp.DynamoDB, vp.OS)
	}
}

func envOr(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}
