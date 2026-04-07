// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestParseStringListEnv(t *testing.T) {
	tests := []struct {
		name     string
		envValue string
		expected []string
	}{
		{
			name:     "empty env var returns nil",
			envValue: "",
			expected: nil,
		},
		{
			name:     "single value",
			envValue: "tlf",
			expected: []string{"tlf"},
		},
		{
			name:     "multiple comma-separated values",
			envValue: "tlf,lfprojects,jdf",
			expected: []string{"tlf", "lfprojects", "jdf"},
		},
		{
			name:     "values with whitespace are trimmed",
			envValue: " tlf , lfprojects , jdf ",
			expected: []string{"tlf", "lfprojects", "jdf"},
		},
		{
			name:     "values are lowercased",
			envValue: "TLF,LFProjects,JDF",
			expected: []string{"tlf", "lfprojects", "jdf"},
		},
		{
			name:     "empty entries between commas are dropped",
			envValue: "tlf,,jdf",
			expected: []string{"tlf", "jdf"},
		},
		{
			name:     "only whitespace returns nil",
			envValue: "   ",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("TEST_LIST_ENV", tt.envValue)
			got := parseStringListEnv("TEST_LIST_ENV")
			if len(got) != len(tt.expected) {
				t.Errorf("parseStringListEnv() = %v, want %v", got, tt.expected)
				return
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Errorf("parseStringListEnv()[%d] = %q, want %q", i, got[i], tt.expected[i])
				}
			}
		})
	}
}

func TestProjectAllowlistDefault(t *testing.T) {
	// When PROJECT_ALLOWLIST is unset, LoadConfig should use the built-in default.
	// We can test parseStringListEnv returns nil and that defaults are applied.
	t.Setenv("PROJECT_ALLOWLIST", "")

	got := parseStringListEnv("PROJECT_ALLOWLIST")
	if got != nil {
		t.Errorf("expected nil for empty PROJECT_ALLOWLIST, got %v", got)
	}

	// Verify defaults contain expected slugs.
	for _, slug := range []string{"tlf", "lfprojects", "lf-charities", "jdf"} {
		if !slices.Contains(defaultProjectAllowlist, slug) {
			t.Errorf("defaultProjectAllowlist missing expected slug %q", slug)
		}
	}
}

func TestProjectFamilyAllowlistDefault(t *testing.T) {
	// When PROJECT_FAMILY_ALLOWLIST is unset, LoadConfig should use the built-in default.
	t.Setenv("PROJECT_FAMILY_ALLOWLIST", "")

	got := parseStringListEnv("PROJECT_FAMILY_ALLOWLIST")
	if got != nil {
		t.Errorf("expected nil for empty PROJECT_FAMILY_ALLOWLIST, got %v", got)
	}

	// Verify defaults contain expected slugs.
	for _, slug := range []string{"test-project-group", "agentic-ai-foundation"} {
		if !slices.Contains(defaultProjectFamilyAllowlist, slug) {
			t.Errorf("defaultProjectFamilyAllowlist missing expected slug %q", slug)
		}
	}
}

func TestProjectAllowlistOverride(t *testing.T) {
	// When PROJECT_ALLOWLIST is set, it should override the built-in defaults.
	t.Setenv("PROJECT_ALLOWLIST", "my-project,another-project")

	got := parseStringListEnv("PROJECT_ALLOWLIST")
	want := []string{"my-project", "another-project"}

	if len(got) != len(want) {
		t.Fatalf("parseStringListEnv(PROJECT_ALLOWLIST) = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("parseStringListEnv(PROJECT_ALLOWLIST)[%d] = %q, want %q", i, got[i], want[i])
		}
	}

	// Verify default slugs are not present when overridden.
	for _, slug := range defaultProjectAllowlist {
		if slices.Contains(got, slug) {
			t.Errorf("expected default slug %q to be absent when env var is overridden", slug)
		}
	}
}

func TestProjectFamilyAllowlistOverride(t *testing.T) {
	// When PROJECT_FAMILY_ALLOWLIST is set, it should override the built-in defaults.
	t.Setenv("PROJECT_FAMILY_ALLOWLIST", "my-family,other-family")

	got := parseStringListEnv("PROJECT_FAMILY_ALLOWLIST")
	want := []string{"my-family", "other-family"}

	if len(got) != len(want) {
		t.Fatalf("parseStringListEnv(PROJECT_FAMILY_ALLOWLIST) = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("parseStringListEnv(PROJECT_FAMILY_ALLOWLIST)[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func writeAllowlistFile(t *testing.T, content string) string {
	t.Helper()
	f := filepath.Join(t.TempDir(), "allowlist")
	if err := os.WriteFile(f, []byte(content), 0600); err != nil {
		t.Fatalf("writeAllowlistFile: %v", err)
	}
	return f
}

func TestReadYAMLListFile(t *testing.T) {
	tests := []struct {
		name        string
		fileContent string
		wantResult  []string
		wantErr     bool
	}{
		{
			name:        "multiple entries",
			fileContent: "- tlf\n- lfprojects\n- lf-charities\n",
			wantResult:  []string{"tlf", "lfprojects", "lf-charities"},
		},
		{
			name:        "entries are lowercased",
			fileContent: "- TLF\n- LFProjects\n",
			wantResult:  []string{"tlf", "lfprojects"},
		},
		{
			name:        "entries are whitespace-trimmed",
			fileContent: "-  tlf  \n-  lfprojects  \n",
			wantResult:  []string{"tlf", "lfprojects"},
		},
		{
			name:        "path with leading/trailing whitespace is trimmed",
			fileContent: "- tlf\n",
			wantResult:  []string{"tlf"},
		},
		{
			name:        "commented entries are ignored",
			fileContent: "- tlf\n# - lfenergy\n- lfprojects\n# - lfc\n",
			wantResult:  []string{"tlf", "lfprojects"},
		},
		{
			name:        "empty file returns nil",
			fileContent: "",
			wantResult:  nil,
		},
		{
			name:        "invalid yaml returns error",
			fileContent: "- valid\n  bad indent: [unclosed",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := writeAllowlistFile(t, tt.fileContent)

			// Wrap path in whitespace for the whitespace-trim case.
			input := path
			if tt.name == "path with leading/trailing whitespace is trimmed" {
				input = "  " + path + "  "
			}

			got, err := readYAMLListFile(input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("readYAMLListFile() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if !slices.Equal(got, tt.wantResult) {
				t.Errorf("readYAMLListFile() = %v, want %v", got, tt.wantResult)
			}
		})
	}
}

func TestReadYAMLListFileMissing(t *testing.T) {
	_, err := readYAMLListFile("/nonexistent/path/allowlist")
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

// setRequiredEnvs sets the minimum environment variables required for
// LoadConfig to succeed, then restores them via t.Cleanup.
func setRequiredEnvs(t *testing.T) {
	t.Helper()
	t.Setenv("HEIMDALL_PRIVATE_KEY", "test-key")
	t.Setenv("AUTH0_TENANT", "test-tenant")
	t.Setenv("AUTH0_CLIENT_ID", "test-client-id")
	t.Setenv("AUTH0_PRIVATE_KEY", "test-auth0-key")
	t.Setenv("PROJECT_SERVICE_URL", "http://project-service")
	t.Setenv("COMMITTEE_SERVICE_URL", "http://committee-service")
}

func TestLoadConfigAllowlistPrecedence_FileOverridesEnv(t *testing.T) {
	setRequiredEnvs(t)
	// env var value that should be ignored
	t.Setenv("PROJECT_ALLOWLIST", "from-env")
	t.Setenv("PROJECT_FAMILY_ALLOWLIST", "family-from-env")

	allowlistFile := writeAllowlistFile(t, "- from-file\n")
	familyFile := writeAllowlistFile(t, "- family-from-file\n")
	t.Setenv("PROJECT_ALLOWLIST_FILE", allowlistFile)
	t.Setenv("PROJECT_FAMILY_ALLOWLIST_FILE", familyFile)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !slices.Equal(cfg.ProjectAllowlist, []string{"from-file"}) {
		t.Errorf("ProjectAllowlist = %v, want [from-file]", cfg.ProjectAllowlist)
	}
	if !slices.Equal(cfg.ProjectFamilyAllowlist, []string{"family-from-file"}) {
		t.Errorf("ProjectFamilyAllowlist = %v, want [family-from-file]", cfg.ProjectFamilyAllowlist)
	}
}

func TestLoadConfigAllowlistPrecedence_EnvOverridesDefaults(t *testing.T) {
	setRequiredEnvs(t)
	t.Setenv("PROJECT_ALLOWLIST_FILE", "")
	t.Setenv("PROJECT_FAMILY_ALLOWLIST_FILE", "")
	t.Setenv("PROJECT_ALLOWLIST", "env-slug")
	t.Setenv("PROJECT_FAMILY_ALLOWLIST", "env-family-slug")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !slices.Equal(cfg.ProjectAllowlist, []string{"env-slug"}) {
		t.Errorf("ProjectAllowlist = %v, want [env-slug]", cfg.ProjectAllowlist)
	}
	if !slices.Equal(cfg.ProjectFamilyAllowlist, []string{"env-family-slug"}) {
		t.Errorf("ProjectFamilyAllowlist = %v, want [env-family-slug]", cfg.ProjectFamilyAllowlist)
	}
}

func TestLoadConfigAllowlistPrecedence_DefaultsWhenEnvEmpty(t *testing.T) {
	setRequiredEnvs(t)
	t.Setenv("PROJECT_ALLOWLIST_FILE", "")
	t.Setenv("PROJECT_FAMILY_ALLOWLIST_FILE", "")
	t.Setenv("PROJECT_ALLOWLIST", "")
	t.Setenv("PROJECT_FAMILY_ALLOWLIST", "")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !slices.Equal(cfg.ProjectAllowlist, defaultProjectAllowlist) {
		t.Errorf("ProjectAllowlist = %v, want defaults %v", cfg.ProjectAllowlist, defaultProjectAllowlist)
	}
	if !slices.Equal(cfg.ProjectFamilyAllowlist, defaultProjectFamilyAllowlist) {
		t.Errorf("ProjectFamilyAllowlist = %v, want defaults %v", cfg.ProjectFamilyAllowlist, defaultProjectFamilyAllowlist)
	}
}

func TestLoadConfigAllowlistPrecedence_WhitespaceFilePathFallsBack(t *testing.T) {
	setRequiredEnvs(t)
	// Whitespace-only file paths should fall back to env var.
	t.Setenv("PROJECT_ALLOWLIST_FILE", "   ")
	t.Setenv("PROJECT_FAMILY_ALLOWLIST_FILE", "   ")
	t.Setenv("PROJECT_ALLOWLIST", "env-slug")
	t.Setenv("PROJECT_FAMILY_ALLOWLIST", "env-family-slug")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !slices.Equal(cfg.ProjectAllowlist, []string{"env-slug"}) {
		t.Errorf("ProjectAllowlist = %v, want [env-slug]", cfg.ProjectAllowlist)
	}
	if !slices.Equal(cfg.ProjectFamilyAllowlist, []string{"env-family-slug"}) {
		t.Errorf("ProjectFamilyAllowlist = %v, want [env-family-slug]", cfg.ProjectFamilyAllowlist)
	}
}

func TestProjectAllowlistCaseInsensitive(t *testing.T) {
	// Allowlist entries should be stored lowercase regardless of input case.
	t.Setenv("PROJECT_ALLOWLIST", "MyProject,ANOTHER-PROJECT,Mixed-Case")

	got := parseStringListEnv("PROJECT_ALLOWLIST")
	want := []string{"myproject", "another-project", "mixed-case"}

	if len(got) != len(want) {
		t.Fatalf("parseStringListEnv(PROJECT_ALLOWLIST) = %v, want %v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("parseStringListEnv(PROJECT_ALLOWLIST)[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
