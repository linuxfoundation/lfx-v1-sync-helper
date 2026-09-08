// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

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

func TestLoadMinimalConfigDefaults(t *testing.T) {
	t.Setenv("NATS_URL", "")
	t.Setenv("NATS_FETCH_MAX_WAIT", "")

	cfg := LoadMinimalConfig()

	if cfg.NATSURL != defaultNATSURL {
		t.Errorf("NATSURL = %q, want %q", cfg.NATSURL, defaultNATSURL)
	}
	if cfg.NATSFetchMaxWait != defaultNATSFetchMaxWait {
		t.Errorf("NATSFetchMaxWait = %v, want %v", cfg.NATSFetchMaxWait, defaultNATSFetchMaxWait)
	}
}

func TestLoadMinimalConfigEnvOverrides(t *testing.T) {
	t.Setenv("NATS_URL", "nats://custom:4222")
	t.Setenv("NATS_FETCH_MAX_WAIT", "90s")

	cfg := LoadMinimalConfig()

	if cfg.NATSURL != "nats://custom:4222" {
		t.Errorf("NATSURL = %q, want %q", cfg.NATSURL, "nats://custom:4222")
	}
	if cfg.NATSFetchMaxWait != 90*time.Second {
		t.Errorf("NATSFetchMaxWait = %v, want 90s", cfg.NATSFetchMaxWait)
	}
}

func TestLoadMinimalConfigInvalidDurationFallsBack(t *testing.T) {
	t.Setenv("NATS_FETCH_MAX_WAIT", "not-a-duration")

	cfg := LoadMinimalConfig()

	if cfg.NATSFetchMaxWait != defaultNATSFetchMaxWait {
		t.Errorf("NATSFetchMaxWait = %v, want default %v on invalid input", cfg.NATSFetchMaxWait, defaultNATSFetchMaxWait)
	}
}

func TestCommitteeSkipMemberNotificationsConfig(t *testing.T) {
	tests := []struct {
		name   string
		envVal string
		want   bool
	}{
		{"unset defaults to true (skip)", "", true},
		{"explicit true", "true", true},
		{"explicit false", "false", false},
		{"FALSE uppercase", "FALSE", false},
		{"false with whitespace", "  false  ", false},
		{"0 re-enables notifications", "0", false},
		{"no re-enables notifications", "no", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setRequiredEnvs(t)
			os.Unsetenv("COMMITTEE_SKIP_MEMBER_NOTIFICATIONS")
			if tt.envVal != "" {
				t.Setenv("COMMITTEE_SKIP_MEMBER_NOTIFICATIONS", tt.envVal)
			}
			cfg, err := LoadConfig()
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if cfg.CommitteeSkipMemberNotifications != tt.want {
				t.Errorf("CommitteeSkipMemberNotifications = %v, want %v", cfg.CommitteeSkipMemberNotifications, tt.want)
			}
		})
	}
}

// clearV1DBEnv resets every V1_DB_* and DATABASE_URL env var, since
// t.Setenv only sets/restores vars a test explicitly touches and
// buildV1DatabaseDSN/LoadConfig read all of them.
func clearV1DBEnv(t *testing.T) {
	t.Helper()
	for _, v := range []string{
		"DATABASE_URL",
		"V1_DB_HOST",
		"V1_DB_PORT",
		"V1_DB_NAME",
		"V1_DB_USER",
		"V1_DB_PASSWORD",
		"V1_DB_SSLMODE",
	} {
		t.Setenv(v, "")
	}
}

func TestBuildV1DatabaseDSN_NoHost(t *testing.T) {
	clearV1DBEnv(t)

	if dsn := buildV1DatabaseDSN(); dsn != "" {
		t.Errorf("buildV1DatabaseDSN() = %q, want empty string when V1_DB_HOST is unset", dsn)
	}
}

func TestBuildV1DatabaseDSN_Defaults(t *testing.T) {
	clearV1DBEnv(t)
	t.Setenv("V1_DB_HOST", "db.example.com")
	t.Setenv("V1_DB_USER", "svc")

	dsn := buildV1DatabaseDSN()

	want := "host='db.example.com' port='5432' dbname='sfdc' user='svc' sslmode='prefer'"
	if dsn != want {
		t.Errorf("buildV1DatabaseDSN() = %q, want %q", dsn, want)
	}
}

func TestBuildV1DatabaseDSN_AllFieldsOverridden(t *testing.T) {
	clearV1DBEnv(t)
	t.Setenv("V1_DB_HOST", "db.example.com")
	t.Setenv("V1_DB_PORT", "5433")
	t.Setenv("V1_DB_NAME", "otherdb")
	t.Setenv("V1_DB_USER", "svc")
	t.Setenv("V1_DB_PASSWORD", "hunter2")
	t.Setenv("V1_DB_SSLMODE", "require")

	dsn := buildV1DatabaseDSN()

	want := "host='db.example.com' port='5433' dbname='otherdb' user='svc' password='hunter2' sslmode='require'"
	if dsn != want {
		t.Errorf("buildV1DatabaseDSN() = %q, want %q", dsn, want)
	}
}

func TestBuildV1DatabaseDSN_TrimsWhitespaceExceptPassword(t *testing.T) {
	clearV1DBEnv(t)
	t.Setenv("V1_DB_HOST", "  db.example.com  ")
	t.Setenv("V1_DB_USER", "  svc  ")
	// Leading/trailing whitespace in the password is significant and must
	// be preserved verbatim.
	t.Setenv("V1_DB_PASSWORD", "  hunter2  ")

	dsn := buildV1DatabaseDSN()

	want := "host='db.example.com' port='5432' dbname='sfdc' user='svc' password='  hunter2  ' sslmode='prefer'"
	if dsn != want {
		t.Errorf("buildV1DatabaseDSN() = %q, want %q", dsn, want)
	}
}

func TestBuildV1DatabaseDSN_EscapesSpecialCharactersInPassword(t *testing.T) {
	clearV1DBEnv(t)
	t.Setenv("V1_DB_HOST", "db.example.com")
	t.Setenv("V1_DB_USER", "svc")
	t.Setenv("V1_DB_PASSWORD", `p'\ss"word`)

	dsn := buildV1DatabaseDSN()

	want := `host='db.example.com' port='5432' dbname='sfdc' user='svc' password='p\'\\ss"word' sslmode='prefer'`
	if dsn != want {
		t.Errorf("buildV1DatabaseDSN() = %q, want %q", dsn, want)
	}
}

func TestBuildV1DatabaseDSN_BlankPasswordOmitted(t *testing.T) {
	clearV1DBEnv(t)
	t.Setenv("V1_DB_HOST", "db.example.com")
	t.Setenv("V1_DB_USER", "svc")

	dsn := buildV1DatabaseDSN()

	if strings.Contains(dsn, "password=") {
		t.Errorf("buildV1DatabaseDSN() = %q, want no password= keyword when V1_DB_PASSWORD is unset", dsn)
	}
}

func TestLoadConfig_DatabaseURLTakesPrecedenceOverDiscreteVars(t *testing.T) {
	setRequiredEnvs(t)
	clearV1DBEnv(t)
	t.Setenv("DATABASE_URL", "postgres://explicit-dsn")
	t.Setenv("V1_DB_HOST", "db.example.com")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.DatabaseURL != "postgres://explicit-dsn" {
		t.Errorf("DatabaseURL = %q, want DATABASE_URL to take precedence", cfg.DatabaseURL)
	}
}

func TestLoadConfig_AssemblesDatabaseURLFromDiscreteVarsWhenUnset(t *testing.T) {
	setRequiredEnvs(t)
	clearV1DBEnv(t)
	t.Setenv("V1_DB_HOST", "db.example.com")
	t.Setenv("V1_DB_USER", "svc")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	want := "host='db.example.com' port='5432' dbname='sfdc' user='svc' sslmode='prefer'"
	if cfg.DatabaseURL != want {
		t.Errorf("DatabaseURL = %q, want %q", cfg.DatabaseURL, want)
	}
}

func TestLoadConfig_EmptyDatabaseURLWhenNoV1DBHost(t *testing.T) {
	setRequiredEnvs(t)
	clearV1DBEnv(t)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.DatabaseURL != "" {
		t.Errorf("DatabaseURL = %q, want empty when neither DATABASE_URL nor V1_DB_HOST is set", cfg.DatabaseURL)
	}
}

func TestParseIntEnvClamped(t *testing.T) {
	tests := []struct {
		name    string
		envVal  string
		def     int
		minV    int
		maxV    int
		want    int
		wantEnv bool // when true, set env var; when false, unset
	}{
		{name: "unset returns default", def: 8, minV: 1, maxV: 64, want: 8, wantEnv: false},
		{name: "valid within range", envVal: "16", def: 8, minV: 1, maxV: 64, want: 16, wantEnv: true},
		{name: "clamp low", envVal: "-5", def: 8, minV: 1, maxV: 64, want: 1, wantEnv: true},
		{name: "clamp high", envVal: "99999", def: 8, minV: 1, maxV: 64, want: 64, wantEnv: true},
		{name: "invalid falls back to default", envVal: "abc", def: 8, minV: 1, maxV: 64, want: 8, wantEnv: true},
		{name: "empty falls back to default", envVal: "", def: 8, minV: 1, maxV: 64, want: 8, wantEnv: false},
		{name: "whitespace-only falls back to default", envVal: "   ", def: 8, minV: 1, maxV: 64, want: 8, wantEnv: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const name = "TEST_PARSE_INT_ENV_CLAMPED"
			if err := os.Unsetenv(name); err != nil {
				t.Fatalf("Unsetenv: %v", err)
			}
			if tt.wantEnv {
				t.Setenv(name, tt.envVal)
			}
			got := parseIntEnvClamped(name, tt.def, tt.minV, tt.maxV)
			if got != tt.want {
				t.Errorf("parseIntEnvClamped(%q, %d, %d, %d) = %d, want %d", tt.envVal, tt.def, tt.minV, tt.maxV, got, tt.want)
			}
		})
	}
}

func TestResolveV1MappingsDatabaseURL(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		want    string
		wantErr bool
	}{
		{
			name: "explicit V1_MAPPINGS_DATABASE_URL wins",
			cfg:  Config{V1MappingsDatabaseURL: "postgres://u:p@h:5432/d?sslmode=disable", V1MappingsPGHost: "ignored", V1MappingsPGUser: "ignored", V1MappingsPGPassword: "ignored", V1MappingsPGDatabase: "ignored"},
			want: "postgres://u:p@h:5432/d?sslmode=disable",
		},
		{
			name: "compose from V1_MAPPINGS_PG* fields",
			cfg:  Config{V1MappingsPGHost: "pg.local", V1MappingsPGUser: "app", V1MappingsPGPassword: "secret", V1MappingsPGDatabase: "mydb"},
			want: "postgres://app:secret@pg.local:5432/mydb",
		},
		{
			name: "compose with custom port",
			cfg:  Config{V1MappingsPGHost: "pg.local", V1MappingsPGPort: "5433", V1MappingsPGUser: "app", V1MappingsPGPassword: "secret", V1MappingsPGDatabase: "mydb"},
			want: "postgres://app:secret@pg.local:5433/mydb",
		},
		{
			name: "password with special chars is percent-encoded",
			cfg:  Config{V1MappingsPGHost: "pg.local", V1MappingsPGUser: "app", V1MappingsPGPassword: "p@ss:word/#!", V1MappingsPGDatabase: "mydb"},
			// Assembled via concatenation so secretlint's PostgreSQL
			// connection-string heuristic does not flag the literal.
			want: "postgres" + "://app:p%40ss%3Aword%2F%23%21@pg.local:5432/mydb",
		},
		{
			name:    "no V1_MAPPINGS_DATABASE_URL and no V1_MAPPINGS_PG* fields errors",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name:    "missing V1_MAPPINGS_PGPASSWORD errors",
			cfg:     Config{V1MappingsPGHost: "pg.local", V1MappingsPGUser: "app", V1MappingsPGDatabase: "mydb"},
			wantErr: true,
		},
		{
			name: "top-level DatabaseURL (v1 SFDC DSN) is NOT used as v1-mappings DSN",
			cfg:  Config{DatabaseURL: "postgres://sfdc:x@sfdc-host:5432/sfdc", V1MappingsPGHost: "pg.local", V1MappingsPGUser: "app", V1MappingsPGPassword: "secret", V1MappingsPGDatabase: "mappings"},
			want: "postgres://app:secret@pg.local:5432/mappings",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.cfg.ResolveV1MappingsDatabaseURL()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ResolveV1MappingsDatabaseURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ResolveV1MappingsDatabaseURL() = %q, want %q", got, tt.want)
			}
		})
	}
}
