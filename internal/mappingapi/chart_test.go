// Copyright The Linux Foundation and each contributor to LFX.
// SPDX-License-Identifier: MIT

package mappingapi

import (
	"bytes"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Render the real chart, not a copied authorization policy. Set HELM_BIN when
// Helm is installed outside PATH. These tests require no Kubernetes cluster.
func renderChart(t *testing.T, settings ...string) ([]byte, error) {
	t.Helper()
	return renderChartPath(t, filepath.Join("..", "..", "charts", "lfx-v1-sync-helper"), settings...)
}

func renderChartPath(t *testing.T, chart string, settings ...string) ([]byte, error) {
	t.Helper()
	helm := os.Getenv("HELM_BIN")
	if helm == "" {
		var err error
		helm, err = exec.LookPath("helm")
		if err != nil {
			t.Skip("Helm not installed; set HELM_BIN to run chart policy tests")
		}
	}
	args := []string{"template", "fixture", chart, "--namespace", "fixture-sync", "--set", "app.image.tag=development", "--set", "mappingApi.image.tag=development"}
	for _, setting := range settings {
		args = append(args, "--set", setting)
	}
	return exec.Command(helm, args...).CombinedOutput()
}

func TestChartMappingAPIImageTagConvention(t *testing.T) {
	chart := t.TempDir()
	if err := os.CopyFS(chart, os.DirFS(filepath.Join("..", "..", "charts", "lfx-v1-sync-helper"))); err != nil {
		t.Fatal(err)
	}
	metadataPath := filepath.Join(chart, "Chart.yaml")
	metadata, err := os.ReadFile(metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	// Published charts carry appVersion; source-chart installs must supply
	// mappingApi.image.tag, independently of the sync helper's app.image.tag.
	if err := os.WriteFile(metadataPath, append(metadata, []byte("\nappVersion: fixture-chart-version\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, path, tag, want string }{
		{"local explicit tag", filepath.Join("..", "..", "charts", "lfx-v1-sync-helper"), "fixture-api-tag", "fixture-api-tag"},
		{"packaged appVersion fallback", chart, "", "fixture-chart-version"},
		{"explicit tag overrides appVersion", chart, "fixture-api-tag", "fixture-api-tag"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output, err := renderChartPath(t, tc.path, "mappingApi.enabled=true", "mappingApi.accessTeamId=fixture-callers", "mappingApi.gateway.hostname=lfx-api.example.test", "mappingApi.image.tag="+tc.tag)
			if err != nil {
				t.Fatalf("render: %v: %s", err, output)
			}
			if !bytes.Contains(output, []byte("image: \"ghcr.io/linuxfoundation/lfx-v1-sync-helper/v1-mapping-api:"+tc.want+"\"")) {
				t.Fatalf("expected API image tag %s: %s", tc.want, output)
			}
		})
	}
}

func TestChartMappingAPIDisabledByDefault(t *testing.T) {
	output, err := renderChart(t)
	if err != nil {
		t.Fatalf("render: %v: %s", err, output)
	}
	if bytes.Contains(output, []byte("component: mapping-api")) || bytes.Contains(output, []byte("kind: RuleSet")) || bytes.Contains(output, []byte("/v1-sync-helper/mappings")) {
		t.Fatal("mapping API should not be installed by default")
	}
}

func TestChartMappingResourcesUseDedicatedTemplates(t *testing.T) {
	output, err := renderChart(t, "mappingApi.enabled=true", "mappingApi.accessTeamId=fixture-callers", "mappingApi.gateway.hostname=lfx-api.example.test")
	if err != nil {
		t.Fatalf("render: %v: %s", err, output)
	}
	want := map[string]string{
		"Deployment": "mapping-api-deployment.yaml",
		"Service":    "mapping-api-service.yaml",
		"HTTPRoute":  "httproute.yaml",
		"Middleware": "heimdall-middleware.yaml",
		"RuleSet":    "ruleset.yaml",
	}
	for _, document := range bytes.Split(output, []byte("\n---\n")) {
		var resource struct {
			Kind     string `yaml:"kind"`
			Metadata struct {
				Name string `yaml:"name"`
			} `yaml:"metadata"`
		}
		if err := yaml.Unmarshal(document, &resource); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(resource.Metadata.Name, "mapping-api") {
			continue
		}
		filename, ok := want[resource.Kind]
		if !ok {
			t.Fatalf("unexpected or duplicate mapping resource: %s", resource.Kind)
		}
		source := "# Source: lfx-v1-sync-helper/templates/" + filename
		if !bytes.Contains(document, []byte(source+"\n")) {
			t.Fatalf("%s must be rendered from %s", resource.Kind, filename)
		}
		delete(want, resource.Kind)
	}
	if len(want) != 0 {
		t.Fatalf("missing mapping resource templates: %v", want)
	}
}

func TestChartMappingAPIDefaultAccessTeam(t *testing.T) {
	output, err := renderChart(t, "mappingApi.enabled=true", "mappingApi.gateway.hostname=lfx-api.example.test")
	if err != nil {
		t.Fatalf("render: %v: %s", err, output)
	}
	if !bytes.Contains(output, []byte(`object: "team:v1-maping-callers"`)) {
		t.Fatal("RuleSet must use the configured default access team")
	}
}

func TestChartRequiresExplicitAuthorizationConfiguration(t *testing.T) {
	for _, team := range []string{"", " ", "team:fixture", "fixture*", "fixture/other"} {
		t.Run(team, func(t *testing.T) {
			output, err := renderChart(t, "mappingApi.enabled=true", "mappingApi.accessTeamId="+team, "mappingApi.gateway.hostname=lfx-api.example.test")
			if err == nil {
				t.Fatalf("unsafe team ID accepted: %s", output)
			}
			if !bytes.Contains(output, []byte("mappingApi.accessTeamId")) {
				t.Fatalf("unexpected failure: %s", output)
			}
		})
	}
	if output, err := renderChart(t, "mappingApi.enabled=true", "mappingApi.accessTeamId=fixture-callers"); err == nil || !bytes.Contains(output, []byte("mappingApi.gateway.hostname")) {
		t.Fatalf("expected hostname validation: %v %s", err, output)
	}
}

func TestChartMandatoryTeamRuleAndForwardAuth(t *testing.T) {
	output, err := renderChart(t, "mappingApi.enabled=true", "mappingApi.accessTeamId=fixture-callers", "mappingApi.gateway.hostname=lfx-api.example.test", "openfga.enabled=false", "heimdall.enabled=false")
	if err != nil {
		t.Fatalf("render: %v: %s", err, output)
	}
	documents := map[string]map[string]any{}
	decoder := yaml.NewDecoder(bytes.NewReader(output))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal(err)
		}
		kind, _ := doc["kind"].(string)
		metadata, _ := doc["metadata"].(map[string]any)
		name, _ := metadata["name"].(string)
		if strings.Contains(name, "mapping-api") {
			documents[kind] = doc
			if metadata["namespace"] != "fixture-sync" {
				t.Fatalf("%s in incorrect namespace", kind)
			}
		}
	}
	for _, kind := range []string{"Deployment", "Service", "HTTPRoute", "Middleware", "RuleSet"} {
		if documents[kind] == nil {
			t.Fatalf("missing %s", kind)
		}
	}
	rules := documents["RuleSet"]["spec"].(map[string]any)["rules"].([]any)
	if len(rules) != 1 {
		t.Fatalf("unexpected rules: %+v", rules)
	}
	rule := rules[0].(map[string]any)
	match := rule["match"].(map[string]any)
	methods := match["methods"].([]any)
	if len(methods) != 1 || methods[0] != "GET" {
		t.Fatal("lookup rule must only allow GET")
	}
	routes := match["routes"].([]any)
	if len(routes) != 1 || routes[0].(map[string]any)["path"] != "/v1-sync-helper/mappings/:resource_type/:uid" {
		t.Fatal("RuleSet must protect the sync-helper mappings endpoint")
	}
	steps := rule["execute"].([]any)
	if len(steps) != 3 {
		t.Fatalf("unexpected policy steps: %+v", steps)
	}
	if steps[0].(map[string]any)["authenticator"] != "oidc" {
		t.Fatal("OIDC authentication must precede authorization")
	}
	check := steps[1].(map[string]any)
	if check["authorizer"] != "openfga_check" {
		t.Fatal("team authorization is mandatory even with unrelated auth flags disabled")
	}
	values := check["config"].(map[string]any)["values"].(map[string]any)
	if values["relation"] != "member" || values["object"] != "team:fixture-callers" {
		t.Fatalf("incorrect membership check: %+v", values)
	}
	finalizer := steps[2].(map[string]any)
	if finalizer["finalizer"] != "create_jwt" || finalizer["config"].(map[string]any)["values"].(map[string]any)["aud"] != "lfx-v1-mapping-api" {
		t.Fatal("API-audience JWT must only be issued after membership authorization")
	}
	route := documents["HTTPRoute"]["spec"].(map[string]any)["rules"].([]any)[0].(map[string]any)
	routeMatches := route["matches"].([]any)
	if len(routeMatches) != 1 {
		t.Fatal("expected only the mappings route prefix")
	}
	path := routeMatches[0].(map[string]any)["path"].(map[string]any)
	if path["type"] != "PathPrefix" || path["value"] != "/v1-sync-helper/mappings/" {
		t.Fatal("HTTPRoute must expose the sync-helper mappings prefix")
	}
	filter := route["filters"].([]any)[0].(map[string]any)["extensionRef"].(map[string]any)
	if filter["name"] != "lfx-v1-sync-helper-mapping-api-heimdall" {
		t.Fatal("HTTPRoute must use local ForwardAuth middleware")
	}
	forwardAuth := documents["Middleware"]["spec"].(map[string]any)["forwardAuth"].(map[string]any)
	headers := forwardAuth["authResponseHeaders"].([]any)
	if len(headers) != 1 || headers[0] != "Authorization" {
		t.Fatal("authorized JWT must replace external bearer token")
	}
	routeYAML, err := yaml.Marshal(route)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(routeYAML, []byte("/livez")) || bytes.Contains(routeYAML, []byte("/readyz")) {
		t.Fatal("probes must not be externally exposed")
	}
	ruleYAML, err := yaml.Marshal(rule)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(ruleYAML, []byte("anonymous_authenticator")) || bytes.Contains(ruleYAML, []byte("allow_all")) {
		t.Fatal("authorization fallback is forbidden")
	}
}
