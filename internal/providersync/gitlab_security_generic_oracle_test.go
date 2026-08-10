package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var oracleGitLabSecurityNormalizedAt = time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)

var oracleGitLabSecurityGoOnlyFields = map[string]string{
	"org_id":      "stamped from the Go claim for tenant-scoped persistence",
	"repo_id":     "provided by the Go route after repository identity resolution",
	"last_synced": "stamped from normalizedAt by the Go route",
}

func decodeGitLabSecurityOracleMap(t *testing.T, value any) map[string]any {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func buildGitLabSecurityVulnerabilityRowForOracle(t *testing.T, input map[string]any) gitLabSecurityAlertRow {
	t.Helper()
	claim := nativeTestClaim("gitlab", "security")
	row, include, err := normalizeGitLabSecurityAlert(
		claim, input["repo_id"].(string), "gitlab_vulnerability",
		decodeGitLabSecurityOracleMap(t, input["raw_alert"]), oracleGitLabSecurityNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !include {
		t.Fatal("vulnerability oracle item did not produce a row")
	}
	return row
}

func buildGitLabSecurityDependencyRowForOracle(t *testing.T, input map[string]any) gitLabSecurityAlertRow {
	t.Helper()
	claim := nativeTestClaim("gitlab", "security")
	row, include, err := normalizeGitLabSecurityAlert(
		claim, input["repo_id"].(string), "gitlab_dependency",
		func() map[string]any {
			payload := decodeGitLabSecurityOracleMap(t, input["raw_alert"])
			payload["package_name"] = input["package_name"]
			return payload
		}(), oracleGitLabSecurityNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !include {
		t.Fatal("dependency oracle item did not produce a row")
	}
	return row
}

func oracleGitLabSecurityVulnerabilityCases() []oracleCase {
	return []oracleCase{
		{
			ID: "full_mapping",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"raw_alert": map[string]any{
					"id": 101, "severity": "high", "state": "detected", "name": "SQL Injection",
					"created_at":  "2026-01-15T10:30:00.000Z",
					"identifiers": []any{map[string]any{"type": "other", "name": "ignored"}, map[string]any{"type": "cve", "name": "CVE-2026-1234"}},
					"links":       map[string]any{"url": "https://gitlab.example/vuln/101"},
				},
			},
		},
		{
			ID: "optional_fields_missing",
			Input: map[string]any{
				"repo_id":   "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"raw_alert": map[string]any{"id": 202, "severity": "low", "state": "resolved", "name": "XSS", "created_at": "2026-01-16T10:30:00Z"},
			},
		},
	}
}

func oracleGitLabSecurityDependencyCases() []oracleCase {
	return []oracleCase{{
		ID: "dependency_mapping",
		Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "package_name": "lodash",
			"raw_alert": map[string]any{"id": 303, "severity": "critical", "url": "https://gitlab.example/vuln/303", "name": "Prototype Pollution"},
		},
	}}
}

func TestGenericOracleMatchesLivePythonForGitLabSecurityVulnerabilityRows(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/security/vulnerability", oracleGitLabSecurityVulnerabilityCases(),
		buildGitLabSecurityVulnerabilityRowForOracle, oracleGitLabSecurityGoOnlyFields,
	)
}

func TestGenericOracleMatchesLivePythonForGitLabSecurityDependencyRows(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/security/dependency", oracleGitLabSecurityDependencyCases(),
		buildGitLabSecurityDependencyRowForOracle, oracleGitLabSecurityGoOnlyFields,
	)
}

type gitLabSecurityTraversalTraceRow struct {
	AlertID string `json:"alert_id"`
	Source  string `json:"source"`
}

type gitLabSecurityTraversalTrace struct {
	ProducerRequests  []string                          `json:"producer_requests"`
	UsageRequestCount int                               `json:"usage_request_count"`
	Rows              []gitLabSecurityTraversalTraceRow `json:"rows"`
}

func TestGenericOracleMatchesLivePythonForGitLabSecurityTraversal(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/security/trace", oracleGitLabSecurityTraversalCases(),
		buildGitLabSecurityTraversalTrace, nil,
	)
}

func oracleGitLabSecurityTraversalCases() []oracleCase {
	return []oracleCase{
		{
			ID: "single_page_window",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "max_alerts": 1000,
				"since": "2026-07-22T00:00:00Z", "until": "2026-07-23T12:00:00Z",
				"findings": []any{
					map[string]any{"id": 1, "created_at": "2026-07-21T10:00:00Z", "name": "old"},
					map[string]any{"id": 2, "created_at": "2026-07-22T10:00:00Z", "name": "in"},
					map[string]any{"id": 3, "created_at": "2026-07-23T13:00:00Z", "name": "after"},
				},
				"dependencies": []any{map[string]any{"name": "pkg", "vulnerabilities": []any{map[string]any{"id": 4, "name": "dependency"}}}},
				"response_headers": map[string]any{
					"findings": map[string]any{"X-Next-Page": "2"}, "dependencies": map[string]any{"X-Next-Page": "2"},
				},
			},
		},
		{
			ID: "plain_forbidden_optional_endpoint",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "max_alerts": 1000,
				"since": "2026-07-01T00:00:00Z", "until": "2026-12-31T00:00:00Z",
				"findings_status": http.StatusForbidden, "findings": []any{},
				"dependencies": []any{map[string]any{"name": "pkg", "vulnerabilities": []any{map[string]any{"id": 5, "name": "dependency"}}}},
			},
		},
		{
			ID: "core_failure_stops_second_endpoint",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "max_alerts": 1000,
				"since": "2026-07-01T00:00:00Z", "until": "2026-12-31T00:00:00Z",
				"findings_status": http.StatusBadRequest,
				"dependencies":    []any{map[string]any{"name": "pkg", "vulnerabilities": []any{map[string]any{"id": 6, "name": "not fetched"}}}},
			},
		},
	}
}

func buildGitLabSecurityTraversalTrace(t *testing.T, input map[string]any) gitLabSecurityTraversalTrace {
	t.Helper()
	responses := map[string]gitLabSecurityHTTPResponse{
		"/api/v4/projects/123": {body: `{"id":123,"name":"api","path_with_namespace":"acme/api"}`},
	}
	for _, endpoint := range []struct {
		path string
		key  string
	}{
		{path: "/api/v4/projects/123/vulnerability_findings", key: "findings"},
		{path: "/api/v4/projects/123/dependencies", key: "dependencies"},
	} {
		body, err := json.Marshal(input[endpoint.key])
		if err != nil {
			t.Fatal(err)
		}
		status := http.StatusOK
		if value, ok := input[endpoint.key+"_status"].(int); ok {
			status = value
		}
		headers := http.Header{}
		if configured, ok := input["response_headers"].(map[string]any); ok {
			if endpointHeaders, ok := configured[endpoint.key].(map[string]any); ok {
				for key, value := range endpointHeaders {
					headers.Set(key, stringValue(value))
				}
			}
		}
		responses[endpoint.path] = gitLabSecurityHTTPResponse{status: status, headers: headers, body: string(body)}
	}
	doer := &gitLabSecurityRouteDoer{responses: responses}
	claim := nativeTestClaim("gitlab", "security")
	claim.SinceAt = oracleGitLabSecurityTraceTime(t, input, "since")
	claim.BeforeAt = oracleGitLabSecurityTraceTime(t, input, "until")
	maxAlerts, ok := input["max_alerts"].(int)
	if !ok {
		t.Fatalf("max_alerts=%T", input["max_alerts"])
	}
	batch, err := (GitLabSecurityRouteHandler{MaxAlerts: maxAlerts}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabSecurityRouteClient(t, doer), oracleGitLabSecurityNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	trace := gitLabSecurityTraversalTrace{
		ProducerRequests:  make([]string, 0, len(doer.requests)),
		UsageRequestCount: batch.Evidence.Requests,
		Rows:              make([]gitLabSecurityTraversalTraceRow, 0),
	}
	for _, request := range doer.requests {
		trace.ProducerRequests = append(trace.ProducerRequests, oracleGitLabSecurityRequestPath(request))
	}
	if len(batch.Effects) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	for _, raw := range batch.Effects[0].Rows {
		var row gitLabSecurityAlertRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		trace.Rows = append(trace.Rows, gitLabSecurityTraversalTraceRow{AlertID: row.AlertID, Source: row.Source})
	}
	return trace
}

func oracleGitLabSecurityTraceTime(t *testing.T, input map[string]any, key string) *time.Time {
	t.Helper()
	value, ok := input[key].(string)
	if !ok || value == "" {
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return &parsed
}

func oracleGitLabSecurityRequestPath(request *http.Request) string {
	query := request.URL.Query()
	encoded := url.Values{}
	keys := make([]string, 0, len(query))
	for key := range query {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		values := append([]string(nil), query[key]...)
		sort.Strings(values)
		for _, value := range values {
			encoded.Add(key, value)
		}
	}
	if encoded.Encode() == "" {
		return request.URL.Path
	}
	return request.URL.Path + "?" + encoded.Encode()
}
