package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleSecurityGoOnlyFields = map[string]string{
	"org_id":      "stamped from the Go claim for tenant-scoped persistence",
	"repo_id":     "provided by the Go route after repository identity resolution",
	"last_synced": "stamped from normalizedAt by the Go route",
}

var oracleSecurityNormalizedAt = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func buildSecurityRowForOracle(t *testing.T, input map[string]any) securityAlertRow {
	t.Helper()
	encoded, err := json.Marshal(input["raw_alert"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var item gitHubSecurityPayload
	if err := decoder.Decode(&item); err != nil {
		t.Fatal(err)
	}
	row, ok := normalizeGitHubSecurityAlert(nativeTestClaim("github", "security"), input["repo_id"].(string), input["source"].(string), item, oracleSecurityNormalizedAt)
	if !ok {
		t.Fatal("security oracle item did not produce a row")
	}
	return row
}

func oracleSecurityCases() []oracleCase {
	return []oracleCase{
		{ID: "dependabot", Input: map[string]any{"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "source": "dependabot", "raw_alert": map[string]any{"number": 1, "state": "open", "html_url": "https://example.invalid/a", "created_at": "2026-07-22T10:00:00Z", "security_advisory": map[string]any{"severity": "high", "cve_id": "CVE-2026-0001", "summary": "summary", "description": "description"}, "dependency": map[string]any{"package": map[string]any{"name": "widget"}}}}},
		{ID: "code-scanning", Input: map[string]any{"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "source": "code_scanning", "raw_alert": map[string]any{"number": 2, "state": "dismissed", "html_url": "https://example.invalid/b", "created_at": "2026-07-22T11:00:00Z", "dismissed_at": "2026-07-22T12:00:00Z", "rule": map[string]any{"severity": "critical", "description": "rule"}, "most_recent_instance": map[string]any{"message": map[string]any{"text": "message"}}}}},
		{ID: "advisory", Input: map[string]any{"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "source": "advisory", "raw_alert": map[string]any{"ghsa_id": "GHSA-demo", "state": "published", "severity": "medium", "cve_id": "CVE-2026-0002", "html_url": "https://example.invalid/c", "summary": "summary", "description": "description", "created_at": "2026-07-22T12:00:00Z"}}},
	}
}

func TestGenericOracleMatchesLivePythonForSecurityRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "github/security/row", oracleSecurityCases(), buildSecurityRowForOracle, oracleSecurityGoOnlyFields)
}
