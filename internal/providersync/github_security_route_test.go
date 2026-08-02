package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type gitHubSecurityDoer struct{ requests []string }

func (doer *gitHubSecurityDoer) Do(request *http.Request) (*http.Response, error) {
	doer.requests = append(doer.requests, request.URL.Path)
	body := gitHubRepositoryFixture
	switch request.URL.Path {
	case "/repos/acme/api/dependabot/alerts":
		body = `[{"number":1,"state":"open","html_url":"https://example.invalid/a","created_at":"2026-07-22T10:00:00Z","security_advisory":{"severity":"high","cve_id":"CVE-2026-0001","summary":"dependency issue","description":"dependency description"},"dependency":{"package":{"name":"widget"}}}]`
	case "/repos/acme/api/code-scanning/alerts":
		body = `[{"number":2,"state":"open","html_url":"https://example.invalid/b","created_at":"2026-07-22T11:00:00Z","dismissed_at":"2026-07-22T12:00:00Z","rule":{"severity":"critical","description":"rule description"},"most_recent_instance":{"message":{"text":"instance message"}}}]`
	case "/repos/acme/api/security-advisories":
		body = `[{"ghsa_id":"GHSA-demo","state":"published","severity":"medium","cve_id":"CVE-2026-0002","html_url":"https://example.invalid/c","summary":"advisory summary","description":"advisory description","created_at":"2026-07-22T12:00:00Z"}]`
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func TestGitHubSecurityRouteEmitsEachPythonSource(t *testing.T) {
	t.Parallel()
	// Given
	doer := &gitHubSecurityDoer{}
	claim := nativeTestClaim("github", "security")
	client := gitHubRepositoryClient(t, doer, "https://api.github.com")

	// When
	batch, err := (GitHubSecurityRouteHandler{}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))

	// Then
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "security_alerts" || len(batch.Effects[0].Rows) != 3 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	rows := make([]securityAlertRow, 0, 3)
	for _, raw := range batch.Effects[0].Rows {
		var row securityAlertRow
		if err := json.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	byID := make(map[string]securityAlertRow, len(rows))
	for _, row := range rows {
		byID[row.AlertID] = row
	}
	dependabot := byID["dependabot:1"]
	if dependabot.OrgID != claim.OrgID || dependabot.PackageName == nil || *dependabot.PackageName != "widget" {
		t.Fatalf("dependabot=%+v", dependabot)
	}
	codeScanning := byID["code_scanning:2"]
	if codeScanning.FixedAt != nil || codeScanning.DismissedAt == nil {
		t.Fatalf("code scanning=%+v", codeScanning)
	}
	advisory := byID["advisory:GHSA-demo"]
	if advisory.State == nil || *advisory.State != "published" {
		t.Fatalf("advisory=%+v", advisory)
	}
}

func TestSecurityAlertValidationRejectsCrossTenantRow(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "security")
	row := securityAlertRow{OrgID: "other-org", RepoID: "c7198fbc-1945-3717-05d8-eb78866b4e79", AlertID: "dependabot:1", Source: "dependabot", CreatedAt: time.Now().UTC(), LastSynced: time.Now().UTC()}
	if err := row.validate(claim); err == nil {
		t.Fatal("cross-tenant row passed validation")
	}
}
