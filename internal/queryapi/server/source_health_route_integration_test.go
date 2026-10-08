//go:build integration

package server

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
)

// Free text that must never reach the served field: it sits in every error
// column and in a stage the platform does not name.
const sourceHealthSecret = "token=SECRET-PROBE https://internal.example/hook"

type sourceHealthRow struct {
	Provider    string  `json:"provider"`
	Scope       string  `json:"scope"`
	LastSyncAt  *string `json:"lastSyncAt"`
	LastFailure *struct {
		OccurredAt string `json:"occurredAt"`
		Stage      string `json:"stage"`
	} `json:"lastFailure"`
}

type sourceHealthConfig struct {
	id, org, name, provider string
	active                  bool
	lastSyncAt              string // "" = NULL
	lastSyncSuccess         string // "", "true" or "false"
	lastSyncError           string // "" = NULL
	// run: the newest run of the configuration; runStatus < 0 = no run.
	runStatus int
	runResult string
	runError  string
	// foreignRun attaches a newer failed run of ANOTHER org's job.
	foreignRun bool
}

func seedSourceHealth(t *testing.T, pool *pgxpool.Pool, configs []sourceHealthConfig) {
	t.Helper()
	ctx := t.Context()
	nullable := func(s string) any {
		if s == "" {
			return nil
		}
		return s
	}
	for i, c := range configs {
		n := string(rune('0' + i))
		jobID := "bbbbbbbb-0000-0000-0000-0000000000" + n + n
		runID := "cccccccc-0000-0000-0000-0000000000" + n + n
		if _, err := pool.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, is_active, last_sync_at, last_sync_success, last_sync_error, created_at, updated_at)
			VALUES ($1,$2,$3,$4,('["acme/'||$3||'"]')::json,$5,$6::timestamptz,$7::boolean,$8,now(),now())`,
			c.id, c.org, c.name, c.provider, c.active, nullable(c.lastSyncAt), nullable(c.lastSyncSuccess), nullable(c.lastSyncError)); err != nil {
			t.Fatalf("seed config %s: %v", c.name, err)
		}
		if c.runStatus >= 0 {
			if _, err := pool.Exec(ctx, `INSERT INTO scheduled_jobs (id, org_id, sync_config_id, name, job_type, schedule_cron, status, created_at, updated_at)
				VALUES ($1,$2,$3,'job-'||$4::text,'sync','0 * * * *',0,now(),now())`, jobID, c.org, c.id, c.name); err != nil {
				t.Fatalf("seed job %s: %v", c.name, err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO job_runs (id, job_id, status, started_at, completed_at, result, error, created_at)
				VALUES ($1,$2,$3,'2026-03-01T00:00:00Z','2026-03-01T00:05:00Z',$4::json,$5,'2026-03-01T00:00:00Z')`,
				runID, jobID, c.runStatus, nullable(c.runResult), nullable(c.runError)); err != nil {
				t.Fatalf("seed run %s: %v", c.name, err)
			}
		}
		if c.foreignRun {
			foreignJob := "dddddddd-0000-0000-0000-0000000000" + n + n
			if _, err := pool.Exec(ctx, `INSERT INTO scheduled_jobs (id, org_id, sync_config_id, name, job_type, schedule_cron, status, created_at, updated_at)
				VALUES ($1,'org-other',$2,'foreign-job','sync','0 * * * *',0,now(),now())`, foreignJob, c.id); err != nil {
				t.Fatalf("seed foreign job %s: %v", c.name, err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO job_runs (id, job_id, status, started_at, completed_at, result, error, created_at)
				VALUES ($1,$2,3,'2027-01-01T00:00:00Z','2027-01-01T00:05:00Z','{"error_category":"timeout"}',$3,'2027-01-01T00:00:00Z')`,
				"eeeeeeee-0000-0000-0000-0000000000"+n+n, foreignJob, sourceHealthSecret); err != nil {
				t.Fatalf("seed foreign run %s: %v", c.name, err)
			}
		}
	}
}

// sourceHealthFixture holds one config per state and per failure clause for
// org-1, and one config for org-other that org-1 must never see.
func sourceHealthFixture() []sourceHealthConfig {
	id := func(n string) string { return "aaaaaaaa-0000-0000-0000-0000000000" + n + n }
	return []sourceHealthConfig{
		// never synced: no time, no run, no failure. Also carries a foreign
		// failed run on a job of another org: it must not supply a failure.
		{id: id("0"), org: "org-1", name: "never", provider: "github", active: true, runStatus: -1, foreignRun: true},
		// stale: an old success, no failure.
		{id: id("1"), org: "org-1", name: "stale", provider: "gitlab", active: true, lastSyncAt: "2025-01-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 2, runResult: `{"rows":1}`},
		// failed, every error column holds free text; the category is named.
		{id: id("2"), org: "org-1", name: "failed", provider: "jira", active: true, lastSyncAt: "2026-03-01T00:05:00Z", lastSyncSuccess: "false", lastSyncError: sourceHealthSecret,
			runStatus: 3, runResult: `{"stage":"` + sourceHealthSecret + `","error_category":"provider_rate_limited"}`, runError: sourceHealthSecret},
		// each remaining clause of "the latest sync failed", alone:
		{id: id("3"), org: "org-1", name: "clause-sync-error", provider: "linear", active: true, lastSyncError: sourceHealthSecret, runStatus: -1},
		{id: id("4"), org: "org-1", name: "clause-flag", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "false", runStatus: -1},
		{id: id("5"), org: "org-1", name: "clause-run-failed", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 3, runResult: `{"stage":"` + sourceHealthSecret + `"}`},
		{id: id("6"), org: "org-1", name: "clause-run-cancelled", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 4, runResult: `{"error_category":"cancelled"}`},
		{id: id("7"), org: "org-1", name: "clause-run-error", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 2, runError: sourceHealthSecret},
		// not served: inactive, and another org's.
		{id: id("8"), org: "org-1", name: "inactive", provider: "github", active: false, lastSyncError: sourceHealthSecret, runStatus: -1},
		{id: id("9"), org: "org-other", name: "foreign", provider: "bitbucket", active: true, lastSyncError: sourceHealthSecret, runStatus: -1},
	}
}

const sourceHealthQuery = `query SourceHealth($orgId: String!) { sourceHealth(orgId: $orgId) { provider scope lastSyncAt lastFailure { occurredAt stage } } }`

func decodeSourceHealth(t *testing.T, body string) []sourceHealthRow {
	t.Helper()
	var envelope struct {
		Data struct {
			SourceHealth []sourceHealthRow `json:"sourceHealth"`
		} `json:"data"`
		Errors []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal([]byte(body), &envelope); err != nil || len(envelope.Errors) > 0 {
		t.Fatalf("decode: err=%v body=%s", err, body)
	}
	return envelope.Data.SourceHealth
}

// assertSourceHealthServed checks the state the field exists to reach: every
// state is distinct, and nothing of another org or of any free text is there.
func assertSourceHealthServed(t *testing.T, body string) {
	t.Helper()
	if strings.Contains(body, "SECRET-PROBE") || strings.Contains(body, "internal.example") {
		t.Fatalf("free text reached the field: %s", body)
	}
	rows := decodeSourceHealth(t, body)
	byScope := map[string]sourceHealthRow{}
	for _, r := range rows {
		byScope[r.Scope] = r
	}
	if len(rows) != 8 {
		t.Fatalf("rows = %d, want the 8 active configs of org-1 only: %s", len(rows), body)
	}
	for _, foreign := range []string{"acme/foreign", "acme/inactive", "bitbucket"} {
		if strings.Contains(body, foreign) {
			t.Fatalf("a row that is not org-1's active config is served (%s): %s", foreign, body)
		}
	}

	never := byScope["acme/never"]
	if never.LastSyncAt != nil || never.LastFailure != nil || never.Provider != "github" {
		t.Fatalf("never synced must have no time and no failure: %+v", never)
	}
	stale := byScope["acme/stale"]
	if stale.LastSyncAt == nil || !strings.HasPrefix(*stale.LastSyncAt, "2025-01-01T00:00:00") || stale.LastFailure != nil {
		t.Fatalf("stale must carry its old success time and no failure: %+v", stale)
	}
	failed := byScope["acme/failed"]
	if failed.LastSyncAt != nil {
		t.Fatalf("a failed sync is not a last successful sync: %+v", failed)
	}
	if failed.LastFailure == nil || failed.LastFailure.Stage != "provider_rate_limited" || !strings.HasPrefix(failed.LastFailure.OccurredAt, "2026-03-01T00:05:00") {
		t.Fatalf("failed must carry the run time and the named category: %+v", failed)
	}

	wantStage := map[string]string{
		"acme/clause-sync-error":    SourceHealthStageOtherForTest,
		"acme/clause-flag":          SourceHealthStageOtherForTest,
		"acme/clause-run-failed":    SourceHealthStageOtherForTest,
		"acme/clause-run-cancelled": "cancelled",
		"acme/clause-run-error":     SourceHealthStageOtherForTest,
	}
	for scope, stage := range wantStage {
		row := byScope[scope]
		if row.LastFailure == nil || row.LastFailure.Stage != stage {
			t.Fatalf("%s: lastFailure = %+v, want a failure with stage %q", scope, row.LastFailure, stage)
		}
	}
}

// SourceHealthStageOtherForTest mirrors datahealth.SourceHealthStageOther; the
// test pins the served word, not the Go constant.
const SourceHealthStageOtherForTest = "other"

func TestSourceHealthRoute_ServedToAViewerOverTheSignedEnvelope(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	seedSourceHealth(t, pool, sourceHealthFixture())
	handler, priv := sourceHealthHandler(t, pool)
	valid := time.Now().Add(time.Hour)

	post := func(role, org, asked string) *httpResult {
		token := signDataHealthEnvelope(t, priv, principal.Claims{OrgID: org, Role: role}, valid)
		rec := postGraphQLWithVariables(t, handler, registeredSourceHealthDocument, token, map[string]any{"orgId": asked})
		return &httpResult{code: rec.Code, body: rec.Body.String()}
	}

	// Every member role is served, the operator-only gate is not applied.
	for _, role := range []string{"", "viewer", "member", "operator", "admin"} {
		res := post(role, "org-1", "org-1")
		if res.code != http.StatusOK {
			t.Fatalf("role %q: code=%d body=%s", role, res.code, res.body)
		}
		assertSourceHealthServed(t, res.body)
	}

	// org-other sees its own config and nothing of org-1.
	own := post("viewer", "org-other", "org-other")
	if !strings.Contains(own.body, "bitbucket") || strings.Contains(own.body, "github") || strings.Contains(own.body, "jira") {
		t.Fatalf("org-other must see only its own source: %s", own.body)
	}

	// An orgId that is not the caller's org is refused before any read.
	cross := post("viewer", "org-1", "org-other")
	if !strings.Contains(cross.body, "Access denied") || strings.Contains(cross.body, "bitbucket") {
		t.Fatalf("cross-org ask must be refused: %s", cross.body)
	}
	empty := post("viewer", "", "org-1")
	if strings.Contains(empty.body, "github") {
		t.Fatalf("an envelope with no org must be refused: %s", empty.body)
	}
}

type httpResult struct {
	code int
	body string
}

func TestSourceHealthRoute_NoDataHealthFieldIsReachableThroughIt(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	seedSourceHealth(t, pool, sourceHealthFixture())
	handler, priv := sourceHealthHandler(t, pool)
	ch := &countingMCPClient{}
	listener := internalidentity.MCP(newMCPHandlerWithLimits(ch, pool, allMCPRootsEnabled(), getenvFunc(func(string) string { return "" }), mcpDefaultLimits()))
	for _, selection := range []string{
		`sourceHealth(orgId: "org-1") { message }`,
		`sourceHealth(orgId: "org-1") { rowsIngested }`,
		`sourceHealth(orgId: "org-1") { lastFailure { message } }`,
		`sourceHealth(orgId: "org-1") { connectors { provider } }`,
		`sourceHealth(orgId: "org-1") { identityMapping { totalIdentities } }`,
		`dataHealth(team: "ALL") { connectors { provider } }`,
	} {
		rec := mcpDo(listener, http.MethodPost, mcpHeaders("org-1", "viewer", "false", "false"), mcpBody(t, "{ "+selection+" }", nil))
		body := rec.Body.String()
		if !strings.Contains(body, "errors") || strings.Contains(body, "SECRET-PROBE") || strings.Contains(body, "github") {
			t.Fatalf("%s must be refused: %d %s", selection, rec.Code, body)
		}
	}

	// The operator-only operation is refused as it was.
	rec := postGraphQLWithVariables(t, handler, registeredConnectorsDataHealthDocument,
		signDataHealthEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "viewer"}, time.Now().Add(time.Hour)), map[string]any{"teamId": "ALL"})
	if !strings.Contains(rec.Body.String(), "Data health requires operator access") || strings.Contains(rec.Body.String(), "github") {
		t.Fatalf("connectorsDataHealth must stay operator-only: %s", rec.Body.String())
	}
}

func TestSourceHealthRoute_FailedReadFailsLoudly(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	handler, priv := sourceHealthHandler(t, pool)
	if _, err := pool.Exec(t.Context(), `DROP TABLE job_runs CASCADE`); err != nil {
		t.Fatal(err)
	}
	token := signDataHealthEnvelope(t, priv, principal.Claims{OrgID: "org-1", Role: "viewer"}, time.Now().Add(time.Hour))
	rec := postGraphQLWithVariables(t, handler, registeredSourceHealthDocument, token, map[string]any{"orgId": "org-1"})
	body := rec.Body.String()
	if !strings.Contains(body, `"errors"`) || strings.Contains(body, `"sourceHealth":[]`) {
		t.Fatalf("an unreadable source must be an error, never an empty list: %s", body)
	}
	if strings.Contains(body, "job_runs") {
		t.Fatalf("the error must not carry the database error text: %s", body)
	}
}

func sourceHealthHandler(t *testing.T, pool *pgxpool.Pool) (http.HandlerFunc, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, _, _, _ := newQueryHandler(emptyCHClient{}, pool, verifier, itTestSchemaDigest, os.Getenv)
	return handler, priv
}

// The member-level field over the MCP listener: acr sends the least role, and
// an operator claim is still refused there, for this root too.
func TestSourceHealthRoute_ServedOverMCPWithoutAnOperatorClaim(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	seedSourceHealth(t, pool, sourceHealthFixture())
	ch := &countingMCPClient{}
	listener := internalidentity.MCP(newMCPHandlerWithLimits(ch, pool, allMCPRootsEnabled(), getenvFunc(func(string) string { return "" }), mcpDefaultLimits()))
	body := mcpBody(t, sourceHealthQuery, map[string]any{"orgId": "org-1"})

	rec := mcpDo(listener, http.MethodPost, mcpHeaders("org-1", "viewer", "false", "false"), body)
	if rec.Code != http.StatusOK {
		t.Fatalf("viewer over MCP: %d %s", rec.Code, rec.Body.String())
	}
	assertSourceHealthServed(t, rec.Body.String())

	rec = mcpDo(listener, http.MethodPost, mcpHeaders("org-1", "viewer", "false", "false"), mcpBody(t, sourceHealthQuery, map[string]any{"orgId": "org-other"}))
	if reason, _ := mcpReason(t, rec); reason != mcpReasonOrgMismatch || strings.Contains(rec.Body.String(), "bitbucket") {
		t.Fatalf("a different orgId over MCP must be refused as an org mismatch: %d %s", rec.Code, rec.Body.String())
	}

	for _, role := range []string{"admin", "owner", "operator"} {
		rec = mcpDo(listener, http.MethodPost, mcpHeaders("org-1", role, "false", "false"), body)
		if reason, _ := mcpReason(t, rec); rec.Code != http.StatusForbidden || reason != mcpReasonElevatedClaim {
			t.Fatalf("role %s over MCP must stay refused as elevated_claim: %d %s", role, rec.Code, rec.Body.String())
		}
	}
	if n := ch.calls.Load(); n != 0 {
		t.Fatalf("the source-health read must not touch ClickHouse, calls=%d", n)
	}
}
