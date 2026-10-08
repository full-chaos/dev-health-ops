//go:build integration

package server

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"

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
	// targets is the raw sync_targets JSON ("" = []); stats the raw
	// last_sync_stats JSON ("" = NULL); integration, when set, is an
	// integration the config belongs to (created in the seed), createdAt its
	// creation time.
	targets, stats, integration, createdAt, parentID string
	lastSyncAt                                       string // "" = NULL
	lastSyncSuccess                                  string // "", "true" or "false"
	lastSyncError                                    string // "" = NULL
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
	seenIntegration := map[string]bool{}
	for i, c := range configs {
		n := fmt.Sprintf("%02d", i)
		jobID := "bbbbbbbb-0000-0000-0000-00000000" + n + n
		runID := "cccccccc-0000-0000-0000-00000000" + n + n
		if c.integration != "" && !seenIntegration[c.integration] {
			seenIntegration[c.integration] = true
			pgseed.Integration(ctx, t, pool, c.integration, c.org, c.provider)
		}
		targets := c.targets
		if targets == "" {
			targets = "[]"
		}
		created := c.createdAt
		if created == "" {
			created = "2026-01-01T00:00:00Z"
		}
		if _, err := pool.Exec(ctx, `INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, is_active, last_sync_at, last_sync_success, last_sync_error, last_sync_stats, integration_id, created_at, updated_at, parent_id)
			VALUES ($1,$2,$3,$4,$5::json,$6,$7::timestamptz,$8::boolean,$9,$10::json,$11::uuid,$12::timestamptz,$12::timestamptz,$13::uuid)`,
			c.id, c.org, c.name, c.provider, targets, c.active, nullable(c.lastSyncAt), nullable(c.lastSyncSuccess), nullable(c.lastSyncError), nullable(c.stats), nullable(c.integration), created, nullable(c.parentID)); err != nil {
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
			foreignJob := "dddddddd-0000-0000-0000-00000000" + n + n
			if _, err := pool.Exec(ctx, `INSERT INTO scheduled_jobs (id, org_id, sync_config_id, name, job_type, schedule_cron, status, created_at, updated_at)
				VALUES ($1,'org-other',$2,'foreign-job','sync','0 * * * *',0,now(),now())`, foreignJob, c.id); err != nil {
				t.Fatalf("seed foreign job %s: %v", c.name, err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO job_runs (id, job_id, status, started_at, completed_at, result, error, created_at)
				VALUES ($1,$2,3,'2027-01-01T00:00:00Z','2027-01-01T00:05:00Z','{"error_category":"timeout"}',$3,'2027-01-01T00:00:00Z')`,
				"eeeeeeee-0000-0000-0000-00000000"+n+n, foreignJob, sourceHealthSecret); err != nil {
				t.Fatalf("seed foreign run %s: %v", c.name, err)
			}
		}
	}
}

// sourceHealthFixture holds one config per state and per failure clause for
// org-1, and configs that must not be served. name is the key the test reads a
// row by (the field serves no name); rows come back ordered by provider, then id.
func sourceHealthFixture() []sourceHealthConfig {
	id := func(n int) string { return fmt.Sprintf("aaaaaaaa-0000-0000-0000-0000000000%02d", n) }
	integ := func(n int) string { return fmt.Sprintf("99999999-0000-0000-0000-0000000000%02d", n) }
	return []sourceHealthConfig{
		// never synced: no time, no run, no failure. Also carries a foreign
		// failed run on a job of another org: it must not supply a failure.
		{id: id(0), org: "org-1", name: "never", provider: "github", active: true, targets: `["git"]`, runStatus: -1, foreignRun: true},
		// stale: an old success, no failure.
		{id: id(1), org: "org-1", name: "stale", provider: "gitlab", active: true, targets: `["prs","git"]`, lastSyncAt: "2025-01-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 2, runResult: `{"rows":1}`},
		// failed: every error column holds free text; the category is named; a
		// target holds free text.
		{id: id(2), org: "org-1", name: "failed", provider: "jira", active: true, targets: `["work-items","acme/secret-repo"]`, lastSyncAt: "2026-03-01T00:05:00Z", lastSyncSuccess: "false", lastSyncError: sourceHealthSecret,
			runStatus: 3, runResult: `{"stage":"` + sourceHealthSecret + `","error_category":"provider_rate_limited"}`, runError: sourceHealthSecret},
		// each remaining clause of "the latest sync failed", alone:
		{id: id(3), org: "org-1", name: "clause-sync-error", provider: "linear", active: true, targets: `["acme/secret-repo"]`, lastSyncError: sourceHealthSecret, runStatus: -1},
		{id: id(4), org: "org-1", name: "clause-flag", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "false", runStatus: -1},
		{id: id(5), org: "org-1", name: "clause-run-failed", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 3, runResult: `{"stage":"` + sourceHealthSecret + `"}`},
		{id: id(6), org: "org-1", name: "clause-run-cancelled", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 4, runResult: `{"error_category":"cancelled"}`},
		{id: id(7), org: "org-1", name: "clause-run-error", provider: "linear", active: true, lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: 2, runError: sourceHealthSecret},
		// deactivated by a failure: listed, with its failure and the stage of its stats.
		{id: id(8), org: "org-1", name: "deactivated-by-failure", provider: "pagerduty", active: false, targets: `["operational"]`, lastSyncAt: "2026-03-02T00:00:00Z", lastSyncSuccess: "false", lastSyncError: sourceHealthSecret,
			stats: `{"error_category":"pagerduty_sync_disabled"}`, runStatus: -1},
		// deactivated, each failure signal alone: listed.
		{id: id(9), org: "org-1", name: "inactive-flag-only", provider: "zendesk", active: false, lastSyncAt: "2026-03-02T00:00:00Z", lastSyncSuccess: "false", runStatus: -1},
		{id: id(13), org: "org-1", name: "inactive-error-only", provider: "datadog", active: false, lastSyncError: sourceHealthSecret, runStatus: -1},
		// configs of one integration: the canonical one (oldest top-level) is
		// listed as never synced. A child config older than it does not take
		// its place. A non-canonical config with no stamp and no run is not
		// listed (it would read "never synced" when the integration synced);
		// each own signal alone (time, flag, error, run) lists one.
		{id: id(10), org: "org-1", name: "canonical", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2026-01-01T00:00:00Z", runStatus: -1},
		{id: id(14), org: "org-1", name: "older-child", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2025-12-01T00:00:00Z", parentID: id(10), runStatus: -1},
		{id: id(11), org: "org-1", name: "non-canonical-silent", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2026-01-02T00:00:00Z", runStatus: -1},
		{id: id(12), org: "org-1", name: "non-canonical-stamped", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2026-01-03T00:00:00Z", lastSyncAt: "2026-02-03T00:00:00Z", lastSyncSuccess: "true", runStatus: -1},
		{id: id(15), org: "org-1", name: "nc-time-only", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2026-01-04T00:00:00Z", lastSyncAt: "2026-02-04T00:00:00Z", runStatus: -1},
		{id: id(16), org: "org-1", name: "nc-flag-only", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2026-01-05T00:00:00Z", lastSyncSuccess: "true", runStatus: -1},
		{id: id(17), org: "org-1", name: "nc-error-only", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2026-01-06T00:00:00Z", lastSyncError: sourceHealthSecret, runStatus: -1},
		{id: id(18), org: "org-1", name: "nc-run-only", provider: "bitbucket", active: true, integration: integ(1), createdAt: "2026-01-07T00:00:00Z", runStatus: 2, runResult: `{"rows":1}`},
		// an integration with an active config is never absent. B: the canonical
		// config is inactive and healthy, the others are active and silent: the
		// oldest active one is listed (scope "git"), not the newer one.
		{id: id(30), org: "org-1", name: "b-canonical-inactive", provider: "gitlab", active: false, integration: integ(2), createdAt: "2026-01-01T00:00:00Z", lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: -1},
		{id: id(31), org: "org-1", name: "b-silent-oldest", provider: "gitlab", active: true, integration: integ(2), createdAt: "2026-01-02T00:00:00Z", targets: `["git"]`, runStatus: -1},
		{id: id(32), org: "org-1", name: "b-silent-newer", provider: "gitlab", active: true, integration: integ(2), createdAt: "2026-01-03T00:00:00Z", targets: `["prs"]`, runStatus: -1},
		// C: an inactive parent and an active child only: the child is listed.
		{id: id(33), org: "org-1", name: "c-parent-inactive", provider: "github", active: false, integration: integ(3), createdAt: "2026-01-01T00:00:00Z", lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: -1},
		{id: id(34), org: "org-1", name: "c-child-active", provider: "github", active: true, integration: integ(3), createdAt: "2026-01-02T00:00:00Z", parentID: id(33), targets: `["git"]`, runStatus: -1},
		// D: an active top-level config is listed before an older active child.
		{id: id(35), org: "org-1", name: "d-canonical-inactive", provider: "gitlab", active: false, integration: integ(4), createdAt: "2026-01-01T00:00:00Z", lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: -1},
		{id: id(36), org: "org-1", name: "d-child-active", provider: "gitlab", active: true, integration: integ(4), createdAt: "2026-01-10T00:00:00Z", parentID: id(35), targets: `["git"]`, runStatus: -1},
		{id: id(37), org: "org-1", name: "d-top-active", provider: "gitlab", active: true, integration: integ(4), createdAt: "2026-01-20T00:00:00Z", targets: `["prs"]`, runStatus: -1},
		// E: the integration has a listed config (stamped, non-canonical), so an
		// older silent active config of it is not added by the fallback.
		{id: id(38), org: "org-1", name: "e-canonical-inactive", provider: "gitlab", active: false, integration: integ(5), createdAt: "2026-01-01T00:00:00Z", lastSyncAt: "2026-02-01T00:00:00Z", lastSyncSuccess: "true", runStatus: -1},
		{id: id(39), org: "org-1", name: "e-silent-older", provider: "gitlab", active: true, integration: integ(5), createdAt: "2026-01-02T00:00:00Z", runStatus: -1},
		{id: id(40), org: "org-1", name: "e-stamped", provider: "gitlab", active: true, integration: integ(5), createdAt: "2026-01-03T00:00:00Z", lastSyncAt: "2026-02-05T00:00:00Z", lastSyncSuccess: "true", runStatus: -1},
		// not served: inactive with no failure, and another org's.
		{id: id(20), org: "org-1", name: "inactive", provider: "github", active: false, runStatus: -1},
		{id: id(21), org: "org-other", name: "foreign", provider: "opsgenie", active: true, lastSyncError: sourceHealthSecret, runStatus: -1},
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
	for _, leak := range []string{"SECRET-PROBE", "internal.example", "secret-repo", "acme/"} {
		if strings.Contains(body, leak) {
			t.Fatalf("free text %q reached the field: %s", leak, body)
		}
	}
	rows := decodeSourceHealth(t, body)

	// The rows of org-1 that are listed, in the order the field answers: provider, then id.
	listed := []string{"never", "canonical", "non-canonical-stamped", "nc-time-only", "nc-flag-only", "nc-error-only", "nc-run-only", "stale", "failed",
		"clause-sync-error", "clause-flag", "clause-run-failed", "clause-run-cancelled", "clause-run-error", "deactivated-by-failure",
		"inactive-flag-only", "inactive-error-only", "d-top-active", "b-silent-oldest", "c-child-active", "e-stamped"}
	providerOf, idOf := map[string]string{}, map[string]string{}
	for _, c := range sourceHealthFixture() {
		providerOf[c.name], idOf[c.name] = c.provider, c.id
	}
	sort.SliceStable(listed, func(i, j int) bool {
		if providerOf[listed[i]] != providerOf[listed[j]] {
			return providerOf[listed[i]] < providerOf[listed[j]]
		}
		return idOf[listed[i]] < idOf[listed[j]]
	})
	if len(rows) != len(listed) {
		t.Fatalf("rows = %d, want the %d listed configs of org-1 only: %s", len(rows), len(listed), body)
	}
	byName := map[string]sourceHealthRow{}
	for i, name := range listed {
		if rows[i].Provider != providerOf[name] {
			t.Fatalf("row %d is provider %q, want %q (%s): %s", i, rows[i].Provider, providerOf[name], name, body)
		}
		byName[name] = rows[i]
	}
	for _, foreign := range []string{"opsgenie"} {
		if strings.Contains(body, foreign) {
			t.Fatalf("a row that is not org-1's is served (%s): %s", foreign, body)
		}
	}
	// "inactive" (github, no failure) and "non-canonical-silent" (bitbucket) are not listed:
	// github appears for "never" only, bitbucket for "canonical" and "non-canonical-stamped" only.
	counts := map[string]int{}
	for _, r := range rows {
		counts[r.Provider]++
	}
	if counts["github"] != 2 || counts["bitbucket"] != 6 {
		t.Fatalf("github=%d bitbucket=%d, want 2 and 6 (an inactive config without failure, a silent non-canonical config and an older child are not listed): %s", counts["github"], counts["bitbucket"], body)
	}

	for name, scope := range map[string]string{"b-silent-oldest": "git", "c-child-active": "git", "d-top-active": "prs"} {
		if got := byName[name].Scope; got != scope || byName[name].LastFailure != nil || byName[name].LastSyncAt != nil {
			t.Fatalf("%s: row %+v, want the one listed config of its integration (scope %q, never synced)", name, byName[name], scope)
		}
	}
	if e := byName["e-stamped"]; e.LastSyncAt == nil || !strings.HasPrefix(*e.LastSyncAt, "2026-02-05T00:00:00") {
		t.Fatalf("e-stamped: %+v", e)
	}
	never := byName["never"]
	if never.LastSyncAt != nil || never.LastFailure != nil || never.Scope != "git" {
		t.Fatalf("never synced must have no time and no failure: %+v", never)
	}
	canonical := byName["canonical"]
	if canonical.LastSyncAt != nil || canonical.LastFailure != nil || canonical.Scope != "all" {
		t.Fatalf("the canonical config of an integration that has not synced reads never synced: %+v", canonical)
	}
	if st := byName["non-canonical-stamped"]; st.LastSyncAt == nil || !strings.HasPrefix(*st.LastSyncAt, "2026-02-03T00:00:00") || st.LastFailure != nil {
		t.Fatalf("a non-canonical config with its own stamp speaks for itself: %+v", st)
	}
	stale := byName["stale"]
	if stale.LastSyncAt == nil || !strings.HasPrefix(*stale.LastSyncAt, "2025-01-01T00:00:00") || stale.LastFailure != nil || stale.Scope != "git, prs" {
		t.Fatalf("stale must carry its old success time and no failure: %+v", stale)
	}
	failed := byName["failed"]
	if failed.LastSyncAt != nil || failed.Scope != "work-items" {
		t.Fatalf("a failed sync is not a last successful sync, and free-text targets are dropped from the scope: %+v", failed)
	}
	if failed.LastFailure == nil || failed.LastFailure.Stage != "provider_rate_limited" || !strings.HasPrefix(failed.LastFailure.OccurredAt, "2026-03-01T00:05:00") {
		t.Fatalf("failed must carry the run time and the named category: %+v", failed)
	}
	if byName["clause-sync-error"].Scope != "other" {
		t.Fatalf("a scope of free text only reads other: %+v", byName["clause-sync-error"])
	}

	wantStage := map[string]string{
		"clause-sync-error":      "other",
		"clause-flag":            "other",
		"clause-run-failed":      "other",
		"clause-run-cancelled":   "cancelled",
		"clause-run-error":       "other",
		"deactivated-by-failure": "pagerduty_sync_disabled",
		"inactive-flag-only":     "other",
		"inactive-error-only":    "other",
		"nc-error-only":          "other",
	}
	for name, stage := range wantStage {
		row := byName[name]
		if row.LastFailure == nil || row.LastFailure.Stage != stage {
			t.Fatalf("%s: lastFailure = %+v, want a failure with stage %q", name, row.LastFailure, stage)
		}
	}
	if byName["deactivated-by-failure"].LastSyncAt != nil {
		t.Fatalf("a source deactivated by a failure shows no last successful sync: %+v", byName["deactivated-by-failure"])
	}
}

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
	if !strings.Contains(own.body, "opsgenie") || strings.Contains(own.body, "github") || strings.Contains(own.body, "jira") {
		t.Fatalf("org-other must see only its own source: %s", own.body)
	}

	// An orgId that is not the caller's org is refused before any read.
	cross := post("viewer", "org-1", "org-other")
	if !strings.Contains(cross.body, "Access denied") || strings.Contains(cross.body, "opsgenie") {
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
	if reason, _ := mcpReason(t, rec); reason != mcpReasonOrgMismatch || strings.Contains(rec.Body.String(), "opsgenie") {
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
