//go:build integration

package syncadmin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// integrationOrgLog is a goroutine-safe log sink: the handler logs on the server's goroutine.
type integrationOrgLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *integrationOrgLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *integrationOrgLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// integrationOrgRequests counts every request the Jira discovery client sends, to any host, and answers none.
type integrationOrgRequests struct{ count atomic.Int64 }

func (r *integrationOrgRequests) RoundTrip(*http.Request) (*http.Response, error) {
	r.count.Add(1)
	return nil, errors.New("no provider call is expected in this test")
}

// integrationOrgDecryptor opens any stored credential to a synthetic Jira credential, so a discovery that resolved a
// stored credential would send a request (counted above).
type integrationOrgDecryptor struct{}

func (integrationOrgDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"email":"bot@example.invalid","api_token":"synthetic-token","base_url":"https://jira.example.invalid"}`), nil
}

// startIntegrationOrgVenue is startCascadeVenue with the update path's Jira discovery wired (a decryptor and a counting
// HTTP client) and its log captured.
func startIntegrationOrgVenue(t *testing.T) (*cascadeVenue, *integrationOrgLog, *integrationOrgRequests) {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	org := uuid.NewString()
	var feature string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = $1`, canonicalIncidentFeatureKey).Scan(&feature); err != nil {
		t.Fatal(err)
	}
	pgseed.Org(ctx, t, pool, org, "enterprise")
	pgseed.OrgOverride(ctx, t, pool, org, feature, true)
	logs := &integrationOrgLog{}
	requests := &integrationOrgRequests{}
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Pool: pool, Guard: testGuard(t), Logger: slog.New(slog.NewJSONHandler(logs, nil)),
		Decryptor: integrationOrgDecryptor{}, JiraHTTP: &http.Client{Transport: requests}}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	materializer, err := schedsync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	return &cascadeVenue{t: t, pool: pool, org: org, server: server, materializer: materializer}, logs, requests
}

// otherOrgIntegration is an integration of another org (with a stored credential and a planner-managed configuration
// of its own, both of that org), and one configuration of the venue's org pointed at it: the data anomaly no API path
// makes, seeded directly.
type otherOrgIntegration struct {
	config, integration, otherConfig uuid.UUID
}

func (v *cascadeVenue) seedOtherOrgIntegration(provider, stored string) otherOrgIntegration {
	v.t.Helper()
	ctx := context.Background()
	one := v.seed(provider, stored, nil)
	otherOrg := uuid.NewString()
	pgseed.Org(ctx, v.t, v.pool, otherOrg, "enterprise")
	credential, integration, otherConfig := uuid.New(), uuid.New(), uuid.New()
	v.exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, $3, 'default', true, 'synthetic-ciphertext', '{"base_url": "https://jira.example.invalid"}'::json, now(), now())`,
		credential, otherOrg, provider)
	v.exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, 'other org integration', '{"other_org_option": "other"}'::json, true, now(), now())`,
		integration, otherOrg, provider, credential)
	v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, source_id, created_at, updated_at) VALUES ($1, $2, 'other org config', $3, $4::json, '{"other_org_option": "other"}'::json, true, true, $5, NULL, now(), now())`,
		otherConfig, otherOrg, provider, stored, integration)
	v.exec(`UPDATE sync_configurations SET integration_id = $2 WHERE id = $1`, one.config, integration)
	return otherOrgIntegration{config: one.config, integration: integration, otherConfig: otherConfig}
}

// otherOrgState is every row of the other org the PATCH could touch, and the configuration's own row, as text.
func (v *cascadeVenue) otherOrgState(seeded otherOrgIntegration) string {
	v.t.Helper()
	var state string
	if err := v.pool.QueryRow(context.Background(), `SELECT
  (SELECT row_to_json(i)::text FROM integrations i WHERE i.id = $1) || '|' ||
  (SELECT row_to_json(c)::text FROM sync_configurations c WHERE c.id = $2) || '|' ||
  (SELECT row_to_json(c)::text FROM sync_configurations c WHERE c.id = $3) || '|' ||
  (SELECT count(*)::text FROM integration_datasets WHERE integration_id = $1)`,
		seeded.integration, seeded.otherConfig, seeded.config).Scan(&state); err != nil {
		v.t.Fatal(err)
	}
	return state
}

// patchWhileHeld PATCHes the configuration while a second connection holds the other org's integration row FOR UPDATE.
// answered is false when the PATCH did not answer in 10 s (it waits on that row); the row is then released and the
// PATCH's answer returned.
func (v *cascadeVenue) patchWhileHeld(config, held uuid.UUID, body string) (status int, text string, answered bool) {
	v.t.Helper()
	ctx := context.Background()
	holder, err := v.pool.Begin(ctx)
	if err != nil {
		v.t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, `SELECT 1 FROM integrations WHERE id = $1 FOR UPDATE`, held); err != nil {
		v.t.Fatal(err)
	}
	type answer struct {
		status int
		body   string
		err    error
	}
	requestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	done := make(chan answer, 1)
	go func() {
		status, body, err := v.callCtx(requestCtx, "PATCH", "/api/v1/admin/sync-configs/"+config.String(), body)
		done <- answer{status, body, err}
	}()
	var got answer
	select {
	case got = <-done:
		answered = true
	case <-time.After(10 * time.Second):
	}
	if err := holder.Rollback(ctx); err != nil {
		v.t.Fatal(err)
	}
	if !answered {
		got = <-done
	}
	if got.err != nil {
		v.t.Fatalf("PATCH: %v", got.err)
	}
	return got.status, got.body, answered
}

// A PATCH of a configuration whose integration belongs to another org is refused whole, before any read or write keyed
// by that integration: the 422 "the sync configuration has no integration" for every body (with the
// ci_artifact_exclude_prefixes field only when the body carries that key), answered while the other org's integration
// row is held, no dataset row inserted, the other org's rows and the configuration byte-equal, and no source discovery
// (no request with any credential, no discovery log event). RED at main: the PATCH answers 200 (or waits on the other
// org's row), and the Jira path reads the other org's integration and planner configuration and runs a discovery.
func TestSyncConfigPatchRefusesAnIntegrationOfAnotherOrg(t *testing.T) {
	v, logs, requests := startIntegrationOrgVenue(t)
	for _, cell := range []struct {
		name, provider, stored, body string
		field                        bool
	}{
		{"jira options (discovery trigger)", "jira", `["work-items"]`, `{"sync_options":{"fetch_comments":false}}`, false},
		{"github targets (dataset row insert)", "github", `["tests"]`, `{"sync_targets":["tests","work-items"]}`, false},
		{"github options (runtime options)", "github", `["tests"]`, `{"sync_options":{"fetch_comments":false}}`, false},
		{"github empty body", "github", `["tests"]`, `{}`, false},
		{"github list key", "github", `["tests"]`, `{"sync_options":{"ci_artifact_exclude_prefixes":["ci-mine-"]}}`, true},
	} {
		t.Run(cell.name, func(t *testing.T) {
			v.t = t
			seeded := v.seedOtherOrgIntegration(cell.provider, cell.stored)
			before := v.otherOrgState(seeded)
			logsBefore, requestsBefore := len(logs.String()), requests.count.Load()
			status, body, answered := v.patchWhileHeld(seeded.config, seeded.integration, cell.body)
			if !answered {
				t.Errorf("the PATCH waited on the other org's integration row (held FOR UPDATE)")
			}
			if status != 422 || !strings.Contains(body, "the sync configuration has no integration") {
				t.Errorf("PATCH %s: %d %s, want the 422 \"the sync configuration has no integration\"", cell.body, status, body)
			}
			if got := strings.Contains(body, `"field"`); got != cell.field {
				t.Errorf("PATCH %s: field in the answer = %t, want %t: %s", cell.body, got, cell.field, body)
			}
			if after := v.otherOrgState(seeded); after != before {
				t.Errorf("the PATCH changed the other org's rows or the configuration:\n%s\n->\n%s", before, after)
			}
			if sent := requests.count.Load() - requestsBefore; sent != 0 {
				t.Errorf("the PATCH sent %d provider requests", sent)
			}
			if logged := logs.String()[logsBefore:]; strings.Contains(logged, "jira_project_discovery_on_update") {
				t.Errorf("the PATCH ran a source discovery on the other org's integration: %s", logged)
			}
		})
	}
}

// Control: the same bodies on configurations of the venue's own org answer as on main (200).
func TestSyncConfigPatchOfAnOwnIntegrationAnswersAsBefore(t *testing.T) {
	v, _, requests := startIntegrationOrgVenue(t)
	for _, cell := range []struct {
		name, provider, stored, body string
	}{
		{"jira options", "jira", `["work-items"]`, `{"sync_options":{"fetch_comments":false}}`},
		{"github targets", "github", `["tests"]`, `{"sync_targets":["tests","work-items"]}`},
		{"github options", "github", `["tests"]`, `{"sync_options":{"fetch_comments":false}}`},
		{"github empty body", "github", `["tests"]`, `{}`},
		{"github list key", "github", `["tests"]`, `{"sync_options":{"ci_artifact_exclude_prefixes":["ci-mine-"]}}`},
	} {
		t.Run(cell.name, func(t *testing.T) {
			v.t = t
			one := v.seed(cell.provider, cell.stored, []string{"tests"})
			status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+one.config.String(), cell.body)
			if status != 200 {
				t.Errorf("PATCH %s of an own configuration: %d %s, want 200", cell.body, status, body)
			}
		})
	}
	if sent := requests.count.Load(); sent != 0 {
		t.Errorf("%d provider requests: an own integration with no stored credential skips discovery", sent)
	}
}
