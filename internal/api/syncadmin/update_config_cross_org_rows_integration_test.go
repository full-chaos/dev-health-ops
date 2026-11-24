//go:build integration

package syncadmin

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// crossOrgRowsJira answers the Jira project search of a source discovery with one synthetic project, and counts requests.
type crossOrgRowsJira struct{ requests atomic.Int64 }

func (j *crossOrgRowsJira) RoundTrip(request *http.Request) (*http.Response, error) {
	j.requests.Add(1)
	body := `{"values":[{"id":"10001","key":"SYN","name":"Synthetic"}],"isLast":true}`
	if !strings.HasPrefix(request.URL.Path, "/rest/api/3/project/search") {
		return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}},
		Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

type crossOrgRowsDecryptor struct{}

func (crossOrgRowsDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"email":"bot@example.com","api_token":"synthetic-token","base_url":"https://acme.atlassian.net"}`), nil
}

// startCrossOrgRowsVenue is startCascadeVenue with the update path's Jira discovery wired to a synthetic Jira.
func startCrossOrgRowsVenue(t *testing.T) (*cascadeVenue, *crossOrgRowsJira) {
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
	jira := &crossOrgRowsJira{}
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Pool: pool, Guard: testGuard(t), Logger: quiet(),
		Decryptor: crossOrgRowsDecryptor{}, JiraHTTP: &http.Client{Transport: jira}}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	materializer, err := schedsync.NewNativeMaterializer(pool)
	if err != nil {
		t.Fatal(err)
	}
	return &cascadeVenue{t: t, pool: pool, org: org, server: server, materializer: materializer}, jira
}

func (v *cascadeVenue) rowText(sql string, args ...any) string {
	v.t.Helper()
	var text string
	if err := v.pool.QueryRow(context.Background(), sql, args...).Scan(&text); err != nil {
		v.t.Fatal(err)
	}
	return text
}

// A child configuration of ANOTHER org whose parent_id is a configuration of the venue's org (a legacy-row anomaly: no
// Go writer sets parent_id) is not written by a PATCH of that parent: the child row and its scheduled job stay byte-equal.
// RED with the cascade's org filter dropped (update_config.go cascadeToChildren): the parent's targets and is_active
// are cascaded into the other org's child and a scheduled job is written for it.
func TestParentPatchLeavesAChildOfAnotherOrgUntouched(t *testing.T) {
	v := startCascadeVenue(t, true)
	ctx := context.Background()
	parent := v.seed("github", `["tests"]`, []string{"tests"})
	otherOrg := uuid.NewString()
	pgseed.Org(ctx, t, v.pool, otherOrg, "enterprise")
	child, job := uuid.New(), uuid.New()
	v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, parent_id, source_id, created_at, updated_at) VALUES ($1, $2, 'other org child', 'github', '["tests"]'::json, '{"schedule_cron":"0 * * * *"}'::json, true, false,
NULL, $3, NULL, now(), now())`, child, otherOrg, parent.config)
	v.exec(`INSERT INTO scheduled_jobs (id,org_id,name,sync_config_id,job_type,schedule_cron,timezone,status,is_running,created_at,updated_at)
VALUES ($1::uuid,$2,'job-'||$1::text,$3::uuid,'sync','0 * * * *','UTC',1,FALSE,now(),now())`, job, otherOrg, child)
	state := func() string {
		return v.rowText(`SELECT (SELECT row_to_json(c)::text FROM sync_configurations c WHERE c.id = $1) || '|' ||
  COALESCE((SELECT string_agg(row_to_json(j)::text, ',' ORDER BY j.id) FROM scheduled_jobs j WHERE j.sync_config_id = $1), '')`, child)
	}
	before := state()
	status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+parent.config.String(), `{"sync_targets":["tests","work-items"],"is_active":false}`)
	if status != 200 {
		t.Fatalf("PATCH of the parent: %d %s", status, body)
	}
	if after := state(); after != before {
		t.Errorf("the PATCH of a parent wrote a child of another org (or its scheduled job):\n%s\n->\n%s", before, after)
	}
}

// Another org's planner-managed configuration on the venue's own Jira integration (the reverse link) is not read by
// the update path's discovery: its id never reaches the discovered sources (planner_managed_sync_config_id) and its row
// stays byte-equal. The venue's own configuration is not planner-managed, so the other org's row is the only planner
// row on the integration. RED at 404037040edc: the discovery's planner read has no org filter (update_config.go
// discoverIntegrationSources).
func TestJiraDiscoveryReadsNoPlannerConfigOfAnotherOrg(t *testing.T) {
	v, jira := startCrossOrgRowsVenue(t)
	ctx := context.Background()
	one := v.seed("jira", `["work-items"]`, nil)
	credential := uuid.New()
	v.exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
VALUES ($1, $2, 'jira', 'default', true, 'synthetic-ciphertext', '{}'::json, now(), now())`, credential, v.org)
	v.exec(`UPDATE integrations SET credential_id = $2 WHERE id = $1`, one.integration, credential)
	v.exec(`UPDATE sync_configurations SET planner_managed = false WHERE id = $1`, one.config)
	otherOrg := uuid.NewString()
	pgseed.Org(ctx, t, v.pool, otherOrg, "enterprise")
	otherPlanner := uuid.New()
	v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, parent_id, source_id, created_at, updated_at) VALUES ($1, $2, 'other org planner', 'jira', '["work-items"]'::json, '{"other_org_option":"other"}'::json, true, true,
$3, NULL, NULL, now(), now())`, otherPlanner, otherOrg, one.integration)
	before := v.rowText(`SELECT row_to_json(c)::text FROM sync_configurations c WHERE c.id = $1`, otherPlanner)
	status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+one.config.String(), `{"sync_options":{"fetch_comments":false}}`)
	if status != 200 {
		t.Fatalf("PATCH: %d %s", status, body)
	}
	var sources, tagged int
	if err := v.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE strpos(metadata::text, $2) > 0)
FROM integration_sources WHERE integration_id = $1`, one.integration, otherPlanner.String()).Scan(&sources, &tagged); err != nil {
		t.Fatal(err)
	}
	if jira.requests.Load() == 0 || sources == 0 {
		t.Fatalf("premise: the discovery did not run (%d Jira requests, %d sources)", jira.requests.Load(), sources)
	}
	if tagged != 0 {
		t.Errorf("%d discovered source(s) carry another org's planner configuration id: the discovery read that org's row", tagged)
	}
	if after := v.rowText(`SELECT row_to_json(c)::text FROM sync_configurations c WHERE c.id = $1`, otherPlanner); after != before {
		t.Errorf("the other org's planner configuration changed:\n%s\n->\n%s", before, after)
	}
}
