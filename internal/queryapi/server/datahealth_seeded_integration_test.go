//go:build integration

package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/datahealth"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
)

// The data-health reads, executed against a real ClickHouse and a real
// Postgres: the SQL, the scan types of nullable columns, org scoping, and the
// three lineage tables. Neither store can be faked for these: the reads join,
// FINAL-collapse and aggregate.

const dataHealthClickHouseDDL = `
CREATE TABLE git_commits (
    repo_id UUID, hash String, message Nullable(String), author_name Nullable(String),
    author_email Nullable(String), author_when DateTime64(3, 'UTC'),
    committer_name Nullable(String), committer_email Nullable(String),
    committer_when DateTime64(3, 'UTC'), parents UInt32, last_synced DateTime64(3, 'UTC'),
    org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, hash);

CREATE TABLE deployments (
    repo_id UUID, deployment_id String, status Nullable(String), environment Nullable(String),
    started_at Nullable(DateTime64(3, 'UTC')), finished_at Nullable(DateTime64(3, 'UTC')),
    deployed_at Nullable(DateTime64(3, 'UTC')), merged_at Nullable(DateTime64(3, 'UTC')),
    pull_request_number Nullable(UInt32), release_ref String DEFAULT '',
    release_ref_confidence Float64 DEFAULT 0.0, last_synced DateTime64(3, 'UTC'),
    org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, deployment_id);

CREATE TABLE work_items (
    repo_id UUID, work_item_id String, provider String, title String, description Nullable(String),
    type String, status String, status_raw String, project_key String, project_id String,
    assignees Array(String), reporter String, created_at DateTime64(3), updated_at DateTime64(3),
    started_at Nullable(DateTime64(3)), completed_at Nullable(DateTime64(3)),
    closed_at Nullable(DateTime64(3)), labels Array(String), story_points Nullable(Float64),
    sprint_id String, sprint_name String, parent_id String, epic_id String, url String,
    last_synced DateTime64(3), org_id String DEFAULT 'default'
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (repo_id, work_item_id);

CREATE TABLE repos (
    id UUID, repo String, ref Nullable(String), created_at DateTime64(3, 'UTC'),
    settings Nullable(String), tags Nullable(String), last_synced DateTime64(3, 'UTC'),
    org_id String, provider String, source_id Nullable(UUID)
) ENGINE = ReplacingMergeTree(last_synced) ORDER BY (org_id, id);

CREATE TABLE identities (
    org_id String DEFAULT 'default', canonical_id String, identity_uuid UUID,
    display_name Nullable(String), email Nullable(String),
    provider_identities String DEFAULT '{}', team_ids Array(String) DEFAULT [],
    is_active UInt8 DEFAULT 1, updated_at DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at) ORDER BY (org_id, canonical_id);

CREATE TABLE work_item_metrics_daily (org_id String, day Date, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY (org_id, day);
CREATE TABLE repo_metrics_daily (org_id String, day Date, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY (org_id, day);
CREATE TABLE team_metrics_daily (org_id String, day Date, computed_at DateTime('UTC')) ENGINE = MergeTree ORDER BY (org_id, day);
CREATE TABLE work_unit_investments (org_id String, work_unit_id String, computed_at DateTime64(3, 'UTC')) ENGINE = MergeTree ORDER BY (org_id, work_unit_id);
`

func TestDataHealthReadersSeededRealStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
	defer cancel()

	chInst, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	defer func() { _ = chInst.Close(context.Background()) }()
	pgInst, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start Postgres: %v", err)
	}
	defer func() { _ = pgInst.Close(context.Background()) }()

	opts, err := stdclickhouse.ParseDSN(chInst.URI)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := stdclickhouse.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	for _, stmt := range strings.Split(dataHealthClickHouseDDL, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("clickhouse ddl %q: %v", stmt, err)
		}
	}
	pool, err := pgxpool.New(ctx, pgInst.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	// The migrated schema (CHAOS-6769 ledger): the hand-written sync_configurations, scheduled_jobs
	// and job_runs lacked the real tables' NOT NULL columns.
	pgschema.Apply(ctx, t, pool)

	const org, other = "dh-org", "dh-other"
	mustExec := func(sql string) {
		t.Helper()
		if err := conn.Exec(ctx, sql); err != nil {
			t.Fatalf("seed %q: %v", sql, err)
		}
	}
	const repoA, repoB, repoC = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333333"
	for _, r := range [][3]string{{repoA, "acme/a", org}, {repoB, "acme/b", org}, {repoC, "foreign/c", other}} {
		mustExec(fmt.Sprintf("INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('%s','%s',now64(3),now64(3),'%s','github')", r[0], r[1], r[2]))
	}
	// the same repository ids exist in another org under other names: joins
	// must not see them
	for _, r := range [][3]string{{repoA, "foreign/dup-a", other}, {repoB, "foreign/dup-b", other}} {
		mustExec(fmt.Sprintf("INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('%s','%s',now64(3),now64(3),'%s','github')", r[0], r[1], r[2]))
	}
	// commits: ann observed twice (unmapped), known once (mapped by e-mail), NULL e-mail skipped, foreign org ignored
	for i, c := range []struct{ name, email, org string }{
		{"Ann", "Ann@Example.com", org}, {"Ann", "ann@example.com", org}, {"Known", "known@example.com", org},
		{"Nobody", "", org}, {"Foreign", "foreign@example.com", other},
	} {
		email := "NULL"
		if c.email != "" {
			email = "'" + c.email + "'"
		}
		mustExec(fmt.Sprintf("INSERT INTO git_commits (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id) VALUES ('%s','h%d','%s',%s,now64(3),now64(3),1,now64(3),'%s')", repoA, i, c.name, email, c.org))
	}
	mustExec(fmt.Sprintf("INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, assignees, reporter, created_at, updated_at, labels, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id) VALUES ('%s','w1','jira','t','story','done','done','PRJ','p',['jdoe'],'r',now64(3),now64(3),[],'','','','','',now64(3),'%s')", repoA, org))
	mustExec(fmt.Sprintf("INSERT INTO work_items (repo_id, work_item_id, provider, title, type, status, status_raw, project_key, project_id, assignees, reporter, created_at, updated_at, labels, sprint_id, sprint_name, parent_id, epic_id, url, last_synced, org_id) VALUES ('%s','w2','','t','story','done','done','','p',[''],'r',now64(3),now64(3),[],'','','','','',now64(3),'%s')", repoB, org))
	mustExec(fmt.Sprintf("INSERT INTO identities (org_id, canonical_id, identity_uuid, display_name, email, provider_identities, team_ids, is_active, updated_at) VALUES ('%s','known-c',generateUUIDv4(),'Known Person','known@example.com','{\"jira\":[\"jdoe-alias\"]}',[],1,now64(6)),('%s','ann-c',generateUUIDv4(),NULL,'ann@corp.example','{}',['team-x'],1,now64(6)),('%s','off',generateUUIDv4(),'Off','off@example.com','{}',[],0,now64(6))", org, org, org))
	// deployments: repoA covered by PR number, repoB uncovered, foreign ignored
	mustExec(fmt.Sprintf("INSERT INTO deployments (repo_id, deployment_id, pull_request_number, release_ref, last_synced, org_id) VALUES ('%s','d1',7,'',now64(3),'%s'),('%s','d2',NULL,'',now64(3),'%s'),('%s','d3',NULL,'v1',now64(3),'%s')", repoA, org, repoB, org, repoC, other))
	for _, table := range []string{"work_item_metrics_daily", "repo_metrics_daily", "team_metrics_daily"} {
		mustExec(fmt.Sprintf("INSERT INTO %s (org_id, day, computed_at) VALUES ('%s', '2026-01-01', '2026-01-02 03:04:05'), ('%s', '2026-01-02', '2026-01-03 03:04:05'), ('%s','2026-01-03','2030-01-01 00:00:00')", table, org, org, other))
	}

	pgExec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("pg seed: %v", err)
		}
	}
	const cfgGH, cfgJira, cfgOff, cfgForeign, jobGH, jobJira = "aaaaaaaa-0000-0000-0000-000000000001", "aaaaaaaa-0000-0000-0000-000000000002", "aaaaaaaa-0000-0000-0000-000000000003", "aaaaaaaa-0000-0000-0000-000000000004", "bbbbbbbb-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"
	pgExec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, is_active, last_sync_at, last_sync_success, last_sync_stats, created_at, updated_at) VALUES
		($1,$2,'gh-main','github','["acme/a","acme/b"]',true,'2026-02-01T10:00:00Z',true,'{"rows_ingested":11}',now(),now()),
		($3,$2,'jira-main','jira','[]',true,NULL,false,NULL,now(),now()),
		($4,$2,'inactive','github','[]',false,NULL,NULL,NULL,now(),now()),
		($5,$6,'foreign','github','[]',true,NULL,NULL,NULL,now(),now())`, cfgGH, org, cfgJira, cfgOff, cfgForeign, other)
	pgExec(`INSERT INTO scheduled_jobs (id, org_id, sync_config_id, name, job_type, schedule_cron, status, created_at, updated_at) VALUES ($1,$2,$3,'job-a','sync','0 * * * *',0,now(),now()),($4,$2,$5,'job-b','sync','0 * * * *',0,now(),now())`, jobGH, org, cfgGH, jobJira, cfgJira)
	pgExec(`INSERT INTO job_runs (id, job_id, status, started_at, completed_at, result, error, created_at) VALUES
		('cccccccc-0000-0000-0000-000000000001',$1,2,'2026-01-01T00:00:00Z','2026-01-01T00:10:00Z','{"rows":1}',NULL,'2026-01-01T00:00:00Z'),
		('cccccccc-0000-0000-0000-000000000002',$2,3,'2026-03-01T00:00:00Z','2026-03-01T00:05:00Z','{"stage":"fetch","rows":4}','boom','2026-03-01T00:00:00Z'),
		('cccccccc-0000-0000-0000-000000000003',$2,2,'2026-02-01T00:00:00Z','2026-02-01T00:05:00Z','{"rows":9}',NULL,'2026-02-01T00:00:00Z')`, jobGH, jobJira)

	// a job of another org attached to this org's Jira configuration, with a
	// newer failed run: it must not supply this configuration's newest run
	pgExec(`INSERT INTO scheduled_jobs (id, org_id, sync_config_id, name, job_type, schedule_cron, status, created_at, updated_at) VALUES ('bbbbbbbb-0000-0000-0000-000000000009', $1, $2, 'job-foreign','sync','0 * * * *',0,now(),now())`, other, cfgJira)
	pgExec(`INSERT INTO job_runs (id, job_id, status, started_at, completed_at, result, error, created_at) VALUES
		('cccccccc-0000-0000-0000-000000000009','bbbbbbbb-0000-0000-0000-000000000009',3,'2027-01-01T00:00:00Z','2027-01-01T00:05:00Z','{"stage":"foreign","rows":99}','foreign boom','2027-01-01T00:00:00Z')`)

	ch, err := dhclickhouse.NewClickHouseQueryClientWithOptions(dhclickhouse.Options{DSN: chInst.URI})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()

	reader := &datahealth.Reader{ClickHouse: ch, Postgres: pool}
	got, err := reader.Resolve(ctx, org, "")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if reader.Degraded() {
		t.Fatal("no read may fail against the real schema")
	}

	// connectors: active org configs only, ordered provider then name
	if len(got.Connectors) != 2 || got.Connectors[0].Provider != "github" || got.Connectors[1].Provider != "jira" {
		t.Fatalf("connectors = %+v", got.Connectors)
	}
	gh, jira := got.Connectors[0], got.Connectors[1]
	if gh.Scope != "acme/a, acme/b" || gh.RowsIngested != 11 || gh.LastFailure != nil || gh.LastSyncAt == nil {
		t.Fatalf("github connector = %+v", gh)
	}
	if jira.Scope != "jira-main" || jira.RowsIngested != 4 || (jira.LastFailure != nil && jira.LastFailure.Message == "foreign boom") {
		t.Fatalf("the NEWEST job run (created_at) supplies the stats fallback: %+v", jira)
	}
	if jira.LastFailure == nil || jira.LastFailure.Message != "boom" || jira.LastFailure.Stage == nil || *jira.LastFailure.Stage != "fetch" ||
		!jira.LastFailure.OccurredAt.Equal(time.Date(2026, 3, 1, 0, 5, 0, 0, time.UTC)) {
		t.Fatalf("jira failure = %+v", jira.LastFailure)
	}

	// identity mapping: ann (2 commits) is unmapped, known is mapped by e-mail, jdoe unmapped assignee
	im := got.IdentityMapping
	if im.UnmappedCount != 2 || *im.UnmappedIdentities[0].Email != "ann@example.com" || *im.UnmappedIdentities[0].ObservedCount != 2 ||
		im.UnmappedIdentities[1].Provider != "jira" || *im.UnmappedIdentities[1].DisplayName != "jdoe" {
		t.Fatalf("identity mapping = %+v", im)
	}
	if len(im.SuggestedAliases) != 1 || im.SuggestedAliases[0].SuggestedCanonicalID != "ann-c" {
		t.Fatalf("ann@example.com suggests ann-c by local part: %+v", im.SuggestedAliases)
	}
	scoped := (&datahealth.Reader{ClickHouse: ch, Postgres: pool}).IdentityMapping(ctx, org, "team-y")
	if len(scoped.SuggestedAliases) != 0 {
		t.Fatalf("ann-c is scoped to team-x, so team-y gets no suggestion: %+v", scoped.SuggestedAliases)
	}

	// coverage
	dep, wi := got.MappingCoverage.Deployments, got.MappingCoverage.WorkItems
	if dep.TotalRepos != 2 || dep.CoveredRepos != 1 || dep.CoveragePct != 50.0 || len(dep.Missing) != 1 || dep.Missing[0].RepoName != "acme/b" {
		t.Fatalf("deployment coverage = %+v", dep)
	}
	if wi.TotalRepos != 2 || wi.CoveredRepos != 1 || len(wi.Missing) != 1 || wi.Missing[0].RepoName != "acme/b" {
		t.Fatalf("work-item coverage = %+v", wi)
	}

	// lineage: newest computed_at of THIS org, and its row count
	lin := (&datahealth.Reader{ClickHouse: ch}).MetricLineage(ctx, org, "throughput")
	if lin == nil || !lin.ComputedAt.Equal(time.Date(2026, 1, 3, 3, 4, 5, 0, time.UTC)) || *lin.RowCount != 2 {
		t.Fatalf("lineage = %+v", lin)
	}
	// an org with no rows: the engine's zero time, as the Python read reports it
	empty := (&datahealth.Reader{ClickHouse: ch}).MetricLineage(ctx, "no-such-org", "review_load")
	if empty == nil || empty.ComputedAt.Year() != 1970 || *empty.RowCount != 0 {
		t.Fatalf("empty-org lineage = %+v", empty)
	}
}
