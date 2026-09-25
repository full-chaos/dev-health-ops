//go:build integration

package operational

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/synchandoff"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonSyncNowProgram runs the admin "Sync Now" producer,
// create_sync_execution_trigger (triggered_by manual, mode incremental), for
// each configuration and commits.
const pythonSyncNowProgram = `
import json, sys, uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from dev_health_ops.models.settings import SyncConfiguration
from dev_health_ops.sync.execution_trigger import create_sync_execution_trigger
payload = json.loads(sys.stdin.read())
engine = create_engine(payload["uri"], poolclass=NullPool)
out = []
for config_id in payload["configs"]:
    with Session(engine) as session:
        try:
            config = session.get(SyncConfiguration, uuid.UUID(config_id))
            result = create_sync_execution_trigger(session, config, str(config.org_id), triggered_by="manual", mode="incremental")
            session.commit()
            out.append("ok" if result is not None and result.awaiting_materialization else "not routed")
        except Exception as exc:
            out.append("raise " + type(exc).__name__)
engine.dispose()
print(json.dumps(out))
`

type handoffOracleCase struct {
	name, configID, installation, repoID, fullName string
}

// TestWebhookHandoffVenueOracleMatchesLivePython is the CHAOS-6695
// acceptance gate. Over the same seeded rows, the Go webhook path -- the
// worker's TriggerScopedSync records the request, the scheduler's minter
// mints it through synchandoff.Mint -- must leave the same scheduling rows
// (the marker job, the occurrence, its manual trigger) as Python's admin
// "Sync Now" producer, create_sync_execution_trigger, for the configuration
// the delivery routes to. The occurrence's scheduled_for is the delivery's
// created_at on Go and the call's now on Python, so each side's identity is
// checked against its own scheduled_for and the rest is compared as rows.
func TestWebhookHandoffVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the webhook hand-off oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	const org = "00000000-0000-4000-8000-0000000c6695"
	var cases []handoffOracleCase
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, venue *venueoracle.Venue) map[string]map[string]any {
			cases = seedHandoffOracle(t, ctx, admin, org)
			return nil
		},
	})

	python := pyoracle.Resolve(t, root)
	parsed, err := url.Parse(venue.AdminURI(t, venue.SourceDB))
	if err != nil {
		t.Fatal(err)
	}
	parsed.Scheme = "postgresql+psycopg2"
	configs := make([]string, len(cases))
	for index, c := range cases {
		configs[index] = c.configID
	}
	input, _ := json.Marshal(map[string]any{"uri": parsed.String(), "configs": configs})
	command := exec.Command(python, "-c", pythonSyncNowProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_SDK_DISABLED=true", "ENVIRONMENT=test")
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var outcomes []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &outcomes); err != nil || len(outcomes) != len(cases) {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	for index, outcome := range outcomes {
		if outcome != "ok" {
			t.Fatalf("%s: python Sync Now = %s, want a routed hand-off", cases[index].name, outcome)
		}
	}

	pool, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := &PostgresStore{pool: pool}
	deliveredAt := time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC)
	for index, c := range cases {
		payload := fmt.Sprintf(`{"installation":{"id":%s},"repository":{"id":%s,"full_name":%q}}`, c.installation, c.repoID, c.fullName)
		result, err := store.TriggerScopedSync(ctx, uuid.NewString(), "github", "push", []byte(payload), deliveredAt.Add(time.Duration(index)*time.Second))
		if err != nil || !result.Processed || result.SyncConfigID != c.configID {
			t.Fatalf("%s: go webhook = %+v, %v; want routed to %s", c.name, result, err, c.configID)
		}
	}
	if err := schedsync.MintWebhookSyncRequests(ctx, pool, time.Now().UTC(), 50); err != nil {
		t.Fatal(err)
	}

	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	for _, c := range cases {
		for _, table := range []string{"occurrence", "trigger", "job"} {
			query := handoffRowQuery(table, c.configID)
			pythonRows, goRows := venueoracle.TableRows(t, ctx, source, query), venueoracle.TableRows(t, ctx, goDB, query)
			if pythonRows != goRows {
				t.Errorf("%s %s rows differ:\n python %s\n go     %s", c.name, table, pythonRows, goRows)
			}
		}
		for plane, uri := range map[string]string{"python": source, "go": goDB} {
			var occurrenceID string
			var scheduledFor time.Time
			conn, err := pgxpool.New(ctx, uri)
			if err != nil {
				t.Fatal(err)
			}
			err = conn.QueryRow(ctx, `SELECT occurrence_id, scheduled_for FROM scheduled_sync_occurrences WHERE sync_config_id = $1::uuid`,
				c.configID).Scan(&occurrenceID, &scheduledFor)
			conn.Close()
			if err != nil {
				t.Fatalf("%s %s occurrence: %v", c.name, plane, err)
			}
			if want := synchandoff.OccurrenceIdentity(c.configID, scheduledFor); occurrenceID != want {
				t.Errorf("%s %s occurrence_id %s is not the identity of its scheduled_for (%s)", c.name, plane, occurrenceID, want)
			}
		}
	}
	if t.Failed() {
		return
	}
	t.Logf("%d configurations: scheduling rows identical on both planes", len(cases))
	venueoracle.WriteProof(t)
}

// handoffRowQuery reads one configuration's scheduling rows without the
// values that differ by construction (ids, timestamps, scheduled_for).
func handoffRowQuery(table, configID string) string {
	switch table {
	case "occurrence":
		return `SELECT o.identity_version, o.org_id, o.reconcile_status, o.reconcile_attempt_count, j.name AS job
FROM scheduled_sync_occurrences o JOIN scheduled_jobs j ON j.id = o.scheduled_job_id
WHERE o.sync_config_id = '` + configID + `'::uuid`
	case "trigger":
		return `SELECT t.mode, t.since, t.before, t.source_ids, t.dataset_keys, t.triggered_by
FROM sync_manual_triggers t JOIN scheduled_sync_occurrences o USING (occurrence_id)
WHERE o.sync_config_id = '` + configID + `'::uuid`
	default:
		return `SELECT name, job_type, provider, schedule_cron, timezone, job_config::text, status, is_running, run_count, failure_count
FROM scheduled_jobs WHERE sync_config_id = '` + configID + `'::uuid`
	}
}

// seedHandoffOracle writes one GitHub installation and integration with four
// repositories, each routed a different way: a child configuration with
// sync_targets, a child with full_resync set, a child with a schedule, and a
// planner-managed parent a repository without a child routes to.
func seedHandoffOracle(t *testing.T, ctx context.Context, admin *pgxpool.Pool, org string) []handoffOracleCase {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	const at = "2026-01-01 00:00:00+00"
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES ($1, 'ho', 'ho', '{}', 'enterprise', true, $2, $2)`, org, at)
	exec(`INSERT INTO github_app_installations (id, installation_id, org_id, created_at, updated_at) VALUES (gen_random_uuid(), 6695, $1, $2, $2)`, org, at)
	integrationID := uuid.NewString()
	exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'github', 'ho', '{}', true, $3, $3)`, integrationID, org, at)
	parentID := uuid.NewString()
	exec(`INSERT INTO sync_configurations (id, org_id, name, provider, integration_id, sync_targets, sync_options, is_active, planner_managed, created_at, updated_at)
VALUES ($1, $2, 'ho parent', 'github', $3, '["git","prs"]', '{}', true, true, $4, $4)`, parentID, org, integrationID, at)
	type repo struct{ name, targets, options string }
	repos := []repo{
		{"child targets", `["git","prs"]`, `{}`},
		{"child full resync", `["git"]`, `{"full_resync": true}`},
		{"child scheduled", `["work-items"]`, `{"schedule_cron": "0 */6 * * *", "timezone": "Europe/Paris"}`},
		{"parent route", "", ""},
	}
	var cases []handoffOracleCase
	for index, r := range repos {
		sourceID := uuid.NewString()
		repoID := fmt.Sprint(4200 + index)
		fullName := "ho/repo-" + repoID
		metadata := `{}`
		if r.targets == "" {
			metadata = `{"planner_managed_sync_config_id": "` + parentID + `"}`
		}
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
	is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repository', $4, $5, $5, $6::json, true, $7, $7)`, sourceID, org, integrationID, repoID, fullName, metadata, at)
		configID := parentID
		if r.targets != "" {
			configID = uuid.NewString()
			exec(`INSERT INTO sync_configurations (id, org_id, name, provider, integration_id, source_id, sync_targets, sync_options, is_active,
	planner_managed, created_at, updated_at)
VALUES ($1, $2, $3, 'github', $4, $5, $6::json, $7::json, true, false, $8, $8)`, configID, org, "ho "+r.name, integrationID, sourceID, r.targets, r.options, at)
		}
		cases = append(cases, handoffOracleCase{name: r.name, configID: configID, installation: "6695", repoID: repoID, fullName: fullName})
	}
	return cases
}
