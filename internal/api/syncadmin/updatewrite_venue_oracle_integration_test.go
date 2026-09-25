//go:build integration

package syncadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonUpdateWriteProgram runs each case through the api's own
// sync.py _reconcile_dataset_rows_for_sync_targets (with the config's
// stored targets as previous_sync_targets, as update_sync_config passes
// them) or _upsert_scheduled_job (on the config as loaded), each case in
// its own session, committed; "ok" or "raise <class>".
const pythonUpdateWriteProgram = `
import asyncio, json, sys, uuid
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import NullPool
from dev_health_ops.api.admin.routers import sync as router
from dev_health_ops.models.settings import SyncConfiguration
payload = json.loads(sys.stdin.read())
async def main():
    engine = create_async_engine(payload["uri"], poolclass=NullPool)
    out = []
    for case in payload["cases"]:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            try:
                config = await session.get(SyncConfiguration, uuid.UUID(case["config_id"]))
                if case["kind"] == "reconcile":
                    await router._reconcile_dataset_rows_for_sync_targets(
                        session, case["org"], config.integration_id, str(config.provider), case["targets"],
                        list(config.sync_targets or []), config_id=config.id)
                else:
                    await router._upsert_scheduled_job(session, config, case["org"])
                await session.commit()
                out.append("ok")
            except Exception as exc:
                await session.rollback()
                out.append("raise " + type(exc).__name__ + ": " + str(exc)[:300])
    await engine.dispose()
    print(json.dumps(out))
asyncio.run(main())
`

type updateWriteCase struct {
	Kind     string   `json:"kind"`
	Org      string   `json:"org"`
	ConfigID string   `json:"config_id"`
	Targets  []string `json:"targets"`
}

// TestUpdateWriteEnginesVenueOracleMatchesLivePython runs the update path's
// dataset reconciliation and the shared scheduled-job upsert on two copies
// of one seeded database: the api's own Python functions on one, the Go
// functions as the api role on the other, case by case. It requires the
// same outcome per case and the same integration_datasets and
// scheduled_jobs rows (raw text; job timestamps compared as seed or moved).
func TestUpdateWriteEnginesVenueOracleMatchesLivePython(t *testing.T) {
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	org := uuid.New().String()
	const seedAt = "2026-01-01 00:00:00+00"
	var cases []updateWriteCase
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			cases = seedUpdateWriteCases(t, ctx, admin, org, seedAt)
			return nil
		},
	})

	python := pyoracle.Resolve(t, root)
	input, _ := json.Marshal(map[string]any{
		"uri":   asyncpgURI(t, venue.AdminURI(t, venue.SourceDB)),
		"cases": cases,
	})
	command := exec.Command(python, "-c", pythonUpdateWriteProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"), "OTEL_SDK_DISABLED=true", "ENVIRONMENT=test",
		"JWT_SECRET_KEY=venue-oracle-update-write-engines-secret-key!")
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var pythonOutcomes []string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &pythonOutcomes); err != nil || len(pythonOutcomes) != len(cases) {
		t.Fatalf("decode python outcomes: %v\n%s", err, output)
	}

	// A harness failure (every case raising the same way) must not read as
	// agreement: at least one case has to complete on the Python plane.
	completed := 0
	for _, outcome := range pythonOutcomes {
		if outcome == "ok" {
			completed++
		}
	}
	if completed == 0 {
		t.Fatalf("no Python case completed: %v", pythonOutcomes)
	}

	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	for index, c := range cases {
		configID := uuid.MustParse(c.ConfigID)
		outcome := "ok"
		err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
			if c.Kind == "upsert" {
				return upsertScheduledJob(ctx, tx, c.Org, configID, now)
			}
			var integrationID uuid.UUID
			var provider string
			var targets *string
			if err := tx.QueryRow(ctx, `SELECT integration_id, provider, sync_targets::text FROM sync_configurations WHERE id = $1`, configID).
				Scan(&integrationID, &provider, &targets); err != nil {
				return err
			}
			stored, err := decodeStored(targets)
			if err != nil {
				return err
			}
			previous, err := pyIterate(stored)
			if err != nil {
				return err
			}
			return reconcileDatasetRowsForSyncTargets(ctx, tx, logger, c.Org, integrationID, provider, c.Targets, previous, configID)
		})
		if err != nil {
			outcome = "raise"
		}
		pythonOutcome := pythonOutcomes[index]
		if strings.HasPrefix(pythonOutcome, "raise") {
			pythonOutcome = "raise"
		}
		if outcome != pythonOutcome {
			t.Errorf("case %d (%s %s %v): go %s (%v), python %s", index, c.Kind, c.ConfigID, c.Targets, outcome, err, pythonOutcomes[index])
		}
	}

	queries := map[string]string{
		"integration_datasets": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', d.org_id, i.name,
  d.dataset_key, d.is_enabled, d.options::text) AS r FROM integration_datasets d JOIN integrations i ON i.id = d.integration_id) AS rows`,
		"scheduled_jobs": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', j.org_id, c.name, j.name, j.job_type,
  j.provider, j.schedule_cron, j.timezone, j.job_config::text, j.status, j.is_running, j.run_count, j.failure_count,
  CASE WHEN j.created_at = '` + seedAt + `' THEN 'seed' ELSE 'new' END,
  CASE WHEN j.updated_at = '` + seedAt + `' THEN 'seed' ELSE 'moved' END) AS r
  FROM scheduled_jobs j JOIN sync_configurations c ON c.id = j.sync_config_id) AS rows`,
	}
	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	for _, table := range []string{"integration_datasets", "scheduled_jobs"} {
		pythonRows, goRows := venueoracle.TableRows(t, ctx, source, queries[table]), venueoracle.TableRows(t, ctx, goDB, queries[table])
		if pythonRows != goRows {
			t.Errorf("%s rows differ:\n python %s\n go     %s", table, pythonRows, goRows)
		}
		t.Logf("%s: %d rows identical", table, strings.Count(goRows, "\n")+map[bool]int{true: 0, false: 1}[goRows == ""])
	}
	// The writes happened: datasets enabled and disabled, jobs inserted
	// and moved, a job left untouched.
	state := venueoracle.TableRows(t, ctx, goDB, `SELECT
  (SELECT count(*) FROM integration_datasets WHERE is_enabled),
  (SELECT count(*) FROM integration_datasets WHERE NOT is_enabled),
  (SELECT count(*) FROM scheduled_jobs WHERE created_at != '`+seedAt+`'),
  (SELECT count(*) FROM scheduled_jobs WHERE created_at = '`+seedAt+`' AND updated_at != '`+seedAt+`'),
  (SELECT count(*) FROM scheduled_jobs WHERE updated_at = '`+seedAt+`')`)
	if fields := strings.Fields(state); len(fields) != 5 || fields[0] == "0" || fields[1] == "0" || fields[2] == "0" || fields[3] == "0" || fields[4] == "0" {
		t.Errorf("writes not observed (enabled, disabled, inserted jobs, moved jobs, untouched jobs) = %s", state)
	}
	t.Logf("%d cases; write state: %s", len(cases), state)
	venueoracle.WriteProof(t)
}

// seedUpdateWriteCases writes one integration per reconciliation case (the
// edited whole-integration config, its datasets and any sibling configs)
// and one config per upsert case (with its existing sync jobs), and
// returns the cases in order.
func seedUpdateWriteCases(t *testing.T, ctx context.Context, admin *pgxpool.Pool, org, at string) []updateWriteCase {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES ($1, 'uw', 'uw', '{}', 'enterprise', true, $2, $2)`, org, at)
	integration := func(name, provider string) uuid.UUID {
		id := uuid.New()
		exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at) VALUES ($1, $2, $3, $4, '{}', true, $5, $5)`,
			id, org, provider, name, at)
		return id
	}
	config := func(integrationID *uuid.UUID, name, provider, targets, options string, active bool, sourceID *uuid.UUID) uuid.UUID {
		id := uuid.New()
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, source_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5::json, $6::json, $7, $8, $9, $10, $11, $11)`,
			id, org, name, provider, targets, options, active, integrationID != nil, integrationID, sourceID, at)
		return id
	}
	dataset := func(integrationID uuid.UUID, key string, enabled bool, options string) {
		exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, $5, $6::json)`,
			uuid.New(), org, integrationID, key, enabled, options)
	}
	var cases []updateWriteCase
	reconcile := func(name, provider, storedTargets string, newTargets []string, datasets map[string]bool, siblings []string) {
		id := integration(name, provider)
		for key, enabled := range datasets {
			dataset(id, key, enabled, `{"kept": 1}`)
		}
		for index, sibling := range siblings {
			config(&id, fmt.Sprintf("%s sibling %d", name, index), provider, sibling, `{}`, index%2 == 0, nil)
		}
		configID := config(&id, name, provider, storedTargets, `{}`, true, nil)
		cases = append(cases, updateWriteCase{Kind: "reconcile", Org: org, ConfigID: configID.String(), Targets: newTargets})
	}
	reconcile("gh add", "github", `["git"]`, []string{"git", "prs", "work-items"},
		map[string]bool{"repo-metadata": true, "commits": true, "prs": false, "security": true}, nil)
	reconcile("gh drop git", "github", `["git", "prs"]`, []string{"prs"},
		map[string]bool{"repo-metadata": true, "commits": true, "commit-stats": true, "files": true, "blame": true, "prs": true, "pr-reviews": true}, nil)
	reconcile("gh drift", "GitHub", `["git"]`, []string{"git"},
		map[string]bool{"commits": true, "work-items": true, "work-item-labels": true, "cicd": false, "security": true}, nil)
	reconcile("gh sibling keeps", "github", `["git", "work-items"]`, []string{"git"},
		map[string]bool{"commits": true, "work-items": true, "work-item-comments": true}, []string{`["work-items"]`})
	reconcile("gh sibling odd targets", "github", `["prs"]`, []string{"prs"},
		map[string]bool{"commits": true, "prs": true}, []string{`"git"`, `null`, `{"cicd": 1}`, `[null, 1, "tests"]`})
	reconcile("gl drop all", "gitlab", `["git", "cicd"]`, []string{},
		map[string]bool{"commits": true, "blame": true, "cicd": true, "files": false}, nil)
	reconcile("pd refused", "pagerduty", `["operational"]`, []string{"operational", "incidents"},
		map[string]bool{"services": false}, nil)
	reconcile("pd operational", "pagerduty", `["operational"]`, []string{"operational"},
		map[string]bool{"services": false, "incidents": true}, nil)
	reconcile("jira previous not a list", "jira", `"work-items"`, []string{"work-items"},
		map[string]bool{"work-items": false}, nil)
	reconcile("linear add", "linear", `[]`, []string{"work-items"}, map[string]bool{}, nil)
	reconcile("launchdarkly none", "launchdarkly", `["feature-flags"]`, []string{},
		map[string]bool{"feature-flags": true}, nil)
	// A sibling on another provider whose targets planner_dataset_keys
	// refuses: the disable pass is dropped.
	{
		id := integration("mixed unreadable sibling", "github")
		dataset(id, "commits", true, `{}`)
		dataset(id, "prs", true, `{}`)
		config(&id, "pd sibling", "pagerduty", `["incidents"]`, `{}`, true, nil)
		configID := config(&id, "mixed unreadable sibling", "github", `["git", "prs"]`, `{}`, true, nil)
		cases = append(cases, updateWriteCase{Kind: "reconcile", Org: org, ConfigID: configID.String(), Targets: []string{"git"}})
	}
	// A source-scoped child sibling does not count toward desired.
	{
		id := integration("child sibling ignored", "github")
		dataset(id, "prs", true, `{}`)
		source := uuid.New()
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repository', 'a/b', 'b', 'a/b', '{}', true, $4, $4)`, source, org, id, at)
		config(&id, "child", "github", `["prs"]`, `{}`, true, &source)
		configID := config(&id, "child sibling ignored", "github", `["prs"]`, `{}`, true, nil)
		cases = append(cases, updateWriteCase{Kind: "reconcile", Org: org, ConfigID: configID.String(), Targets: []string{"git"}})
	}

	job := func(configID uuid.UUID, provider, cron, timezone, jobConfig string, status int) {
		exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, sync_config_id, status,
is_running, run_count, failure_count, created_at, updated_at) VALUES ($1, $2, $3, 'sync', $4, $5, $6, $7::json, $8, $9, false, 3, 1, $10, $10)`,
			uuid.New(), org, "sync-config-"+configID.String(), provider, cron, timezone, jobConfig, configID, status, at)
	}
	upsert := func(name, provider, options string, active bool, existing func(uuid.UUID)) {
		configID := config(nil, name, provider, `["git"]`, options, active, nil)
		if existing != nil {
			existing(configID)
		}
		cases = append(cases, updateWriteCase{Kind: "upsert", Org: org, ConfigID: configID.String()})
	}
	upsert("job insert scheduled", "GitHub", `{"schedule_cron": "0 */6 * * *", "timezone": "Europe/Paris"}`, true, nil)
	upsert("job insert manual", "jira", `{"timezone": ""}`, true, nil)
	upsert("job insert paused", "github", `{"schedule_cron": "0 0 * * *"}`, false, nil)
	upsert("job insert pairs options", "github", `[["schedule_cron", "5 * * * *"], ["timezone", "UTC"]]`, true, nil)
	upsert("job update", "github", `{"schedule_cron": "0 1 * * *", "timezone": "Asia/Tokyo"}`, true, func(id uuid.UUID) {
		job(id, "github", "0 * * * *", "UTC", `{"provider": "github", "sync_config_id": "`+id.String()+`"}`, 1)
	})
	upsert("job unchanged", "github", `{"schedule_cron": "0 2 * * *"}`, true, func(id uuid.UUID) {
		job(id, "github", "0 2 * * *", "UTC", `{"sync_config_id": "`+id.String()+`", "provider": "github"}`, 0)
	})
	upsert("job to paused", "github", `{}`, true, func(id uuid.UUID) {
		job(id, "github", "0 3 * * *", "UTC", `{"provider": "github", "sync_config_id": "`+id.String()+`"}`, 0)
	})
	return cases
}

// asyncpgURI is a venue Postgres URI for SQLAlchemy's asyncpg dialect:
// asyncpg takes no libpq query parameters such as sslmode.
func asyncpgURI(t *testing.T, uri string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	parsed.Scheme = "postgresql+asyncpg"
	parsed.RawQuery = ""
	return parsed.String()
}
