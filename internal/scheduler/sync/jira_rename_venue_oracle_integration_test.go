//go:build integration

package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/programoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonJiraRenameProgram runs the api's own
// discover_sources_for_integration for each case's integration, with the
// Jira client's project listing returning the case's projects, the stored
// credential resolved to fixed fields, and the discovery clock pinned. The
// database is the Python plane's, named by the environment of the run.
const pythonJiraRenameProgram = `
import json, os, sys, uuid
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from dev_health_ops.providers.jira import client as jira_client
from dev_health_ops.sync import discovery
payload = json.loads(sys.stdin.read())
pinned = datetime.fromisoformat(payload["now"])
discovery._now_utc = lambda: pinned
discovery._resolve_credentials = lambda integration: {"base_url": "https://example.atlassian.net", "email": "e@example.com", "api_token": "t"}
engine = create_engine(os.environ["ORACLE_DATABASE_URI"], poolclass=NullPool)
out = []
for case in payload["cases"]:
    jira_client.JiraClient.get_all_projects = lambda self, projects=case["projects"]: projects
    with Session(engine) as session:
        try:
            discovery.discover_sources_for_integration(session, uuid.UUID(case["integration_id"]))
            session.commit()
            out.append("ok")
        except Exception as exc:
            session.rollback()
            out.append("raise " + type(exc).__name__ + ": " + str(exc)[:200])
engine.dispose()
print(json.dumps(out))
`

type jiraRenameCase struct {
	IntegrationID string `json:"integration_id"`
	ConfigID      string `json:"-"`
	Projects      []any  `json:"projects"`
}

// fixedJiraCredentials hands every resolution the same Jira fields; the
// oracle compares the rows discovery writes, not credential handling.
type fixedJiraCredentials struct{}

func (fixedJiraCredentials) ResolveEncrypted(_ context.Context, scope providerfoundation.TenantScope) (providerfoundation.EncryptedCredential, error) {
	return providerfoundation.EncryptedCredential{ID: scope.CredentialID, Provider: "jira", Name: "jira", Active: true,
		Ciphertext: secrets.NewValue("fixed")}, nil
}

func (fixedJiraCredentials) Decrypt(secrets.Value) ([]byte, error) {
	return []byte(`{"base_url": "https://example.atlassian.net", "email": "e@example.com", "api_token": "t"}`), nil
}

// TestJiraRenameVenueOracleMatchesFrozenPython holds the Go discovery's Jira
// upsert -- rename by jira_project_id, the case-variant duplicate fold, and
// the sync watermark moves both make -- to the api's own
// discover_sources_for_integration over two copies of one seeded database,
// case by case: the same outcome, then the same integration_sources and
// sync_watermarks rows (watermark updated_at compared as seed or moved). The
// Python outcomes and the Python plane's rows are frozen: they were executed
// and read once, on a venue of the pinned build.
func TestJiraRenameVenueOracleMatchesFrozenPython(t *testing.T) {
	golden, root := openVenueGolden(t, "venue-jira-rename.golden.json")
	ctx := context.Background()
	const org = "00000000-0000-4000-8000-00000000fa11"
	const seedAt = "2026-01-01 00:00:00+00"
	pinned := time.Date(2026, 9, 25, 9, 30, 0, 123456000, time.UTC)
	var cases []jiraRenameCase
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		Golden: golden,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			cases = seedJiraRenameCases(t, ctx, admin, org, seedAt)
			return nil
		},
	})

	input, err := json.Marshal(map[string]any{"now": pinned.Format(time.RFC3339Nano), "cases": cases})
	if err != nil {
		t.Fatal(err)
	}
	answer := programoracle.Produce(t, golden, root, []programoracle.Program{{
		Name: "jira rename discovery", Text: pythonJiraRenameProgram, Stdin: input,
		Env:    map[string]string{"OTEL_SDK_DISABLED": "true", "ENVIRONMENT": "test"},
		PerRun: oracleDatabase(t, venue), PerRunNames: []string{oracleDatabaseEnv},
	}})[0]
	if answer.ExitCode != 0 {
		t.Fatalf("the Jira rename program exited %d when it was recorded: %s", answer.ExitCode, answer.Stdout)
	}
	var pythonOutcomes []string
	if err := json.Unmarshal([]byte(answer.Stdout), &pythonOutcomes); err != nil || len(pythonOutcomes) != len(cases) {
		t.Fatalf("decode python outcomes: %v\n%s", err, answer.Stdout)
	}
	// Every case is built to complete: a Python failure here is a harness
	// or fixture fault, never agreement to be matched.
	for index, outcome := range pythonOutcomes {
		if outcome != "ok" {
			t.Fatalf("Python case %d did not complete: %s", index, outcome)
		}
	}

	// The Go side runs as the api role: the sync config create route (and
	// the update route, once served) runs this discovery on that role, so its
	// grants (the widened sync_watermarks UPDATE/DELETE included) are
	// exercised here.
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	for index, c := range cases {
		page, _ := json.Marshal(map[string]any{"values": c.Projects, "isLast": true})
		service := &NativeSourceDiscoveryService{
			domainPool:  pool,
			credentials: providerfoundation.CredentialResolver{Repository: fixedJiraCredentials{}, Decryptor: fixedJiraCredentials{}},
			doer:        &fakeSourceDiscoveryDoer{t: t, body: string(page)},
			retry:       fastRetry(),
			logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
			telemetry:   newSourceDiscoveryTelemetry(),
			now:         func() time.Time { return pinned },
		}
		credential := venueoracle.StableUUID(fmt.Sprintf("jira rename: go credential %d", index))
		outcome := "ok"
		_, err := service.Discover(ctx, SourceDiscoveryArgs{
			OrgID: org, IntegrationID: c.IntegrationID, CredentialID: &credential, Provider: "jira",
			SyncOptions: map[string]any{}, ConfigID: c.ConfigID, PlannerManaged: true,
		})
		if err != nil {
			outcome = "raise"
		}
		pythonOutcome := pythonOutcomes[index]
		if strings.HasPrefix(pythonOutcome, "raise") {
			pythonOutcome = "raise"
		}
		if outcome != pythonOutcome {
			t.Errorf("case %d: go %s (%v), python %s", index, outcome, err, pythonOutcomes[index])
		}
	}

	queries := map[string]string{
		"integration_sources": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', s.id, i.name, s.provider,
  s.source_type, s.external_id, s.name, s.full_name, replace(s.metadata::text, c.id::text, 'CFG'), s.is_enabled, s.discovered_at, s.last_seen_at) AS r
  FROM integration_sources s JOIN integrations i ON i.id = s.integration_id
  LEFT JOIN sync_configurations c ON c.integration_id = s.integration_id AND c.planner_managed WHERE s.discovered_at = '` + seedAt + `'
  UNION ALL SELECT concat_ws(' | ', 'new', i.name, s.provider, s.source_type, s.external_id, s.name, s.full_name,
  replace(s.metadata::text, c.id::text, 'CFG'), s.is_enabled, s.discovered_at, s.last_seen_at)
  FROM integration_sources s JOIN integrations i ON i.id = s.integration_id
  LEFT JOIN sync_configurations c ON c.integration_id = s.integration_id AND c.planner_managed WHERE s.discovered_at != '` + seedAt + `') AS rows`,
		"sync_watermarks": `SELECT coalesce(string_agg(r, E'\n' ORDER BY r), '') FROM (SELECT concat_ws(' | ', id, org_id, repo_id, source_id, target,
  dataset_key, coalesce(last_synced_at::text, '<null>'), CASE WHEN updated_at = '` + seedAt + `' THEN 'seed' ELSE 'moved' END) AS r
  FROM sync_watermarks) AS rows`,
	}
	source, goDB := venue.AdminURI(t, venue.SourceDB), venue.AdminURI(t, venue.GoDB)
	for _, table := range []string{"integration_sources", "sync_watermarks"} {
		goRows := venueoracle.TableRows(t, ctx, goDB, queries[table])
		golden.CompareRows(t, "rows:"+table, func() string { return venueoracle.TableRows(t, ctx, source, queries[table]) }, goRows)
		t.Logf("%s: %d rows identical", table, strings.Count(goRows, "\n")+map[bool]int{true: 0, false: 1}[goRows == ""])
	}
	// The writes happened: a renamed row, moved watermarks, a deleted
	// superseded watermark (fewer rows than seeded).
	state := venueoracle.TableRows(t, ctx, goDB, `SELECT
  (SELECT count(*) FROM integration_sources WHERE external_id = 'NEWKEY'),
  (SELECT count(*) FROM sync_watermarks WHERE source_id = 'NEWKEY'),
  (SELECT count(*) FROM sync_watermarks WHERE updated_at != '`+seedAt+`'),
  (SELECT count(*) FROM sync_watermarks)`)
	if fields := strings.Fields(state); len(fields) != 4 || fields[0] == "0" || fields[1] == "0" || fields[2] == "0" {
		t.Errorf("writes not observed (renamed row, moved watermarks, touched watermarks, remaining) = %s", state)
	}
	t.Logf("%d cases; write state: %s", len(cases), state)
	golden.SkipDiff(t)
	venueoracle.WriteGoOnlyProof(t, "Go's Jira rename discovery against the frozen outcomes and rows of Python's discover_sources_for_integration")
	golden.Finish(t)
}

// seedJiraRenameCases writes one Jira integration per case, with its
// planner-managed config, the sources a previous discovery left and their
// sync watermarks, and returns the cases with the project list each
// discovery run sees. Every id is stable: the integration ids are in the
// program's input, and the seeded source and watermark ids are in the
// compared rows.
func seedJiraRenameCases(t *testing.T, ctx context.Context, admin *pgxpool.Pool, org, at string) []jiraRenameCase {
	t.Helper()
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	exec(`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES ($1, 'rename', 'rename', '{}', 'enterprise', true, $2, $2)`, org, at)
	credential := venueoracle.StableUUID("jira rename: credential")
	exec(`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, config, created_at, updated_at) VALUES ($1, $2, 'jira', 'jira', true, '{}', $3, $3)`,
		credential, org, at)
	var cases []jiraRenameCase
	add := func(name string, projects []any, sources [][4]string, watermarks [][4]string) {
		integrationID := venueoracle.StableUUID("jira rename: integration " + name)
		configID := venueoracle.StableUUID("jira rename: configuration " + name)
		exec(`INSERT INTO integrations (id, org_id, provider, credential_id, name, config, is_active, created_at, updated_at) VALUES ($1, $2, 'jira', $3, $4, '{}', true, $5, $5)`,
			integrationID, org, credential, name, at)
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1, $2, $3, 'jira', '["work-items"]', '{}', true, true, $4, $5, $5)`, configID, org, name, integrationID, at)
		for index, source := range sources {
			// external_id, enabled, metadata, discovered_at offset (minutes)
			exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'jira', 'project', $4, $4, $4, $5::json, $6, $7::timestamptz, $7::timestamptz)`,
				venueoracle.StableUUID(fmt.Sprintf("jira rename: source %s %d", name, index)), org, integrationID, source[0], source[2], source[1] == "true", at)
		}
		for index, watermark := range watermarks {
			// source_id, dataset_key, last_synced_at, repo_id
			var synced any
			if watermark[2] != "" {
				synced = watermark[2]
			}
			exec(`INSERT INTO sync_watermarks (id, org_id, repo_id, source_id, target, dataset_key, last_synced_at, updated_at) VALUES ($1, $2, $3, $4, 'work-items', $5, $6, $7)`,
				venueoracle.StableUUID(fmt.Sprintf("jira rename: watermark %s %d", name, index)), org, watermark[3], watermark[0], watermark[1], synced, at)
		}
		cases = append(cases, jiraRenameCase{IntegrationID: integrationID, ConfigID: configID, Projects: projects})
	}
	project := func(id, key string) any {
		return map[string]any{"id": id, "key": key, "name": key + " project", "projectTypeKey": "software"}
	}
	add("simple rename", []any{project("10001", "NEWKEY")},
		[][4]string{{"OLDKEY", "true", `{"project_type_key": "software", "discovered_project": true, "jira_project_id": "10001"}`}},
		[][4]string{{"OLDKEY", "work-items", "2026-05-01 00:00:00+00", "r1"}, {"OLDKEY", "work-item-history", "", "r2"}})
	add("rename onto an existing watermark", []any{project("10002", "NEWKEY")},
		[][4]string{{"GONE", "true", `{"jira_project_id": "10002"}`}},
		[][4]string{{"GONE", "work-items", "2026-06-01 00:00:00+00", "r3"}, {"NEWKEY", "work-items", "2026-04-01 00:00:00+00", "r4"},
			{"GONE", "work-item-comments", "2026-02-01 00:00:00+00", "r5"}, {"NEWKEY", "work-item-comments", "2026-03-01 00:00:00+00", "r6"}})
	add("case-variant duplicates", []any{project("10003", "ENG")},
		[][4]string{{"eng", "true", `{"jira_project_id": "10003"}`}, {"ENG", "false", `{}`}},
		[][4]string{{"eng", "work-items", "2026-05-05 00:00:00+00", "r7"}, {"ENG", "work-item-history", "2026-05-07 00:00:00+00", "r11"},
			{"ENG", "work-items", "2026-05-09 00:00:00+00", "r12"}})
	add("numeric stored id is not a match", []any{project("10004", "NUMNEW")},
		[][4]string{{"NUMOLD", "true", `{"jira_project_id": 10004}`}}, nil)
	add("unchanged key", []any{project("10005", "SAME")},
		[][4]string{{"SAME", "true", `{"jira_project_id": "10005", "kept": 1}`}}, [][4]string{{"SAME", "work-items", "2026-05-05 00:00:00+00", "r8"}})
	add("two rename candidates", []any{project("10006", "TWONEW")},
		[][4]string{{"TWOA", "true", `{"jira_project_id": "10006"}`}, {"TWOB", "false", `{"jira_project_id": "10006"}`}},
		[][4]string{{"TWOA", "work-items", "2026-05-05 00:00:00+00", "r9"}, {"TWOB", "work-items", "2026-05-06 00:00:00+00", "r10"}})
	add("no project id", []any{map[string]any{"key": "NOID", "name": "No id"}},
		[][4]string{{"OLDNOID", "true", `{}`}}, nil)
	return cases
}
