//go:build integration

package backfillrun

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// TestBackfillRunNamesExactlyTheEnabledDatasetRows runs the verb on
// whole-integration configurations whose dataset rows do NOT agree with their
// stored sync_targets (CHAOS-8816) and requires, for github, gitlab, jira and
// linear: the run names exactly the enabled rows the provider supports, in
// DatasetKey order; no row that is off, no row that does not exist and no
// unsupported key is named, so the scheduler has no key to insert a row for;
// the verb itself writes no dataset row; an integration with no enabled
// supported row is refused and nothing is written.
func TestBackfillRunNamesExactlyTheEnabledDatasetRows(t *testing.T) {
	instance, admin := startDatabase(t)
	uri := freshDatabase(t, instance, admin)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := conn.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("seed: %v\n%s", err, sql)
		}
	}
	type shape struct {
		n        int
		provider string
		stored   string
		on, off  []string
		// want is the key list the run must name; nil = the run is refused.
		want []string
	}
	shapes := []shape{
		// The production shape of a GitHub integration: four git rows on and
		// no blame row, the automatic security row, an "incidents" row GitHub
		// has no dataset for; prs present and off. The stored list names
		// git, prs and work-items: a target-computed list would add blame,
		// the three prs keys and the five work-item keys.
		{n: 101, provider: "github", stored: `["git", "prs", "work-items"]`,
			on: []string{"repo-metadata", "commits", "commit-stats", "files", "security", "incidents"}, off: []string{"prs"},
			want: []string{"repo-metadata", "commits", "commit-stats", "files", "security"}},
		// GitLab, mixed families: one prs member on, the canonical row off.
		{n: 102, provider: "GitLab", stored: `["git"]`,
			on: []string{"commits", "pr-comments", "incidents"}, off: []string{"prs", "blame"},
			want: []string{"commits", "pr-comments", "incidents"}},
		// Jira: the incidents row is on though the stored list never named
		// its target; one work-item member is off.
		{n: 103, provider: "jira", stored: `["work-items"]`,
			on: []string{"work-items", "work-item-labels", "incidents"}, off: []string{"work-item-comments"},
			want: []string{"incidents", "work-items", "work-item-labels"}},
		// Linear: the stored list is empty, two rows are on.
		{n: 104, provider: "linear", stored: `[]`,
			on: []string{"work-item-history", "work-items"}, want: []string{"work-items", "work-item-history"}},
		// Every row present and OFF (the migration 0108 shape): refused,
		// though the stored list names the target.
		{n: 105, provider: "linear", stored: `["work-items"]`,
			off: []string{"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"}},
		// No row at all, and only an unsupported key on: refused.
		{n: 106, provider: "jira", stored: `["work-items"]`},
		{n: 107, provider: "github", stored: `["git"]`, on: []string{"incidents"}},
	}
	for _, s := range shapes {
		integration, config := uuidN(0x1a, s.n), uuidN(0xc0, s.n)
		exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, '{}'::json, true, now(), now())`, integration, testOrg, s.provider, fmt.Sprintf("integration-%d", s.n))
		exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1::uuid, $2, $3::uuid, $4, 'repo', $5, $5, $5, '{}'::json, true, now(), now())`,
			uuidN(0x5a, s.n*10), testOrg, integration, s.provider, fmt.Sprintf("org/repo-%d", s.n))
		exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed, integration_id, created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, $5::json, '{}'::json, true, true, $6::uuid, now(), now())`,
			config, testOrg, fmt.Sprintf("rows-%d", s.n), s.provider, s.stored, integration)
		for _, key := range s.on {
			exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES (gen_random_uuid(), $1, $2::uuid, $3, true, '{}'::json)`, testOrg, integration, key)
		}
		for _, key := range s.off {
			exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES (gen_random_uuid(), $1, $2::uuid, $3, false, '{}'::json)`, testOrg, integration, key)
		}
	}
	datasetRows := func() string {
		t.Helper()
		result, err := conn.Query(ctx, `SELECT integration_id::text || ' ' || dataset_key || '=' || is_enabled::text FROM integration_datasets ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		lines, err := pgx.CollectRows(result, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		return strings.Join(lines, "\n")
	}
	before := datasetRows()
	started, refused := 0, 0
	for _, s := range shapes {
		run := scenario{name: fmt.Sprintf("%s rows-%d", s.provider, s.n), id: uuidN(0xc0, s.n), args: []string{"--backfill", "1", "--before", "2026-03-10"}}
		reset(t, uri, run)
		code, _, stderr := goRun(t, uri, run)
		named := triggerDatasetKeys(t, uri)
		if s.want == nil {
			refused++
			if code != cli.ExitRefused || !strings.Contains(stderr, "has no enabled dataset") {
				t.Errorf("%s: exit %d, want the refusal (%d); stderr:\n%s", run.name, code, cli.ExitRefused, stderr)
			}
			for table, lines := range rows(t, uri) {
				if len(lines) != 0 {
					t.Errorf("%s: the refused run left %d rows in %s", run.name, len(lines), table)
				}
			}
			continue
		}
		started++
		if code != cli.ExitOK {
			t.Errorf("%s: exit %d; stderr:\n%s", run.name, code, stderr)
			continue
		}
		if len(named) != 1 || fmt.Sprint(named[0]) != fmt.Sprint(s.want) {
			t.Errorf("%s: the run names %v, want exactly the enabled supported rows %v", run.name, named, s.want)
		}
	}
	if started == 0 || refused == 0 {
		t.Fatalf("%d runs started, %d refused: both kinds must run", started, refused)
	}
	if after := datasetRows(); after != before {
		t.Errorf("the verb changed dataset rows\nbefore:\n%s\nafter:\n%s", before, after)
	}
}
