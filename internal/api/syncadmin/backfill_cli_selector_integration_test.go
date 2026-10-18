//go:build integration

package syncadmin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/backfillrun"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// TestBackfillCLISelectorWritesTheTriggerRowTheWizardWrites: the admin Backfill
// wizard (POST .../backfill with a selector) and `dho backfill run
// --source-ids --dataset-keys` are two doors to one planner input, the
// sync_manual_triggers row. For the same window and the same scope the two rows
// must be equal in every column the planner reads, so the planner cannot tell
// which door a backfill came through.
func TestBackfillCLISelectorWritesTheTriggerRowTheWizardWrites(t *testing.T) {
	v := startCascadeVenue(t, false)
	ctx := context.Background()
	datasets := []string{"repo-metadata", "commits", "commit-stats", "files"}
	one := v.seed("github", `["git"]`, datasets)
	var sources []string
	// The sources are selected for the configuration: `dho backfill run`
	// accepts only a source the configuration's scheduled runs plan
	// (CHAOS-10064), as the wizard lists only those.
	selectedFor := fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, one.config.String())
	for index := 0; index < 4; index++ {
		id := uuid.New()
		name := fmt.Sprintf("acme/selector-%d", index)
		v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', $4, $4, $4, $5::json, true, now(), now())`, id, v.org, one.integration, name, selectedFor)
		sources = append(sources, id.String())
	}
	// Two of the four sources and two of the four datasets, in the order the
	// planner lists them (full_name, then the provider's dataset-key order).
	scopedSources := []string{sources[0], sources[2]}
	scopedKeys := []string{"commits", "files"}

	status, body := v.call("POST", "/api/v1/admin/sync-configs/"+one.config.String()+"/backfill", fmt.Sprintf(
		`{"selector":{"since":"2026-09-01T00:00:00Z","before":"2026-09-03T23:59:59.999999Z","source_ids":["%s","%s"],"dataset_keys":["%s","%s"]}}`,
		scopedSources[0], scopedSources[1], scopedKeys[0], scopedKeys[1]))
	if status != http.StatusAccepted {
		t.Fatalf("the wizard route answered %d %s", status, body)
	}

	// The verb itself, from its argv: flag parsing and wiring are part of what
	// is measured, not only backfillrun.Start.
	connection := v.pool.Config().ConnString()
	var stdout, stderr strings.Builder
	code := backfillrun.Verb().Run(ctx, cli.Env{
		Args: []string{"--config-id", one.config.String(), "--since", "2026-09-01", "--before", "2026-09-04", "--no-wait",
			"--source-ids", scopedSources[0], "--source-ids", scopedSources[1], "--dataset-keys", scopedKeys[0] + "," + scopedKeys[1]},
		Lookup: func(key string) (string, bool) { return connection, key == "COORDINATOR_DATABASE_URI" },
		Stdout: &stdout, Stderr: &stderr,
	})
	if code != cli.ExitOK {
		t.Fatalf("the verb exited %d: %s", code, stderr.String())
	}

	rows, err := v.pool.Query(ctx, `SELECT t.mode || '|' || t.since::text || '|' || t.before::text || '|' || coalesce(t.source_ids::text, 'NULL') || '|' ||
	coalesce(t.dataset_keys::text, 'NULL') || '|' || t.triggered_by
FROM sync_manual_triggers t JOIN scheduled_sync_occurrences o ON o.occurrence_id = t.occurrence_id
WHERE o.sync_config_id = $1 ORDER BY o.scheduled_for`, one.config)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var written []string
	for rows.Next() {
		var row string
		if err := rows.Scan(&row); err != nil {
			t.Fatal(err)
		}
		written = append(written, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(written) != 2 {
		t.Fatalf("%d trigger rows, want the wizard's and the verb's: %v", len(written), written)
	}
	t.Logf("rows=%q", written)
	if written[0] != written[1] {
		t.Errorf("the wizard wrote and the verb wrote different trigger rows:\n%s\n%s", written[0], written[1])
	}
}
