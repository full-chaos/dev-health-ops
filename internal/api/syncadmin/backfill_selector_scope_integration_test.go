//go:build integration

package syncadmin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// TestBackfillSelectorRefusesAnEnabledSourceTheConfigurationDoesNotSelect:
// the Sync admin Backfill wizard's selector names sources explicitly. An
// enabled source the planner-managed configuration does not select is outside
// the scope its scheduled runs plan (CHAOS-10064), so the selector is refused
// with 400 and no trigger is written; a selected source is accepted.
func TestBackfillSelectorRefusesAnEnabledSourceTheConfigurationDoesNotSelect(t *testing.T) {
	v := startCascadeVenue(t, false)
	ctx := context.Background()
	one := v.seed("github", `["git"]`, []string{"repo-metadata", "commits"})
	selected, unselected := uuid.New(), uuid.New()
	v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', 'acme/selected', 'acme/selected', 'acme/selected', $4::json, true, now(), now())`,
		selected, v.org, one.integration, fmt.Sprintf(`{"planner_managed_sync_config_id": %q}`, one.config.String()))
	v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata, is_enabled, discovered_at, last_seen_at)
VALUES ($1, $2, $3, 'github', 'repo', 'acme/unselected', 'acme/unselected', 'acme/unselected', '{}'::json, true, now(), now())`,
		unselected, v.org, one.integration)
	triggers := func() int {
		t.Helper()
		var count int
		if err := v.pool.QueryRow(ctx, `SELECT count(*) FROM sync_manual_triggers t JOIN scheduled_sync_occurrences o ON o.occurrence_id = t.occurrence_id
WHERE o.sync_config_id = $1`, one.config).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	path := "/api/v1/admin/sync-configs/" + one.config.String() + "/backfill"
	selector := func(source uuid.UUID, day string) string {
		return fmt.Sprintf(`{"selector":{"since":"2026-09-%sT00:00:00Z","before":"2026-09-%sT12:00:00Z","source_ids":[%q,%q]}}`, day, day, selected.String(), source.String())
	}

	status, body := v.call("POST", path, selector(unselected, "01"))
	if status != http.StatusBadRequest || !strings.Contains(body, unselected.String()) {
		t.Fatalf("selector with an unselected source: %d %s, want 400 naming it", status, body)
	}
	if got := triggers(); got != 0 {
		t.Fatalf("a refused selector wrote %d trigger rows", got)
	}

	status, body = v.call("POST", path, selector(selected, "02"))
	if status != http.StatusAccepted {
		t.Fatalf("selector with selected sources: %d %s, want 202", status, body)
	}
	if got := triggers(); got != 1 {
		t.Fatalf("an accepted selector wrote %d trigger rows, want 1", got)
	}
}
