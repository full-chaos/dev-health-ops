//go:build integration

package syncdispatchruntime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
)

// CHAOS-14371: providersync settles a unit write whose COMMIT acknowledgement
// was lost by reading the unit back with SELECT ... FOR SHARE. A row lock needs
// UPDATE privilege on the table, so this runs the REAL read-back statement
// (providersync.ReadBackUnitWriteSQL) under the NOSUPERUSER worker role of the
// go-river-migrate grants, beside the other worker-role cells for providersync
// statements (TestBackfillClaimCarriesTheNextChunkAsTheDomainRole). A grant
// that loses UPDATE on sync_run_units turns it red with a 42501.
func TestStoreWriteReadBackRunsAsTheDomainRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	fixture := startBackfillNextVenue(t, ctx)
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	unitID := fixture.unit(t, ctx, backfillNextUnit{
		runID: fixture.run(t, ctx), sourceID: fixture.sourceID, dataset: "tests", mode: "backfill",
		since: since, before: since.AddDate(0, 0, 7),
	})
	now := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	claim := fixture.claim(t, ctx, unitID, now)

	tx, err := fixture.venue.Domain.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT set_config('lock_timeout', '5000ms', true)`); err != nil {
		t.Fatalf("set lock_timeout as the domain role: %v", err)
	}
	var committed, ours bool
	if err := tx.QueryRow(ctx, providersync.ReadBackUnitWriteSQL,
		claim.ID, claim.OrgID, "success", now.Add(time.Second), claim.Owner,
	).Scan(&committed, &ours); err != nil {
		t.Fatalf("read-back as the domain role: %v (refused: %q)", err, fixture.venue.Denials.All())
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if committed || !ours {
		t.Fatalf("read-back of a running claimed unit: committed=%t ours=%t, want false and true", committed, ours)
	}
	if refused := fixture.venue.Denials.All(); len(refused) > 0 {
		t.Fatalf("the domain role was refused: %s", strings.Join(refused, "; "))
	}
}
