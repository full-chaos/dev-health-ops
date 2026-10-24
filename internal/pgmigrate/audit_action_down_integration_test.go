//go:build integration

package pgmigrate_test

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

const markerBackfillAction = "metrics.daily_marker_backfill"

func auditActionAllowed(t *testing.T, uri, action string) bool {
	t.Helper()
	conn := connect(t, uri)
	_, err := conn.Exec(context.Background(), `INSERT INTO worker_operator_audits (
		credential_id, principal_type, principal_id, action, resource_type, resource_id,
		reason_code, correlation_id, status, created_at)
		VALUES (NULL, 'operator', 'dho-workers', $1, 'organization', '*', 'operator_test', 'corr-down', 'started', now())`, action)
	return err == nil
}

// The revision that adds the marker backfill action goes down only while no audit row holds that action. A refused
// downgrade names the count, leaves the row, the schema and the recorded revision as they were, and deletes nothing;
// once the row is gone the same downgrade runs and the check is the narrow one again.
func TestMarkerBackfillAuditActionDowngradeRefusesWhileRowsHoldIt(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	position := 0
	for index, file := range chain {
		if strings.Contains(file.Name, "widen_worker_operator_audits_action_marker_backfill") {
			position = index + 1
		}
	}
	if position == 0 {
		t.Fatal("the revision that widens the audit action check is not in the chain")
	}
	d := newDownInstance(t)
	uri := d.at(t, position)
	if !auditActionAllowed(t, uri, markerBackfillAction) {
		t.Fatalf("the migrated check refuses %s", markerBackfillAction)
	}
	conn := connect(t, uri)
	if _, err := conn.Exec(context.Background(), `INSERT INTO worker_operator_audits (
		credential_id, principal_type, principal_id, action, resource_type, resource_id,
		reason_code, correlation_id, status, created_at)
		VALUES (NULL, 'operator', 'dho-workers', $1, 'organization', '*', 'operator_test', 'corr-down-2', 'started', now())`, markerBackfillAction); err != nil {
		t.Fatal(err)
	}
	before := schemaShape(t, uri)

	code, _, stderr := goDowngrade(t, uri, previousName(chain, position-1))
	if code != cli.ExitFailure || !strings.Contains(stderr, "2 worker_operator_audits rows hold action "+markerBackfillAction) {
		t.Fatalf("downgrade with rows: exit %d stderr %q, want a refusal naming 2 rows", code, stderr)
	}
	var rows int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM worker_operator_audits WHERE action = $1`, markerBackfillAction).Scan(&rows); err != nil || rows != 2 {
		t.Fatalf("audit rows after the refused downgrade = %d (err %v), want 2", rows, err)
	}
	if after := schemaShape(t, uri); after != before {
		t.Fatalf("a refused downgrade changed the schema:\n%s", shapeDiff(before, after))
	}

	// Only the test removes the rows, to prove the no-row case; the downgrade itself never deletes.
	if _, err := conn.Exec(context.Background(), `DELETE FROM worker_operator_audits WHERE action = $1`, markerBackfillAction); err != nil {
		t.Fatal(err)
	}
	code, stdout, stderr := goDowngrade(t, uri, previousName(chain, position-1))
	if code != cli.ExitOK {
		t.Fatalf("downgrade with no rows: exit %d stdout %s stderr %s", code, stdout, stderr)
	}
	if auditActionAllowed(t, uri, markerBackfillAction) {
		t.Fatalf("the check still allows %s after the downgrade", markerBackfillAction)
	}
	if !auditActionAllowed(t, uri, "metrics.daily_start") {
		t.Fatal("the restored check refuses an action it allowed before")
	}
}
