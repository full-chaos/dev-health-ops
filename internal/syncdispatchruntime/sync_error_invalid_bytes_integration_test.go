//go:build integration

package syncdispatchruntime

import (
	"github.com/jackc/pgx/v5"
	"testing"
	"time"
)

// CHAOS-8404 (declared difference of the sanitizer change): the sanitized sync error text is written to Postgres `text` columns, which REJECT an invalid UTF-8
// byte (SQLSTATE 22021). The sync path therefore replaces each invalid byte with one U+FFFD (the pin test of the sanitizer package) and the write succeeds. This row runs the
// real write (terminalizeFeatureDisabledGraph's UPDATE of sync_run_reference_discoveries.error, a `text` column of the migrated schema) with an error text that holds an
// invalid byte and a credential, and reads the stored text back. RED if the byte reaches the column again (the write fails) or the credential is stored.
func TestAnInvalidByteInTheSyncErrorTextIsStoredAsAReplacementRuneAndTheWriteSucceeds(t *testing.T) {
	ctx, pool := startFeatureDisabledPool(t)
	createFeatureDisabledTables(t, ctx, pool)
	seedFeatureDisabledRun(t, ctx, pool, 1)
	if _, err := pool.Exec(ctx, `
INSERT INTO sync_run_reference_discoveries (id,sync_run_id,org_id,status,attempts,available_at,created_at,updated_at)
VALUES ('00000000-0000-4000-8000-0000000000e4',$1,$2,'planned',0,now(),now(),now())`,
		featureDisabledTestRun, featureDisabledTestOrg); err != nil {
		t.Fatal(err)
	}
	run := &finalizeSyncRun{
		id: featureDisabledTestRun, orgID: featureDisabledTestOrg, integrationID: featureDisabledTestIntegration,
		status: "dispatching", totalUnits: 1,
	}
	withTx(t, ctx, pool, func(tx pgx.Tx) {
		if err := terminalizeFeatureDisabledGraph(ctx, tx, run, "prefix-\xff-Bearer abc", time.Now().UTC()); err != nil {
			t.Fatalf("terminalizeFeatureDisabledGraph with an invalid byte in the error text: %v", err)
		}
	})
	var stored string
	if err := pool.QueryRow(ctx, `SELECT error FROM sync_run_reference_discoveries WHERE sync_run_id=$1`, featureDisabledTestRun).Scan(&stored); err != nil {
		t.Fatal(err)
	}
	if want := "prefix-�-[REDACTED]"; stored != want {
		t.Fatalf("stored error = %q, want %q (one replacement rune for the invalid byte, the credential redacted)", stored, want)
	}
}
