//go:build integration

package postgres

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestDiagnoseRolePostureNamesTheGapAndNeverLeaksConnectionMaterial is the
// regression test for the CHAOS-3142 diagnosability gap: a
// coordinator_postgres readiness failure used to surface only as a check
// name, with no reason logged anywhere, because CheckRolePosture collapses
// every failure mode into the same opaque ErrUnavailable. Diagnosing the
// real incident (coordinatorPosture() required fixed_schedule_occurrences,
// which did not exist on a database two Alembic migrations behind head, so
// coordinatorGrantStatements' to_regclass guard silently skipped its GRANT)
// took manually re-deriving and re-running the posture's table list by hand
// against the live database.
//
// This pins two things DiagnoseRolePosture must get right, using the SAME
// production grant harness (startGrantHarness / ApplyPinnedMigrations) the
// coordinator statement-privilege suite uses, so the roles and grants under
// test are the real ones, not a model of them:
//
//  1. A missing-table gap and a missing-privilege gap are both reported,
//     each naming the actual table (and, for the privilege gap, the actual
//     missing privilege) -- not a generic "something is wrong".
//  2. Every gap's rendered log line (PostureGap.String(), exactly what
//     cmd/dev-health-reconciler/dependencies.go's logCoordinatorPostureGaps
//     logs) contains the table/privilege it names and NEVER contains this
//     test's own connection material: the full connection URI, the
//     coordinator role's password, or a bare "postgres://" scheme prefix.
func TestDiagnoseRolePostureNamesTheGapAndNeverLeaksConnectionMaterial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)

	// Missing-table gap: a table the posture requires that was never
	// created. This is exactly CHAOS-3142's shape -- a synthetic posture is
	// used here (rather than dropping a table the harness's other suites
	// depend on) purely to isolate the missing-table path.
	const missingTable = "table_that_does_not_exist_for_this_test"
	tableGaps, err := DiagnoseRolePosture(ctx, coordinator, grantCoordinatorRole, RolePosture{
		RequiredTables: []TablePrivilege{{TableName: missingTable, AllowUpdate: true}},
	})
	if err != nil {
		t.Fatalf("DiagnoseRolePosture (missing table): %v", err)
	}
	if len(tableGaps) != 1 || !tableGaps[0].TableMissing || tableGaps[0].TableName != missingTable {
		t.Fatalf("expected exactly one table-missing gap naming %q, got %+v", missingTable, tableGaps)
	}

	// Missing-privilege gap: a real, existing coordinator table with one
	// privilege the posture requires actually revoked.
	if _, err := admin.Exec(ctx,
		"REVOKE UPDATE ON TABLE public.worker_job_outbox FROM "+grantCoordinatorRole,
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx,
			"GRANT UPDATE ON TABLE public.worker_job_outbox TO "+grantCoordinatorRole,
		); err != nil {
			t.Errorf("restore worker_job_outbox UPDATE: %v", err)
		}
	})
	privilegeGaps, err := DiagnoseRolePosture(ctx, coordinator, grantCoordinatorRole, RolePosture{
		RequiredTables: []TablePrivilege{{TableName: "worker_job_outbox", AllowUpdate: true}},
	})
	if err != nil {
		t.Fatalf("DiagnoseRolePosture (missing privilege): %v", err)
	}
	if len(privilegeGaps) != 1 || privilegeGaps[0].TableMissing || privilegeGaps[0].TableName != "worker_job_outbox" {
		t.Fatalf("expected exactly one worker_job_outbox privilege gap, got %+v", privilegeGaps)
	}
	if len(privilegeGaps[0].Missing) != 1 || privilegeGaps[0].Missing[0] != "UPDATE" {
		t.Fatalf("expected the gap to name UPDATE specifically, got %+v", privilegeGaps[0].Missing)
	}

	// The redaction guarantee: render every gap exactly the way
	// logCoordinatorPostureGaps does, and assert the line names its own
	// table/privilege but never contains this test's own connection
	// material.
	secrets := []string{uri, grantCoordinatorPass, "postgres://", "@"}
	for _, gap := range append(tableGaps, privilegeGaps...) {
		line := gap.String()
		if !strings.Contains(line, gap.TableName) {
			t.Errorf("log line %q does not name its own table %q", line, gap.TableName)
		}
		for _, secret := range secrets {
			if secret != "" && strings.Contains(line, secret) {
				t.Errorf("log line %q leaked connection material (%q)", line, secret)
			}
		}
	}
}

// TestDiagnoseRolePostureReturnsNoGapsOnceGrantsAreCorrect closes the loop:
// once the revoked privilege is restored, the diagnostic reports nothing --
// it does not fire spuriously on a healthy role.
func TestDiagnoseRolePostureReturnsNoGapsOnceGrantsAreCorrect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	_, uri := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, grantCoordinatorRole, grantCoordinatorPass)

	gaps, err := DiagnoseRolePosture(ctx, coordinator, grantCoordinatorRole, CoordinatorPosture())
	if err != nil {
		t.Fatalf("DiagnoseRolePosture: %v", err)
	}
	if len(gaps) != 0 {
		t.Errorf("expected no gaps against the real migration-emitted grants, got %+v", gaps)
	}
	if err := CheckCoordinatorAuthorization(ctx, coordinator, grantCoordinatorRole, grantSchema); err != nil {
		t.Fatalf("CheckCoordinatorAuthorization disagrees with DiagnoseRolePosture's empty result: %v", err)
	}
}
