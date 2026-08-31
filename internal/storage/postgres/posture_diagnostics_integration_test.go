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
	admin, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)

	// Missing-table gap: a table the posture requires that was never
	// created. This is exactly CHAOS-3142's shape -- a synthetic posture is
	// used here (rather than dropping a table the harness's other suites
	// depend on) purely to isolate the missing-table path.
	const missingTable = "table_that_does_not_exist_for_this_test"
	tableGaps, err := DiagnoseRolePosture(ctx, coordinator, roles.coordinator, RolePosture{
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
		"REVOKE UPDATE ON TABLE public.worker_job_outbox FROM "+roles.coordinator,
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx,
			"GRANT UPDATE ON TABLE public.worker_job_outbox TO "+roles.coordinator,
		); err != nil {
			t.Errorf("restore worker_job_outbox UPDATE: %v", err)
		}
	})
	privilegeGaps, err := DiagnoseRolePosture(ctx, coordinator, roles.coordinator, RolePosture{
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
	_, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)

	gaps, err := DiagnoseRolePosture(ctx, coordinator, roles.coordinator, CoordinatorPosture())
	if err != nil {
		t.Fatalf("DiagnoseRolePosture: %v", err)
	}
	if len(gaps) != 0 {
		t.Errorf("expected no gaps against the real migration-emitted grants, got %+v", gaps)
	}
	if err := CheckCoordinatorAuthorization(ctx, coordinator, roles.coordinator, grantSchema); err != nil {
		t.Fatalf("CheckCoordinatorAuthorization disagrees with DiagnoseRolePosture's empty result: %v", err)
	}
}

// TestDiagnoseRolePostureCatchesTableWideExcessOnAColumnScopedTable is the
// CHAOS-4675 regression test: executed, live evidence for the exact prod
// state that ticket reported, and proof it is now caught at migrate time
// instead of surfacing later as workerctl's runtime_role_unauthorized.
//
// CHAOS-4675 confirmed on the shared local stack (2026-08-31,
// has_table_privilege('devhealth_coordinator','public.integration_credentials',
// 'SELECT') = false while all five declared column grants were present) that
// go-river-migrate's own posture telemetry (dev_health_runtime_posture_missing)
// read 0 for every role -- "posture confirmed" -- in a state where a
// coordinator process would still fail at startup, IF the role had also held
// a table-wide grant left over on that same column-scoped table: nothing
// this test's baseline (pre-fix) DiagnoseRolePosture call checks would have
// noticed a table-wide grant coexisting with the correct column grants,
// because that function only ever asked "is anything declared missing."
//
// This test builds the real coordinator role from the real migration-emitted
// grants (startGrantHarness / CoordinatorPosture(), the SAME production
// manifest CheckCoordinatorAuthorization asserts against), then adds ONE
// excess table-wide GRANT on integration_credentials -- a table
// CoordinatorPosture() declares column-scoped only -- reproducing "the
// column-level grants are all present AND a table-wide grant also exists"
// without touching anything DiagnoseRolePosture already treats as missing.
//
// Before the CHAOS-4675 fix (diagnoseColumnScopedExcess wired into
// DiagnoseRolePosture): this excess grant produced ZERO gaps here --
// exactly the metric's "posture confirmed" blindness the ticket named as the
// actual root defect -- while CheckCoordinatorAuthorization still correctly
// rejected the role. After the fix: DiagnoseRolePosture reports the excess
// as a gap naming the table and the leaked privilege, agreeing with
// CheckCoordinatorAuthorization's rejection instead of contradicting it.
func TestDiagnoseRolePostureCatchesTableWideExcessOnAColumnScopedTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	admin, uri, roles := startGrantHarness(t, ctx)
	coordinator := connectAs(t, ctx, uri, roles.coordinator, grantCoordinatorPass)

	// Sanity: the harness's real migration-emitted grants start clean on both
	// sides, exactly like TestDiagnoseRolePostureReturnsNoGapsOnceGrantsAreCorrect.
	if gaps, err := DiagnoseRolePosture(ctx, coordinator, roles.coordinator, CoordinatorPosture()); err != nil {
		t.Fatalf("DiagnoseRolePosture (baseline): %v", err)
	} else if len(gaps) != 0 {
		t.Fatalf("expected a clean baseline before injecting excess, got %+v", gaps)
	}
	if err := CheckCoordinatorAuthorization(ctx, coordinator, roles.coordinator, grantSchema); err != nil {
		t.Fatalf("CheckCoordinatorAuthorization (baseline): %v", err)
	}

	// Inject the excess: a table-wide SELECT grant on integration_credentials,
	// left alongside the five column grants CoordinatorPosture() already
	// declares and the migration already applied for that table.
	if _, err := admin.Exec(ctx,
		"GRANT SELECT ON TABLE public.integration_credentials TO "+roles.coordinator,
	); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx,
			"REVOKE SELECT ON TABLE public.integration_credentials FROM "+roles.coordinator,
		); err != nil {
			t.Errorf("revoke injected excess grant: %v", err)
		}
	})

	// The real startup gate every coordinator-role binary depends on must
	// reject this state -- it did before this test existed, and still must.
	if err := CheckCoordinatorAuthorization(ctx, coordinator, roles.coordinator, grantSchema); err == nil {
		t.Fatal("CheckCoordinatorAuthorization accepted a table-wide grant on a column-scoped table -- test setup is not exercising the excess this test is about")
	}

	// The migrate-time diagnostic must now agree with that rejection instead
	// of reading "posture confirmed" (CHAOS-4675's actual defect).
	gaps, err := DiagnoseRolePosture(ctx, coordinator, roles.coordinator, CoordinatorPosture())
	if err != nil {
		t.Fatalf("DiagnoseRolePosture (excess): %v", err)
	}
	var found *PostureGap
	for i := range gaps {
		if gaps[i].TableName == "integration_credentials" && len(gaps[i].Excess) > 0 {
			found = &gaps[i]
		}
	}
	if found == nil {
		t.Fatalf("DiagnoseRolePosture did not report the excess table-wide grant on integration_credentials -- "+
			"CheckCoordinatorAuthorization rejects this role but the migrate-time diagnostic reads it as clean, "+
			"exactly the CHAOS-4675 blindness; got gaps: %+v", gaps)
	}
	if !containsString(found.Excess, "SELECT") {
		t.Errorf("expected the excess gap to name SELECT specifically, got %+v", found.Excess)
	}
	if line := found.String(); !strings.Contains(line, "integration_credentials") || !strings.Contains(line, "SELECT") {
		t.Errorf("PostureGap.String() should name the table and the leaked privilege, got %q", line)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
