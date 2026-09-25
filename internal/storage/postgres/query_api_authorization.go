package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// queryAPIWritePosture is the WRITE privileges query-api needs for the saved
// report mutations (CHAOS-6803, CHAOS-6098): the five GraphQL mutations
// createSavedReport, updateSavedReport, deleteSavedReport, cloneSavedReport
// and triggerReport.
//
// It is deliberately NOT a RolePosture in the sense CheckRolePosture reads
// one. query-api has no least-privilege role or full manifest yet (its
// registry DSN is documented as the registry-table OWNER's), so a
// "hold exactly these" posture would have to enumerate every relation its read
// plane touches and would refuse a role the moment it held one more. This
// declaration is ADDITIVE: it says which writes the role must hold and
// nothing about what else it may hold. The full posture is a separate ticket.
//
// Each entry is what the mutation code actually executes, and no more
// (SELECT is always implied by TablePrivilege):
//
//   - saved_reports: create and clone insert, update updates, delete deletes.
//   - scheduled_jobs: a report schedule is created (insert) or rewritten
//     (update) by create and update. A deleted report leaves its job row, as
//     the Python resolver does, so there is no delete.
//   - report_runs: triggerReport inserts the pending run. The rows of a deleted
//     report go by the database's ON DELETE CASCADE, which the foreign key's
//     own trigger performs without a privilege on the child table.
//   - worker_job_outbox: triggerReport publishes the on-demand execution in the
//     same transaction as the run (insert; the publisher's conflict read is the
//     implied SELECT).
//
// scheduled_report_occurrences is absent on purpose: only the SCHEDULED
// execution path writes it, and that path is the scheduler's, not this
// role's.
func queryAPIWritePosture() RolePosture {
	return RolePosture{
		RequiredTables: []TablePrivilege{
			{"saved_reports", true, true, true},
			{"scheduled_jobs", true, true, false},
			{"report_runs", true, false, false},
			{"worker_job_outbox", true, false, false},
		},
	}
}

// QueryAPIWritePosture returns the additive write manifest for the query-api
// role. internal/rivermigrate derives the GRANT statements from this same
// declaration, and CheckQueryAPIWriteGrants asserts it, so the grant side and
// the readiness side are one list.
func QueryAPIWritePosture() RolePosture {
	return queryAPIWritePosture()
}

// CheckQueryAPIWriteGrants is query-api's readiness check for its write
// grants. It is meant to run only when the deployment NAMES a query-api role
// (QUERY_API_DATABASE_ROLE); a deployment that names none has not opted in,
// and the caller must not call it.
//
// It proves two things and nothing else:
//
//   - the active login IS the named role. Grants on a role the pool does not
//     log in as would read as ready while every write ran under a different
//     identity.
//   - the named role holds every privilege queryAPIWritePosture declares.
//
// It does NOT refuse a role that holds MORE (no catch-all, no excess check):
// that is the full least-privilege posture's job.
func CheckQueryAPIWriteGrants(ctx context.Context, pool *pgxpool.Pool, namedRole string) error {
	if pool == nil || !validRuntimeIdentifier(namedRole) {
		return ErrUnavailable
	}
	var login string
	if err := pool.QueryRow(ctx, "SELECT current_user").Scan(&login); err != nil {
		return fmt.Errorf("%w: reading the active login: %w", ErrUnavailable, err)
	}
	if login != namedRole {
		return fmt.Errorf("%w: %w: the pool logs in as %q, not the named query-api role %q",
			ErrUnavailable, ErrPostureRefused, login, namedRole)
	}
	gaps, err := DiagnoseRolePosture(ctx, pool, namedRole, queryAPIWritePosture())
	if err != nil {
		return err
	}
	for _, gap := range gaps {
		// Excess gaps belong to a "hold exactly" posture; this one is additive.
		if len(gap.Excess) > 0 {
			continue
		}
		return fmt.Errorf("%w: %w for role %q: first missing write grant: %s",
			ErrUnavailable, ErrPostureRefused, namedRole, gap.String())
	}
	return nil
}
