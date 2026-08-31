package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

// postureGateRoles is the fixed, ordered label set for the telemetry this
// gate emits. Ordered so writePostureTelemetry produces a stable, diffable
// fragment run to run.
var postureGateRoles = []string{"domain", "queue", "coordinator"}

// postureGateResult is the executed-proof outcome CHAOS-4261 requires:
// go-river-migrate must assert the live database actually holds the
// declared grant posture after applying it, not merely trust that the
// GRANT statements it just issued succeeded. A GRANT guarded by
// to_regclass silently no-ops when the required table does not exist yet
// (an Alembic revision behind head -- see deploy/go-workers/README.md), so
// "the migration returned no error" and "the role is actually ready" are
// two different claims; only this executed check proves the second one.
//
// Scope, stated precisely because it is easy to over-claim: OK==true means
// no declared table/column/sequence requirement in DomainPosture/
// QueuePosture/CoordinatorPosture was found missing, AND (CHAOS-4675) no
// table-wide privilege excess was found on a table any of those postures
// declares column-scoped only. It does NOT mean a role holds nothing beyond
// that posture in every sense: a wrong River-schema grant, bad role
// membership, a role attribute problem, or a table-wide excess on a table
// the posture does not mention at all can all coexist with OK==true here --
// see DiagnoseRolePosture's own doc comment, which states this narrower
// remaining limitation explicitly. Proving the full property is what each
// runtime binary's own strict, current_user-bound startup check
// (CheckDomainAuthorization/CheckQueueAuthorization/
// CheckCoordinatorAuthorization) is for, and nothing here substitutes for
// it -- but the column-scoped-granularity drift that let CHAOS-4675 read as
// "posture confirmed" while workerctl still failed with
// runtime_role_unauthorized is now caught here too, at migrate time, rather
// than only at the next CLI invocation.
type postureGateResult struct {
	OK             bool
	GrantsApplied  map[string]int
	PostureMissing map[string]int
	// PostureExcess counts, per role, the table-wide-privilege-on-a-
	// column-scoped-table gaps DiagnoseRolePosture reported (CHAOS-4675).
	// Disjoint from PostureMissing: a gap is either something declared and
	// absent, or something present and undeclared, never both.
	PostureExcess map[string]int
}

// checkExecutedGrantPosture re-derives, against the live database, whether
// all three runtime roles actually hold the grants go-river-migrate just
// applied, using postgresstore.DiagnoseRolePosture -- deliberately NOT
// CheckDomainAuthorization/CheckCoordinatorAuthorization/
// CheckQueueAuthorization. Those three assert `current_user = expectedRole`
// (rolePostureQuery's own doc comment: a read-only check on "the active
// login"), so they can only run over a connection actually authenticated
// as that role -- which this one-shot command, connected solely as the
// migration/admin identity with no domain/queue/coordinator password in
// its environment, structurally cannot open. DiagnoseRolePosture takes a
// role NAME rather than binding to the caller's own identity, so it works
// from this admin connection; every runtime binary that DOES connect as
// one of these roles (reconciler, scheduler, worker, workerctl) still
// gates its own readiness on the strict current_user-bound check at
// startup, so the full "no excess privilege of any kind" property
// (CheckRolePosture's doc comment) is still proven somewhere for every
// role. This gate adds the "nothing declared is missing" half immediately
// after the grants that are supposed to satisfy it, PLUS (CHAOS-4675) the
// one slice of the excess half that was proven to drift silently: a
// table-wide grant surviving on a table declared column-scoped. It does not
// generalize to excess detection everywhere CheckRolePosture looks -- see
// DiagnoseRolePosture's doc comment for exactly which slice this covers.
//
// The queue role's own readiness check (queueAuthorizationQuery) has no
// admin-callable per-table diagnostic of its own -- postgresstore.QueuePosture
// exists purely to let DiagnoseRolePosture answer that for this gate,
// without changing queueAuthorizationQuery itself. An earlier revision of
// this gate reported only whether go-river-migrate granted the queue role
// ANYTHING at all (a single stray or leftover grant read as "complete"); the
// per-table posture below closes that gap the same way domain/coordinator
// already work.
//
// Takes no schema parameter: DiagnoseRolePosture (unlike CheckRolePosture)
// never inspects the River schema at all, so a River-schema privilege
// problem is invisible to this gate regardless -- one more reason OK==true
// here is a narrower claim than "this role is fully ready" (see
// postureGateResult's doc comment).
func checkExecutedGrantPosture(
	ctx context.Context,
	pool *pgxpool.Pool,
	domainRole, queueRole, coordinatorRole string,
	logger *slog.Logger,
) postureGateResult {
	result := postureGateResult{
		OK:             true,
		GrantsApplied:  make(map[string]int, len(postureGateRoles)),
		PostureMissing: make(map[string]int, len(postureGateRoles)),
		PostureExcess:  make(map[string]int, len(postureGateRoles)),
	}

	checkTablePosture := func(roleLabel, roleName string, posture postgresstore.RolePosture) {
		declared := len(posture.RequiredTables) + len(posture.ColumnScoped) + len(posture.RequiredSequences)
		gaps, err := postgresstore.DiagnoseRolePosture(ctx, pool, roleName, posture)
		if err != nil {
			// The diagnostic query itself failed (unreachable database, an
			// invalid role/schema identifier). Report every declared
			// requirement as missing rather than claiming a gap count we
			// could not actually measure.
			result.OK = false
			result.PostureMissing[roleLabel] = declared
			result.GrantsApplied[roleLabel] = 0
			logger.Error("runtime grant posture check failed", "role", roleLabel, "reason", "diagnostic_unavailable")
			return
		}
		// A gap is either something declared and absent (Missing/TableMissing)
		// or something present and undeclared (Excess, CHAOS-4675) -- never
		// both, so this partition is exhaustive and disjoint over gaps.
		var missing, excess []postgresstore.PostureGap
		for _, gap := range gaps {
			if len(gap.Excess) > 0 {
				excess = append(excess, gap)
				continue
			}
			missing = append(missing, gap)
		}
		result.PostureMissing[roleLabel] = len(missing)
		result.PostureExcess[roleLabel] = len(excess)
		result.GrantsApplied[roleLabel] = declared - len(missing)
		if !logPostureCheck(logger, roleLabel, declared, missing, excess) {
			result.OK = false
		}
	}

	checkTablePosture("domain", domainRole, postgresstore.DomainPosture())
	checkTablePosture("queue", queueRole, postgresstore.QueuePosture())
	checkTablePosture("coordinator", coordinatorRole, postgresstore.CoordinatorPosture())

	return result
}

// logPostureCheck emits the structured log line(s) describing one role's
// posture-check outcome and reports whether that role's posture is OK
// (both missing and excess empty). Split out of checkTablePosture's
// closure so the exact log text is unit-testable without a live database
// connection (CHAOS-4675 round-1 codex finding, P3): the prior inline
// version logged "runtime grant posture confirmed" whenever missing was
// empty, even when excess was not -- a false "confirmed" line immediately
// followed by the contradicting excess-gap line for the same role.
// Logging "confirmed" now requires BOTH slices empty.
func logPostureCheck(logger *slog.Logger, roleLabel string, declared int, missing, excess []postgresstore.PostureGap) bool {
	if len(missing) == 0 && len(excess) == 0 {
		logger.Info("runtime grant posture confirmed", "role", roleLabel, "grants_applied", declared)
		return true
	}
	if len(missing) > 0 {
		details := make([]string, len(missing))
		for i, gap := range missing {
			details[i] = gap.String()
		}
		logger.Error("runtime grant posture gap", "role", roleLabel, "gaps", details)
	}
	if len(excess) > 0 {
		// The CHAOS-4675 case: the role also holds a table-wide grant on a
		// table declared column-scoped -- the granularity drift that let
		// workerctl fail runtime_role_unauthorized right after a
		// "posture confirmed" run.
		details := make([]string, len(excess))
		for i, gap := range excess {
			details[i] = gap.String()
		}
		logger.Error("runtime grant posture excess", "role", roleLabel, "gaps", details)
	}
	return false
}

// postureFailureKind names which kind(s) of gap made postureGateResult.OK
// false, summed across all three roles, for go-river-migrate's terminal
// stderr message (main.go). Before CHAOS-4675 round 2 that message always
// said "missing privileges" even when the actual failure was an
// excess-only gap (missing=0, excess>0) -- misleading an operator reading
// only the final stderr line, not the preceding structured logs.
func postureFailureKind(result postureGateResult) string {
	var missingTotal, excessTotal int
	for _, n := range result.PostureMissing {
		missingTotal += n
	}
	for _, n := range result.PostureExcess {
		excessTotal += n
	}
	switch {
	case missingTotal > 0 && excessTotal > 0:
		return "missing and excess privileges"
	case excessTotal > 0:
		return "excess privileges"
	default:
		return "missing privileges"
	}
}

// writePostureTelemetry renders checkExecutedGrantPosture's result as a
// Prometheus text-format fragment. go-river-migrate is a one-shot command
// with no health/metrics HTTP endpoint of its own to scrape, so this is
// written to the process's own stdout -- the deploy log a runbook or a
// textfile-collector step can capture -- alongside the structured slog
// lines checkExecutedGrantPosture already emitted, which are the primary,
// immediately actionable telemetry for an operator or SigNoz log search.
func writePostureTelemetry(w io.Writer, result postureGateResult) {
	fmt.Fprintln(w, "# HELP dev_health_runtime_grants_applied_total Declared runtime-role grants confirmed present immediately after go-river-migrate.")
	fmt.Fprintln(w, "# TYPE dev_health_runtime_grants_applied_total counter")
	for _, role := range postureGateRoles {
		fmt.Fprintf(w, "dev_health_runtime_grants_applied_total{role=%q} %d\n", role, result.GrantsApplied[role])
	}
	fmt.Fprintln(w, "# HELP dev_health_runtime_posture_missing Declared runtime-role grants NOT confirmed present immediately after go-river-migrate.")
	fmt.Fprintln(w, "# TYPE dev_health_runtime_posture_missing gauge")
	for _, role := range postureGateRoles {
		fmt.Fprintf(w, "dev_health_runtime_posture_missing{role=%q} %d\n", role, result.PostureMissing[role])
	}
	fmt.Fprintln(w, "# HELP dev_health_runtime_posture_excess_grants Table-wide privileges held on a column-scoped table immediately after go-river-migrate (CHAOS-4675).")
	fmt.Fprintln(w, "# TYPE dev_health_runtime_posture_excess_grants gauge")
	for _, role := range postureGateRoles {
		fmt.Fprintf(w, "dev_health_runtime_posture_excess_grants{role=%q} %d\n", role, result.PostureExcess[role])
	}
}
