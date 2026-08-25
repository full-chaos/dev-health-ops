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
type postureGateResult struct {
	OK             bool
	GrantsApplied  map[string]int
	PostureMissing map[string]int
}

// checkExecutedGrantPosture re-derives, against the live database,
// whether the domain and coordinator roles actually hold the grants
// go-river-migrate just applied, using
// postgresstore.DiagnoseRolePosture -- deliberately NOT
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
// startup, so the "no excess privilege" half of the property
// (CheckRolePosture's doc comment) is still proven somewhere for every
// role -- this gate only adds the "nothing is missing, and name what is"
// half, immediately after the grants that are supposed to satisfy it.
//
// The queue role has no RolePosture-shaped manifest or admin-callable
// per-table diagnostic today (queueAuthorizationQuery is a single opaque,
// current_user-bound boolean like the other two strict checks) -- see
// internal/storage/postgres/queue_authorization.go. Rather than
// hand-maintaining a THIRD copy of its table list here purely to name
// gaps, this reports a coarse signal: whether go-river-migrate granted the
// queue role anything at all. Zero rows is unambiguously wrong (the same
// failure mode this ticket exists to catch); a nonzero count does not by
// itself prove completeness, which is the queue role's own startup
// readiness check's job.
func checkExecutedGrantPosture(
	ctx context.Context,
	pool *pgxpool.Pool,
	domainRole, queueRole, coordinatorRole, schema string,
	logger *slog.Logger,
) postureGateResult {
	result := postureGateResult{
		OK:             true,
		GrantsApplied:  make(map[string]int, len(postureGateRoles)),
		PostureMissing: make(map[string]int, len(postureGateRoles)),
	}

	checkTablePosture := func(roleLabel, roleName string, posture postgresstore.RolePosture) {
		declared := len(posture.RequiredTables) + len(posture.ColumnScoped)
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
		result.PostureMissing[roleLabel] = len(gaps)
		result.GrantsApplied[roleLabel] = declared - len(gaps)
		if len(gaps) == 0 {
			logger.Info("runtime grant posture confirmed", "role", roleLabel, "grants_applied", declared)
			return
		}
		result.OK = false
		details := make([]string, len(gaps))
		for i, gap := range gaps {
			details[i] = gap.String()
		}
		logger.Error("runtime grant posture gap", "role", roleLabel, "gaps", details)
	}

	checkTablePosture("domain", domainRole, postgresstore.DomainPosture())
	checkTablePosture("coordinator", coordinatorRole, postgresstore.CoordinatorPosture())

	queueGrantCount, err := countTableGrants(ctx, pool, queueRole)
	switch {
	case err != nil:
		result.OK = false
		result.GrantsApplied["queue"] = 0
		result.PostureMissing["queue"] = 1
		logger.Error("runtime grant posture check failed", "role", "queue", "reason", "grant_count_unavailable")
	case queueGrantCount == 0:
		result.OK = false
		result.GrantsApplied["queue"] = 0
		result.PostureMissing["queue"] = 1
		logger.Error("runtime grant posture gap", "role", "queue", "reason", "no_table_grants_present")
	default:
		result.GrantsApplied["queue"] = queueGrantCount
		result.PostureMissing["queue"] = 0
		logger.Info("runtime grant posture confirmed", "role", "queue", "grants_applied", queueGrantCount)
	}

	return result
}

// countTableGrants reports how many information_schema.role_table_grants
// rows PostgreSQL has recorded for the given role. Safe to run from any
// connection: unlike rolePostureQuery/queueAuthorizationQuery, this view
// does not bind to current_user.
func countTableGrants(ctx context.Context, pool *pgxpool.Pool, role string) (int, error) {
	var count int
	err := pool.QueryRow(
		ctx, "SELECT count(*) FROM information_schema.role_table_grants WHERE grantee = $1", role,
	).Scan(&count)
	return count, err
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
}
