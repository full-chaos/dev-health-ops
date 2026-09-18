package remaining

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// membershipMarkerLagAlertBound is "about two sync intervals" -- context
// from a chris-ruled standing decision: investment reads stay scoped to the
// latest complete membership marker whenever one exists -- a stale
// correct number is preferred over a live wrong one, never a time-bound
// fallback to unscoped). This alert is the operational backstop for that
// policy: it fires when the marker this run just published still trails
// the newest investment computation by more than the bound, not merely
// the few-second gap the two writers ordinarily finish within.
const membershipMarkerLagAlertBound = 2 * time.Hour

// latestInvestmentComputedAtQuery reads the SAME quantity
// cmd/query-api/internal/analytics's own latestInvestmentClockSource()
// (investmentmembershipscope.go) reads on the query-api read side --
// max(computed_at) over work_unit_investments for one org -- so this
// write-side check observes the identical clock the read-side scope gate
// already compares the marker against, not a close-but-different one.
const latestInvestmentComputedAtQuery = `
	SELECT max(computed_at)
	FROM work_unit_investments
	WHERE org_id = {org_id:String}
`

// membershipMarkerLag is the pure decision: how far behind a freshly
// published marker is relative to the newest investment computation, and
// whether that exceeds the alert bound. Mirrors investment_membership_
// scope.py's own lag_seconds formula (investmentmembershipscope.go's
// investmentMembershipScopeStateSource: dateDiff('second', marker,
// investment), clamped to >= 0 -- an investment computed before or at the
// marker is not a lag, never a negative one). No investment row at all
// for the org is not a lag either: there is nothing newer for the marker
// to trail.
func membershipMarkerLag(markerCompletedAt time.Time, latestInvestmentComputedAt *time.Time) (lag time.Duration, exceeds bool) {
	if latestInvestmentComputedAt == nil {
		return 0, false
	}
	lag = latestInvestmentComputedAt.Sub(markerCompletedAt)
	if lag < 0 {
		lag = 0
	}
	return lag, lag > membershipMarkerLagAlertBound
}

// membershipMarkerLagChecker is the narrow capability ComputeOrg needs to
// verify a freshly published marker's own freshness against the newest
// investment computation, so a test can substitute a fake without a live
// ClickHouse connection -- same reasoning as chqueryEdgeReader/
// membershipDistributionFetcher above.
type membershipMarkerLagChecker interface {
	CheckMembershipMarkerLag(
		ctx context.Context, orgID string, markerCompletedAt time.Time,
	) (lag time.Duration, exceeds bool, err error)
}

// chConnMarkerLagChecker is the production membershipMarkerLagChecker,
// wrapping a live ClickHouse connection.
type chConnMarkerLagChecker struct {
	conn membershipWriterConn
}

func (checker chConnMarkerLagChecker) CheckMembershipMarkerLag(
	ctx context.Context, orgID string, markerCompletedAt time.Time,
) (time.Duration, bool, error) {
	return checkMembershipMarkerLag(ctx, checker.conn, orgID, markerCompletedAt)
}

// checkMembershipMarkerLag queries the latest investment computed_at for
// orgID and reports the marker's own lag relative to it. A query failure
// is reported through the returned error but ComputeOrg never treats it
// as fatal -- the marker is already published and correct; this is an
// observability concern, not a projection one, the same swallow-and-
// report shape the retention prune failure already uses just above this
// call site.
func checkMembershipMarkerLag(
	ctx context.Context, conn membershipWriterConn, orgID string, markerCompletedAt time.Time,
) (lag time.Duration, exceeds bool, err error) {
	rows, err := conn.Query(ctx, latestInvestmentComputedAtQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return 0, false, err
	}
	defer rows.Close()

	var latest *time.Time
	if rows.Next() {
		if scanErr := rows.Scan(&latest); scanErr != nil {
			return 0, false, scanErr
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return 0, false, rowsErr
	}
	lag, exceeds = membershipMarkerLag(markerCompletedAt, latest)
	return lag, exceeds, nil
}
