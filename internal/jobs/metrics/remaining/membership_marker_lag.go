package remaining

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// membershipMarkerLagAlertBoundEnv names the override for
// membershipMarkerLagAlertBoundDefault. Unset or empty keeps the default;
// present and invalid refuses construction (see resolveMembershipMarkerLagAlertBound)
// rather than silently substituting a value nobody chose.
const membershipMarkerLagAlertBoundEnv = "WORKER_REMAINING_MEMBERSHIP_MARKER_LAG_ALERT_BOUND"

// membershipMarkerLagAlertBoundDefault is the overturnable default for
// membershipMarkerLagAlertBoundEnv. Its meaning: once a freshly published
// marker trails the newest investment computation by more than this, the
// read-side scope gate (investmentMembershipScopeStateSource,
// cmd/query-api/internal/analytics/investmentmembershipscope.go) is in
// its unscoped_fallback branch for this org -- latest_investment_computed_at
// is past latest_run_completed_at, so investment reads are NOT filtered to
// this marker's membership generation at all; every investment row is
// read unscoped instead, trading the marker's precision (excluding units
// this generation has not yet integrated) for freshness. That is the
// degradation worth an operator's attention, not the ordinary few-second
// gap the two independent writers ordinarily finish within.
const membershipMarkerLagAlertBoundDefault = 2 * time.Hour

// resolveMembershipMarkerLagAlertBound reads membershipMarkerLagAlertBoundEnv
// through lookup and returns the bound to use. Unset or blank: the default,
// no refusal -- nobody asked for an override. Set but not a duration, or set
// to a value <= 0 (a lag can never itself be negative, and a zero bound
// would alert on the ordinary write-order gap between the two independent
// materializers): refused, by name, at construction -- matching
// NewMembershipExecutor's existing "refuses at construction rather than per
// partition" discipline, so a typo in the override is never silently
// swallowed into "the alert never fires" or "the alert always fires".
func resolveMembershipMarkerLagAlertBound(lookup func(string) (string, bool)) (time.Duration, error) {
	raw, present := lookup(membershipMarkerLagAlertBoundEnv)
	raw = strings.TrimSpace(raw)
	if !present || raw == "" {
		return membershipMarkerLagAlertBoundDefault, nil
	}
	bound, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration: %w", membershipMarkerLagAlertBoundEnv, err)
	}
	if bound <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration, got %s", membershipMarkerLagAlertBoundEnv, bound)
	}
	return bound, nil
}

// latestInvestmentComputedAtQuery reads the SAME quantity
// cmd/query-api/internal/analytics's own latestInvestmentClockSource()
// (investmentmembershipscope.go) reads on the query-api read side --
// max(computed_at) over work_unit_investments for one org -- so this
// write-side check observes the identical clock the read-side scope gate
// already compares the marker against, not a close-but-different one.
// maxOrNull rather than plain max: computed_at is not Nullable, so a plain
// max() over zero matching rows returns the column's zero value (the
// 1970 epoch) rather than SQL NULL -- indistinguishable, on scan, from a
// real (if absurd) investment row. maxOrNull returns NULL in that case,
// which is what lets checkMembershipMarkerLag tell "no investment row for
// this org at all" apart from "one exists, dated the zero epoch" and, in
// turn, is why membershipMarkerLag's own nil check is correct.
const latestInvestmentComputedAtQuery = `
	SELECT maxOrNull(computed_at)
	FROM work_unit_investments
	WHERE org_id = {org_id:String}
`

// membershipMarkerLagResult carries both timestamps the decision was made
// from, alongside the decision itself, so a caller can log the full basis
// of an alert rather than only its outcome.
type membershipMarkerLagResult struct {
	Lag                        time.Duration
	Exceeds                    bool
	Bound                      time.Duration
	LatestInvestmentComputedAt *time.Time
}

// membershipMarkerLag is the pure decision: how far behind a freshly
// published marker is relative to the newest investment computation, and
// whether that exceeds bound. Mirrors investment_membership_scope.py's own
// lag_seconds formula (investmentmembershipscope.go's
// investmentMembershipScopeStateSource: dateDiff('second', marker,
// investment), clamped to >= 0 -- an investment computed before or at the
// marker is not a lag, never a negative one). No investment row at all
// for the org is not a lag either: there is nothing newer for the marker
// to trail.
func membershipMarkerLag(
	markerCompletedAt time.Time, latestInvestmentComputedAt *time.Time, bound time.Duration,
) (lag time.Duration, exceeds bool) {
	if latestInvestmentComputedAt == nil {
		return 0, false
	}
	lag = latestInvestmentComputedAt.Sub(markerCompletedAt)
	if lag < 0 {
		lag = 0
	}
	return lag, lag > bound
}

// membershipMarkerLagChecker is the narrow capability ComputeOrg needs to
// verify a freshly published marker's own freshness against the newest
// investment computation, so a test can substitute a fake without a live
// ClickHouse connection -- same reasoning as chqueryEdgeReader/
// membershipDistributionFetcher above. The alert bound is baked into the
// checker at construction (resolveMembershipMarkerLagAlertBound), not
// passed per call, so every call against one executor uses the same,
// already-validated bound.
type membershipMarkerLagChecker interface {
	CheckMembershipMarkerLag(
		ctx context.Context, orgID string, markerCompletedAt time.Time,
	) (membershipMarkerLagResult, error)
}

// chConnMarkerLagChecker is the production membershipMarkerLagChecker,
// wrapping a live ClickHouse connection.
type chConnMarkerLagChecker struct {
	conn  membershipWriterConn
	bound time.Duration
}

func (checker chConnMarkerLagChecker) CheckMembershipMarkerLag(
	ctx context.Context, orgID string, markerCompletedAt time.Time,
) (membershipMarkerLagResult, error) {
	return checkMembershipMarkerLag(ctx, checker.conn, orgID, markerCompletedAt, checker.bound)
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
	bound time.Duration,
) (membershipMarkerLagResult, error) {
	rows, err := conn.Query(ctx, latestInvestmentComputedAtQuery, clickhouse.Named("org_id", orgID))
	if err != nil {
		return membershipMarkerLagResult{}, err
	}
	defer rows.Close()

	var latest *time.Time
	if rows.Next() {
		if scanErr := rows.Scan(&latest); scanErr != nil {
			return membershipMarkerLagResult{}, scanErr
		}
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return membershipMarkerLagResult{}, rowsErr
	}
	lag, exceeds := membershipMarkerLag(markerCompletedAt, latest, bound)
	return membershipMarkerLagResult{
		Lag: lag, Exceeds: exceeds, Bound: bound, LatestInvestmentComputedAt: latest,
	}, nil
}
