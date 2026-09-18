package providersync

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// TestGuardDeploymentLifecycleRegressionsCarriesForwardOnFailure constructs
// the exact regression this guard exists to prevent: a batch row arrives
// with a nil started_at/finished_at/status because its statuses lookup
// FAILED this pass, for a key whose currently winning physical row already
// has real values. The guard must carry the stored values into the row
// instead of letting the nil win, and must report exactly one carried
// event naming that row's own key.
func TestGuardDeploymentLifecycleRegressionsCarriesForwardOnFailure(t *testing.T) {
	priorStarted := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)
	priorFinished := time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)
	priorStatus := "in_progress"

	row := deploymentComparatorRow(time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	row.StartedAt = nil
	row.FinishedAt = nil
	row.Status = nil
	row.LifecycleLookupFailed = true
	rows := []deploymentRow{row}

	stored := map[deploymentLifecycleGuardKey]storedDeploymentLifecycle{
		{RepoID: row.RepoID, DeploymentID: row.DeploymentID}: {StartedAt: &priorStarted, FinishedAt: &priorFinished, Status: &priorStatus},
	}

	carried := guardDeploymentLifecycleRegressions(rows, stored)

	if rows[0].StartedAt == nil || !rows[0].StartedAt.Equal(priorStarted) {
		t.Fatalf("StartedAt=%v want=%v", rows[0].StartedAt, priorStarted)
	}
	if rows[0].FinishedAt == nil || !rows[0].FinishedAt.Equal(priorFinished) {
		t.Fatalf("FinishedAt=%v want=%v", rows[0].FinishedAt, priorFinished)
	}
	if rows[0].Status == nil || *rows[0].Status != priorStatus {
		t.Fatalf("Status=%v want=%q -- a failed lookup must not downgrade a known-running status to nil (terminal)", rows[0].Status, priorStatus)
	}
	if len(carried) != 1 || carried[0].RepoID != row.RepoID || carried[0].DeploymentID != row.DeploymentID {
		t.Fatalf("carried=%+v want exactly one event for %s/%s", carried, row.RepoID, row.DeploymentID)
	}
}

// TestGuardDeploymentLifecycleRegressionsCarriesForwardStatusAloneOnFailure
// proves the guard fires from Status alone: a stored row with a known
// status but no started_at/finished_at (a GitHub deployment whose statuses
// lookup previously found a status entry with an unparseable timestamp,
// deploymentLifecycleFromStatuses' own skip case) must still have its
// status carried forward, not just discarded because the other two fields
// are already nil.
func TestGuardDeploymentLifecycleRegressionsCarriesForwardStatusAloneOnFailure(t *testing.T) {
	priorStatus := "success"

	row := deploymentComparatorRow(time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	row.StartedAt = nil
	row.FinishedAt = nil
	row.Status = nil
	row.LifecycleLookupFailed = true
	rows := []deploymentRow{row}

	stored := map[deploymentLifecycleGuardKey]storedDeploymentLifecycle{
		{RepoID: row.RepoID, DeploymentID: row.DeploymentID}: {Status: &priorStatus},
	}

	carried := guardDeploymentLifecycleRegressions(rows, stored)

	if rows[0].Status == nil || *rows[0].Status != priorStatus {
		t.Fatalf("Status=%v want=%q", rows[0].Status, priorStatus)
	}
	if len(carried) != 1 {
		t.Fatalf("carried=%+v want exactly one event", carried)
	}
}

// TestGuardDeploymentLifecycleRegressionsAllowsHonestEmptySuccess is the
// class ruling's own distinguishing case: a row whose lookup SUCCEEDED but
// found no signal (LifecycleLookupFailed false) must stay nil even when a
// prior row exists -- that nil is the honest value, never overwritten by a
// stale one.
func TestGuardDeploymentLifecycleRegressionsAllowsHonestEmptySuccess(t *testing.T) {
	priorStarted := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)
	priorStatus := "success"

	row := deploymentComparatorRow(time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	row.StartedAt = nil
	row.FinishedAt = nil
	row.Status = nil
	row.LifecycleLookupFailed = false // the lookup succeeded and found nothing
	rows := []deploymentRow{row}

	stored := map[deploymentLifecycleGuardKey]storedDeploymentLifecycle{
		{RepoID: row.RepoID, DeploymentID: row.DeploymentID}: {StartedAt: &priorStarted, Status: &priorStatus},
	}

	carried := guardDeploymentLifecycleRegressions(rows, stored)

	if rows[0].StartedAt != nil || rows[0].FinishedAt != nil || rows[0].Status != nil {
		t.Fatalf("StartedAt=%v FinishedAt=%v Status=%v want all nil: a successful empty lookup must not be overwritten by a stale stored value", rows[0].StartedAt, rows[0].FinishedAt, rows[0].Status)
	}
	if len(carried) != 0 {
		t.Fatalf("carried=%+v want none", carried)
	}
}

// TestGuardDeploymentLifecycleRegressionsAllowsBrandNewDeployment is the
// no-prior-row case: a key absent from `stored` (never synced before) has
// nothing to protect, so a failed lookup's nil simply passes through.
func TestGuardDeploymentLifecycleRegressionsAllowsBrandNewDeployment(t *testing.T) {
	row := deploymentComparatorRow(time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	row.StartedAt = nil
	row.FinishedAt = nil
	row.Status = nil
	row.LifecycleLookupFailed = true
	rows := []deploymentRow{row}

	carried := guardDeploymentLifecycleRegressions(rows, map[deploymentLifecycleGuardKey]storedDeploymentLifecycle{})

	if rows[0].StartedAt != nil || rows[0].FinishedAt != nil || rows[0].Status != nil {
		t.Fatalf("StartedAt=%v FinishedAt=%v Status=%v want all nil: nothing to carry forward for a brand-new deployment", rows[0].StartedAt, rows[0].FinishedAt, rows[0].Status)
	}
	if len(carried) != 0 {
		t.Fatalf("carried=%+v want none", carried)
	}
}

// TestGuardDeploymentLifecycleRegressionsLeavesEveryOtherColumnUntouched
// proves the guard corrects ONLY started_at/finished_at/status: a row with
// an environment change AND a failed lookup must still carry the fresh
// environment through untouched.
func TestGuardDeploymentLifecycleRegressionsLeavesEveryOtherColumnUntouched(t *testing.T) {
	priorStarted := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)

	row := deploymentComparatorRow(time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC))
	row.StartedAt = nil
	row.FinishedAt = nil
	row.Status = nil
	row.LifecycleLookupFailed = true
	freshEnvironment := "staging"
	row.Environment = &freshEnvironment
	rows := []deploymentRow{row}

	stored := map[deploymentLifecycleGuardKey]storedDeploymentLifecycle{
		{RepoID: row.RepoID, DeploymentID: row.DeploymentID}: {StartedAt: &priorStarted},
	}
	guardDeploymentLifecycleRegressions(rows, stored)

	if rows[0].Environment == nil || *rows[0].Environment != freshEnvironment {
		t.Fatalf("Environment=%v want=%q: the guard must not touch other columns", rows[0].Environment, freshEnvironment)
	}
}

// deploymentLifecycleGuardConn is a driver.Conn stand-in for the
// WriteEffect-level guard tests: it counts Query calls and, when
// queryRows is set, returns it for every Query -- otherwise an empty
// result, the same shape deploymentEffectsRecordingConn (gitlab_deployments_
// effects_test.go) already uses for this package's other effect tests.
type deploymentLifecycleGuardConn struct {
	driver.Conn
	queries   int
	queryRows driver.Rows
	prepares  int
	batch     *deploymentEffectsRecordingBatch
}

func (conn *deploymentLifecycleGuardConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	conn.queries++
	if conn.queryRows != nil {
		return conn.queryRows, nil
	}
	return emptyDeploymentEffectsRows{}, nil
}

func (conn *deploymentLifecycleGuardConn) PrepareBatch(
	context.Context, string, ...driver.PrepareBatchOption,
) (driver.Batch, error) {
	conn.prepares++
	if conn.batch == nil {
		conn.batch = &deploymentEffectsRecordingBatch{}
	}
	return conn.batch, nil
}

// deploymentLifecycleGuardRows is a single-row driver.Rows fake carrying
// exactly the columns loadStoredDeploymentLifecycle's SELECT projects.
type deploymentLifecycleGuardRows struct {
	repoID, deploymentID  string
	startedAt, finishedAt *time.Time
	status                *string
	served                bool
}

func (r *deploymentLifecycleGuardRows) Next() bool {
	if r.served {
		return false
	}
	r.served = true
	return true
}

func (r *deploymentLifecycleGuardRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.repoID
	*dest[1].(*string) = r.deploymentID
	*dest[2].(**time.Time) = r.startedAt
	*dest[3].(**time.Time) = r.finishedAt
	*dest[4].(**string) = r.status
	return nil
}

func (r *deploymentLifecycleGuardRows) ScanStruct(any) error             { return nil }
func (r *deploymentLifecycleGuardRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *deploymentLifecycleGuardRows) Totals(...any) error              { return nil }
func (r *deploymentLifecycleGuardRows) Columns() []string                { return nil }
func (r *deploymentLifecycleGuardRows) Close() error                     { return nil }
func (r *deploymentLifecycleGuardRows) Err() error                       { return nil }
func (r *deploymentLifecycleGuardRows) HasData() bool                    { return true }

// TestGitHubDeploymentsEffectsWriteCarriesLifecycleForwardOnFailure is the
// end-to-end proof at the WriteEffect boundary: a row marked
// LifecycleLookupFailed, with a stored prior row, is written with the
// prior started_at/finished_at/status, not NULL.
func TestGitHubDeploymentsEffectsWriteCarriesLifecycleForwardOnFailure(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	priorStarted := time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)
	priorFinished := time.Date(2026, 7, 22, 10, 1, 0, 0, time.UTC)
	priorStatus := "success"

	row := deploymentEffectsUnitRow(claim, "701")
	row.LifecycleLookupFailed = true
	effect := deploymentEffectsUnitEffect(t, []deploymentRow{row})

	conn := &deploymentLifecycleGuardConn{queryRows: &deploymentLifecycleGuardRows{
		repoID: row.RepoID, deploymentID: row.DeploymentID,
		startedAt: &priorStarted, finishedAt: &priorFinished, status: &priorStatus,
	}}
	sink := GitHubDeploymentsClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}

	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatalf("write error=%v", err)
	}
	if conn.queries != 1 {
		t.Fatalf("queries=%d want exactly 1 (the carry-forward read)", conn.queries)
	}
	if conn.batch == nil || len(conn.batch.values) != 1 {
		t.Fatalf("batch=%+v", conn.batch)
	}
	gotStarted, ok := conn.batch.values[0][4].(*time.Time)
	if !ok || gotStarted == nil || !gotStarted.Equal(priorStarted) {
		t.Fatalf("appended started_at=%v want=%v", conn.batch.values[0][4], priorStarted)
	}
	gotFinished, ok := conn.batch.values[0][5].(*time.Time)
	if !ok || gotFinished == nil || !gotFinished.Equal(priorFinished) {
		t.Fatalf("appended finished_at=%v want=%v", conn.batch.values[0][5], priorFinished)
	}
	gotStatus, ok := conn.batch.values[0][2].(*string)
	if !ok || gotStatus == nil || *gotStatus != priorStatus {
		t.Fatalf("appended status=%v want=%q", conn.batch.values[0][2], priorStatus)
	}
}

// TestGitHubDeploymentsEffectsWriteSkipsCarryForwardQueryWhenNothingFailed
// pins the class ruling: the carry-forward read is not issued at all when
// no row in the batch has LifecycleLookupFailed set.
func TestGitHubDeploymentsEffectsWriteSkipsCarryForwardQueryWhenNothingFailed(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	row := deploymentEffectsUnitRow(claim, "702")
	effect := deploymentEffectsUnitEffect(t, []deploymentRow{row})

	conn := &deploymentLifecycleGuardConn{}
	sink := GitHubDeploymentsClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}

	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatalf("write error=%v", err)
	}
	if conn.queries != 0 {
		t.Fatalf("queries=%d want 0: no row had LifecycleLookupFailed set", conn.queries)
	}
}

// TestGitHubDeploymentsEffectsWriteKeepsNullOnSuccessfulEmptyLookup is the
// class ruling's own distinguishing case at the WriteEffect boundary: a row
// whose lookup succeeded but found nothing (LifecycleLookupFailed false)
// writes NULL even though a stored prior row exists -- the guard's read is
// never even issued for it.
func TestGitHubDeploymentsEffectsWriteKeepsNullOnSuccessfulEmptyLookup(t *testing.T) {
	claim := nativeTestClaim("github", "deployments")
	row := deploymentEffectsUnitRow(claim, "703")
	// LifecycleLookupFailed left false: a successful-but-empty lookup.
	effect := deploymentEffectsUnitEffect(t, []deploymentRow{row})

	conn := &deploymentLifecycleGuardConn{queryRows: &deploymentLifecycleGuardRows{
		repoID: row.RepoID, deploymentID: row.DeploymentID,
		startedAt: timePointerForLifecycleGuardTest(time.Date(2026, 7, 22, 9, 59, 0, 0, time.UTC)),
	}}
	sink := GitHubDeploymentsClickHouseEffects{
		Conn: conn, Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}

	if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
		t.Fatalf("write error=%v", err)
	}
	if conn.queries != 0 {
		t.Fatalf("queries=%d want 0: the row never had LifecycleLookupFailed set", conn.queries)
	}
	if conn.batch == nil || len(conn.batch.values) != 1 {
		t.Fatalf("batch=%+v", conn.batch)
	}
	if gotStarted, ok := conn.batch.values[0][4].(*time.Time); !ok || gotStarted != nil {
		t.Fatalf("appended started_at=%v want nil: a successful empty lookup must stay honest", conn.batch.values[0][4])
	}
	if gotStatus, ok := conn.batch.values[0][2].(*string); !ok || gotStatus != nil {
		t.Fatalf("appended status=%v want nil: a successful empty lookup must stay honest", conn.batch.values[0][2])
	}
}

func timePointerForLifecycleGuardTest(value time.Time) *time.Time { return &value }
