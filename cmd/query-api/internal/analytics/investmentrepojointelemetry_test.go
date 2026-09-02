package analytics

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/graph/model"
)

// resetRepoJoinDedupCollisionCooldown clears the package-level cooldown
// state and restores the real clock -- every test using it must defer this
// so state does not leak between tests (this package's tests can run with
// -parallel, and even without it, the map is process-global for the
// package's test binary).
func resetRepoJoinDedupCollisionCooldown(t *testing.T) {
	t.Helper()
	repoJoinDedupCollisionMu.Lock()
	repoJoinDedupCollisionLastChecked = map[string]time.Time{}
	repoJoinDedupCollisionMu.Unlock()
	repoJoinDedupCollisionNow = time.Now
	t.Cleanup(func() {
		repoJoinDedupCollisionMu.Lock()
		repoJoinDedupCollisionLastChecked = map[string]time.Time{}
		repoJoinDedupCollisionMu.Unlock()
		repoJoinDedupCollisionNow = time.Now
	})
}

func TestRepoJoinDedupCollisionShouldCheck_FirstCallAlwaysAllowed(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	if !repoJoinDedupCollisionShouldCheck("org-a") {
		t.Fatal("first call for a never-seen org must be allowed")
	}
}

func TestRepoJoinDedupCollisionShouldCheck_WithinCooldown_Blocked(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repoJoinDedupCollisionNow = func() time.Time { return now }

	if !repoJoinDedupCollisionShouldCheck("org-a") {
		t.Fatal("first call must be allowed")
	}
	repoJoinDedupCollisionNow = func() time.Time { return now.Add(repoJoinDedupCollisionCooldown - time.Second) }
	if repoJoinDedupCollisionShouldCheck("org-a") {
		t.Fatal("a call inside the cooldown window must be blocked")
	}
}

func TestRepoJoinDedupCollisionShouldCheck_AfterCooldown_Allowed(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repoJoinDedupCollisionNow = func() time.Time { return now }

	if !repoJoinDedupCollisionShouldCheck("org-a") {
		t.Fatal("first call must be allowed")
	}
	repoJoinDedupCollisionNow = func() time.Time { return now.Add(repoJoinDedupCollisionCooldown + time.Second) }
	if !repoJoinDedupCollisionShouldCheck("org-a") {
		t.Fatal("a call after the cooldown window must be allowed")
	}
}

func TestRepoJoinDedupCollisionShouldCheck_DifferentOrgsIndependent(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repoJoinDedupCollisionNow = func() time.Time { return now }

	if !repoJoinDedupCollisionShouldCheck("org-a") {
		t.Fatal("org-a first call must be allowed")
	}
	if !repoJoinDedupCollisionShouldCheck("org-b") {
		t.Fatal("org-b's cooldown must be independent of org-a's")
	}
}

func TestRecordInvestmentRepoJoinDedupCollisions_FiresOnlyWhenExcessPositive(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)

	var recorded []struct {
		orgID  string
		excess int64
	}
	orig := recordInvestmentRepoJoinDedupCollisions
	recordInvestmentRepoJoinDedupCollisions = func(_ context.Context, orgID string, excess int64) {
		recorded = append(recorded, struct {
			orgID  string
			excess int64
		}{orgID, excess})
	}
	t.Cleanup(func() { recordInvestmentRepoJoinDedupCollisions = orig })

	client := (&routingFakeClient{}).on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(2)}}})
	RecordInvestmentRepoJoinDedupCollisions(context.Background(), client, "org-with-collision")

	if len(recorded) != 1 {
		t.Fatalf("recorded = %+v, want exactly one report", recorded)
	}
	if recorded[0].orgID != "org-with-collision" || recorded[0].excess != 2 {
		t.Errorf("recorded[0] = %+v, want {org-with-collision 2}", recorded[0])
	}
}

func TestRecordInvestmentRepoJoinDedupCollisions_NoReportWhenZero(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)

	fired := false
	orig := recordInvestmentRepoJoinDedupCollisions
	recordInvestmentRepoJoinDedupCollisions = func(context.Context, string, int64) { fired = true }
	t.Cleanup(func() { recordInvestmentRepoJoinDedupCollisions = orig })

	client := (&routingFakeClient{}).on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(0)}}})
	RecordInvestmentRepoJoinDedupCollisions(context.Background(), client, "org-clean")

	if fired {
		t.Fatal("must not report when the check finds zero excess versions")
	}
}

func TestRecordInvestmentRepoJoinDedupCollisions_SecondCallWithinCooldownSkipsQuery(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	repoJoinDedupCollisionNow = func() time.Time { return now }

	callCount := 0
	client := (&routingFakeClient{}).on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(0)}}})
	// routingFakeClient does not count per-rule calls directly in a way
	// this test can read, so count via a thin wrapper instead.
	counting := &countingQueryClient{inner: client, calls: &callCount}

	RecordInvestmentRepoJoinDedupCollisions(context.Background(), counting, "org-cooldown")
	RecordInvestmentRepoJoinDedupCollisions(context.Background(), counting, "org-cooldown")

	if callCount != 1 {
		t.Fatalf("query executed %d times for two calls inside the cooldown window, want 1 (the second must be skipped by the per-org cooldown, not re-scan repos)", callCount)
	}
}

// countingQueryClient wraps a QueryClient and counts Query invocations --
// routingFakeClient's own bookkeeping (f.calls) is keyed by matched rule
// text and lives in resolve_test.go, not a plain exported count, so this is
// the simplest way to assert "the real check ran at most once."
type countingQueryClient struct {
	inner QueryClient
	calls *int
}

func (c *countingQueryClient) Query(ctx context.Context, statement string, bindings []clickhouse.Binding) (clickhouse.RowScanner, error) {
	*c.calls++
	return c.inner.Query(ctx, statement, bindings)
}

// spyRepoJoinDedupCollisions installs a spy for
// recordInvestmentRepoJoinDedupCollisions and restores the original on
// cleanup, returning the slice its calls land in.
func spyRepoJoinDedupCollisions(t *testing.T) *[]string {
	t.Helper()
	var orgs []string
	orig := recordInvestmentRepoJoinDedupCollisions
	recordInvestmentRepoJoinDedupCollisions = func(_ context.Context, orgID string, _ int64) {
		orgs = append(orgs, orgID)
	}
	t.Cleanup(func() { recordInvestmentRepoJoinDedupCollisions = orig })
	return &orgs
}

// CHAOS-4773 codex round-1 P2: the dedup-collision telemetry was wired into
// resolveSankey/resolveSankeyCoverage only, even though investmentContextFor's
// repaired repos join is shared by timeseries/breakdown/flowmatrix too.
// These three tests drive the real Resolve() entrypoint end to end (not the
// unexported resolver directly) so a future refactor of the wiring still has
// to keep the observable behavior: an investment+REPO request of any of the
// three kinds fires the check exactly once.

func TestResolve_Timeseries_InvestmentRepo_FiresRepoJoinDedupCheck(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	orgs := spyRepoJoinDedupCollisions(t)

	client := (&routingFakeClient{}).
		on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(1)}}}).
		on("SELECT", &fakeRowScanner{})

	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		Timeseries:    []model.TimeseriesRequestInput{tsInput(model.DimensionInputRepo, model.MeasureInputCount)},
	}
	if _, err := Resolve(context.Background(), client, "org-ts-repo", batch); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(*orgs) != 1 || (*orgs)[0] != "org-ts-repo" {
		t.Fatalf("recorded orgs = %v, want exactly [org-ts-repo]", *orgs)
	}
}

func TestResolve_Timeseries_InvestmentNonRepo_DoesNotFireRepoJoinDedupCheck(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	orgs := spyRepoJoinDedupCollisions(t)

	client := (&routingFakeClient{}).
		on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(1)}}}).
		on("SELECT", &fakeRowScanner{})

	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		Timeseries:    []model.TimeseriesRequestInput{tsInput(model.DimensionInputTeam, model.MeasureInputCount)},
	}
	if _, err := Resolve(context.Background(), client, "org-ts-team", batch); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(*orgs) != 0 {
		t.Fatalf("recorded orgs = %v, want none -- TEAM dimension does not compile the repos join", *orgs)
	}
}

func TestResolve_Breakdown_InvestmentRepo_FiresRepoJoinDedupCheck(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	orgs := spyRepoJoinDedupCollisions(t)

	client := (&routingFakeClient{}).
		on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(1)}}}).
		on("SELECT", &fakeRowScanner{})

	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		Breakdowns:    []model.BreakdownRequestInput{bdInput(model.DimensionInputRepo, model.MeasureInputCount)},
	}
	if _, err := Resolve(context.Background(), client, "org-bd-repo", batch); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(*orgs) != 1 || (*orgs)[0] != "org-bd-repo" {
		t.Fatalf("recorded orgs = %v, want exactly [org-bd-repo]", *orgs)
	}
}

// CHAOS-4773 codex round 2 (P2, EXECUTED): flow matrix NEVER compiles the
// repos join, for ANY dimension -- REPO/TEAM/WORK_TYPE route to fixed
// hand-written templates before investmentContextFor is ever called, and
// the investment-dimension branch (AUTHOR/THEME/SUBCATEGORY) calls
// investmentContextFor with a one-element dimensions list that is never
// REPO. Round 1 wired the check into resolveFlowMatrix anyway, keyed only
// on req.Dimension == DimensionRepo -- true for the FIXED-TEMPLATE REPO
// case, which never joins repos, so it fired an irrelevant scan and burned
// the org's cooldown. Fixed by removing the call from resolveFlowMatrix
// entirely; these two tests pin "never fires" for the two ways it could
// previously have fired: dimension=REPO (round 1's actual bug) and
// dimension=THEME (the investment path, to prove this isn't reachable from
// there either, now or if someone "fixes" the dimension check without
// understanding why it can never be reachable).
func TestResolve_FlowMatrix_RepoDimension_NeverFiresRepoJoinDedupCheck(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	orgs := spyRepoJoinDedupCollisions(t)

	// If the check ever fires, it observes excess=5 (unambiguously
	// non-zero) so a regression is impossible to miss as a false-negative
	// green from an excess=0 coincidence.
	client := (&routingFakeClient{}).
		on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(5)}}}).
		on("SELECT", &fakeRowScanner{})

	fmUseInvestment := true
	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension:     model.DimensionInputRepo,
			Measure:       model.MeasureInputCount,
			DateRange:     &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:      50,
			MaxEdges:      200,
			UseInvestment: &fmUseInvestment,
		},
	}
	if _, err := Resolve(context.Background(), client, "org-fm-repo", batch); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(*orgs) != 0 {
		t.Fatalf("recorded orgs = %v, want none -- flow matrix's REPO dimension uses fixed templates that never join repos", *orgs)
	}
}

func TestResolve_FlowMatrix_ThemeDimension_NeverFiresRepoJoinDedupCheck(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)
	orgs := spyRepoJoinDedupCollisions(t)

	client := (&routingFakeClient{}).
		on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(5)}}}).
		on("SELECT", &fakeRowScanner{})

	fmUseInvestment := true
	batch := model.AnalyticsRequestInput{
		UseInvestment: boolPtr(true),
		FlowMatrix: &model.FlowMatrixRequestInput{
			Dimension:     model.DimensionInputTheme,
			Measure:       model.MeasureInputCount,
			DateRange:     &model.DateRangeInput{StartDate: mustGraphQLDate("2026-01-01"), EndDate: mustGraphQLDate("2026-01-07")},
			MaxNodes:      50,
			MaxEdges:      200,
			UseInvestment: &fmUseInvestment,
		},
	}
	if _, err := Resolve(context.Background(), client, "org-fm-theme", batch); err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(*orgs) != 0 {
		t.Fatalf("recorded orgs = %v, want none -- flow matrix's investment-dimension branch calls investmentContextFor with a one-element dimensions list that is never REPO", *orgs)
	}
}

// TestRecordInvestmentRepoJoinDedupCollisions_ErrorShortensCooldown is
// codex round 4's finding, closed: a query error used to claim the full
// 5-minute cooldown up front and never release it early, so a transient
// ClickHouse error silenced this check for the whole window. Fixed to
// mirror the sibling CHAOS-4759 guard's identical repair
// (argMaxNullTransitionErrorCooldown): a failed check shortens the claim to
// repoJoinDedupCollisionErrorCooldown so the NEXT request retries soon.
func TestRecordInvestmentRepoJoinDedupCollisions_ErrorShortensCooldown(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)

	now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	repoJoinDedupCollisionNow = func() time.Time { return now }

	failingClient := (&routingFakeClient{}).onErr("excess_repo_versions", errInjectedFetchFailure)
	RecordInvestmentRepoJoinDedupCollisions(context.Background(), failingClient, "org-1")
	if got := len(failingClient.calls); got != 1 {
		t.Fatalf("failing call: expected exactly one query attempt, got %d", got)
	}

	// Barely past the SHORT error cooldown, still well inside the full
	// success cooldown -- must be allowed to retry.
	repoJoinDedupCollisionNow = func() time.Time { return now.Add(repoJoinDedupCollisionErrorCooldown + time.Second) }

	client := (&routingFakeClient{}).on("excess_repo_versions", &fakeRowScanner{rows: [][]any{{int64(0)}}})
	RecordInvestmentRepoJoinDedupCollisions(context.Background(), client, "org-1")
	if got := len(client.calls); got != 1 {
		t.Fatalf("retry after error cooldown: expected exactly one query, got %d -- a fetch error must not consume the full 5-minute cooldown", got)
	}
}

// TestRecordInvestmentRepoJoinDedupCollisions_FetchFailureIsReported is
// codex round 4's finding, closed: a check failure must be reported
// through recordRepoJoinDedupCollisionFetchFailure (counter + warn log),
// not merely logged at debug -- a level this platform's default config
// drops, which would let a persistently broken check silently stop
// observing this org forever.
func TestRecordInvestmentRepoJoinDedupCollisions_FetchFailureIsReported(t *testing.T) {
	resetRepoJoinDedupCollisionCooldown(t)

	var capturedOrgID string
	var capturedErr error
	orig := recordRepoJoinDedupCollisionFetchFailure
	recordRepoJoinDedupCollisionFetchFailure = func(_ context.Context, orgID string, err error) {
		capturedOrgID = orgID
		capturedErr = err
	}
	t.Cleanup(func() { recordRepoJoinDedupCollisionFetchFailure = orig })

	client := (&routingFakeClient{}).onErr("excess_repo_versions", errInjectedFetchFailure)
	RecordInvestmentRepoJoinDedupCollisions(context.Background(), client, "org-1")

	if capturedOrgID != "org-1" {
		t.Errorf("captured org_id = %q, want %q", capturedOrgID, "org-1")
	}
	if !errors.Is(capturedErr, errInjectedFetchFailure) {
		t.Errorf("captured err = %v, want it to wrap %v", capturedErr, errInjectedFetchFailure)
	}
}
