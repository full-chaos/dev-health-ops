package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubWorkItemEngineTestDestinations is the engine-dependent set exercised by
// the composition doubles below.
var githubWorkItemEngineTestDestinations = []string{
	"investment_classifications_daily",
	"investment_metrics_daily",
	"issue_type_metrics_daily",
}

type stubWorkItemConn struct{ driver.Conn }

func githubWorkItemCompositionLease() providerfoundation.LeaseGuard {
	return providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })
}

// TestNewGitHubWorkItemClickHouseEffectsRejectsMissingDependencies observes each
// constructor guard failing on its own. A constructor that silently accepted a
// nil connection would hand back a sink whose every adapter panics on first
// use, far from the call site that omitted it.
func TestNewGitHubWorkItemClickHouseEffectsRejectsMissingDependencies(t *testing.T) {
	t.Parallel()
	conn := stubWorkItemConn{}
	lease := githubWorkItemCompositionLease()

	for _, testCase := range []struct {
		name  string
		conn  driver.Conn
		lease providerfoundation.LeaseGuard
	}{
		{name: "nil conn", conn: nil, lease: lease},
		{name: "nil lease", conn: conn, lease: nil},
		{name: "both nil", conn: nil, lease: nil},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			sink, err := NewGitHubWorkItemClickHouseEffects(testCase.conn, testCase.lease)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
			// The zero sink must not be mistakable for a usable one.
			if !reflect.DeepEqual(sink, GitHubWorkItemClickHouseEffects{}) {
				t.Fatalf("rejected build returned a populated sink: %+v", sink)
			}
			if sink.complete() {
				t.Fatal("rejected build reported itself complete")
			}
		})
	}
}

// TestNewGitHubWorkItemClickHouseEffectsWiresAllSixteen is the constructor
// tripwire: every canonical destination must be concrete before the returned
// sink may be used.
func TestNewGitHubWorkItemClickHouseEffectsWiresAllSixteen(t *testing.T) {
	t.Parallel()
	sink, err := NewGitHubWorkItemClickHouseEffects(
		stubWorkItemConn{}, githubWorkItemCompositionLease(),
	)
	if err != nil {
		t.Fatal(err)
	}
	missing := sink.MissingDestinations()
	if len(missing) != 0 {
		t.Fatalf("missing=%v want none", missing)
	}
	if !sink.complete() {
		t.Fatal("fully constructed sink did not report itself complete")
	}
	for _, destination := range workItemRouteDestinations() {
		adapter, known := sink.adapterForDestination(destination)
		if !known {
			t.Fatalf("destination %q is not dispatchable", destination)
		}
		if adapter == nil {
			t.Errorf("destination %q should be wired but is nil", destination)
		}
	}
	if sink.Lease == nil {
		t.Fatal("composite lease guard was not installed")
	}
}

// TestGitHubWorkItemSinkMissingDestinationsIsClauseLevel nils each of the
// sixteen slots in turn against an otherwise COMPLETE sink. A single mutation of
// the whole completeness check would be killed by any one of these; only the
// per-slot sweep proves no individual slot is unchecked.
func TestGitHubWorkItemSinkMissingDestinationsIsClauseLevel(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "work-items")
	effects, err := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{})
	if err != nil {
		t.Fatal(err)
	}
	complete := githubWorkItemEffectsFixture(newSemanticWorkItemEffectBackend())
	if got := complete.MissingDestinations(); len(got) != 0 {
		t.Fatalf("fixture is not complete: missing=%v", got)
	}

	for _, destination := range workItemRouteDestinations() {
		t.Run(destination, func(t *testing.T) {
			t.Parallel()
			mutated := githubWorkItemSinkWithout(
				githubWorkItemEffectsFixture(newSemanticWorkItemEffectBackend()),
				destination,
			)
			if got := mutated.MissingDestinations(); !reflect.DeepEqual(
				got, []string{destination},
			) {
				t.Fatalf("missing=%v want=[%s]", got, destination)
			}
			if mutated.complete() {
				t.Fatalf("sink without %s reported complete", destination)
			}
			// The gate must reject writes to EVERY destination, not only the
			// one that is missing: a partial sink is not partially usable.
			for _, effect := range effects {
				if err := mutated.WriteEffect(
					context.Background(), claim, effect,
				); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("write %s with %s missing: error=%v",
						effect.Destination, destination, err)
				}
				if _, err := mutated.InspectEffect(
					context.Background(), claim, effect,
				); !errors.Is(err, ErrInvalidConfiguration) {
					t.Fatalf("inspect %s with %s missing: error=%v",
						effect.Destination, destination, err)
				}
			}
		})
	}
}

// githubWorkItemSinkWithout clears exactly one slot by destination name. It
// switches over the same canonical list the production dispatch does, so a
// destination added without a case here fails the test rather than silently
// leaving the sink complete.
func githubWorkItemSinkWithout(
	sink GitHubWorkItemClickHouseEffects,
	destination string,
) GitHubWorkItemClickHouseEffects {
	switch destination {
	case "ai_attribution":
		sink.AIAttribution = nil
	case "estimate_coverage_metrics_daily":
		sink.EstimateCoverageMetricsDaily = nil
	case "investment_classifications_daily":
		sink.InvestmentClassificationsDaily = nil
	case "investment_metrics_daily":
		sink.InvestmentMetricsDaily = nil
	case "issue_type_metrics_daily":
		sink.IssueTypeMetricsDaily = nil
	case "sprints":
		sink.Sprints = nil
	case "work_item_cycle_times":
		sink.WorkItemCycleTimes = nil
	case "work_item_dependencies":
		sink.WorkItemDependencies = nil
	case "work_item_interactions":
		sink.WorkItemInteractions = nil
	case "work_item_metrics_daily":
		sink.WorkItemMetricsDaily = nil
	case "work_item_reopen_events":
		sink.WorkItemReopenEvents = nil
	case "work_item_state_durations_daily":
		sink.WorkItemStateDurationsDaily = nil
	case "work_item_team_attributions":
		sink.WorkItemTeamAttributions = nil
	case "work_item_transitions":
		sink.WorkItemTransitions = nil
	case "work_item_user_metrics_daily":
		sink.WorkItemUserMetricsDaily = nil
	case "work_items":
		sink.WorkItems = nil
	default:
		panic("unmapped destination " + destination)
	}
	return sink
}

// TestGitHubWorkItemDerivedDaysMirrorsResolveDateRange pins the day list against
// resolve_date_range composed with _date_range. Each case names the Python
// input it stands for.
func TestGitHubWorkItemDerivedDaysMirrorsResolveDateRange(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 6, 9, 15, 0, 0, time.UTC)
	day := func(year int, month time.Month, date int) time.Time {
		return time.Date(year, month, date, 0, 0, 0, 0, time.UTC)
	}
	instant := func(value time.Time) *time.Time { return &value }

	for _, testCase := range []struct {
		name  string
		since *time.Time
		// before is the EXCLUSIVE window end.
		before *time.Time
		want   []time.Time
	}{{
		// No flags: before defaults to utc_today()+1d, so end_day is today and
		// backfill is 1.
		name: "no window is the single run day",
		want: []time.Time{day(2026, 8, 6)},
	}, {
		// --before 2026-08-06 => end_day 2026-08-05, backfill 1.
		name:   "midnight before is exclusive",
		before: instant(day(2026, 8, 6)),
		want:   []time.Time{day(2026, 8, 5)},
	}, {
		// No Python analogue -- a date flag cannot express it. A window ending
		// mid-day still covers that day, so it must not be dropped.
		name:   "mid-day before keeps the partially covered day",
		before: instant(time.Date(2026, 8, 6, 14, 30, 0, 0, time.UTC)),
		want:   []time.Time{day(2026, 8, 6)},
	}, {
		// --since 2026-08-03 --before 2026-08-06 => end_day 08-05,
		// backfill_days = (08-05 - 08-03).days + 1 = 3.
		name:   "since and before span an inclusive ascending range",
		since:  instant(day(2026, 8, 3)),
		before: instant(day(2026, 8, 6)),
		want:   []time.Time{day(2026, 8, 3), day(2026, 8, 4), day(2026, 8, 5)},
	}, {
		name:   "since equal to end day is a single day",
		since:  instant(day(2026, 8, 5)),
		before: instant(day(2026, 8, 6)),
		want:   []time.Time{day(2026, 8, 5)},
	}, {
		// A since timestamp mid-day still starts on that day: Python takes a
		// date, and the day is covered.
		name:   "since is truncated to its day",
		since:  instant(time.Date(2026, 8, 4, 23, 59, 59, 0, time.UTC)),
		before: instant(day(2026, 8, 6)),
		want:   []time.Time{day(2026, 8, 4), day(2026, 8, 5)},
	}} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			claim := githubWorkItemOracleClaim()
			claim.SinceAt, claim.BeforeAt = testCase.since, testCase.before
			days, err := githubWorkItemDerivedDays(claim, normalizedAt)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(days, testCase.want) {
				t.Fatalf("days=%v want=%v", days, testCase.want)
			}
		})
	}
}

func TestGitHubWorkItemDerivedDaysFailsClosedOnUnusableWindows(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 6, 9, 15, 0, 0, time.UTC)
	day := func(year int, month time.Month, date int) time.Time {
		return time.Date(year, month, date, 0, 0, 0, 0, time.UTC)
	}
	instant := func(value time.Time) *time.Time { return &value }

	for _, testCase := range []struct {
		name         string
		since        *time.Time
		before       *time.Time
		normalizedAt time.Time
	}{{
		// cli.py:110 exits here rather than deriving an empty range.
		name:  "since after end day",
		since: instant(day(2026, 8, 9)), before: instant(day(2026, 8, 6)),
	}, {
		name: "zero normalizedAt", normalizedAt: time.Time{},
	}, {
		name: "zero before", before: instant(time.Time{}),
	}, {
		name: "zero since", since: instant(time.Time{}),
	}, {
		// One day past the cap. The planner chunks backfills, so a window this
		// wide means it did not run.
		name:   "window wider than the backfill cap",
		since:  instant(day(2026, 8, 6).AddDate(0, 0, -githubWorkItemDerivedMaxBackfillDays)),
		before: instant(day(2026, 8, 7)),
	}} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			claim := githubWorkItemOracleClaim()
			claim.SinceAt, claim.BeforeAt = testCase.since, testCase.before
			at := normalizedAt
			if testCase.name == "zero normalizedAt" {
				at = testCase.normalizedAt
			}
			if _, err := githubWorkItemDerivedDays(claim, at); !errors.Is(
				err, ErrInvalidConfiguration,
			) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
}

// TestGitHubWorkItemEngineSeamIsInvokedPerDay pins the concrete engine's outer
// composition contract. All three engine destinations are per-day in Python
// and stamp `day=d` (job_work_items.py:1346/:1387/:1433 inside the :1238 loop),
// so a seam called once per window cannot express them and assignment would
// keep only the last day. The hostile double makes both failures observable.
func TestGitHubWorkItemEngineSeamIsInvokedPerDay(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	since := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	recorder := &githubWorkItemRecordingEngine{}
	deriver := GitHubWorkItemDeriver{
		Source: &fakeGitHubWorkItemDerivationContextSource{}, engine: recorder,
	}

	derived, err := deriver.Derive(
		context.Background(), claim, githubWorkItemDeriverFixture(claim),
		time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	wantDays := []time.Time{
		time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 4, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC),
	}
	if !reflect.DeepEqual(recorder.days, wantDays) {
		t.Fatalf("engine saw days=%v want=%v", recorder.days, wantDays)
	}
	// Accumulated, not overwritten: one row per day per destination.
	for _, destination := range githubWorkItemEngineTestDestinations {
		rows := derived[destination]
		if len(rows) != len(wantDays) {
			t.Fatalf("%s has %d rows over %d days; the merge is not accumulating",
				destination, len(rows), len(wantDays))
		}
		for index, row := range rows {
			want := `{"day":"` + wantDays[index].Format("2006-01-02") + `"}`
			if string(row) != want {
				t.Errorf("%s row[%d]=%s want=%s", destination, index, row, want)
			}
		}
	}
}

type githubWorkItemRecordingEngine struct{ days []time.Time }

func (engine *githubWorkItemRecordingEngine) Derive(
	_ context.Context,
	_ Claim,
	_ githubWorkItemRows,
	day time.Time,
	_ time.Time,
	_ githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	engine.days = append(engine.days, day)
	produced := make(map[string][]json.RawMessage, len(githubWorkItemEngineTestDestinations))
	for _, destination := range githubWorkItemEngineTestDestinations {
		produced[destination] = []json.RawMessage{
			json.RawMessage(`{"day":"` + day.Format("2006-01-02") + `"}`),
		}
	}
	return produced, nil
}

// TestGitHubWorkItemDerivedDaysCapIsTheStatedThreeSixtySix asserts the cap's
// VALUE, not merely that some cap exists.
//
// The window here is written from LITERAL DATES with no reference to
// githubWorkItemDerivedMaxBackfillDays. Deriving the fixture from the constant
// -- as the fail-closed table above does -- keeps passing if the constant is
// changed to any other number, so it measures the mechanism and not the bound.
func TestGitHubWorkItemDerivedDaysCapIsTheStatedThreeSixtySix(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 6, 9, 15, 0, 0, time.UTC)
	instant := func(value time.Time) *time.Time { return &value }
	// 2025-08-06 .. 2026-08-06 inclusive is 366 days (2026 is not a leap year).
	atCap := time.Date(2025, 8, 6, 0, 0, 0, 0, time.UTC)
	overCap := time.Date(2025, 8, 5, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)

	claim := githubWorkItemOracleClaim()
	claim.SinceAt, claim.BeforeAt = instant(atCap), instant(before)
	days, err := githubWorkItemDerivedDays(claim, normalizedAt)
	if err != nil {
		t.Fatalf("a 366-day window must be accepted: %v", err)
	}
	if len(days) != 366 {
		t.Fatalf("days=%d want=366", len(days))
	}
	if !days[0].Equal(atCap) {
		t.Errorf("first day=%v want=%v", days[0], atCap)
	}
	if want := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC); !days[365].Equal(want) {
		t.Errorf("last day=%v want=%v", days[365], want)
	}

	claim.SinceAt = instant(overCap)
	if _, err := githubWorkItemDerivedDays(claim, normalizedAt); !errors.Is(
		err, ErrInvalidConfiguration,
	) {
		t.Fatalf("a 367-day window must be refused: error=%v", err)
	}
}

// githubWorkItemDeriverFixture is a corpus that produces rows on every ported
// derived surface, so a multi-day assertion cannot pass by comparing empties.
func githubWorkItemDeriverFixture(claim Claim) githubWorkItemRows {
	return githubWorkItemRows{
		WorkItems: []githubWorkItemRow{{
			WorkItemID: "acme/api#1", Provider: "github", Title: "t", Type: "issue",
			Status: "todo", ProjectID: stringPointer("acme/api"),
			CreatedAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			UpdatedAt: time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC),
			OrgID:     claim.OrgID,
		}},
		StatusTransitions: []githubWorkItemTransitionRow{{
			WorkItemID: "acme/api#1", Provider: "github",
			OccurredAt: time.Date(2026, 8, 3, 6, 0, 0, 0, time.UTC),
			FromStatus: "todo", ToStatus: "in_progress", OrgID: claim.OrgID,
		}, {
			WorkItemID: "acme/api#1", Provider: "github",
			OccurredAt: time.Date(2026, 8, 4, 6, 0, 0, 0, time.UTC),
			FromStatus: "in_progress", ToStatus: "done", OrgID: claim.OrgID,
		}},
	}
}

// TestGitHubWorkItemDeriverMirrorsCHAOS3494WriteAmplification is the
// manifest-layer half of the two-layer duplicate assertion.
//
// Python calls compute_work_item_team_attributions INSIDE its day loop with no
// `day` argument, so an n-day window emits the same rows n times. The port
// mirrors that per D16. The collapse to one row is asserted separately, at the
// readback layer, where it actually happens.
func TestGitHubWorkItemDeriverMirrorsCHAOS3494WriteAmplification(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	rows := githubWorkItemDeriverFixture(claim)
	normalizedAt := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	source := &fakeGitHubWorkItemDerivationContextSource{}
	deriver := GitHubWorkItemDeriver{
		Source: source, engine: githubWorkItemStubEngine{},
	}

	single := claim
	singleBefore := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	singleSince := time.Date(2026, 8, 5, 0, 0, 0, 0, time.UTC)
	single.SinceAt, single.BeforeAt = &singleSince, &singleBefore
	oneDay, err := deriver.Derive(context.Background(), single, rows, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	oneDayAttributions := oneDay["work_item_team_attributions"]
	if len(oneDayAttributions) == 0 {
		t.Fatal("fixture produced no team attributions; the assertion would be vacuous")
	}

	multi := claim
	multiBefore := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	multiSince := time.Date(2026, 8, 3, 0, 0, 0, 0, time.UTC)
	multi.SinceAt, multi.BeforeAt = &multiSince, &multiBefore
	threeDay, err := deriver.Derive(context.Background(), multi, rows, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}

	const days = 3
	attributions := threeDay["work_item_team_attributions"]
	if len(attributions) != days*len(oneDayAttributions) {
		t.Fatalf("team attributions=%d want=%d (%d days x %d)",
			len(attributions), days*len(oneDayAttributions), days,
			len(oneDayAttributions))
	}
	// Not merely the same count -- the same BYTES, repeated. That is what makes
	// it write amplification rather than three days of distinct output.
	for index, row := range attributions {
		want := oneDayAttributions[index%len(oneDayAttributions)]
		if !reflect.DeepEqual(row, want) {
			t.Fatalf("attribution[%d]=%s want=%s (rows must repeat byte-identically)",
				index, row, want)
		}
	}

	// Control: a day-carrying surface must NOT repeat identically, or the
	// assertion above would be measuring a loop that ignores `day` everywhere.
	durations := threeDay["work_item_state_durations_daily"]
	if len(durations) < 2 {
		t.Fatalf("state durations=%d; control needs at least two", len(durations))
	}
	if reflect.DeepEqual(durations[0], durations[1]) {
		t.Fatal("state duration rows repeat identically across days; " +
			"the day parameter is not reaching the builder")
	}
}

// TestGitHubWorkItemDeriverLoadsDerivationContextOncePerRun pins the context
// load outside the day loop, matching job_work_items.py:1195-1209.
func TestGitHubWorkItemDeriverLoadsDerivationContextOncePerRun(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	since := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	claim.SinceAt, claim.BeforeAt = &since, &before
	source := &countingGitHubWorkItemDerivationContextSource{}
	deriver := GitHubWorkItemDeriver{Source: source, engine: githubWorkItemStubEngine{}}

	if _, err := deriver.Derive(
		context.Background(), claim, githubWorkItemDeriverFixture(claim),
		time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	); err != nil {
		t.Fatal(err)
	}
	if source.loads != 1 {
		t.Fatalf("derivation context loads=%d want=1 over a five-day window", source.loads)
	}
}

type countingGitHubWorkItemDerivationContextSource struct {
	loads           int
	storedEdgeLoads int
}

func (source *countingGitHubWorkItemDerivationContextSource) Load(
	context.Context, Claim, githubWorkItemDerivationLoadRequest,
) (githubWorkItemDerivationFacts, error) {
	source.loads++
	return githubWorkItemDerivationFacts{}, nil
}

func (source *countingGitHubWorkItemDerivationContextSource) LoadStoredInheritableEdges(
	context.Context, Claim, []string,
) ([]githubWorkItemDependencyRow, error) {
	source.storedEdgeLoads++
	return nil, nil
}

// githubWorkItemStubEngine isolates composition mechanics from the real config
// engines so error, omission and multi-day accumulation paths stay focused.
type githubWorkItemStubEngine struct{ err error }

func (engine githubWorkItemStubEngine) Derive(
	_ context.Context,
	_ Claim,
	_ githubWorkItemRows,
	day time.Time,
	_ time.Time,
	_ githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	if engine.err != nil {
		return nil, engine.err
	}
	produced := make(map[string][]json.RawMessage, len(githubWorkItemEngineTestDestinations))
	for _, destination := range githubWorkItemEngineTestDestinations {
		// Stamped with the day it was handed: an engine call hoisted out of the
		// loop, or a merge that assigned instead of appending, shows up as a
		// missing or wrong-dated row rather than as an indistinguishable empty.
		produced[destination] = []json.RawMessage{
			json.RawMessage(`{"day":"` + day.Format("2006-01-02") + `"}`),
		}
	}
	return produced, nil
}

// TestGitHubWorkItemDeriverFailsClosedWithoutConfiguredEngine proves a caller
// that bypasses the atomic constructor cannot fabricate engine-backed success.
func TestGitHubWorkItemDeriverFailsClosedWithoutConfiguredEngine(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	deriver := GitHubWorkItemDeriver{Source: &fakeGitHubWorkItemDerivationContextSource{}}

	derived, err := deriver.Derive(
		context.Background(), claim, githubWorkItemDeriverFixture(claim),
		time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrGitHubWorkItemsDerivationsUnavailable) {
		t.Fatalf("error=%v want ErrGitHubWorkItemsDerivationsUnavailable", err)
	}
	if derived != nil {
		t.Fatalf("failed derivation returned rows: %v", derived)
	}
	for _, destination := range githubWorkItemEngineTestDestinations {
		if !strings.Contains(err.Error(), destination) {
			t.Errorf("error %q does not name %q", err.Error(), destination)
		}
	}
	// The ported six must NOT appear: naming them would misreport what is
	// actually missing.
	for _, destination := range githubWorkItemDerivedOwnedDestinations {
		if strings.Contains(err.Error(), destination) {
			t.Errorf("error %q names ported destination %q", err.Error(), destination)
		}
	}
}

func TestGitHubWorkItemDeriverFailsClosedOnUnusableInput(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	rows := githubWorkItemDeriverFixture(claim)
	normalizedAt := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	source := &fakeGitHubWorkItemDerivationContextSource{}

	foreignDataset := claim
	foreignDataset.Dataset = "work-item-labels"
	foreignProvider := claim
	foreignProvider.Provider = "gitlab"

	for _, testCase := range []struct {
		name    string
		deriver GitHubWorkItemDeriver
		claim   Claim
		at      time.Time
	}{
		{name: "nil source", deriver: GitHubWorkItemDeriver{}, claim: claim, at: normalizedAt},
		{
			name: "zero normalizedAt", deriver: GitHubWorkItemDeriver{Source: source},
			claim: claim, at: time.Time{},
		},
		{
			// The canonical claim is the only one the composite serves; an
			// alias dataset reaching here means the family collapsed wrong.
			name: "alias dataset", deriver: GitHubWorkItemDeriver{Source: source},
			claim: foreignDataset, at: normalizedAt,
		},
		{
			name: "foreign provider", deriver: GitHubWorkItemDeriver{Source: source},
			claim: foreignProvider, at: normalizedAt,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			if _, err := testCase.deriver.Derive(
				context.Background(), testCase.claim, rows, testCase.at,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
}

// TestGitHubWorkItemDeriverRejectsEngineOverreach guards the seam boundary: an
// engine that restated a ported destination would give one surface two
// producers, with nothing comparing them.
func TestGitHubWorkItemDeriverRejectsEngineOverreach(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	rows := githubWorkItemDeriverFixture(claim)
	normalizedAt := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name        string
		destination string
	}{
		{name: "restates a ported destination", destination: "work_item_team_attributions"},
		{name: "invents a destination", destination: "not_a_destination"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			deriver := GitHubWorkItemDeriver{
				Source: &fakeGitHubWorkItemDerivationContextSource{},
				engine: githubWorkItemOverreachingEngine{destination: testCase.destination},
			}
			if _, err := deriver.Derive(
				context.Background(), claim, rows, normalizedAt,
			); !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
		})
	}
}

type githubWorkItemOverreachingEngine struct{ destination string }

func (engine githubWorkItemOverreachingEngine) Derive(
	_ context.Context,
	_ Claim,
	_ githubWorkItemRows,
	_ time.Time,
	_ time.Time,
	_ githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	produced := map[string][]json.RawMessage{
		engine.destination: {},
	}
	for _, destination := range githubWorkItemEngineTestDestinations {
		produced[destination] = []json.RawMessage{}
	}
	return produced, nil
}

// TestGitHubWorkItemMergeDerivedRowsGuardsTheOwnedSet exercises the merge guard
// directly. No ported builder can currently emit an unowned destination, so
// without this test the guard is unreachable code that reads as protection --
// it survived a mutation to `if false` until this case existed. It becomes
// reachable the moment the owned-subset constant and the builders drift apart,
// which is exactly when one destination would silently gain two producers.
func TestGitHubWorkItemMergeDerivedRowsGuardsTheOwnedSet(t *testing.T) {
	t.Parallel()
	owned := func() map[string][]json.RawMessage {
		derived := make(map[string][]json.RawMessage)
		for _, destination := range githubWorkItemDerivedOwnedDestinations {
			derived[destination] = []json.RawMessage{}
		}
		return derived
	}

	t.Run("rejects a destination outside the owned set", func(t *testing.T) {
		t.Parallel()
		// A canonical derived destination the ported builders do NOT own: the
		// realistic drift, not an invented string.
		if err := githubWorkItemMergeDerivedRows(owned(), map[string][]json.RawMessage{
			"investment_metrics_daily": {},
		}); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error=%v want ErrInvalidConfiguration", err)
		}
	})

	t.Run("accumulates rather than overwriting", func(t *testing.T) {
		t.Parallel()
		derived := owned()
		first := json.RawMessage(`{"n":1}`)
		second := json.RawMessage(`{"n":2}`)
		for _, produced := range []json.RawMessage{first, second} {
			if err := githubWorkItemMergeDerivedRows(
				derived,
				map[string][]json.RawMessage{"work_item_team_attributions": {produced}},
			); err != nil {
				t.Fatal(err)
			}
		}
		got := derived["work_item_team_attributions"]
		if !reflect.DeepEqual(got, []json.RawMessage{first, second}) {
			t.Fatalf("merged=%v want both rows in call order", got)
		}
	})
}

// TestGitHubWorkItemDeriverPropagatesEngineFailure keeps a broken engine from
// being read as an absent one.
func TestGitHubWorkItemDeriverPropagatesEngineFailure(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	sentinel := errors.New("engine exploded")
	deriver := GitHubWorkItemDeriver{
		Source: &fakeGitHubWorkItemDerivationContextSource{},
		engine: githubWorkItemStubEngine{err: sentinel},
	}
	if _, err := deriver.Derive(
		context.Background(), claim, githubWorkItemDeriverFixture(claim),
		time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC),
	); !errors.Is(err, sentinel) {
		t.Fatalf("error=%v want the engine's own failure", err)
	}
}

// githubWorkItemSilentEngine returns success and nothing else -- the fail-open
// shape: a producer that answers "fine, no rows" where it should have refused.
type githubWorkItemSilentEngine struct {
	rows map[string][]json.RawMessage
}

func (engine githubWorkItemSilentEngine) Derive(
	_ context.Context,
	_ Claim,
	_ githubWorkItemRows,
	_ time.Time,
	_ time.Time,
	_ githubWorkItemDerivationContext,
) (map[string][]json.RawMessage, error) {
	return engine.rows, nil
}

// TestGitHubWorkItemCompositionNeverFailsOpen probes the shape that blocked both
// engine PRs: an input that should raise instead producing an empty-but-plausible
// result. Nothing here is inferred from reading the code -- each case is run and
// the refusal observed.
func TestGitHubWorkItemCompositionNeverFailsOpen(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	rows := githubWorkItemDeriverFixture(claim)
	normalizedAt := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)

	t.Run("an engine that returns nothing at all is not a complete derivation", func(t *testing.T) {
		t.Parallel()
		deriver := GitHubWorkItemDeriver{
			Source: &fakeGitHubWorkItemDerivationContextSource{},
			engine: githubWorkItemSilentEngine{rows: nil},
		}
		if _, err := deriver.Derive(
			context.Background(), claim, rows, normalizedAt,
		); !errors.Is(err, ErrGitHubWorkItemsDerivationsUnavailable) {
			t.Fatalf("error=%v want ErrGitHubWorkItemsDerivationsUnavailable", err)
		}
	})

	t.Run("an engine covering only some destinations is refused", func(t *testing.T) {
		t.Parallel()
		partial := map[string][]json.RawMessage{
			githubWorkItemEngineTestDestinations[0]: {},
		}
		deriver := GitHubWorkItemDeriver{
			Source: &fakeGitHubWorkItemDerivationContextSource{},
			engine: githubWorkItemSilentEngine{rows: partial},
		}
		_, err := deriver.Derive(context.Background(), claim, rows, normalizedAt)
		if !errors.Is(err, ErrGitHubWorkItemsDerivationsUnavailable) {
			t.Fatalf("error=%v want ErrGitHubWorkItemsDerivationsUnavailable", err)
		}
		// The still-missing two must be named; the one it did cover must not.
		for _, destination := range githubWorkItemEngineTestDestinations[1:] {
			if !strings.Contains(err.Error(), destination) {
				t.Errorf("error %q does not name still-missing %q", err.Error(), destination)
			}
		}
		if strings.Contains(err.Error(), githubWorkItemEngineTestDestinations[0]) {
			t.Errorf("error %q names %q, which the engine did cover",
				err.Error(), githubWorkItemEngineTestDestinations[0])
		}
	})

	t.Run("a deliberately corrupted sink refuses every write", func(t *testing.T) {
		t.Parallel()
		// The complete constructor no longer has an expected-error path. Model
		// partial construction by removing one concrete engine adapter after
		// construction; the composite gate must still refuse EVERY destination,
		// not merely the missing one.
		sink, err := NewGitHubWorkItemClickHouseEffects(
			stubWorkItemConn{}, githubWorkItemCompositionLease(),
		)
		if err != nil {
			t.Fatal(err)
		}
		sink.InvestmentMetricsDaily = nil
		if !reflect.DeepEqual(
			sink.MissingDestinations(), []string{githubInvestmentMetricsDestination},
		) {
			t.Fatalf("missing=%v want investment metrics", sink.MissingDestinations())
		}
		effects, buildErr := BuildGitHubWorkItemEffects(GitHubWorkItemEffectRows{})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		sinkClaim := nativeTestClaim("github", "work-items")
		for _, effect := range effects {
			if writeErr := sink.WriteEffect(
				context.Background(), sinkClaim, effect,
			); !errors.Is(writeErr, ErrInvalidConfiguration) {
				t.Fatalf("write %s on an incomplete sink: error=%v",
					effect.Destination, writeErr)
			}
		}
	})
}

// TestGitHubWorkItemDeriverComposesTheFullSixteenEffectManifest is the
// composition proof: real non-engine builders plus a focused engine stub,
// through the route's effect construction,
// into a complete sink that routes all sixteen.
func TestGitHubWorkItemDeriverComposesTheFullSixteenEffectManifest(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	rows := githubWorkItemDeriverFixture(claim)
	normalizedAt := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	deriver := GitHubWorkItemDeriver{
		Source: &fakeGitHubWorkItemDerivationContextSource{},
		engine: githubWorkItemStubEngine{},
	}

	derived, err := deriver.Derive(context.Background(), claim, rows, normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	if len(derived) != len(githubWorkItemDerivedDestinations) {
		t.Fatalf("derived destinations=%d want=%d",
			len(derived), len(githubWorkItemDerivedDestinations))
	}
	for _, destination := range githubWorkItemDerivedDestinations {
		if _, produced := derived[destination]; !produced {
			t.Fatalf("derived map is missing %q", destination)
		}
	}

	effects, err := buildGitHubWorkItemsRouteEffects(rows, derived)
	if err != nil {
		t.Fatal(err)
	}
	if len(effects) != len(workItemRouteDestinations()) {
		t.Fatalf("effects=%d want=%d", len(effects), len(workItemRouteDestinations()))
	}

	// A complete sink -- the shape NewGitHubWorkItemClickHouseEffects will
	// return once PR-C lands -- must route every one of them.
	backend := newSemanticWorkItemEffectBackend()
	sink := githubWorkItemEffectsFixture(backend)
	sinkClaim := nativeTestClaim("github", "work-items")
	for _, effect := range effects {
		if err := sink.WriteEffect(context.Background(), sinkClaim, effect); err != nil {
			t.Fatalf("write %s: %v", effect.Destination, err)
		}
		inspection, err := sink.InspectEffect(context.Background(), sinkClaim, effect)
		if err != nil || inspection != EffectExact {
			t.Fatalf("inspect %s=%s error=%v", effect.Destination, inspection, err)
		}
	}
	if len(backend.writeCounts) != len(workItemRouteDestinations()) {
		t.Fatalf("destinations written=%d want=%d",
			len(backend.writeCounts), len(workItemRouteDestinations()))
	}
}
