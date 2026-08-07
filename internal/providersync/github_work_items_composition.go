package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// ErrGitHubWorkItemSinkIncomplete reports a composite sink built while one or
// more destination adapters are unported. It always wraps the destination names
// so a caller learns WHICH surfaces are absent, never only that some are.
var ErrGitHubWorkItemSinkIncomplete = errors.New(
	"github work-item clickhouse sink is missing destination adapters",
)

// NewGitHubWorkItemClickHouseEffects wires every landed destination adapter
// onto the composite sink.
//
// It REGISTERS AND ACTIVATES NOTHING. cmd/dev-health-worker/provider_sync.go
// gains no case for the work-item family here, the provider matrix still marks
// all five aliases route_ready: false, and the route still returns a nil
// watermark -- so the only thing that can reach this constructor today is a
// test. Activation is a separate change that must first satisfy the unmet D17
// obligation recorded on GitHubWorkItemsIncomplete.
//
// The returned sink is USABLE ONLY when the error is nil. On an incomplete
// build the sink is returned alongside the error rather than zeroed, because
// the caller that wants to report the gap needs MissingDestinations from it.
//
// A caller that drops the error is still safe THROUGH THE COMMITTER, and the
// claim stops exactly there: WriteEffect and InspectEffect are the only methods
// EffectSink and EffectReadback expose, both go through resolve, and resolve is
// the only caller of complete -- so every write and readback on that surface
// fails closed. The adapter fields are exported and independently usable, so a
// caller holding the struct can invoke one directly and never reach complete.
// That is by design, and it is not a path the committer has.
//
// Lease flow mirrors the established BuildExecutor pattern: one ClickHouse
// connection and the LeaseSession that owns the claim, passed to every adapter
// that accepts a guard. The seven direct adapters deliberately hold no guard of
// their own -- the composite asserts the lease before and after each write on
// their behalf -- so the guard here is the only one they get.
func NewGitHubWorkItemClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) (GitHubWorkItemClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return GitHubWorkItemClickHouseEffects{}, ErrInvalidConfiguration
	}
	sink := GitHubWorkItemClickHouseEffects{
		Lease: lease,

		// Seven direct fact surfaces.
		AIAttribution:        GitHubAIAttributionClickHouseAdapter{Conn: conn},
		Sprints:              GitHubSprintsClickHouseAdapter{Conn: conn},
		WorkItemDependencies: GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn},
		WorkItemInteractions: GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn},
		WorkItemReopenEvents: GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn},
		WorkItemTransitions:  GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn},
		WorkItems:            GitHubWorkItemsClickHouseAdapter{Conn: conn},

		// Metric triplet.
		WorkItemCycleTimes: GitHubWorkItemCycleTimesClickHouseEffects{
			Conn: conn, Lease: lease,
		},
		WorkItemMetricsDaily: GitHubWorkItemMetricsDailyClickHouseEffects{
			Conn: conn, Lease: lease,
		},
		WorkItemUserMetricsDaily: GitHubWorkItemUserMetricsDailyClickHouseEffects{
			Conn: conn, Lease: lease,
		},

		// Derived landing surfaces.
		EstimateCoverageMetricsDaily: GitHubEstimateCoverageClickHouseEffects{
			Conn: conn, Lease: lease,
		},
		WorkItemStateDurationsDaily: GitHubWorkItemStateDurationsClickHouseEffects{
			Conn: conn, Lease: lease,
		},
		WorkItemTeamAttributions: GitHubWorkItemTeamAttributionsClickHouseEffects{
			Conn: conn, Lease: lease,
		},

		// investment_classifications_daily, investment_metrics_daily and
		// issue_type_metrics_daily stay nil until their engines are ported.
		// They are left unset rather than filled with a no-op adapter: a no-op
		// would let complete() report a sink that silently writes nothing to
		// three real tables.
	}
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, fmt.Errorf(
			"%w: %s", ErrGitHubWorkItemSinkIncomplete, strings.Join(missing, ", "),
		)
	}
	return sink, nil
}

// githubWorkItemDerivedOwnedDestinations is what the ported builders speak for
// today: the metric triplet plus the three derived landing surfaces. It is a
// strict subset of githubWorkItemDerivedDestinations, which the route requires
// in full.
var githubWorkItemDerivedOwnedDestinations = func() []string {
	owned := slices.Concat(
		githubWorkItemMetricTripletDestinations,
		githubWorkItemDerivedSurfaceDestinations,
	)
	slices.Sort(owned)
	return owned
}()

// githubWorkItemDerivedEngineDestinations is what the unported engines owe: the
// canonical derived set minus what the ported builders already speak for.
// Derived rather than hand-listed, so it cannot disagree with either.
var githubWorkItemDerivedEngineDestinations = func() []string {
	owed := make([]string, 0, len(githubWorkItemDerivedDestinations))
	for _, destination := range githubWorkItemDerivedDestinations {
		if !slices.Contains(githubWorkItemDerivedOwnedDestinations, destination) {
			owed = append(owed, destination)
		}
	}
	return owed
}()

// githubWorkItemEngineDeriver is the PR-C seam for the three engine-dependent
// destinations. It is deliberately UNEXPORTED and has no production setter: the
// only thing that can install one is a test inside this package, which is what
// lets the composition be proven end-to-end without fabricating the engines'
// output in production. When the engines land they replace this seam.
//
// IT TAKES A DAY AND IS CALLED ONCE PER DAY. All three destinations it will
// serve are per-day in Python and stamp `day=d` on every row: issue-type
// metrics (job_work_items.py:1346), investment classifications (:1387) and
// investment metrics (:1433) are all built inside the :1238 day loop, from
// state that resets each iteration. A seam invoked once for the whole window
// could not express that shape -- and a merge that ASSIGNED rather than
// appended would silently keep only the last day's rows the moment a real
// per-day engine replaced the stub. Shaped correctly now, while the only
// implementation is a test double and the change costs nothing.
//
// PR-C OBLIGATION: the ported engines emit rows for the day they are handed,
// not for the window. The per-day merge accumulates across days exactly as it
// does for the ported builders.
type githubWorkItemEngineDeriver interface {
	Derive(
		context.Context,
		Claim,
		githubWorkItemRows,
		time.Time,
		time.Time,
		githubWorkItemDerivationContext,
	) (map[string][]json.RawMessage, error)
}

// GitHubWorkItemDeriver implements githubWorkItemsDeriver over the ported
// builders. It owns the multi-day loop the route's single Derive call hides.
type GitHubWorkItemDeriver struct {
	// Source loads the donor attribution facts. A nil Source is a
	// configuration error, never a reason to derive without context: a missing
	// donor writes a DIFFERENT team onto every derived surface rather than
	// omitting a row, which D17 ratifies as fail-closed.
	Source githubWorkItemDerivationContextSource

	// engine is nil in every production build; see githubWorkItemEngineDeriver.
	engine githubWorkItemEngineDeriver
}

// githubWorkItemDerivedMaxBackfillDays bounds the day loop.
//
// DIVERGENCE FROM PYTHON, deliberate: job_work_items.py bounds nothing here
// because its window arrives from a CLI flag an operator typed. A sync unit's
// window arrives from the planner, which chunks backfills (backfill.chunker),
// so a window wider than a year means the chunker did not run -- and every day
// in the loop re-derives every surface over the full row set. Failing closed on
// an absurd window is the same rail, for the same reason, as the 100k donor cap
// in loadGitHubWorkItemDerivationContext: a silently enormous loop produces a
// confidently wrong-looking result far later than an immediate refusal does.
const githubWorkItemDerivedMaxBackfillDays = 366

// githubWorkItemDerivedDays mirrors resolve_date_range (utils/cli.py:81-117)
// composed with _date_range (job_work_items.py:112-117), which together produce
// the `days` list that job_work_items.py:1238 iterates.
//
// Python works in whole dates; a sync unit carries instants. The mapping:
//
//   - `before` is EXCLUSIVE, so the last included day is the day containing the
//     last instant strictly before BeforeAt. Expressing it as
//     date(BeforeAt - 1ns) reduces to Python's `before - 1 day` exactly on the
//     midnight-aligned boundaries a date flag can produce, and keeps a
//     mid-day BeforeAt from dropping the partially covered day it does cover.
//   - no BeforeAt means Python's default `before = utc_today() + 1 day`, whose
//     end_day is today. normalizedAt is this run's clock, so its UTC date is
//     that day.
//   - SinceAt present computes backfill_days from the range, as cli.py:108-112
//     does; absent, the range is the single end day (backfill defaults to 1).
//
// Days ascend, matching the order Python appends rows in.
func githubWorkItemDerivedDays(claim Claim, normalizedAt time.Time) ([]time.Time, error) {
	if normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	endDay := githubWorkItemDerivedUTCDate(normalizedAt)
	if claim.BeforeAt != nil {
		if claim.BeforeAt.IsZero() {
			return nil, ErrInvalidConfiguration
		}
		endDay = githubWorkItemDerivedUTCDate(claim.BeforeAt.Add(-time.Nanosecond))
	}
	startDay := endDay
	if claim.SinceAt != nil {
		if claim.SinceAt.IsZero() {
			return nil, ErrInvalidConfiguration
		}
		startDay = githubWorkItemDerivedUTCDate(*claim.SinceAt)
	}
	if startDay.After(endDay) {
		// cli.py:110 exits on --since after --before. A unit whose window is
		// inverted is malformed, not an empty-but-valid range: deriving zero
		// days would land an all-empty derived manifest that readback then
		// treats as a successful no-op.
		return nil, ErrInvalidConfiguration
	}
	days := make([]time.Time, 0, githubWorkItemDerivedMaxBackfillDays)
	for day := startDay; !day.After(endDay); day = day.AddDate(0, 0, 1) {
		if len(days) == githubWorkItemDerivedMaxBackfillDays {
			return nil, ErrInvalidConfiguration
		}
		days = append(days, day)
	}
	return days, nil
}

func githubWorkItemDerivedUTCDate(value time.Time) time.Time {
	utc := value.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}

// Derive runs every ported derived builder once per day in the unit's window.
//
// The derivation context is loaded ONCE, outside the loop. That mirrors
// job_work_items.py:1216-1236, where the context and the linked-issue resolver
// are built before `for d in days`, and it is what
// buildGitHubWorkItemDerivedSurfaces documents as its caller's contract. A
// per-day reload would also re-read donor facts that cannot change within a
// run, and would give two days of one run different attribution.
//
// CHAOS-3494 IS MIRRORED, NOT FIXED. buildGitHubWorkItemTeamAttributions takes
// no day, and Python calls it INSIDE the day loop
// (job_work_items.py:1260), so an n-day window recomputes and re-emits
// byte-identical attribution rows n times. The call stays inside this loop for
// the same reason: D16 requires the port to mirror the producer bug-for-bug so
// the differential oracle can prove parity, and hoisting it out would silently
// repair a defect the oracle is pinning. work_item_team_attributions is the
// only affected surface -- every other derived destination carries `day` in its
// rows, so its per-day output genuinely differs.
//
// The duplicate rows are NOT deduplicated here. They collapse at persistence:
// the adapter applies githubWorkItemDerivedSortingKeyDedupe to both the write
// and the readback expectation, and the table is a ReplacingMergeTree. That
// split is deliberate and separately asserted -- multiplicity n at this layer,
// collapse to one at the readback layer. Deduplicating here would erase the
// mirrored defect from the manifest the oracle compares.
func (deriver GitHubWorkItemDeriver) Derive(
	ctx context.Context,
	claim Claim,
	rows githubWorkItemRows,
	normalizedAt time.Time,
) (map[string][]json.RawMessage, error) {
	if ctx == nil || deriver.Source == nil || claim.Validate() != nil ||
		claim.Provider != "github" || claim.Dataset != "work-items" ||
		normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	days, err := githubWorkItemDerivedDays(claim, normalizedAt)
	if err != nil {
		return nil, err
	}
	derivationContext, err := loadGitHubWorkItemDerivationContext(
		ctx, claim, rows, deriver.Source, normalizedAt,
	)
	if err != nil {
		return nil, err
	}
	// Every owned destination is present from the start, so a destination that
	// legitimately produces no rows on any day is still reported as evaluated.
	derived := make(map[string][]json.RawMessage, len(githubWorkItemDerivedDestinations))
	for _, destination := range githubWorkItemDerivedOwnedDestinations {
		derived[destination] = []json.RawMessage{}
	}
	for _, day := range days {
		triplet, err := buildGitHubWorkItemMetricTriplet(
			claim, rows, day, normalizedAt, derivationContext,
		)
		if err != nil {
			return nil, err
		}
		tripletRows, err := triplet.derivedRows()
		if err != nil {
			return nil, err
		}
		if err := githubWorkItemMergeDerivedRows(derived, tripletRows); err != nil {
			return nil, err
		}
		surfaces, err := buildGitHubWorkItemDerivedSurfaces(
			claim, rows, day, normalizedAt, derivationContext,
		)
		if err != nil {
			return nil, err
		}
		surfaceRows, err := surfaces.derivedRows()
		if err != nil {
			return nil, err
		}
		if err := githubWorkItemMergeDerivedRows(derived, surfaceRows); err != nil {
			return nil, err
		}
		if deriver.engine == nil {
			continue
		}
		engineRows, err := deriver.engine.Derive(
			ctx, claim, rows, day, normalizedAt, derivationContext,
		)
		if err != nil {
			return nil, err
		}
		if err := githubWorkItemMergeEngineRows(derived, engineRows); err != nil {
			return nil, err
		}
	}
	if missing := githubWorkItemMissingDerivedDestinations(derived); len(missing) > 0 {
		// Fails closed rather than emitting empty slices for the unported
		// destinations. An empty slice is indistinguishable from "evaluated,
		// produced nothing", which is precisely the claim this port cannot
		// honestly make about a metric whose engine does not exist yet.
		return nil, fmt.Errorf(
			"%w: %s",
			ErrGitHubWorkItemsDerivationsUnavailable, strings.Join(missing, ", "),
		)
	}
	return derived, nil
}

// githubWorkItemMergeDerivedRows appends one builder's output onto the
// accumulator. Appending rather than assigning is what preserves per-day
// accumulation across the loop, including the CHAOS-3494 duplicates.
func githubWorkItemMergeDerivedRows(
	derived map[string][]json.RawMessage,
	produced map[string][]json.RawMessage,
) error {
	for destination, rows := range produced {
		existing, owned := derived[destination]
		if !owned {
			// A builder claiming a destination outside the owned set means the
			// subsets have drifted apart; landing it would give one
			// destination two producers.
			return ErrInvalidConfiguration
		}
		derived[destination] = append(existing, rows...)
	}
	return nil
}

// githubWorkItemMergeEngineRows accumulates the engine seam's per-day output.
//
// It deliberately does NOT pre-open the engine's destinations. A key is created
// only by an engine that actually emitted for it, so a destination the engine
// never produced stays ABSENT and the missing-destination check fails the run
// closed. Pre-opening them would hand every unported destination an empty slice
// -- indistinguishable from "evaluated, produced nothing", which is exactly the
// fabrication this deriver exists to refuse.
//
// Membership is checked against the ENGINE's destinations rather than against
// presence in `derived`, so an engine restating a ported builder's destination
// is rejected instead of giving that surface two producers with nothing
// comparing them.
func githubWorkItemMergeEngineRows(
	derived map[string][]json.RawMessage,
	produced map[string][]json.RawMessage,
) error {
	for destination, rows := range produced {
		if !slices.Contains(githubWorkItemDerivedEngineDestinations, destination) {
			return ErrInvalidConfiguration
		}
		derived[destination] = append(derived[destination], rows...)
	}
	return nil
}

// githubWorkItemMissingDerivedDestinations names the canonical derived
// destinations the deriver could not produce, in canonical order.
func githubWorkItemMissingDerivedDestinations(
	derived map[string][]json.RawMessage,
) []string {
	missing := make([]string, 0, len(githubWorkItemDerivedDestinations))
	for _, destination := range githubWorkItemDerivedDestinations {
		if _, produced := derived[destination]; !produced {
			missing = append(missing, destination)
		}
	}
	return missing
}

var _ githubWorkItemsDeriver = GitHubWorkItemDeriver{}
