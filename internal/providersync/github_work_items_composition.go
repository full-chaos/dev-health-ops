package providersync

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// ErrGitHubWorkItemSinkIncomplete reports a composite sink built while one or
// more destination adapters are missing. It always wraps the destination names
// so a caller learns WHICH surfaces are absent, never only that some are.
var ErrGitHubWorkItemSinkIncomplete = errors.New(
	"github work-item clickhouse sink is missing destination adapters",
)

// NewGitHubWorkItemClickHouseEffects wires every landed destination adapter
// onto the composite sink.
//
// WIRING: WIRED, and this paragraph used to say the opposite:
//
// SUPERSEDED: "It REGISTERS AND ACTIVATES NOTHING.
// SUPERSEDED: internal/workerservice/provider_sync.go gains no case for the
// SUPERSEDED: work-item family here, the provider matrix still marks all five
// SUPERSEDED: aliases route_ready: false, and the route still returns a nil
// SUPERSEDED: watermark -- so the only thing that can reach this constructor
// SUPERSEDED: today is a test. Activation is a separate change that must first
// SUPERSEDED: satisfy the unmet D17 obligation recorded on
// SUPERSEDED: GitHubWorkItemsIncomplete."
//
// All four of those claims are now false. Corrected, each against what
// actually implements it:
//
//   - provider_sync.go DOES have the case:
//     `provider == "github" && dataset == "work-items"` calls this
//     constructor and installs GitHubWorkItemsRouteHandler with the sink as
//     sink and readback. It builds no deriver: the unit writes raw rows only
//     and the daily job writes every table computed from them.
//   - route_ready is TRUE for all five aliases: execution_registry.go:246
//     sets descriptor.RouteReady unconditionally for the github work-item
//     family, and :247-249 additionally sets Plannable for the canonical
//     dataset. The comment immediately above it states the matrix reports all
//     five "as native and ready" deliberately.
//   - the watermark is NOT unconditionally nil, but it is not unconditionally
//     set either: github_work_items_route.go:437-441 fills it only when BOTH
//     `len(incomplete) == 0` AND `claim.BeforeAt != nil`. Incompleteness
//     (:110) is one of two suppression causes, not the only one -- a COMPLETE
//     but unwindowed claim also yields nil, and a claim with no BeforeAt is a
//     supported derivation, mirroring Python's default `before = utc_today() +
//     1 day` (see this file's own note at the `no BeforeAt` bullet). The
//     original text's "still returns a nil watermark" was wrong as an absolute;
//     so would be "returns one except when incomplete".
//   - production, not a test, reaches this constructor -- via the
//     provider_sync.go case above.
//
// Still true and NOT retracted: GitHubWorkItemsIncomplete and its policy are
// live (applyGitHubWorkItemsIncompletePolicy, called from complete_route.go
// :317 and :429, chunked_executor.go:99, chunked_stream_executor.go:160). What
// is stale is only the claim that the unmet D17 obligation still gates
// activation -- activation happened.
//
// Cite the case predicate and constructor names above rather than the line
// numbers alone; the names are what survive an edit that shifts these anchors.
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
// metrics is CHAOS-4244's dev_health_work_item_team_attributions_written_total
// series (providerfoundation.Metrics.RecordWorkItemTeamAttributionWritten). A
// nil metrics keeps the sink working with no live series, matching every
// other optional dependency in this package (see BudgetWaitObserver's doc).
func NewGitHubWorkItemClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	metrics *providerfoundation.Metrics,
) (GitHubWorkItemClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return GitHubWorkItemClickHouseEffects{}, ErrInvalidConfiguration
	}
	sink := GitHubWorkItemClickHouseEffects{
		Lease: lease,

		// Project membership (CHAOS-4194). Kept adjacent to the direct fact
		// surfaces because that is what they are: observed provider events, not
		// derivations. They are listed separately only because Python has no
		// counterpart for either -- it drops PullRequest board items outright,
		// and never wrote a `projects` row for a Projects V2 target at all.
		ProjectMembershipTransitions: GitHubProjectMembershipClickHouseAdapter{Conn: conn},
		Projects:                     GitHubProjectCatalogClickHouseAdapter{Conn: conn},

		// Seven direct fact surfaces.
		AIAttribution:        GitHubAIAttributionClickHouseAdapter{Conn: conn},
		Sprints:              GitHubSprintsClickHouseAdapter{Conn: conn},
		WorkItemDependencies: GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn},
		WorkItemInteractions: GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn},
		WorkItemReopenEvents: GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn},
		WorkItemTransitions:  GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn},
		WorkItems:            GitHubWorkItemsClickHouseAdapter{Conn: conn},
	}
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, fmt.Errorf(
			"%w: %s", ErrGitHubWorkItemSinkIncomplete, strings.Join(missing, ", "),
		)
	}
	return sink, nil
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

func githubWorkItemDerivedUTCDate(value time.Time) time.Time {
	utc := value.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}
