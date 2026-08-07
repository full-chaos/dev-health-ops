package providersync

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitHubWorkItemEffectRows is the complete Python work-item write surface.
// Every field becomes an effect, including an empty one. Keeping conditional
// emptiness in the manifest distinguishes "this destination was evaluated and
// produced no rows" from "the composite forgot a destination".
type GitHubWorkItemEffectRows struct {
	AIAttribution                  []json.RawMessage
	EstimateCoverageMetricsDaily   []json.RawMessage
	InvestmentClassificationsDaily []json.RawMessage
	InvestmentMetricsDaily         []json.RawMessage
	IssueTypeMetricsDaily          []json.RawMessage
	Sprints                        []json.RawMessage
	WorkItemCycleTimes             []json.RawMessage
	WorkItemDependencies           []json.RawMessage
	WorkItemInteractions           []json.RawMessage
	WorkItemMetricsDaily           []json.RawMessage
	WorkItemReopenEvents           []json.RawMessage
	WorkItemStateDurationsDaily    []json.RawMessage
	WorkItemTeamAttributions       []json.RawMessage
	WorkItemTransitions            []json.RawMessage
	WorkItemUserMetricsDaily       []json.RawMessage
	WorkItems                      []json.RawMessage
}

// BuildGitHubWorkItemEffects constructs one deterministic, readback-fenced
// effect for every destination owned by the Python composite unit. The order
// is the canonical workItemRouteDestinations order, which also matches the
// EffectCommitter's stable destination order.
func BuildGitHubWorkItemEffects(rows GitHubWorkItemEffectRows) ([]EffectBatch, error) {
	values := []struct {
		destination string
		rows        []json.RawMessage
	}{
		{"ai_attribution", rows.AIAttribution},
		{"estimate_coverage_metrics_daily", rows.EstimateCoverageMetricsDaily},
		{"investment_classifications_daily", rows.InvestmentClassificationsDaily},
		{"investment_metrics_daily", rows.InvestmentMetricsDaily},
		{"issue_type_metrics_daily", rows.IssueTypeMetricsDaily},
		{"sprints", rows.Sprints},
		{"work_item_cycle_times", rows.WorkItemCycleTimes},
		{"work_item_dependencies", rows.WorkItemDependencies},
		{"work_item_interactions", rows.WorkItemInteractions},
		{"work_item_metrics_daily", rows.WorkItemMetricsDaily},
		{"work_item_reopen_events", rows.WorkItemReopenEvents},
		{"work_item_state_durations_daily", rows.WorkItemStateDurationsDaily},
		{"work_item_team_attributions", rows.WorkItemTeamAttributions},
		{"work_item_transitions", rows.WorkItemTransitions},
		{"work_item_user_metrics_daily", rows.WorkItemUserMetricsDaily},
		{"work_items", rows.WorkItems},
	}
	canonical := workItemRouteDestinations()
	if len(values) != len(canonical) {
		return nil, ErrInvalidConfiguration
	}
	effects := make([]EffectBatch, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for index, value := range values {
		if value.destination != canonical[index] {
			return nil, ErrInvalidConfiguration
		}
		if _, duplicate := seen[value.destination]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		seen[value.destination] = struct{}{}
		effect, err := BuildEffectBatch(
			value.destination, EffectReadbackRequired, value.rows,
		)
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

// GitHubWorkItemEffectIdentity is the semantic namespace an individual
// destination adapter must use for both its write and its readback. It is
// derived only from the frozen claim and effect manifest, never from a row.
// Destination tables do not share a physical generation column, so adapters
// retain responsibility for mapping this identity onto each table's natural
// key and version semantics.
type GitHubWorkItemEffectIdentity struct {
	OrgID         string
	Provider      string
	Dataset       string
	Generation    string
	Destination   string
	ContentDigest string
	RowCount      int
}

func newGitHubWorkItemEffectIdentity(
	claim Claim,
	effect EffectBatch,
) (GitHubWorkItemEffectIdentity, error) {
	if claim.Validate() != nil || claim.Provider != "github" ||
		!isWorkItemFamilyDataset(claim.Dataset) ||
		!slices.Contains(workItemRouteDestinations(), effect.Destination) ||
		!validDigest(effect.ContentDigest) ||
		effect.Recovery != EffectReadbackRequired || effect.PayloadBytes < 0 {
		return GitHubWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(
		effect.Destination, effect.Recovery, effect.Rows,
	)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest ||
		rebuilt.PayloadBytes != effect.PayloadBytes {
		return GitHubWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" ||
		strings.TrimSpace(identity.Generation) == "" {
		return GitHubWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	return identity, nil
}

// GitHubWorkItemEffectAdapter is implemented by one explicitly named
// destination adapter below. The identity parameter is intentionally separate
// from EffectBatch: readback must fence tenant + generation before comparing
// destination-specific semantic rows.
type GitHubWorkItemEffectAdapter interface {
	WriteGitHubWorkItemEffect(
		context.Context,
		GitHubWorkItemEffectIdentity,
		EffectBatch,
	) error
	InspectGitHubWorkItemEffect(
		context.Context,
		GitHubWorkItemEffectIdentity,
		EffectBatch,
	) (EffectInspection, error)
}

// GitHubWorkItemClickHouseEffects is an unregistered composite dispatcher.
// Named fields make the 16-table ownership visible and prevent a dynamic
// destination registry from silently accepting or omitting a surface.
// Concrete ClickHouse adapters are added per table; this foundation does not
// activate any route or alias.
type GitHubWorkItemClickHouseEffects struct {
	Lease providerfoundation.LeaseGuard

	AIAttribution                  GitHubWorkItemEffectAdapter
	EstimateCoverageMetricsDaily   GitHubWorkItemEffectAdapter
	InvestmentClassificationsDaily GitHubWorkItemEffectAdapter
	InvestmentMetricsDaily         GitHubWorkItemEffectAdapter
	IssueTypeMetricsDaily          GitHubWorkItemEffectAdapter
	Sprints                        GitHubWorkItemEffectAdapter
	WorkItemCycleTimes             GitHubWorkItemEffectAdapter
	WorkItemDependencies           GitHubWorkItemEffectAdapter
	WorkItemInteractions           GitHubWorkItemEffectAdapter
	WorkItemMetricsDaily           GitHubWorkItemEffectAdapter
	WorkItemReopenEvents           GitHubWorkItemEffectAdapter
	WorkItemStateDurationsDaily    GitHubWorkItemEffectAdapter
	WorkItemTeamAttributions       GitHubWorkItemEffectAdapter
	WorkItemTransitions            GitHubWorkItemEffectAdapter
	WorkItemUserMetricsDaily       GitHubWorkItemEffectAdapter
	WorkItems                      GitHubWorkItemEffectAdapter
}

func (sink GitHubWorkItemClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := adapter.WriteGitHubWorkItemEffect(ctx, identity, effect); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink GitHubWorkItemClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect)
	if err != nil {
		return EffectConflict, err
	}
	switch inspection {
	case EffectExact, EffectAbsent, EffectConflict:
		return inspection, nil
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

func (sink GitHubWorkItemClickHouseEffects) resolve(
	claim Claim,
	effect EffectBatch,
) (GitHubWorkItemEffectIdentity, GitHubWorkItemEffectAdapter, error) {
	identity, err := newGitHubWorkItemEffectIdentity(claim, effect)
	if err != nil || !sink.complete() {
		return GitHubWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	adapter, known := sink.adapterForDestination(effect.Destination)
	if !known {
		return GitHubWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

// adapterForDestination is the ONE place a destination name maps onto a slot.
// resolve and MissingDestinations both go through it, so the dispatch table and
// the completeness report cannot drift into disagreeing about which surfaces
// this sink owns -- the failure mode a second hand-written list invites, and
// the one the named-field struct exists to prevent in the first place.
//
// The bool distinguishes "not a work-item destination at all" from "a
// destination whose adapter is nil". Callers need both: the first is a
// programming error, the second is the honest PR-C gap.
func (sink GitHubWorkItemClickHouseEffects) adapterForDestination(
	destination string,
) (GitHubWorkItemEffectAdapter, bool) {
	switch destination {
	case "ai_attribution":
		return sink.AIAttribution, true
	case "estimate_coverage_metrics_daily":
		return sink.EstimateCoverageMetricsDaily, true
	case "investment_classifications_daily":
		return sink.InvestmentClassificationsDaily, true
	case "investment_metrics_daily":
		return sink.InvestmentMetricsDaily, true
	case "issue_type_metrics_daily":
		return sink.IssueTypeMetricsDaily, true
	case "sprints":
		return sink.Sprints, true
	case "work_item_cycle_times":
		return sink.WorkItemCycleTimes, true
	case "work_item_dependencies":
		return sink.WorkItemDependencies, true
	case "work_item_interactions":
		return sink.WorkItemInteractions, true
	case "work_item_metrics_daily":
		return sink.WorkItemMetricsDaily, true
	case "work_item_reopen_events":
		return sink.WorkItemReopenEvents, true
	case "work_item_state_durations_daily":
		return sink.WorkItemStateDurationsDaily, true
	case "work_item_team_attributions":
		return sink.WorkItemTeamAttributions, true
	case "work_item_transitions":
		return sink.WorkItemTransitions, true
	case "work_item_user_metrics_daily":
		return sink.WorkItemUserMetricsDaily, true
	case "work_items":
		return sink.WorkItems, true
	default:
		return nil, false
	}
}

// MissingDestinations names every canonical destination this sink cannot serve,
// in workItemRouteDestinations order. It is the typed form of "13 of 16": a
// caller, a test, or an operator reading a constructor error learns WHICH
// surfaces are absent rather than only that something is.
//
// A destination the dispatch switch does not know is reported as missing rather
// than skipped. If workItemRouteDestinations ever grows an entry nobody wired,
// that entry must surface here -- silently omitting it would let the sink
// report itself complete while a destination had no adapter at all.
func (sink GitHubWorkItemClickHouseEffects) MissingDestinations() []string {
	canonical := workItemRouteDestinations()
	missing := make([]string, 0, len(canonical))
	for _, destination := range canonical {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

// complete gates every write and readback. It stays an ALL-SIXTEEN gate while
// the three engine-dependent destinations are unported: a sink that served the
// thirteen it has would land a generation whose derived surfaces are silently
// absent, which is exactly the "evaluated and produced no rows" versus "the
// composite forgot a destination" distinction GitHubWorkItemEffectRows exists
// to preserve.
func (sink GitHubWorkItemClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

// The sibling-sink convention, and the reason it matters more here than
// elsewhere: this composite is not registered yet, so nothing in the tree calls
// it through either interface. Without these assertions a signature drift in
// EffectSink or EffectReadback would compile cleanly and surface only when
// activation first wires the sink up -- the most expensive moment to find it.
var _ EffectSink = GitHubWorkItemClickHouseEffects{}
var _ EffectReadback = GitHubWorkItemClickHouseEffects{}
