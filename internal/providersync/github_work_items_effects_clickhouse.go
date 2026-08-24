package providersync

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/workitemcontract"
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
	// CHAOS-4194. Board memberships and the `projects` catalogue row that makes
	// their destination resolvable.
	ProjectMembershipTransitions []json.RawMessage
	Projects                     []json.RawMessage
}

// githubWorkItemEffectRowsByDestination is intentionally a projection map,
// not another destination manifest. workitemcontract owns the ordered semantic
// list; this map only tells the GitHub writer which typed rows belong to each
// declared destination. Keep an entry for an empty projection: omitting it
// changes a completed family into an indistinguishable partial write and breaks
// readback/recovery.
var githubWorkItemEffectRowsByDestination = map[string]func(GitHubWorkItemEffectRows) []json.RawMessage{
	"ai_attribution":                   func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.AIAttribution },
	"estimate_coverage_metrics_daily":  func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.EstimateCoverageMetricsDaily },
	"investment_classifications_daily": func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.InvestmentClassificationsDaily },
	"investment_metrics_daily":         func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.InvestmentMetricsDaily },
	"issue_type_metrics_daily":         func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.IssueTypeMetricsDaily },
	"sprints":                          func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.Sprints },
	"work_item_cycle_times":            func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemCycleTimes },
	"work_item_dependencies":           func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemDependencies },
	"work_item_interactions":           func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemInteractions },
	"work_item_metrics_daily":          func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemMetricsDaily },
	"work_item_reopen_events":          func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemReopenEvents },
	"work_item_state_durations_daily":  func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemStateDurationsDaily },
	"work_item_team_attributions":      func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemTeamAttributions },
	"work_item_transitions":            func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemTransitions },
	"work_item_user_metrics_daily":     func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItemUserMetricsDaily },
	"project_membership_transitions":   func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.ProjectMembershipTransitions },
	"projects":                         func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.Projects },
	"work_items":                       func(rows GitHubWorkItemEffectRows) []json.RawMessage { return rows.WorkItems },
}

func githubWorkItemRouteDestinations() []string {
	return workitemcontract.GitHubEffectDestinations()
}

// workItemFamilyRouteDestinations is the subset every work-item provider's
// route writes. See workItemRouteDestinations for why the two lists differ.
func workItemFamilyRouteDestinations() []string {
	return workitemcontract.FamilyRouteDestinations()
}

// BuildGitHubWorkItemEffects constructs one deterministic, readback-fenced
// effect for every destination owned by the Python composite unit. The order
// is the canonical githubWorkItemRouteDestinations order, which also matches the
// EffectCommitter's stable destination order.
func BuildGitHubWorkItemEffects(rows GitHubWorkItemEffectRows) ([]EffectBatch, error) {
	destinations := githubWorkItemRouteDestinations()
	if len(destinations) == 0 || len(githubWorkItemEffectRowsByDestination) != len(destinations) {
		return nil, ErrInvalidConfiguration
	}
	effects := make([]EffectBatch, 0, len(destinations))
	seen := make(map[string]struct{}, len(destinations))
	for _, destination := range destinations {
		rowProjection, exists := githubWorkItemEffectRowsByDestination[destination]
		if destination == "" || !exists || rowProjection == nil {
			return nil, ErrInvalidConfiguration
		}
		if _, duplicate := seen[destination]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		seen[destination] = struct{}{}
		effect, err := BuildEffectBatch(
			destination, EffectReadbackRequired, rowProjection(rows),
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
		// The GITHUB list: this identity fences the github work-item sink,
		// which owns two surfaces beyond the shared family route.
		!slices.Contains(githubWorkItemRouteDestinations(), effect.Destination) ||
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

// GitHubWorkItemClickHouseEffects is the active canonical composite dispatcher.
// Named fields make the 16-table ownership visible and prevent a dynamic
// destination registry from silently accepting or omitting a surface.
// Concrete ClickHouse adapters are injected by the canonical GitHub work-item
// worker constructor. Direct alias claims are rejected before this dispatcher
// is constructed, so it can only receive the collapsed work-items family.
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
	ProjectMembershipTransitions   GitHubWorkItemEffectAdapter
	Projects                       GitHubWorkItemEffectAdapter
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
	case "project_membership_transitions":
		return sink.ProjectMembershipTransitions, true
	case "projects":
		return sink.Projects, true
	default:
		return nil, false
	}
}

// MissingDestinations names every canonical destination this sink cannot serve,
// in workItemRouteDestinations order. A caller, a test, or an operator reading
// a constructor error learns WHICH
// surfaces are absent rather than only that something is.
//
// A destination the dispatch switch does not know is reported as missing rather
// than skipped. If workItemRouteDestinations ever grows an entry nobody wired,
// that entry must surface here -- silently omitting it would let the sink
// report itself complete while a destination had no adapter at all.
func (sink GitHubWorkItemClickHouseEffects) MissingDestinations() []string {
	// The GITHUB list, not the shared family one: this sink owns two surfaces
	// no other provider writes, and completeness has to demand them or the two
	// adapters could go missing without anything noticing.
	canonical := githubWorkItemRouteDestinations()
	missing := make([]string, 0, len(canonical))
	for _, destination := range canonical {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

// complete gates every write and readback. It is an ALL-DESTINATIONS gate --
// eighteen since CHAOS-4194 -- because a partially constructed sink would land
// a generation whose surfaces are silently absent, which is exactly the
// "evaluated and produced no rows" versus "the composite forgot a destination"
// distinction GitHubWorkItemEffectRows exists to preserve. The count is not
// spelled in code on purpose: workitemcontract owns it.
func (sink GitHubWorkItemClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

// The canonical worker constructs this composite through both interfaces.
// Preserve these assertions so an interface drift fails at build time rather
// than at a runtime route switch.
var _ EffectSink = GitHubWorkItemClickHouseEffects{}
var _ EffectReadback = GitHubWorkItemClickHouseEffects{}
