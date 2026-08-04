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
	var adapter GitHubWorkItemEffectAdapter
	switch effect.Destination {
	case "ai_attribution":
		adapter = sink.AIAttribution
	case "estimate_coverage_metrics_daily":
		adapter = sink.EstimateCoverageMetricsDaily
	case "investment_classifications_daily":
		adapter = sink.InvestmentClassificationsDaily
	case "investment_metrics_daily":
		adapter = sink.InvestmentMetricsDaily
	case "issue_type_metrics_daily":
		adapter = sink.IssueTypeMetricsDaily
	case "sprints":
		adapter = sink.Sprints
	case "work_item_cycle_times":
		adapter = sink.WorkItemCycleTimes
	case "work_item_dependencies":
		adapter = sink.WorkItemDependencies
	case "work_item_interactions":
		adapter = sink.WorkItemInteractions
	case "work_item_metrics_daily":
		adapter = sink.WorkItemMetricsDaily
	case "work_item_reopen_events":
		adapter = sink.WorkItemReopenEvents
	case "work_item_state_durations_daily":
		adapter = sink.WorkItemStateDurationsDaily
	case "work_item_team_attributions":
		adapter = sink.WorkItemTeamAttributions
	case "work_item_transitions":
		adapter = sink.WorkItemTransitions
	case "work_item_user_metrics_daily":
		adapter = sink.WorkItemUserMetricsDaily
	case "work_items":
		adapter = sink.WorkItems
	default:
		return GitHubWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func (sink GitHubWorkItemClickHouseEffects) complete() bool {
	return sink.AIAttribution != nil &&
		sink.EstimateCoverageMetricsDaily != nil &&
		sink.InvestmentClassificationsDaily != nil &&
		sink.InvestmentMetricsDaily != nil &&
		sink.IssueTypeMetricsDaily != nil && sink.Sprints != nil &&
		sink.WorkItemCycleTimes != nil && sink.WorkItemDependencies != nil &&
		sink.WorkItemInteractions != nil && sink.WorkItemMetricsDaily != nil &&
		sink.WorkItemReopenEvents != nil &&
		sink.WorkItemStateDurationsDaily != nil &&
		sink.WorkItemTeamAttributions != nil && sink.WorkItemTransitions != nil &&
		sink.WorkItemUserMetricsDaily != nil && sink.WorkItems != nil
}
