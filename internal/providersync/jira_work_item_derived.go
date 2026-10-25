package providersync

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// jiraWorkItemSyncSinkDestinations is the set of destinations the sync sink
// accepts beside the direct Jira tables: ai_attribution only, as an explicit
// evaluated-empty effect. The nine tables computed from stored work-item rows
// are written by the daily job, never by a sync unit, so the sink refuses
// them.
var jiraWorkItemSyncSinkDestinations = []string{
	"ai_attribution",
}

// jiraDerivedGitHubAdapter supplies Jira's frozen identity to the migrated
// schema-specific adapters. The evaluated-empty AI destination is deliberately
// a no-I/O adapter: write is a no-op and readback remains EffectAbsent.
type jiraDerivedGitHubAdapter struct {
	destination string
	delegate    JiraWorkItemEffectAdapter
}

func (adapter jiraDerivedGitHubAdapter) validate(
	identity JiraWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if identity.Provider != "jira" || !isWorkItemFamilyDataset(identity.Dataset) ||
		identity.Destination != adapter.destination || effect.Destination != adapter.destination ||
		effect.Recovery != EffectReadbackRequired || identity.OrgID == "" ||
		identity.RowCount != len(effect.Rows) || identity.ContentDigest != effect.ContentDigest {
		return ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return ErrInvalidConfiguration
	}
	if adapter.destination == "ai_attribution" {
		if len(effect.Rows) != 0 {
			return ErrInvalidConfiguration
		}
		return nil
	}
	for _, raw := range effect.Rows {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return ErrInvalidConfiguration
		}
		var provider string
		providerRaw, ok := fields["provider"]
		if !ok || json.Unmarshal(providerRaw, &provider) != nil || provider != "jira" {
			return ErrInvalidConfiguration
		}
		var orgID string
		orgRaw, ok := fields["org_id"]
		if !ok || json.Unmarshal(orgRaw, &orgID) != nil || orgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	return nil
}

func (adapter jiraDerivedGitHubAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity JiraWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if err := adapter.validate(identity, effect); err != nil {
		return err
	}
	if adapter.destination == "ai_attribution" {
		return nil
	}
	if adapter.delegate == nil {
		return ErrInvalidConfiguration
	}
	return adapter.delegate.WriteGitHubWorkItemEffect(ctx, identity, effect)
}

func (adapter jiraDerivedGitHubAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity JiraWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	if err := adapter.validate(identity, effect); err != nil {
		return EffectConflict, err
	}
	if adapter.destination == "ai_attribution" {
		return EffectAbsent, nil
	}
	if adapter.delegate == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return adapter.delegate.InspectGitHubWorkItemEffect(ctx, identity, effect)
}

var _ JiraWorkItemEffectAdapter = jiraDerivedGitHubAdapter{}

// JiraWorkItemDerivedClickHouseEffects is the provider-local dispatcher of the
// evaluated-empty ai_attribution effect. Direct Jira facts and worklogs stay
// in their existing sinks.
// It dispatches ai_attribution only. The nine other adapter fields write
// tables that the daily job owns; no destination reaches them, and they stay
// only until the dead-code removal that follows the cut.
type JiraWorkItemDerivedClickHouseEffects struct {
	Lease         providerfoundation.LeaseGuard
	AIAttribution JiraWorkItemEffectAdapter
}

// metrics (CHAOS-4320 round 7, codex round 6 P1) -- see the identical
// parameter's doc comment on NewGitLabWorkItemDerivedClickHouseEffects.
func NewJiraWorkItemDerivedClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	metrics *providerfoundation.Metrics,
) (JiraWorkItemDerivedClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return JiraWorkItemDerivedClickHouseEffects{}, ErrInvalidConfiguration
	}
	wrap := func(destination string, delegate JiraWorkItemEffectAdapter) JiraWorkItemEffectAdapter {
		return jiraDerivedGitHubAdapter{destination: destination, delegate: delegate}
	}
	sink := JiraWorkItemDerivedClickHouseEffects{
		Lease:         lease,
		AIAttribution: wrap("ai_attribution", nil),
	}
	if len(sink.MissingDestinations()) > 0 {
		return sink, ErrInvalidConfiguration
	}
	return sink, nil
}

func (sink JiraWorkItemDerivedClickHouseEffects) adapterForDestination(
	destination string,
) (JiraWorkItemEffectAdapter, bool) {
	switch destination {
	case "ai_attribution":
		return sink.AIAttribution, true
	default:
		return nil, false
	}
}

func (sink JiraWorkItemDerivedClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(jiraWorkItemSyncSinkDestinations))
	for _, destination := range jiraWorkItemSyncSinkDestinations {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink JiraWorkItemDerivedClickHouseEffects) resolve(
	claim Claim,
	effect EffectBatch,
) (JiraWorkItemEffectIdentity, JiraWorkItemEffectAdapter, error) {
	if claim.Validate() != nil || claim.Provider != "jira" ||
		!isWorkItemFamilyDataset(claim.Dataset) ||
		!slices.Contains(jiraWorkItemSyncSinkDestinations, effect.Destination) ||
		effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) ||
		effect.PayloadBytes < 0 || sink.Lease == nil || len(sink.MissingDestinations()) > 0 {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	identity := JiraWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" || strings.TrimSpace(identity.Generation) == "" {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	adapter, known := sink.adapterForDestination(effect.Destination)
	if !known || adapter == nil {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func (sink JiraWorkItemDerivedClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil {
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

func (sink JiraWorkItemDerivedClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectGitHubWorkItemEffect(ctx, identity, effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	switch inspection {
	case EffectExact, EffectAbsent, EffectConflict:
		return inspection, nil
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

var _ EffectSink = JiraWorkItemDerivedClickHouseEffects{}
var _ EffectReadback = JiraWorkItemDerivedClickHouseEffects{}

// JiraWorkItemCompositeClickHouseEffects is the provider-local dispatcher for
// the complete route batch: the direct Jira effects and the evaluated-empty
// ai_attribution effect. Activation remains outside this file.
type JiraWorkItemCompositeClickHouseEffects struct {
	Direct  JiraAtlassianClickHouseEffects
	Derived JiraWorkItemDerivedClickHouseEffects
}

func NewJiraWorkItemCompositeClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	metrics *providerfoundation.Metrics,
) (JiraWorkItemCompositeClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return JiraWorkItemCompositeClickHouseEffects{}, ErrInvalidConfiguration
	}
	derived, err := NewJiraWorkItemDerivedClickHouseEffects(conn, lease, metrics)
	if err != nil {
		return JiraWorkItemCompositeClickHouseEffects{}, err
	}
	sink := JiraWorkItemCompositeClickHouseEffects{
		Direct: NewJiraAtlassianClickHouseEffects(conn, lease), Derived: derived,
	}
	if len(sink.MissingDestinations()) > 0 {
		return sink, ErrInvalidConfiguration
	}
	return sink, nil
}

func (sink JiraWorkItemCompositeClickHouseEffects) MissingDestinations() []string {
	missing := sink.Direct.MissingDestinations()
	missing = append(missing, sink.Derived.MissingDestinations()...)
	return missing
}

func (sink JiraWorkItemCompositeClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	switch {
	case slices.Contains(jiraAtlassianEffectDestinations, effect.Destination):
		return sink.Direct.WriteEffect(ctx, claim, effect)
	case slices.Contains(jiraWorkItemSyncSinkDestinations, effect.Destination):
		return sink.Derived.WriteEffect(ctx, claim, effect)
	default:
		return ErrInvalidConfiguration
	}
}

func (sink JiraWorkItemCompositeClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	switch {
	case slices.Contains(jiraAtlassianEffectDestinations, effect.Destination):
		return sink.Direct.InspectEffect(ctx, claim, effect)
	case slices.Contains(jiraWorkItemSyncSinkDestinations, effect.Destination):
		return sink.Derived.InspectEffect(ctx, claim, effect)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
}

var _ EffectSink = JiraWorkItemCompositeClickHouseEffects{}
var _ EffectReadback = JiraWorkItemCompositeClickHouseEffects{}
