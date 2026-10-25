package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

var linearAIAttributionLabelKinds = map[string]string{
	"ai-assisted": "ai_assisted", "agent-created": "agent_created",
	"ai-review": "ai_review", "copilot": "ai_assisted",
	"claude-code": "ai_assisted", "codex": "ai_assisted",
	"cursor": "ai_assisted", "windsurf": "ai_assisted",
}

func normalizeLinearWorkItemAIAttributions(
	claim Claim,
	rows linearWorkItemRows,
	normalizedAt time.Time,
) ([]githubAIAttributionRow, error) {
	if claim.Validate() != nil || claim.Provider != "linear" ||
		claim.Dataset != "work-items" || normalizedAt.IsZero() {
		return nil, ErrInvalidConfiguration
	}
	tenant, err := uuid.Parse(claim.OrgID)
	if err != nil {
		return nil, providerfoundation.ErrInvalidScope
	}
	result := make([]githubAIAttributionRow, 0)
	for _, item := range rows.WorkItems {
		if item.Provider != "linear" || item.OrgID != claim.OrgID ||
			item.WorkItemID == "" || item.CreatedAt.IsZero() || item.Labels == nil {
			return nil, providerfoundation.ErrInvalidScope
		}
		for index, label := range item.Labels {
			kind, matched := linearAIAttributionLabelKinds[strings.ToLower(strings.TrimSpace(label))]
			if !matched {
				continue
			}
			evidence := map[string]any{"label": label}
			encodedEvidence, marshalErr := json.Marshal(evidence)
			if marshalErr != nil {
				return nil, providerfoundation.ErrNormalizationInvalid
			}
			identity := strings.Join([]string{
				claim.OrgID, item.WorkItemID, "issue_label", fmt.Sprint(index), string(encodedEvidence),
			}, "|")
			result = append(result, githubAIAttributionRow{
				RecordID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity)),
				OrgID:    tenant, Provider: "linear", SubjectType: "issue",
				SubjectID: item.WorkItemID, RepoID: nil, Kind: kind,
				Source: "issue_label", Confidence: 0.95, Evidence: evidence,
				ObservedAt: item.CreatedAt.UTC(), IngestedAt: normalizedAt.UTC(),
			})
		}
	}
	return result, nil
}

// linearDerivedGitHubAdapter is a narrow provider adapter around the existing
// ClickHouse implementations. The underlying adapters retain their exact SQL,
// coercion, readback, and lease behavior; this wrapper supplies the Linear
// identity and refuses a row that crosses the frozen tenant/provider fence.
type linearDerivedGitHubAdapter struct {
	destination string
	delegate    GitHubWorkItemEffectAdapter
}

func (adapter linearDerivedGitHubAdapter) validate(
	identity LinearWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if identity.Provider != "linear" || identity.Dataset != "work-items" ||
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
		if len(effect.Rows) == 0 {
			return nil
		}
		tenant, parseErr := uuid.Parse(identity.OrgID)
		if parseErr != nil {
			return ErrInvalidConfiguration
		}
		for _, raw := range effect.Rows {
			var row githubAIAttributionRow
			if json.Unmarshal(raw, &row) != nil || row.RecordID == uuid.Nil ||
				row.OrgID != tenant || row.Provider != "linear" ||
				row.SubjectType != "issue" || row.SubjectID == "" || row.RepoID != nil ||
				row.Kind == "" || row.Source != "issue_label" || row.Confidence != 0.95 ||
				row.Evidence == nil || row.ObservedAt.IsZero() || row.IngestedAt.IsZero() {
				return ErrInvalidConfiguration
			}
		}
		return nil
	}
	for _, raw := range effect.Rows {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
			return ErrInvalidConfiguration
		}
		providerRaw, ok := fields["provider"]
		if !ok {
			return ErrInvalidConfiguration
		}
		var provider string
		if err := json.Unmarshal(providerRaw, &provider); err != nil || provider != "linear" {
			return ErrInvalidConfiguration
		}
		orgRaw, ok := fields["org_id"]
		if !ok {
			return ErrInvalidConfiguration
		}
		var orgID string
		if err := json.Unmarshal(orgRaw, &orgID); err != nil || orgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	return nil
}

func (adapter linearDerivedGitHubAdapter) WriteLinearWorkItemEffect(
	ctx context.Context,
	identity LinearWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if err := adapter.validate(identity, effect); err != nil {
		return err
	}
	if adapter.destination == "ai_attribution" && len(effect.Rows) == 0 {
		return nil
	}
	if adapter.delegate == nil {
		return ErrInvalidConfiguration
	}
	return adapter.delegate.WriteGitHubWorkItemEffect(
		ctx, linearRawEffectIdentity(identity), effect,
	)
}

func (adapter linearDerivedGitHubAdapter) InspectLinearWorkItemEffect(
	ctx context.Context,
	identity LinearWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	if err := adapter.validate(identity, effect); err != nil {
		return EffectConflict, err
	}
	if adapter.destination == "ai_attribution" && len(effect.Rows) == 0 {
		return EffectAbsent, nil
	}
	if adapter.delegate == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return adapter.delegate.InspectGitHubWorkItemEffect(
		ctx, linearRawEffectIdentity(identity), effect,
	)
}

var _ LinearWorkItemEffectAdapter = linearDerivedGitHubAdapter{}

// LinearWorkItemDerivedClickHouseEffects is the ai_attribution dispatcher. It
// is separate from the raw-fact dispatcher because it reuses the shared
// ai_attribution adapter behind the Linear identity fence.
// It dispatches ai_attribution only. The nine other adapter fields write
// tables that the daily job owns; no destination reaches them, and they stay
// only until the dead-code removal that follows the cut.
type LinearWorkItemDerivedClickHouseEffects struct {
	Lease         providerfoundation.LeaseGuard
	AIAttribution LinearWorkItemEffectAdapter
}

// metrics (CHAOS-4320 round 7, codex round 6 P1) -- see the identical
// parameter's doc comment on NewGitLabWorkItemDerivedClickHouseEffects.
func NewLinearWorkItemDerivedClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	metrics *providerfoundation.Metrics,
) (LinearWorkItemDerivedClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return LinearWorkItemDerivedClickHouseEffects{}, ErrInvalidConfiguration
	}
	wrap := func(destination string, delegate GitHubWorkItemEffectAdapter) LinearWorkItemEffectAdapter {
		return linearDerivedGitHubAdapter{destination: destination, delegate: delegate}
	}
	sink := LinearWorkItemDerivedClickHouseEffects{
		Lease:         lease,
		AIAttribution: wrap("ai_attribution", GitHubAIAttributionClickHouseAdapter{Conn: conn}),
	}
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, fmt.Errorf("%w: %s", ErrLinearWorkItemDerivedSinkIncomplete, strings.Join(missing, ", "))
	}
	return sink, nil
}

var ErrLinearWorkItemDerivedSinkIncomplete = fmt.Errorf(
	"linear derived work-item clickhouse sink is missing destination adapters",
)

func (sink LinearWorkItemDerivedClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := adapter.WriteLinearWorkItemEffect(ctx, identity, effect); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink LinearWorkItemDerivedClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectLinearWorkItemEffect(ctx, identity, effect)
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

func (sink LinearWorkItemDerivedClickHouseEffects) resolve(
	claim Claim, effect EffectBatch,
) (LinearWorkItemEffectIdentity, LinearWorkItemEffectAdapter, error) {
	identity, err := newLinearWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil || !sink.complete() {
		return LinearWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	adapter, known := sink.adapterForDestination(effect.Destination)
	if !known || adapter == nil {
		return LinearWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func newLinearWorkItemDerivedEffectIdentity(
	claim Claim, effect EffectBatch,
) (LinearWorkItemEffectIdentity, error) {
	if claim.Validate() != nil || claim.Provider != "linear" || claim.Dataset != "work-items" ||
		!linearWorkItemDerivedDestination(effect.Destination) || effect.Recovery != EffectReadbackRequired ||
		!validDigest(effect.ContentDigest) || effect.PayloadBytes < 0 {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	identity := LinearWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" || strings.TrimSpace(identity.Generation) == "" {
		return LinearWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	return identity, nil
}

// linearWorkItemSyncSinkDestinations is the set of destinations the sync sink
// accepts beside the raw Linear tables: ai_attribution only. The nine tables
// computed from stored work-item rows are written by the daily job, never by
// a sync unit, so the sink refuses them.
var linearWorkItemSyncSinkDestinations = []string{
	"ai_attribution",
}

// linearWorkItemDerivedDestination reports whether the sync sink accepts the
// destination beside the raw tables.
func linearWorkItemDerivedDestination(destination string) bool {
	for _, candidate := range linearWorkItemSyncSinkDestinations {
		if candidate == destination {
			return true
		}
	}
	return false
}

func (sink LinearWorkItemDerivedClickHouseEffects) adapterForDestination(
	destination string,
) (LinearWorkItemEffectAdapter, bool) {
	switch destination {
	case "ai_attribution":
		return sink.AIAttribution, true
	default:
		return nil, false
	}
}

func (sink LinearWorkItemDerivedClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(linearWorkItemSyncSinkDestinations))
	for _, destination := range linearWorkItemSyncSinkDestinations {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink LinearWorkItemDerivedClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

var _ EffectSink = LinearWorkItemDerivedClickHouseEffects{}
var _ EffectReadback = LinearWorkItemDerivedClickHouseEffects{}
