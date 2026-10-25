package providersync

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// gitlabWorkItemSyncSinkDestinations is the set of destinations the sync
// sink accepts beside the six raw tables: ai_attribution only. The nine
// tables computed from stored work-item rows are written by the daily job,
// never by a sync unit, so the sink refuses them.
var gitlabWorkItemSyncSinkDestinations = []string{
	"ai_attribution",
}

func buildGitLabTypedDerivedEffect[T any](
	destination string,
	rows []T,
) (EffectBatch, error) {
	encoded, err := json.Marshal(rows)
	if err != nil {
		return EffectBatch{}, err
	}
	var effectRows []json.RawMessage
	if err := json.Unmarshal(encoded, &effectRows); err != nil {
		return EffectBatch{}, ErrEffectRecoveryUnsafe
	}
	return BuildEffectBatch(destination, EffectReadbackRequired, effectRows)
}

func newGitLabWorkItemDerivedEffectIdentity(
	claim Claim,
	effect EffectBatch,
) (GitLabWorkItemEffectIdentity, error) {
	if claim.Validate() != nil || claim.Provider != "gitlab" || claim.Dataset != "work-items" ||
		!gitlabWorkItemDerivedDestination(effect.Destination) ||
		effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) ||
		effect.PayloadBytes < 0 {
		return GitLabWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest ||
		rebuilt.PayloadBytes != effect.PayloadBytes {
		return GitLabWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	identity := GitLabWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if strings.TrimSpace(identity.OrgID) == "" || strings.TrimSpace(identity.Generation) == "" {
		return GitLabWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	return identity, nil
}

// gitlabWorkItemDerivedDestination reports whether the sync sink accepts the
// destination beside the raw tables.
func gitlabWorkItemDerivedDestination(destination string) bool {
	for _, candidate := range gitlabWorkItemSyncSinkDestinations {
		if candidate == destination {
			return true
		}
	}
	return false
}

func gitlabDerivedGitHubIdentity(
	identity GitLabWorkItemEffectIdentity,
) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: identity.OrgID, Provider: identity.Provider, Dataset: identity.Dataset,
		Generation: identity.Generation, Destination: identity.Destination,
		ContentDigest: identity.ContentDigest, RowCount: identity.RowCount,
	}
}

// GitLabWorkItemDerivedClickHouseEffects is the ai_attribution provider sink.
// Its field is a concrete adapter type and its dispatcher is an explicit
// switch, so a destination cannot be accepted without a corresponding
// schema-specific write/readback implementation.
// It dispatches ai_attribution only. The nine other adapter fields write
// tables that the daily job owns; no destination reaches them, and they stay
// only until the dead-code removal that follows the cut.
type GitLabWorkItemDerivedClickHouseEffects struct {
	Lease         providerfoundation.LeaseGuard
	AIAttribution GitLabAIAttributionClickHouseAdapter
}

// metrics (CHAOS-4320 round 7, codex round 6 P1, executed repro): the
// worker's real constructor call left this nil, so RecordTeamAttributionOwnershipChecked's
// nil-receiver-safe no-op silently swallowed every GitLab ownership_checked
// sample despite the plumbing being otherwise correct end to end -- an
// executed write with no sample to show for it. Threaded the same way
// NewGitHubWorkItemClickHouseEffects already accepts and forwards it.
func NewGitLabWorkItemDerivedClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
	metrics *providerfoundation.Metrics,
) (GitLabWorkItemDerivedClickHouseEffects, error) {
	if conn == nil || lease == nil {
		return GitLabWorkItemDerivedClickHouseEffects{}, ErrInvalidConfiguration
	}
	sink := GitLabWorkItemDerivedClickHouseEffects{
		Lease:         lease,
		AIAttribution: GitLabAIAttributionClickHouseAdapter{Conn: conn},
	}
	if missing := sink.MissingDestinations(); len(missing) > 0 {
		return sink, ErrInvalidConfiguration
	}
	return sink, nil
}

func (sink GitLabWorkItemDerivedClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(gitlabWorkItemSyncSinkDestinations))
	checks := []struct {
		name  string
		conn  driver.Conn
		lease providerfoundation.LeaseGuard
	}{
		{"ai_attribution", sink.AIAttribution.Conn, sink.Lease},
	}
	for _, check := range checks {
		if check.conn == nil || check.lease == nil {
			missing = append(missing, check.name)
		}
	}
	return missing
}

func (sink GitLabWorkItemDerivedClickHouseEffects) WriteEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) error {
	identity, err := newGitLabWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil || len(sink.MissingDestinations()) > 0 {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	gitHubIdentity := gitlabDerivedGitHubIdentity(identity)
	switch effect.Destination {
	case "ai_attribution":
		err = sink.AIAttribution.WriteGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	default:
		return ErrInvalidConfiguration
	}
	if err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink GitLabWorkItemDerivedClickHouseEffects) InspectEffect(
	ctx context.Context,
	claim Claim,
	effect EffectBatch,
) (EffectInspection, error) {
	identity, err := newGitLabWorkItemDerivedEffectIdentity(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil || len(sink.MissingDestinations()) > 0 {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	gitHubIdentity := gitlabDerivedGitHubIdentity(identity)
	var inspection EffectInspection
	switch effect.Destination {
	case "ai_attribution":
		inspection, err = sink.AIAttribution.InspectGitHubWorkItemEffect(ctx, gitHubIdentity, effect)
	default:
		return EffectConflict, ErrInvalidConfiguration
	}
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

var _ EffectSink = GitLabWorkItemDerivedClickHouseEffects{}
var _ EffectReadback = GitLabWorkItemDerivedClickHouseEffects{}

// GitLabAIAttributionClickHouseAdapter preserves the provider fence before
// delegating the shared ai_attribution column projection. The shared adapter
// correctly fences tenant UUIDs but intentionally serves multiple providers,
// so this provider-local boundary must reject a recovered or forged GitHub row
// carried by a GitLab effect.
type GitLabAIAttributionClickHouseAdapter struct{ Conn driver.Conn }

func (adapter GitLabAIAttributionClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) error {
	if !validGitLabAIAttributionEffect(identity, effect) {
		return ErrInvalidConfiguration
	}
	return (GitHubAIAttributionClickHouseAdapter{Conn: adapter.Conn}).
		WriteGitHubWorkItemEffect(ctx, identity, effect)
}

func (adapter GitLabAIAttributionClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context,
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) (EffectInspection, error) {
	if !validGitLabAIAttributionEffect(identity, effect) {
		return EffectConflict, ErrInvalidConfiguration
	}
	return (GitHubAIAttributionClickHouseAdapter{Conn: adapter.Conn}).
		InspectGitHubWorkItemEffect(ctx, identity, effect)
}

func validGitLabAIAttributionEffect(
	identity GitHubWorkItemEffectIdentity,
	effect EffectBatch,
) bool {
	if identity.Provider != "gitlab" || !isGitLabAIAttributionDataset(identity.Dataset) ||
		identity.Destination != "ai_attribution" || effect.Destination != "ai_attribution" {
		return false
	}
	rows, err := decodeEffectRows[gitlabAIAttributionRow](effect)
	if err != nil || identity.RowCount != len(rows) {
		return false
	}
	claim := Claim{Unit: Unit{
		OrgID: identity.OrgID, Provider: identity.Provider, Dataset: identity.Dataset,
	}}
	for _, row := range rows {
		if validateGitLabAIAttributionRow(row, claim) != nil {
			return false
		}
	}
	return true
}

var _ GitHubWorkItemEffectAdapter = GitLabAIAttributionClickHouseAdapter{}
