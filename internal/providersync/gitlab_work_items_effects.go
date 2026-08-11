package providersync

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLabWorkItemEffectRows is the six-fact manifest. Each field is a concrete
// normalized row slice aligned with one ClickHouse projection. Each field
// becomes an effect, including an empty field, so recovery can distinguish an
// evaluated destination from one omitted by a broken route.
type GitLabWorkItemEffectRows struct {
	WorkItems         []gitlabWorkItemRow
	StatusTransitions []gitlabWorkItemTransitionRow
	Dependencies      []gitlabWorkItemDependencyRow
	ReopenEvents      []gitlabWorkItemReopenRow
	Interactions      []gitlabWorkItemInteractionRow
	Sprints           []gitlabSprintRow
}

// encodeGitLabWorkItemRows is the sole conversion from a typed projection to
// the existing effect ledger's canonical JSON bytes. Callers cannot provide an
// untyped map or arbitrary evidence blob because every instantiation is one of
// the six concrete ClickHouse row types above.
func encodeGitLabWorkItemRows[T any](values []T) ([]json.RawMessage, error) {
	encoded := make([]json.RawMessage, 0, len(values))
	for _, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		encoded = append(encoded, raw)
	}
	return encoded, nil
}

func BuildGitLabWorkItemEffects(rows GitLabWorkItemEffectRows) ([]EffectBatch, error) {
	effects := make([]EffectBatch, 0, len(gitLabWorkItemRawDestinations))
	for _, destination := range gitLabWorkItemRawDestinations {
		if destination == "" {
			return nil, ErrInvalidConfiguration
		}
		effect, err := buildGitLabWorkItemEffect(destination, rows)
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

// buildGitLabWorkItemEffect is intentionally an explicit typed dispatch. The
// effect ledger accepts JSON bytes at its durable boundary, but each of the six
// inputs remains the exact normalized ClickHouse row type until this function
// performs that one serialization.
func buildGitLabWorkItemEffect(
	destination string, rows GitLabWorkItemEffectRows,
) (EffectBatch, error) {
	switch destination {
	case "work_items":
		values, err := encodeGitLabWorkItemRows(rows.WorkItems)
		if err != nil {
			return EffectBatch{}, err
		}
		return BuildEffectBatch(destination, EffectReadbackRequired, values)
	case "work_item_transitions":
		values, err := encodeGitLabWorkItemRows(rows.StatusTransitions)
		if err != nil {
			return EffectBatch{}, err
		}
		return BuildEffectBatch(destination, EffectReadbackRequired, values)
	case "work_item_dependencies":
		values, err := encodeGitLabWorkItemRows(rows.Dependencies)
		if err != nil {
			return EffectBatch{}, err
		}
		return BuildEffectBatch(destination, EffectReadbackRequired, values)
	case "work_item_reopen_events":
		values, err := encodeGitLabWorkItemRows(rows.ReopenEvents)
		if err != nil {
			return EffectBatch{}, err
		}
		return BuildEffectBatch(destination, EffectReadbackRequired, values)
	case "work_item_interactions":
		values, err := encodeGitLabWorkItemRows(rows.Interactions)
		if err != nil {
			return EffectBatch{}, err
		}
		return BuildEffectBatch(destination, EffectReadbackRequired, values)
	case "sprints":
		values, err := encodeGitLabWorkItemRows(rows.Sprints)
		if err != nil {
			return EffectBatch{}, err
		}
		return BuildEffectBatch(destination, EffectReadbackRequired, values)
	default:
		return EffectBatch{}, ErrInvalidConfiguration
	}
}

// GitLabWorkItemEffectIdentity is derived only from the frozen claim and
// effect manifest. Row org_id values are checked by every adapter and never
// allowed to choose the tenant namespace.
type GitLabWorkItemEffectIdentity struct {
	OrgID         string
	Provider      string
	Dataset       string
	Generation    string
	Destination   string
	ContentDigest string
	RowCount      int
}

func newGitLabWorkItemEffectIdentity(
	claim Claim, effect EffectBatch,
) (GitLabWorkItemEffectIdentity, error) {
	if claim.Validate() != nil || claim.Provider != "gitlab" || claim.Dataset != "work-items" ||
		!gitLabWorkItemDestination(effect.Destination) || effect.Recovery != EffectReadbackRequired ||
		!validDigest(effect.ContentDigest) || effect.PayloadBytes < 0 {
		return GitLabWorkItemEffectIdentity{}, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
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

func gitLabWorkItemDestination(destination string) bool {
	for _, candidate := range gitLabWorkItemRawDestinations {
		if candidate == destination {
			return true
		}
	}
	return false
}

type GitLabWorkItemEffectAdapter interface {
	WriteGitLabWorkItemEffect(context.Context, GitLabWorkItemEffectIdentity, EffectBatch) error
	InspectGitLabWorkItemEffect(context.Context, GitLabWorkItemEffectIdentity, EffectBatch) (EffectInspection, error)
}

// GitLabWorkItemClickHouseEffects is the provider-owned six-destination
// dispatcher. Its constructor is intentionally explicit and unregistered;
// lease/recovery semantics are complete even while the ten derived surfaces
// remain outside this slice.
type GitLabWorkItemClickHouseEffects struct {
	Lease             providerfoundation.LeaseGuard
	WorkItems         GitLabWorkItemEffectAdapter
	StatusTransitions GitLabWorkItemEffectAdapter
	Dependencies      GitLabWorkItemEffectAdapter
	ReopenEvents      GitLabWorkItemEffectAdapter
	Interactions      GitLabWorkItemEffectAdapter
	Sprints           GitLabWorkItemEffectAdapter
}

func NewGitLabWorkItemClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) GitLabWorkItemClickHouseEffects {
	return GitLabWorkItemClickHouseEffects{
		Lease:             lease,
		WorkItems:         GitLabWorkItemsClickHouseAdapter{Delegate: GitHubWorkItemsClickHouseAdapter{Conn: conn}},
		StatusTransitions: GitLabWorkItemTransitionsClickHouseAdapter{Delegate: GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn}},
		Dependencies:      GitLabWorkItemDependenciesClickHouseAdapter{Delegate: GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn}},
		ReopenEvents:      GitLabWorkItemReopenEventsClickHouseAdapter{Delegate: GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn}},
		Interactions:      GitLabWorkItemInteractionsClickHouseAdapter{Delegate: GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn}},
		Sprints:           GitLabSprintsClickHouseAdapter{Delegate: GitHubSprintsClickHouseAdapter{Conn: conn}},
	}
}

func (sink GitLabWorkItemClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := adapter.WriteGitLabWorkItemEffect(ctx, identity, effect); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink GitLabWorkItemClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectGitLabWorkItemEffect(ctx, identity, effect)
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

func (sink GitLabWorkItemClickHouseEffects) resolve(
	claim Claim, effect EffectBatch,
) (GitLabWorkItemEffectIdentity, GitLabWorkItemEffectAdapter, error) {
	identity, err := newGitLabWorkItemEffectIdentity(claim, effect)
	if err != nil || !sink.complete() {
		return GitLabWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	adapter, known := sink.adapterForDestination(effect.Destination)
	if !known || adapter == nil {
		return GitLabWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func (sink GitLabWorkItemClickHouseEffects) adapterForDestination(
	destination string,
) (GitLabWorkItemEffectAdapter, bool) {
	switch destination {
	case "work_items":
		return sink.WorkItems, true
	case "work_item_transitions":
		return sink.StatusTransitions, true
	case "work_item_dependencies":
		return sink.Dependencies, true
	case "work_item_reopen_events":
		return sink.ReopenEvents, true
	case "work_item_interactions":
		return sink.Interactions, true
	case "sprints":
		return sink.Sprints, true
	default:
		return nil, false
	}
}

func (sink GitLabWorkItemClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(gitLabWorkItemRawDestinations))
	for _, destination := range gitLabWorkItemRawDestinations {
		adapter, known := sink.adapterForDestination(destination)
		if !known || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

func (sink GitLabWorkItemClickHouseEffects) complete() bool {
	return len(sink.MissingDestinations()) == 0
}

var _ EffectSink = GitLabWorkItemClickHouseEffects{}
var _ EffectReadback = GitLabWorkItemClickHouseEffects{}

func gitlabRawEffectIdentity(identity GitLabWorkItemEffectIdentity) GitHubWorkItemEffectIdentity {
	return GitHubWorkItemEffectIdentity{
		OrgID: identity.OrgID, Provider: "gitlab", Dataset: identity.Dataset,
		Generation: identity.Generation, Destination: identity.Destination,
		ContentDigest: identity.ContentDigest, RowCount: identity.RowCount,
	}
}

func gitlabRawIdentityValid(identity GitLabWorkItemEffectIdentity, destination string) bool {
	return identity.Provider == "gitlab" && identity.Dataset == "work-items" &&
		identity.OrgID != "" && identity.Generation != "" && identity.Destination == destination
}

// Each adapter validates the provider-owned row shape before delegating to the
// shared ClickHouse implementation. The delegate does not require GitHub in
// its identity, so this remains a true GitLab sink rather than a GitHub alias.
type GitLabWorkItemsClickHouseAdapter struct {
	Delegate GitHubWorkItemsClickHouseAdapter
}

func (adapter GitLabWorkItemsClickHouseAdapter) WriteGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitlabWorkItemRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_items") || effect.Destination != "work_items" {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.Provider != "gitlab" || row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

func (adapter GitLabWorkItemsClickHouseAdapter) InspectGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[gitlabWorkItemRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_items") || effect.Destination != "work_items" {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.Provider != "gitlab" || row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

type GitLabWorkItemTransitionsClickHouseAdapter struct {
	Delegate GitHubWorkItemTransitionsClickHouseAdapter
}

func (adapter GitLabWorkItemTransitionsClickHouseAdapter) WriteGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitlabWorkItemTransitionRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_transitions") || effect.Destination != "work_item_transitions" {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.Provider != "gitlab" || row.OrgID != identity.OrgID {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

func (adapter GitLabWorkItemTransitionsClickHouseAdapter) InspectGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[gitlabWorkItemTransitionRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_transitions") || effect.Destination != "work_item_transitions" {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.Provider != "gitlab" || row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

type GitLabWorkItemDependenciesClickHouseAdapter struct {
	Delegate GitHubWorkItemDependenciesClickHouseAdapter
}

func (adapter GitLabWorkItemDependenciesClickHouseAdapter) WriteGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitlabWorkItemDependencyRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_dependencies") || effect.Destination != "work_item_dependencies" {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
			row.SourceWorkItemID == "" || row.TargetWorkItemID == "" || row.RelationshipType == "" {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

func (adapter GitLabWorkItemDependenciesClickHouseAdapter) InspectGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[gitlabWorkItemDependencyRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_dependencies") || effect.Destination != "work_item_dependencies" {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.RelationshipSemanticsVersion != "canonical-blocks.v2" ||
			row.SourceWorkItemID == "" || row.TargetWorkItemID == "" || row.RelationshipType == "" {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

type GitLabWorkItemReopenEventsClickHouseAdapter struct {
	Delegate GitHubWorkItemReopenEventsClickHouseAdapter
}

func (adapter GitLabWorkItemReopenEventsClickHouseAdapter) WriteGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitlabWorkItemReopenRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_reopen_events") || effect.Destination != "work_item_reopen_events" {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.WorkItemID == "" || row.FromStatus == "" || row.ToStatus == "" {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

func (adapter GitLabWorkItemReopenEventsClickHouseAdapter) InspectGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[gitlabWorkItemReopenRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_reopen_events") || effect.Destination != "work_item_reopen_events" {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.WorkItemID == "" || row.FromStatus == "" || row.ToStatus == "" {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

type GitLabWorkItemInteractionsClickHouseAdapter struct {
	Delegate GitHubWorkItemInteractionsClickHouseAdapter
}

func (adapter GitLabWorkItemInteractionsClickHouseAdapter) WriteGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitlabWorkItemInteractionRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_interactions") || effect.Destination != "work_item_interactions" {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "gitlab" || row.InteractionType != "comment" || row.BodyLength < 0 {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

func (adapter GitLabWorkItemInteractionsClickHouseAdapter) InspectGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[gitlabWorkItemInteractionRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "work_item_interactions") || effect.Destination != "work_item_interactions" {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "gitlab" || row.InteractionType != "comment" || row.BodyLength < 0 {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

type GitLabSprintsClickHouseAdapter struct {
	Delegate GitHubSprintsClickHouseAdapter
}

func (adapter GitLabSprintsClickHouseAdapter) WriteGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) error {
	rows, err := decodeEffectRows[gitlabSprintRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "sprints") || effect.Destination != "sprints" {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "gitlab" || row.SprintID == "" || row.Name == nil || row.State == nil {
			return ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

func (adapter GitLabSprintsClickHouseAdapter) InspectGitLabWorkItemEffect(
	ctx context.Context, identity GitLabWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[gitlabSprintRow](effect)
	if err != nil || !gitlabRawIdentityValid(identity, "sprints") || effect.Destination != "sprints" {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID || row.Provider != "gitlab" || row.SprintID == "" || row.Name == nil || row.State == nil {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return adapter.Delegate.InspectGitHubWorkItemEffect(ctx, gitlabRawEffectIdentity(identity), effect)
}

var _ GitLabWorkItemEffectAdapter = GitLabWorkItemsClickHouseAdapter{}
var _ GitLabWorkItemEffectAdapter = GitLabWorkItemTransitionsClickHouseAdapter{}
var _ GitLabWorkItemEffectAdapter = GitLabWorkItemDependenciesClickHouseAdapter{}
var _ GitLabWorkItemEffectAdapter = GitLabWorkItemReopenEventsClickHouseAdapter{}
var _ GitLabWorkItemEffectAdapter = GitLabWorkItemInteractionsClickHouseAdapter{}
var _ GitLabWorkItemEffectAdapter = GitLabSprintsClickHouseAdapter{}
