package providersync

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// JiraProjectCatalogClickHouseAdapter wraps GitHubProjectCatalogClickHouseAdapter
// with an existence-only readback for Jira's ensure-only project catalog
// rows. Jira issues an EnsureProjectsRow write per membership-owning issue,
// stamped with that sync unit's own normalizedAt as updated_at
// (jira_work_items_route.go's resolveJiraProjectCatalog callers) -- so two
// independent Jira sync units for the SAME project can legitimately write
// two different versions of the same (org_id, provider, id) key. If one
// crashes mid-write and a later, unrelated unit writes a newer version
// before the crashed unit's recovery readback runs, the delegated GitHub
// readback's version comparison reports EffectConflict even though the
// newer row already satisfies the ensure, permanently wedging the older
// unit's recovery (codex review finding, CHAOS-4193) -- the identical class
// of bug already fixed for Linear's equivalent adapter.
type JiraProjectCatalogClickHouseAdapter struct {
	Delegate GitHubProjectCatalogClickHouseAdapter
}

func (adapter JiraProjectCatalogClickHouseAdapter) WriteGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) error {
	return adapter.Delegate.WriteGitHubWorkItemEffect(ctx, identity, effect)
}

func (adapter JiraProjectCatalogClickHouseAdapter) InspectGitHubWorkItemEffect(
	ctx context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch,
) (EffectInspection, error) {
	rows, err := decodeEffectRows[projectmembership.CatalogRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if err := workItemAdapterGuard(
		ctx, identity, effect, "projects", adapter.Delegate.Conn, len(rows),
	); err != nil {
		return EffectConflict, err
	}
	for _, row := range rows {
		if row.OrgID != identity.OrgID {
			return EffectConflict, ErrInvalidConfiguration
		}
	}
	return existenceOnlyProjectCatalogReadback(ctx, adapter.Delegate.Conn, rows)
}

var _ GitHubWorkItemEffectAdapter = JiraProjectCatalogClickHouseAdapter{}

// Jira's first canonical slice owns the six direct fact destinations emitted
// by fetch_jira_work_items_with_extras.  The five historical aliases collapse
// to this one family before route construction; no alias-specific sink may be
// added here.
var jiraWorkItemEffectDestinations = []string{
	"sprints",
	"work_item_dependencies",
	"work_item_interactions",
	"work_item_reopen_events",
	"work_item_transitions",
	"work_items",
	// CHAOS-4193: project-membership history and the projects catalog rows
	// that make it resolvable, per the CHAOS-4193/4194 lock's ensureProjectsRow
	// contract (projectmembership.EnsureProjectsRow's own doc comment).
	"project_membership_transitions",
	"projects",
}

func JiraWorkItemEffectDestinations() []string {
	return append([]string(nil), jiraWorkItemEffectDestinations...)
}

func BuildJiraWorkItemEffects(rows jiraWorkItemRows) ([]EffectBatch, error) {
	projections := map[string]func(jiraWorkItemRows) []json.RawMessage{
		"sprints":                        func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.Sprints) },
		"work_item_dependencies":         func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.Dependencies) },
		"work_item_interactions":         func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.Interactions) },
		"work_item_reopen_events":        func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.ReopenEvents) },
		"work_item_transitions":          func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.Transitions) },
		"work_items":                     func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.WorkItems) },
		"project_membership_transitions": func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.ProjectMemberships) },
		"projects":                       func(value jiraWorkItemRows) []json.RawMessage { return marshalJiraRows(value.Projects) },
	}
	if len(projections) != len(jiraWorkItemEffectDestinations) {
		return nil, ErrInvalidConfiguration
	}
	effects := make([]EffectBatch, 0, len(jiraWorkItemEffectDestinations))
	seen := make(map[string]struct{}, len(jiraWorkItemEffectDestinations))
	for _, destination := range jiraWorkItemEffectDestinations {
		if _, duplicate := seen[destination]; duplicate {
			return nil, ErrInvalidConfiguration
		}
		projection, ok := projections[destination]
		if !ok || projection == nil {
			return nil, ErrInvalidConfiguration
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, projection(rows))
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
		seen[destination] = struct{}{}
	}
	return effects, nil
}

func marshalJiraRows[T any](rows []T) []json.RawMessage {
	result := make([]json.RawMessage, 0, len(rows))
	for _, row := range rows {
		encoded, err := json.Marshal(row)
		if err != nil {
			// All row types are JSON-safe by construction. BuildEffectBatch will
			// reject an empty row if a future type violates that assumption.
			continue
		}
		result = append(result, encoded)
	}
	return result
}

type JiraWorkItemEffectIdentity = GitHubWorkItemEffectIdentity
type JiraWorkItemEffectAdapter = GitHubWorkItemEffectAdapter

// JiraWorkItemClickHouseEffects is provider-local dispatch over the existing
// six direct adapters. The adapters themselves are provider-neutral at the
// storage boundary; the Jira identity and manifest fence this dispatcher
// before any write or readback is attempted.
type JiraWorkItemClickHouseEffects struct {
	Lease              providerfoundation.LeaseGuard
	Sprints            JiraWorkItemEffectAdapter
	Dependencies       JiraWorkItemEffectAdapter
	Interactions       JiraWorkItemEffectAdapter
	Reopens            JiraWorkItemEffectAdapter
	Transitions        JiraWorkItemEffectAdapter
	WorkItems          JiraWorkItemEffectAdapter
	ProjectMemberships JiraWorkItemEffectAdapter
	Projects           JiraWorkItemEffectAdapter
}

// NewJiraWorkItemClickHouseEffects wires the six provider-local destinations
// to the already differential-tested ClickHouse adapters. It is a constructor
// only; registry/activation decides when (or whether) this sink is selected.
func NewJiraWorkItemClickHouseEffects(
	conn driver.Conn,
	lease providerfoundation.LeaseGuard,
) JiraWorkItemClickHouseEffects {
	return JiraWorkItemClickHouseEffects{
		Lease:              lease,
		Sprints:            GitHubSprintsClickHouseAdapter{Conn: conn},
		Dependencies:       GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn},
		Interactions:       GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn},
		Reopens:            GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn},
		Transitions:        GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn},
		WorkItems:          GitHubWorkItemsClickHouseAdapter{Conn: conn},
		ProjectMemberships: GitHubProjectMembershipClickHouseAdapter{Conn: conn},
		Projects:           JiraProjectCatalogClickHouseAdapter{Delegate: GitHubProjectCatalogClickHouseAdapter{Conn: conn}},
	}
}

func (sink JiraWorkItemClickHouseEffects) WriteEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
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

func (sink JiraWorkItemClickHouseEffects) InspectEffect(
	ctx context.Context, claim Claim, effect EffectBatch,
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
	if inspection != EffectExact && inspection != EffectAbsent && inspection != EffectConflict {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspection, nil
}

func (sink JiraWorkItemClickHouseEffects) resolve(
	claim Claim, effect EffectBatch,
) (JiraWorkItemEffectIdentity, JiraWorkItemEffectAdapter, error) {
	if claim.Validate() != nil || claim.Provider != "jira" ||
		!isWorkItemFamilyDataset(claim.Dataset) ||
		!slices.Contains(jiraWorkItemEffectDestinations, effect.Destination) ||
		effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) {
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
	for _, raw := range effect.Rows {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
		}
		var orgID string
		if value, ok := object["org_id"]; !ok || json.Unmarshal(value, &orgID) != nil || orgID != identity.OrgID {
			return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
		}
	}
	adapter, ok := sink.adapterForDestination(effect.Destination)
	if !ok || adapter == nil {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func (sink JiraWorkItemClickHouseEffects) adapterForDestination(
	destination string,
) (JiraWorkItemEffectAdapter, bool) {
	switch destination {
	case "sprints":
		return sink.Sprints, true
	case "work_item_dependencies":
		return sink.Dependencies, true
	case "work_item_interactions":
		return sink.Interactions, true
	case "work_item_reopen_events":
		return sink.Reopens, true
	case "work_item_transitions":
		return sink.Transitions, true
	case "work_items":
		return sink.WorkItems, true
	case "project_membership_transitions":
		return sink.ProjectMemberships, true
	case "projects":
		return sink.Projects, true
	default:
		return nil, false
	}
}

func (sink JiraWorkItemClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(jiraWorkItemEffectDestinations))
	for _, destination := range jiraWorkItemEffectDestinations {
		adapter, ok := sink.adapterForDestination(destination)
		if !ok || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

var _ EffectSink = JiraWorkItemClickHouseEffects{}
var _ EffectReadback = JiraWorkItemClickHouseEffects{}
