package providersync

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var jiraAtlassianEffectDestinations = []string{
	"sprints",
	"work_item_dependencies",
	"work_item_interactions",
	"work_item_reopen_events",
	"work_item_transitions",
	"work_items",
	"worklogs",
	// CHAOS-4193: project-membership history and the projects catalog rows
	// that make it resolvable. This is the PRODUCTION jira route -- the
	// plain JiraWorkItemsRouteHandler these mirror is not what
	// cmd/dev-health-worker/provider_sync.go constructs for a live jira
	// claim.
	"project_membership_transitions",
	"projects",
}

func JiraAtlassianEffectDestinations() []string {
	return append([]string(nil), jiraAtlassianEffectDestinations...)
}

func BuildJiraAtlassianEffects(rows jiraAtlassianRows) ([]EffectBatch, error) {
	projections := map[string]func(jiraAtlassianRows) []json.RawMessage{
		"sprints":                 func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.Sprints) },
		"work_item_dependencies":  func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.Dependencies) },
		"work_item_interactions":  func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.Interactions) },
		"work_item_reopen_events": func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.ReopenEvents) },
		"work_item_transitions":   func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.Transitions) },
		"work_items":              func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.WorkItems) },
		"worklogs":                func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.Worklogs) },
		"project_membership_transitions": func(value jiraAtlassianRows) []json.RawMessage {
			return marshalJiraRows(value.ProjectMemberships)
		},
		"projects": func(value jiraAtlassianRows) []json.RawMessage { return marshalJiraRows(value.Projects) },
	}
	if len(projections) != len(jiraAtlassianEffectDestinations) {
		return nil, ErrInvalidConfiguration
	}
	effects := make([]EffectBatch, 0, len(jiraAtlassianEffectDestinations))
	for _, destination := range jiraAtlassianEffectDestinations {
		projection, ok := projections[destination]
		if !ok || projection == nil {
			return nil, ErrInvalidConfiguration
		}
		effect, err := BuildEffectBatch(destination, EffectReadbackRequired, projection(rows))
		if err != nil {
			return nil, err
		}
		effects = append(effects, effect)
	}
	return effects, nil
}

type jiraAtlassianEffectAdapter interface {
	WriteJiraAtlassianEffect(context.Context, JiraWorkItemEffectIdentity, EffectBatch) error
	InspectJiraAtlassianEffect(context.Context, JiraWorkItemEffectIdentity, EffectBatch) (EffectInspection, error)
}

type jiraAtlassianGitHubAdapter struct{ delegate JiraWorkItemEffectAdapter }

func (adapter jiraAtlassianGitHubAdapter) WriteJiraAtlassianEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) error {
	if adapter.delegate == nil {
		return ErrInvalidConfiguration
	}
	return adapter.delegate.WriteGitHubWorkItemEffect(ctx, identity, effect)
}

func (adapter jiraAtlassianGitHubAdapter) InspectJiraAtlassianEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) (EffectInspection, error) {
	if adapter.delegate == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	return adapter.delegate.InspectGitHubWorkItemEffect(ctx, identity, effect)
}

// JiraAtlassianClickHouseEffects dispatches the seven direct destinations of
// the Atlassian route.  Worklogs use a provider-local adapter because their
// DateTime64(6) schema is not compatible with the six DateTime64(3) adapters.
type JiraAtlassianClickHouseEffects struct {
	Lease              providerfoundation.LeaseGuard
	Sprints            JiraWorkItemEffectAdapter
	Dependencies       JiraWorkItemEffectAdapter
	Interactions       JiraWorkItemEffectAdapter
	Reopens            JiraWorkItemEffectAdapter
	Transitions        JiraWorkItemEffectAdapter
	WorkItems          JiraWorkItemEffectAdapter
	Worklogs           JiraAtlassianWorklogEffectAdapter
	ProjectMemberships JiraWorkItemEffectAdapter
	Projects           JiraWorkItemEffectAdapter
}

func NewJiraAtlassianClickHouseEffects(conn driver.Conn, lease providerfoundation.LeaseGuard) JiraAtlassianClickHouseEffects {
	return JiraAtlassianClickHouseEffects{
		Lease:              lease,
		Sprints:            GitHubSprintsClickHouseAdapter{Conn: conn},
		Dependencies:       GitHubWorkItemDependenciesClickHouseAdapter{Conn: conn},
		Interactions:       GitHubWorkItemInteractionsClickHouseAdapter{Conn: conn},
		Reopens:            GitHubWorkItemReopenEventsClickHouseAdapter{Conn: conn},
		Transitions:        GitHubWorkItemTransitionsClickHouseAdapter{Conn: conn},
		WorkItems:          GitHubWorkItemsClickHouseAdapter{Conn: conn},
		Worklogs:           JiraWorklogsClickHouseAdapter{Conn: conn},
		ProjectMemberships: GitHubProjectMembershipClickHouseAdapter{Conn: conn},
		Projects:           JiraProjectCatalogClickHouseAdapter{Delegate: GitHubProjectCatalogClickHouseAdapter{Conn: conn}},
	}
}

func (sink JiraAtlassianClickHouseEffects) WriteEffect(ctx context.Context, claim Claim, effect EffectBatch) error {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return err
	}
	if err := adapter.WriteJiraAtlassianEffect(ctx, identity, effect); err != nil {
		return err
	}
	return sink.Lease.Assert(ctx)
}

func (sink JiraAtlassianClickHouseEffects) InspectEffect(ctx context.Context, claim Claim, effect EffectBatch) (EffectInspection, error) {
	identity, adapter, err := sink.resolve(claim, effect)
	if err != nil || ctx == nil || sink.Lease == nil {
		return EffectConflict, ErrInvalidConfiguration
	}
	if err := sink.Lease.Assert(ctx); err != nil {
		return EffectConflict, err
	}
	inspection, err := adapter.InspectJiraAtlassianEffect(ctx, identity, effect)
	if err != nil {
		return EffectConflict, err
	}
	if inspection != EffectExact && inspection != EffectAbsent && inspection != EffectConflict {
		return EffectConflict, ErrInvalidConfiguration
	}
	return inspection, nil
}

func (sink JiraAtlassianClickHouseEffects) resolve(claim Claim, effect EffectBatch) (JiraWorkItemEffectIdentity, jiraAtlassianEffectAdapter, error) {
	if claim.Validate() != nil || claim.Provider != "jira" || !isWorkItemFamilyDataset(claim.Dataset) ||
		!slices.Contains(jiraAtlassianEffectDestinations, effect.Destination) ||
		effect.Recovery != EffectReadbackRequired || !validDigest(effect.ContentDigest) {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	rebuilt, err := BuildEffectBatch(effect.Destination, effect.Recovery, effect.Rows)
	if err != nil || rebuilt.ContentDigest != effect.ContentDigest || rebuilt.PayloadBytes != effect.PayloadBytes {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	identity := JiraWorkItemEffectIdentity{OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		Generation: claim.GenerationKey(), Destination: effect.Destination, ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows)}
	if strings.TrimSpace(identity.OrgID) == "" || strings.TrimSpace(identity.Generation) == "" {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	for _, raw := range effect.Rows {
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
		}
		var orgID string
		value, ok := object["org_id"]
		if !ok || json.Unmarshal(value, &orgID) != nil || orgID != identity.OrgID {
			return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
		}
	}
	adapter, ok := sink.adapterForDestination(effect.Destination)
	if !ok || adapter == nil {
		return JiraWorkItemEffectIdentity{}, nil, ErrInvalidConfiguration
	}
	return identity, adapter, nil
}

func (sink JiraAtlassianClickHouseEffects) adapterForDestination(destination string) (jiraAtlassianEffectAdapter, bool) {
	switch destination {
	case "sprints":
		if sink.Sprints == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.Sprints}, true
	case "work_item_dependencies":
		if sink.Dependencies == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.Dependencies}, true
	case "work_item_interactions":
		if sink.Interactions == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.Interactions}, true
	case "work_item_reopen_events":
		if sink.Reopens == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.Reopens}, true
	case "work_item_transitions":
		if sink.Transitions == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.Transitions}, true
	case "work_items":
		if sink.WorkItems == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.WorkItems}, true
	case "worklogs":
		if sink.Worklogs == nil {
			return nil, false
		}
		return sink.Worklogs, true
	case "project_membership_transitions":
		if sink.ProjectMemberships == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.ProjectMemberships}, true
	case "projects":
		if sink.Projects == nil {
			return nil, false
		}
		return jiraAtlassianGitHubAdapter{sink.Projects}, true
	default:
		return nil, false
	}
}

func (sink JiraAtlassianClickHouseEffects) MissingDestinations() []string {
	missing := make([]string, 0, len(jiraAtlassianEffectDestinations))
	for _, destination := range jiraAtlassianEffectDestinations {
		adapter, ok := sink.adapterForDestination(destination)
		if !ok || adapter == nil {
			missing = append(missing, destination)
		}
	}
	return missing
}

type JiraAtlassianWorklogEffectAdapter interface {
	WriteJiraAtlassianEffect(context.Context, JiraWorkItemEffectIdentity, EffectBatch) error
	InspectJiraAtlassianEffect(context.Context, JiraWorkItemEffectIdentity, EffectBatch) (EffectInspection, error)
	WriteJiraWorklogEffect(context.Context, JiraWorkItemEffectIdentity, EffectBatch) error
	InspectJiraWorklogEffect(context.Context, JiraWorkItemEffectIdentity, EffectBatch) (EffectInspection, error)
}

type JiraWorklogsClickHouseAdapter struct{ Conn driver.Conn }

const jiraWorklogsInsert = `INSERT INTO worklogs (work_item_id, provider, worklog_id, author, started_at, time_spent_seconds, created_at, updated_at, last_synced, org_id)`
const jiraWorklogsSelect = `SELECT work_item_id, provider, worklog_id, author, started_at, time_spent_seconds, created_at, updated_at, last_synced, org_id FROM worklogs FINAL WHERE org_id = ? AND provider = ? AND worklog_id = ?`

func (adapter JiraWorklogsClickHouseAdapter) WriteJiraAtlassianEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) error {
	return adapter.WriteJiraWorklogEffect(ctx, identity, effect)
}

func (adapter JiraWorklogsClickHouseAdapter) InspectJiraAtlassianEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) (EffectInspection, error) {
	return adapter.InspectJiraWorklogEffect(ctx, identity, effect)
}

func (adapter JiraWorklogsClickHouseAdapter) WriteJiraWorklogEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) error {
	rows, err := decodeEffectRows[jiraWorklogRow](effect)
	if err != nil {
		return err
	}
	if ctx == nil || effect.Destination != "worklogs" || identity.Destination != "worklogs" || identity.OrgID == "" || identity.RowCount != len(effect.Rows) || (len(rows) > 0 && adapter.Conn == nil) {
		return ErrInvalidConfiguration
	}
	for _, row := range rows {
		if err := validateJiraWorklogIdentity(row, identity); err != nil {
			return err
		}
	}
	rows = dedupeBySortingKey(rows, jiraWorklogSortingKey)
	if len(rows) == 0 {
		return nil
	}
	batch, err := adapter.Conn.PrepareBatch(ctx, jiraWorklogsInsert)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, row := range rows {
		if err := batch.Append(jiraWorklogValues(row)...); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (adapter JiraWorklogsClickHouseAdapter) InspectJiraWorklogEffect(ctx context.Context, identity JiraWorkItemEffectIdentity, effect EffectBatch) (EffectInspection, error) {
	rows, err := decodeEffectRows[jiraWorklogRow](effect)
	if err != nil {
		return EffectConflict, err
	}
	if ctx == nil || effect.Destination != "worklogs" || identity.Destination != "worklogs" || identity.OrgID == "" || identity.RowCount != len(effect.Rows) || (len(rows) > 0 && adapter.Conn == nil) {
		return EffectConflict, ErrInvalidConfiguration
	}
	for _, row := range rows {
		if err := validateJiraWorklogIdentity(row, identity); err != nil {
			return EffectConflict, err
		}
	}
	rows = dedupeBySortingKey(rows, jiraWorklogSortingKey)
	if len(rows) == 0 {
		return EffectAbsent, nil
	}
	return foldWorkItemInspections(rows, func(row jiraWorklogRow) (EffectInspection, error) {
		result, err := adapter.Conn.Query(ctx, jiraWorklogsSelect, row.OrgID, row.Provider, row.WorklogID)
		if err != nil {
			return EffectConflict, err
		}
		defer result.Close()
		var actual jiraWorklogRow
		found := 0
		for result.Next() {
			if err := result.Scan(&actual.WorkItemID, &actual.Provider, &actual.WorklogID, &actual.Author, &actual.StartedAt, &actual.TimeSpentSeconds, &actual.CreatedAt, &actual.UpdatedAt, &actual.LastSynced, &actual.OrgID); err != nil {
				return EffectConflict, err
			}
			found++
		}
		if err := result.Err(); err != nil {
			return EffectConflict, err
		}
		if verdict, final := workItemReadbackVerdict(found); final {
			return verdict, nil
		}
		if verdict, final := workItemVersionVerdict(actual.LastSynced, clickHouseMicros(row.LastSynced)); final {
			return verdict, nil
		}
		if actual.WorkItemID != row.WorkItemID || actual.Provider != row.Provider || actual.WorklogID != row.WorklogID || !stringPointersEqual(actual.Author, row.Author) || !actual.StartedAt.Equal(clickHouseMicros(row.StartedAt)) || actual.TimeSpentSeconds != row.TimeSpentSeconds || !actual.CreatedAt.Equal(clickHouseMicros(row.CreatedAt)) || !actual.UpdatedAt.Equal(clickHouseMicros(row.UpdatedAt)) || actual.OrgID != row.OrgID {
			return EffectConflict, nil
		}
		return EffectExact, nil
	})
}

func validateJiraWorklogIdentity(row jiraWorklogRow, identity JiraWorkItemEffectIdentity) error {
	if row.Provider != "jira" || row.OrgID != identity.OrgID || row.WorkItemID == "" || row.WorklogID == "" || row.StartedAt.IsZero() || row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.LastSynced.IsZero() || row.TimeSpentSeconds < 0 {
		return ErrInvalidConfiguration
	}
	return nil
}

func validateJiraWorklog(row jiraWorklogRow, claim Claim) error {
	if row.Provider != "jira" || row.OrgID == "" || row.OrgID != claim.OrgID || row.WorkItemID == "" || row.WorklogID == "" || row.StartedAt.IsZero() || row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.LastSynced.IsZero() || row.TimeSpentSeconds < 0 {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func jiraWorklogSortingKey(row jiraWorklogRow) string {
	return row.OrgID + "\x00" + row.Provider + "\x00" + row.WorklogID
}

func jiraWorklogValues(row jiraWorklogRow) []any {
	return []any{row.WorkItemID, row.Provider, row.WorklogID, row.Author, clickHouseMicros(row.StartedAt), row.TimeSpentSeconds, clickHouseMicros(row.CreatedAt), clickHouseMicros(row.UpdatedAt), clickHouseMicros(row.LastSynced), row.OrgID}
}

func clickHouseMicros(value time.Time) time.Time { return value.UTC().Truncate(time.Microsecond) }

// Compile-time proof that the new adapter is accepted by the route sink.
var _ JiraAtlassianWorklogEffectAdapter = JiraWorklogsClickHouseAdapter{}
var _ EffectSink = JiraAtlassianClickHouseEffects{}
var _ EffectReadback = JiraAtlassianClickHouseEffects{}
