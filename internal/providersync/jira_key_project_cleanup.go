package providersync

import (
	"context"
	"errors"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// The Jira team catalog used to identify a project by an id built from its
// KEY ("{org_id}:jira:{KEY}") while the Jira work-items route identified the
// same project by its native id. One project was two `projects` rows with one
// project_key: team ownership pointed at the key-built row, every work item
// at the native row. The catalog writers now use the native id (see
// jira_team_catalog.go), and the ownership rows that named a key-built id
// are closed by the catalog's own snapshot rule at the first sync.
//
// What no sync can do is retire the key-built `projects` rows already
// written. A later version of the row does not do it: a reader of `projects`
// does not have to filter is_active, and is_active = 0 also marks real
// completed projects (the same finding that made
// RetireLinearPseudoProjectRows a physical delete). So this file is the
// one-time, operator-invoked cleanup. Nothing in this package calls it; it is
// wired to a dho workers verb.

// ErrJiraKeyProjectCleanupNoNativeRows is returned when the scope holds no
// native-id Jira project row at all. That is the state BEFORE the first team
// catalog sync on the new writer: the key-built rows are then the only
// project rows there are, and the cleanup must not be reported as a run that
// found nothing to do.
var ErrJiraKeyProjectCleanupNoNativeRows = errors.New("providersync: no native-id jira project row in scope; run after a jira team catalog sync")

// jiraKeyProjectIdentityPredicate is the producer-owned shape of the retired
// id: provider 'jira' and an id that starts with THIS ROW'S OWN org_id and
// the ":jira:" marker. A native Jira project id is numeric and can never
// start with its organization's id. Tied to the row's own org_id, never a
// substring test, for the reason linearPseudoProjectIdentityPredicate gives.
const jiraKeyProjectIdentityPredicate = `provider = 'jira' AND startsWith(id, concat(org_id, ':jira:'))`

// jiraKeyProjectNativeSiblingPredicate holds a key-built row back until the
// SAME organization has a native-id row with the SAME project key. Without
// it a run before the re-sync, or for a project the provider no longer
// returns, would delete the only `projects` row that project has.
const jiraKeyProjectNativeSiblingPredicate = `(org_id, ifNull(project_key, '')) IN (` +
	`SELECT org_id, ifNull(project_key, '') FROM projects ` +
	`WHERE provider = 'jira' AND NOT startsWith(id, concat(org_id, ':jira:')) AND ifNull(project_key, '') != '')`

const jiraKeyProjectDeletePredicate = jiraKeyProjectIdentityPredicate + ` AND ` + jiraKeyProjectNativeSiblingPredicate

const jiraKeyProjectCleanupCountQuery = `SELECT ` +
	`countIf(startsWith(id, concat(org_id, ':jira:'))), ` +
	`countIf(startsWith(id, concat(org_id, ':jira:')) AND ` + jiraKeyProjectNativeSiblingPredicate + `), ` +
	`countIf(NOT startsWith(id, concat(org_id, ':jira:'))) ` +
	`FROM projects FINAL WHERE provider = 'jira'`

const jiraKeyProjectCleanupCountQueryScoped = jiraKeyProjectCleanupCountQuery + ` AND org_id = {org_id:String}`

// mutations_sync=1: the reported count must describe rows that are gone when
// this returns, as for RetireLinearPseudoProjectRows.
const jiraKeyProjectCleanupDeleteMutation = `ALTER TABLE projects DELETE WHERE ` + jiraKeyProjectDeletePredicate

const jiraKeyProjectCleanupDeleteMutationScoped = jiraKeyProjectCleanupDeleteMutation + ` AND org_id = {org_id:String}`

// JiraKeyProjectCleanupOutcome is counts only: no id, key or name of a
// project leaves the store through this verb.
type JiraKeyProjectCleanupOutcome struct {
	DryRun bool `json:"dry_run"`
	// NativeRows is the Jira project rows with a native id in scope.
	NativeRows uint64 `json:"native_rows"`
	// KeyBuiltRows is every key-built row found.
	KeyBuiltRows uint64 `json:"key_built_rows"`
	// EligibleRows is the key-built rows with a native-id row of the same
	// organization and key: the rows a real run deletes.
	EligibleRows uint64 `json:"eligible_rows"`
	// HeldRows is the key-built rows left in place because no native-id row
	// has their key yet.
	HeldRows uint64 `json:"held_rows"`
	// DeletedRows is EligibleRows when a real delete ran, 0 in a dry run.
	DeletedRows uint64 `json:"deleted_rows"`
}

// RetireJiraKeyProjectRows deletes the key-built Jira `projects` rows that
// have a native-id row of the same organization and project key, across every
// organization unless orgID scopes it. A dry run counts and deletes nothing.
// Idempotent: a second real run finds no eligible row.
func RetireJiraKeyProjectRows(ctx context.Context, conn driver.Conn, orgID string, dryRun bool) (JiraKeyProjectCleanupOutcome, error) {
	if conn == nil {
		return JiraKeyProjectCleanupOutcome{}, ErrInvalidConfiguration
	}
	orgID = strings.TrimSpace(orgID)
	query := jiraKeyProjectCleanupCountQuery
	mutation := jiraKeyProjectCleanupDeleteMutation
	var namedArgs []any
	if orgID != "" {
		query = jiraKeyProjectCleanupCountQueryScoped
		mutation = jiraKeyProjectCleanupDeleteMutationScoped
		namedArgs = []any{clickhouse.Named("org_id", orgID)}
	}
	outcome := JiraKeyProjectCleanupOutcome{DryRun: dryRun}
	if err := conn.QueryRow(ctx, query, namedArgs...).Scan(
		&outcome.KeyBuiltRows, &outcome.EligibleRows, &outcome.NativeRows,
	); err != nil {
		return JiraKeyProjectCleanupOutcome{}, err
	}
	outcome.HeldRows = outcome.KeyBuiltRows - outcome.EligibleRows
	if outcome.NativeRows == 0 {
		return outcome, ErrJiraKeyProjectCleanupNoNativeRows
	}
	if dryRun || outcome.EligibleRows == 0 {
		return outcome, nil
	}
	mutationCtx := clickhouse.Context(ctx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": "1"}))
	if err := conn.Exec(mutationCtx, mutation, namedArgs...); err != nil {
		return JiraKeyProjectCleanupOutcome{}, err
	}
	outcome.DeletedRows = outcome.EligibleRows
	return outcome, nil
}
