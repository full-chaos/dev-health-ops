package daily

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
)

// # Why the work-item families read a work scope, not a repository
//
// work_item_metrics_daily, work_item_user_metrics_daily,
// work_item_state_durations_daily and estimate_coverage_metrics_daily replace
// rows by a sorting key that has work_scope_id and no repo_id. A row of these
// tables is therefore the row of a work scope. A family that computed one
// repository at a time wrote one row for each repository of a scope under one
// key, and the row that was written last stayed: it held the items of one
// repository only.
//
// The read below gives a family every item of the work scopes that its
// partition touches, from every repository of the organization. The family
// computes each scope once and writes it once. It serves a run of every
// repository and a run of listed repositories with the same code: a listed
// repository that shares a scope with an unlisted one reads the unlisted
// repository's items too.
//
// "The repositories of a work scope" is not a relation here. An item belongs
// to a scope by its own work_scope_id (workItemStateWorkItem.workScopeID, the
// one derivation of this package); repo_id is only where the item row is
// stored.

const (
	// maxWorkItemScopeFilterValues and maxWorkItemScopeFilterBytes bound the
	// scope filter of the items query. The filter is query text: clickhouse-go
	// renders the array into the statement. Above either bound the read drops
	// the filter, reads the items of the day for the whole organization and
	// keeps the rows of its scopes in Go: the same rows, at the cost of a
	// wider read. The byte bound is the rendered size of the array literal and
	// is the rule of package querybound, which keeps a statement below the
	// server's max_query_size.
	maxWorkItemScopeFilterValues = 2000
	maxWorkItemScopeFilterBytes  = querybound.MaxArrayBytes

	// The reasons a read has no scope filter, as the log line names them.
	workItemScopeFilterAboveValues = "values"
	workItemScopeFilterAboveBytes  = "bytes"

	// WorkItemScopeReadLogMessage is logged once for each family and
	// partition with the counts of the read.
	WorkItemScopeReadLogMessage = "daily work item scope read"
	// WorkItemScopeReadUnfilteredLogMessage is logged when the scope filter is
	// above its bound and the read takes the whole organization's day.
	WorkItemScopeReadUnfilteredLogMessage = "daily work item scope read without a scope filter: the filter is above its bound"
)

// workItemScopeKey is the part of the four tables' sorting key that names a
// work scope.
type workItemScopeKey struct {
	provider string
	scope    string
}

// workItemScopedRow is one stored work item row with its storage address and
// its version.
type workItemScopedRow struct {
	workItemMetricsRow
	RepoID     uuid.UUID
	LastSynced time.Time
}

// workItemScopeReadStats are the counts of one read. ItemsOutsidePartition is
// the number of items that a read of the partition's repositories alone would
// not have returned.
type workItemScopeReadStats struct {
	Scopes                int
	ItemsInPartition      int
	ItemsOutsidePartition int
	DuplicateItems        int
	FilterValues          int
	Unfiltered            bool
}

type workItemScopeRead struct {
	// Scopes are the work scopes the read covers: the ones the partition's
	// repositories have an item in for the day. The stale-key rule of the
	// families is bounded by them.
	Scopes       map[workItemScopeKey]struct{}
	Items        []workItemMetricsRow
	Transitions  []workItemStateTransition
	Attributions map[string]workItemPrimaryAttribution
	Stats        workItemScopeReadStats
}

// staleKeyScope is the read's work scopes as the scope of the stale-key rule:
// one (provider, work_scope_id) tuple for each.
func (read workItemScopeRead) staleKeyScope() staleKeyScope {
	tuples := make([][]string, 0, len(read.Scopes))
	for key := range read.Scopes {
		tuples = append(tuples, []string{key.provider, key.scope})
	}
	return newStaleKeyScope(tuples...)
}

// stateItems is the column subset the work_item_state compute reads.
func (read workItemScopeRead) stateItems() []workItemStateWorkItem {
	items := make([]workItemStateWorkItem, 0, len(read.Items))
	for _, item := range read.Items {
		items = append(items, item.workItemStateWorkItem)
	}
	return items
}

// workItemScopeFilter turns the scope set into the values of the superset
// filter. above names the bound that the values are above
// (workItemScopeFilterAboveValues, workItemScopeFilterAboveBytes); it is ""
// when the filter is used. The count bound is checked first.
func workItemScopeFilter(scopes map[workItemScopeKey]struct{}) (values []string, emptyScope bool, above string) {
	seen := make(map[string]struct{}, len(scopes))
	for key := range scopes {
		if key.scope == "" {
			emptyScope = true
			continue
		}
		if _, duplicate := seen[key.scope]; duplicate {
			continue
		}
		seen[key.scope] = struct{}{}
		values = append(values, key.scope)
	}
	sort.Strings(values)
	if len(values) > maxWorkItemScopeFilterValues {
		return nil, emptyScope, workItemScopeFilterAboveValues
	}
	if workItemScopeFilterRenderedBytes(values) > maxWorkItemScopeFilterBytes {
		return nil, emptyScope, workItemScopeFilterAboveBytes
	}
	return values, emptyScope, ""
}

// workItemScopeFilterRenderedBytes is the size of the array literal that
// clickhouse-go writes into the statement for the values: the brackets, each
// rendered element and a two-byte separator between elements.
func workItemScopeFilterRenderedBytes(values []string) int {
	if len(values) == 0 {
		return 2
	}
	size := 2 + 2*(len(values)-1)
	for _, value := range values {
		size += querybound.RenderedStringLen(value)
	}
	return size
}

// workItemScopeFilterSQL is the superset filter. Every non-empty work scope id
// is the value of one of the four columns of its item, so an item of a wanted
// scope always passes; an item of another scope can pass too, and
// keepWorkItemsOfScopes removes it. The scope rule itself stays in Go.
func workItemScopeFilterSQL(emptyScope, filtered bool) string {
	if !filtered {
		return ""
	}
	clause := `
  AND (has(scopes, project_key) OR has(scopes, project_id)
    OR has(scopes, project_name) OR has(scopes, native_team_key)`
	if emptyScope {
		clause += `
    OR (project_key = '' AND project_id = '' AND project_name = '' AND native_team_key = '')`
	}
	return clause + ")"
}

// keepWorkItemsOfScopes keeps the rows whose (provider, work scope id) is in
// the scope set.
func keepWorkItemsOfScopes(rows []workItemScopedRow, scopes map[workItemScopeKey]struct{}) []workItemScopedRow {
	kept := make([]workItemScopedRow, 0, len(rows))
	for _, row := range rows {
		if _, wanted := scopes[workItemScopeKey{provider: row.Provider, scope: row.workScopeID()}]; wanted {
			kept = append(kept, row)
		}
	}
	return kept
}

// countWorkItemOncePerProviderAndID is the rule for one work item id that is
// stored under two repository ids: the item counts once, and the row with the
// newest last_synced is the item (on a tie, the lower repository id).
//
// This is the one place that holds the rule. work_item_cycle_times and
// work_item_transitions identify an item the same way, without a repository.
func countWorkItemOncePerProviderAndID(rows []workItemScopedRow) (kept []workItemScopedRow, duplicates int) {
	type identity struct{ provider, workItemID string }
	position := make(map[identity]int, len(rows))
	kept = make([]workItemScopedRow, 0, len(rows))
	for _, row := range rows {
		key := identity{row.Provider, row.WorkItemID}
		index, seen := position[key]
		if !seen {
			position[key] = len(kept)
			kept = append(kept, row)
			continue
		}
		duplicates++
		if newerWorkItemVersion(row, kept[index]) {
			kept[index] = row
		}
	}
	return kept, duplicates
}

// newerWorkItemVersion says that candidate is the item and current is not:
// the newer last_synced, and for two rows of one last_synced the row of the
// lower repository id (the text form of the id is compared). The tie rule
// makes the result independent of the order the rows are read in.
func newerWorkItemVersion(candidate, current workItemScopedRow) bool {
	if !candidate.LastSynced.Equal(current.LastSynced) {
		return candidate.LastSynced.After(current.LastSynced)
	}
	return candidate.RepoID.String() < current.RepoID.String()
}

// loadWorkItemPartitionScopes reads the work scopes that the given
// repositories have an item in for the day.
//
// The list of repositories has no bound of its own: a partition holds a few,
// the end of a run passes every repository of the run. clickhouse-go writes
// the list INTO the statement text, so the read is done once per chunk of the
// list (package querybound) and the scopes are unioned. A scope is a set
// member, so the union of the chunks is what one statement over the whole
// list returns.
func loadWorkItemPartitionScopes(
	ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID, start, end time.Time,
) (map[workItemScopeKey]struct{}, error) {
	scopes := make(map[workItemScopeKey]struct{})
	for _, chunk := range chunkRepositoryIDs(repoIDs, maxWorkItemScopeFilterBytes) {
		if err := loadWorkItemPartitionScopeChunk(ctx, conn, organizationID, chunk, start, end, scopes); err != nil {
			return nil, err
		}
	}
	return scopes, nil
}

// loadWorkItemOrganizationScopes reads every work scope of the organization
// that has an item for the day, whatever repository the item is stored under.
// It is the scope of the end of an organization-wide run. The statement holds
// no list, so it does not grow with the organization.
func loadWorkItemOrganizationScopes(
	ctx context.Context, conn repositoryRows, organizationID string, start, end time.Time,
) (map[workItemScopeKey]struct{}, error) {
	scopes := make(map[workItemScopeKey]struct{})
	rows, err := conn.Query(ctx, `
SELECT DISTINCT provider, project_key, project_id, native_team_key, project_name
FROM work_items FINAL
WHERE org_id = ?
  AND created_at < ?
  AND (status != 'done' OR completed_at >= ?)`,
		organizationID, end.UTC(), start.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work item organization scopes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item workItemStateWorkItem
		if err := rows.Scan(
			&item.Provider, &item.ProjectKey, &item.ProjectID, &item.NativeTeamKey, &item.ProjectName,
		); err != nil {
			return nil, fmt.Errorf("scan work item organization scope: %w", err)
		}
		scopes[workItemScopeKey{provider: item.Provider, scope: item.workScopeID()}] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work item organization scopes: %w", err)
	}
	return scopes, nil
}

// chunkRepositoryIDs splits the list into consecutive chunks whose array
// literal is at most maxBytes as clickhouse-go renders it (a repository id is
// rendered as its quoted text form). Every id is in exactly one chunk; an
// empty list gives no chunk.
func chunkRepositoryIDs(repoIDs []uuid.UUID, maxBytes int) [][]uuid.UUID {
	if len(repoIDs) == 0 {
		return nil
	}
	rendered := make([]string, len(repoIDs))
	for index, repoID := range repoIDs {
		rendered[index] = repoID.String()
	}
	chunks := make([][]uuid.UUID, 0, 1)
	offset := 0
	for _, chunk := range querybound.ChunkStringsByRenderedBytes(rendered, maxBytes) {
		chunks = append(chunks, repoIDs[offset:offset+len(chunk)])
		offset += len(chunk)
	}
	return chunks
}

func loadWorkItemPartitionScopeChunk(
	ctx context.Context, conn repositoryRows, organizationID string, repoIDs []uuid.UUID, start, end time.Time,
	scopes map[workItemScopeKey]struct{},
) error {
	rows, err := conn.Query(ctx, `
SELECT DISTINCT provider, project_key, project_id, native_team_key, project_name
FROM work_items FINAL
WHERE org_id = ? AND repo_id IN ?
  AND created_at < ?
  AND (status != 'done' OR completed_at >= ?)`,
		organizationID, repoIDs, end.UTC(), start.UTC(),
	)
	if err != nil {
		return fmt.Errorf("load work item partition scopes: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item workItemStateWorkItem
		if err := rows.Scan(
			&item.Provider, &item.ProjectKey, &item.ProjectID, &item.NativeTeamKey, &item.ProjectName,
		); err != nil {
			return fmt.Errorf("scan work item partition scope: %w", err)
		}
		scopes[workItemScopeKey{provider: item.Provider, scope: item.workScopeID()}] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate work item partition scopes: %w", err)
	}
	return nil
}

// The columns of the items query after the ones every family reads. The
// work_item_state family computes from the columns of workItemStateWorkItem
// only, so its read takes typed empty values in place of the five stored
// columns that the other two families compute from.
const (
	workItemScopeMetricsColumns   = `type, assignees, started_at, closed_at, story_points`
	workItemScopeNoMetricsColumns = `'', CAST([], 'Array(String)'), CAST(NULL, 'Nullable(DateTime64(3))'),
       CAST(NULL, 'Nullable(DateTime64(3))'), CAST(NULL, 'Nullable(Float64)')`
)

// loadWorkItemScopeItems is the one items query of a read: the organization's
// items of the day, from every repository, that pass the superset filter. The
// day predicate is the predicate of LoadWorkItemMetricsWorkItems.
func loadWorkItemScopeItems(
	ctx context.Context, conn repositoryRows, organizationID string, start, end time.Time,
	values []string, emptyScope, filtered, metricsColumns bool,
) ([]workItemScopedRow, error) {
	columns := workItemScopeNoMetricsColumns
	if metricsColumns {
		columns = workItemScopeMetricsColumns
	}
	rows, err := conn.Query(ctx, `
WITH ? AS scopes
SELECT repo_id, last_synced,
       work_item_id, provider, status, project_key, project_id, native_team_key, project_name,
       created_at, completed_at, `+columns+`
FROM work_items FINAL
WHERE org_id = ?
  AND created_at < ?
  AND (status != 'done' OR completed_at >= ?)`+workItemScopeFilterSQL(emptyScope, filtered),
		workItemScopeFilterArgument(values), organizationID, end.UTC(), start.UTC(),
	)
	if err != nil {
		return nil, fmt.Errorf("load work item scope items: %w", err)
	}
	defer rows.Close()
	var items []workItemScopedRow
	for rows.Next() {
		var (
			item        workItemScopedRow
			completedAt *time.Time
			startedAt   *time.Time
			closedAt    *time.Time
			storyPoints *float64
		)
		if err := rows.Scan(
			&item.RepoID, &item.LastSynced,
			&item.WorkItemID, &item.Provider, &item.Status, &item.ProjectKey, &item.ProjectID,
			&item.NativeTeamKey, &item.ProjectName, &item.CreatedAt, &completedAt,
			&item.Type, &item.Assignees, &startedAt, &closedAt, &storyPoints,
		); err != nil {
			return nil, fmt.Errorf("scan work item scope item: %w", err)
		}
		item.CompletedAt = completedAt
		item.StartedAt = startedAt
		item.ClosedAt = closedAt
		item.StoryPoints = storyPoints
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work item scope items: %w", err)
	}
	return items, nil
}

// workItemScopeFilterArgument is never nil: a nil slice has no array literal.
func workItemScopeFilterArgument(values []string) []string {
	if values == nil {
		return []string{}
	}
	return values
}

// loadWorkItemScopeTransitions reads the transitions of the items that pass
// the superset filter. The identity and the dedup of a transition are those of
// LoadWorkItemStateTransitions: a transition belongs to its work item, and
// only copies of one event collapse.
func loadWorkItemScopeTransitions(
	ctx context.Context, conn repositoryRows, organizationID string, end time.Time,
	values []string, emptyScope, filtered bool,
) ([]workItemStateTransition, error) {
	rows, err := conn.Query(ctx, `
WITH ? AS scopes
SELECT work_item_id, occurred_at, from_status, to_status
FROM (
	SELECT
		org_id, work_item_id, occurred_at,
		from_status, to_status, from_status_raw, to_status_raw, actor,
		max(last_synced) AS last_synced
	FROM work_item_transitions
	WHERE org_id = ? AND occurred_at < ?
	  AND work_item_id IN (
		SELECT work_item_id FROM work_items WHERE org_id = ?`+workItemScopeFilterSQL(emptyScope, filtered)+`)
	GROUP BY org_id, work_item_id, occurred_at,
		from_status, to_status, from_status_raw, to_status_raw, actor
)`,
		workItemScopeFilterArgument(values), organizationID, end.UTC(), organizationID,
	)
	if err != nil {
		return nil, fmt.Errorf("load work item scope transitions: %w", err)
	}
	defer rows.Close()
	var transitions []workItemStateTransition
	for rows.Next() {
		var transition workItemStateTransition
		if err := rows.Scan(
			&transition.WorkItemID, &transition.OccurredAt, &transition.FromStatus, &transition.ToStatus,
		); err != nil {
			return nil, fmt.Errorf("scan work item scope transition: %w", err)
		}
		transitions = append(transitions, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work item scope transitions: %w", err)
	}
	return transitions, nil
}

// workItemAttributionAddress is where an attribution row is stored. The same
// work item id under two repository ids has two attribution rows, so the
// repository is part of the address.
type workItemAttributionAddress struct {
	repoID     uuid.UUID
	workItemID string
}

// loadWorkItemScopeAttributions reads the primary attribution of the items
// that pass the superset filter, with the latest-snapshot fence of
// LoadWorkItemPrimaryTeamAttributions for each (repository, item).
func loadWorkItemScopeAttributions(
	ctx context.Context, conn repositoryRows, organizationID string,
	values []string, emptyScope, filtered bool,
) (map[workItemAttributionAddress]workItemPrimaryAttribution, error) {
	rows, err := conn.Query(ctx, `
WITH ? AS scopes
SELECT repo_id, work_item_id, ifNull(team_id, ''), ifNull(team_name, '')
FROM work_item_team_attributions FINAL
WHERE org_id = ? AND is_primary = 1
  AND (repo_id, work_item_id, computed_at) IN (
      SELECT repo_id, work_item_id, max(computed_at)
      FROM work_item_team_attributions
      WHERE org_id = ?
        AND (repo_id, work_item_id) IN (
          SELECT repo_id, work_item_id FROM work_items WHERE org_id = ?`+workItemScopeFilterSQL(emptyScope, filtered)+`)
      GROUP BY repo_id, work_item_id)`,
		workItemScopeFilterArgument(values), organizationID, organizationID, organizationID,
	)
	if err != nil {
		return nil, fmt.Errorf("load work item scope attributions: %w", err)
	}
	defer rows.Close()
	attributions := make(map[workItemAttributionAddress]workItemPrimaryAttribution)
	for rows.Next() {
		var (
			address     workItemAttributionAddress
			attribution workItemPrimaryAttribution
		)
		if err := rows.Scan(&address.repoID, &address.workItemID, &attribution.TeamID, &attribution.TeamName); err != nil {
			return nil, fmt.Errorf("scan work item scope attribution: %w", err)
		}
		attributions[address] = attribution
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work item scope attributions: %w", err)
	}
	return attributions, nil
}

// loadWorkItemScopeRead does every read of one family for one partition. A
// failed query fails the read: the caller writes nothing, because a row
// computed from a part of a scope reads as a complete answer. withTransitions
// and metricsColumns name what the family computes from.
func loadWorkItemScopeRead(
	ctx context.Context, conn repositoryRows, family string, run Run, partition Partition,
	scope workItemPartitionScope, withTransitions, metricsColumns bool,
) (workItemScopeRead, error) {
	if conn == nil || strings.TrimSpace(run.OrganizationID) == "" || !scope.start.Before(scope.end) {
		return workItemScopeRead{}, ErrInvalidState
	}
	var scopes map[workItemScopeKey]struct{}
	var err error
	if scope.everyRepository {
		scopes, err = loadWorkItemOrganizationScopes(ctx, conn, run.OrganizationID, scope.start, scope.end)
	} else {
		scopes, err = loadWorkItemPartitionScopes(ctx, conn, run.OrganizationID, scope.repoIDs, scope.start, scope.end)
	}
	if err != nil {
		return workItemScopeRead{}, err
	}
	read := workItemScopeRead{Scopes: scopes, Attributions: map[string]workItemPrimaryAttribution{}}
	read.Stats.Scopes = len(scopes)
	if len(scopes) == 0 {
		return read, nil
	}
	values, emptyScope, above := workItemScopeFilter(scopes)
	filtered := above == ""
	read.Stats.FilterValues, read.Stats.Unfiltered = len(values), !filtered
	if !filtered {
		slog.WarnContext(ctx, WorkItemScopeReadUnfilteredLogMessage,
			"family", family,
			"organization_id", run.OrganizationID,
			"target_day", run.TargetDay,
			"partition_id", partition.ID,
			"scopes", len(scopes),
			"above_bound", above,
			"max_filter_values", maxWorkItemScopeFilterValues,
			"max_filter_bytes", maxWorkItemScopeFilterBytes,
		)
	}
	stored, err := loadWorkItemScopeItems(ctx, conn, run.OrganizationID, scope.start, scope.end, values, emptyScope, filtered, metricsColumns)
	if err != nil {
		return workItemScopeRead{}, err
	}
	kept, duplicates := countWorkItemOncePerProviderAndID(keepWorkItemsOfScopes(stored, scopes))
	read.Stats.DuplicateItems = duplicates
	if withTransitions && len(kept) > 0 {
		read.Transitions, err = loadWorkItemScopeTransitions(ctx, conn, run.OrganizationID, scope.end, values, emptyScope, filtered)
		if err != nil {
			return workItemScopeRead{}, err
		}
	}
	var attributions map[workItemAttributionAddress]workItemPrimaryAttribution
	if len(kept) > 0 {
		attributions, err = loadWorkItemScopeAttributions(ctx, conn, run.OrganizationID, values, emptyScope, filtered)
		if err != nil {
			return workItemScopeRead{}, err
		}
	}
	inPartition := make(map[uuid.UUID]struct{}, len(scope.repoIDs))
	for _, repoID := range scope.repoIDs {
		inPartition[repoID] = struct{}{}
	}
	read.Items = make([]workItemMetricsRow, 0, len(kept))
	for _, row := range kept {
		if _, listed := inPartition[row.RepoID]; listed {
			read.Stats.ItemsInPartition++
		} else {
			read.Stats.ItemsOutsidePartition++
		}
		read.Items = append(read.Items, row.workItemMetricsRow)
		if attribution, found := attributions[workItemAttributionAddress{row.RepoID, row.WorkItemID}]; found {
			read.Attributions[row.WorkItemID] = attribution
		}
	}
	slog.InfoContext(ctx, WorkItemScopeReadLogMessage,
		"family", family,
		"organization_id", run.OrganizationID,
		"target_day", run.TargetDay,
		"partition_id", partition.ID,
		"scopes", read.Stats.Scopes,
		"items_in_partition", read.Stats.ItemsInPartition,
		"items_outside_partition", read.Stats.ItemsOutsidePartition,
		"duplicate_items", read.Stats.DuplicateItems,
		"filter_values", read.Stats.FilterValues,
		"unfiltered", read.Stats.Unfiltered,
	)
	return read, nil
}
