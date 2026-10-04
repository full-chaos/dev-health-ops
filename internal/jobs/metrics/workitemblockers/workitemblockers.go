// Package workitemblockers holds the ClickHouse reads of the "blocked from
// open blockers" rule (CHAOS-8493): the blocking relations of an
// organization and the stored work items those relations name.
//
// The RULE is workitemmetrics.BlockedIntervalsByItem; this package only
// fetches its inputs. It is a package of its own, and not a file of either
// caller, for the reason workitemmetrics is: work_item_state_durations_daily
// has two writers -- the work_item_state daily family
// (internal/jobs/metrics/daily) and the sync-time deriver
// (internal/providersync) -- and both must read the same rows the same way.
// A relation type one of them filtered out, or a timestamp one of them read
// in another zone, would make the two writers disagree about which hours are
// "blocked", and `status` is part of that table's row key.
//
// NO READ HERE SENDS AN UNBOUNDED LIST. clickhouse-go writes a list argument
// into the statement text, and ClickHouse refuses a text above max_query_size
// (querybound). The organization-wide reads send no list at all: the ends of
// the relations are found by a subquery in ClickHouse. The reads keyed on the
// items of one sync unit send their ids in chunks of at most maxArrayBytes.
package workitemblockers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// Querier is the one ClickHouse capability these reads need.
type Querier interface {
	Query(context.Context, string, ...any) (driver.Rows, error)
}

// ErrInvalidRequest reports a read with no connection or no organization.
// These reads are never run unscoped: an empty organization would read every
// tenant's relations.
var ErrInvalidRequest = errors.New("workitemblockers: a connection and an organization are required")

// ErrLimitExceeded reports a bounded read that found more rows than its
// limit. The caller fails: a truncated relation set would leave some items
// un-blocked with nothing in the output saying the read was cut.
var ErrLimitExceeded = errors.New("workitemblockers: the read found more rows than its limit")

// maxArrayBytes caps the rendered size of ONE list of ids in a statement
// (querybound.MaxArrayBytes). A statement here carries at most two lists.
//
// It is a variable only so a test can force many chunks with a small cap;
// nothing in production assigns it.
var maxArrayBytes = querybound.MaxArrayBytes

// blockingRelationFilter selects the work_item_dependencies rows the rule
// reads: the two blocking types under the canonical semantics version. Its
// two parameters are the organization and the semantics version. Every read
// of the relations, and every subquery over them, uses this one text, so no
// two reads can disagree about which rows are blocking relations.
const blockingRelationFilter = `org_id = ?
  AND relationship_type IN ('blocks', 'blocked_by')
  AND relationship_semantics_version = ?`

// namingFilter keeps the relations that name one of a list of ids at either
// end. Its two parameters are that list, twice.
const namingFilter = `
  AND (has(?, source_work_item_id) OR has(?, target_work_item_id))`

// LoadRelations reads the organization's blocking relations:
// the work_item_dependencies rows the "blocked from open blockers" rule reads
// (workitemmetrics.BlockedIntervalsByItem). Only the two blocking types under
// the canonical semantics version are read; every other row has no
// dependable direction and the rule would discard it anyway.
//
// Organization-wide on purpose, where every other read of the daily family is
// per repository: a blocker lives in ANY repository of the organization (or in
// none: a jira or linear issue), so a per-repository read would miss the
// relations this rule exists for. The table's key is
// (org_id, source, target, type), so FINAL is a complete dedup of re-synced
// copies, as it is for work_items.
func LoadRelations(
	ctx context.Context, conn Querier, organizationID string,
) ([]workitemmetrics.BlockingRelation, error) {
	return loadRelations(ctx, conn, organizationID, nil, 0)
}

// LoadRelationsNaming reads the blocking relations that name one of naming
// at EITHER end, and no more than limit of them: the keyed, bounded form the
// sync-time deriver uses for the items of one sync unit. Both ends are
// matched because the two relation forms put the blocked item on different
// sides ("blocks": the target; "blocked_by": the source). More rows than
// limit is ErrLimitExceeded, never a truncated answer. No id reads nothing.
//
// naming holds the unit's work item ids and, for a jira or linear item, the
// external-key form of its issue key (workitemmetrics.ExternalKeyTarget): a
// relation read from text names such an item by its key, not by its id.
//
// The ids go in chunks of at most maxArrayBytes. A relation that names an id
// of two chunks is read twice and kept once.
func LoadRelationsNaming(
	ctx context.Context, conn Querier, organizationID string, naming []string, limit int,
) ([]workitemmetrics.BlockingRelation, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || limit < 1 {
		return nil, ErrInvalidRequest
	}
	if len(naming) == 0 {
		return nil, nil
	}
	return loadRelations(ctx, conn, organizationID, naming, limit)
}

// relationKey is a relation's identity within one organization: the sorting
// key of work_item_dependencies and of work_item_dependency_first_seen after
// org_id.
type relationKey struct{ source, target, relationship string }

func (key relationKey) less(other relationKey) bool {
	if key.source != other.source {
		return key.source < other.source
	}
	if key.target != other.target {
		return key.target < other.target
	}
	return key.relationship < other.relationship
}

func loadRelations(
	ctx context.Context, conn Querier, organizationID string, naming []string, limit int,
) ([]workitemmetrics.BlockingRelation, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidRequest
	}
	// One pass with no list for the organization-wide read; one pass per
	// chunk of ids for the keyed read.
	chunks := [][]string{nil}
	if naming != nil {
		chunks = querybound.ChunkStringsByRenderedBytes(naming, maxArrayBytes)
	}
	relations := map[relationKey]workitemmetrics.BlockingRelation{}
	firstSeen := map[relationKey]time.Time{}
	for _, chunk := range chunks {
		before := len(relations)
		if err := readRelations(ctx, conn, organizationID, chunk, limit, relations); err != nil {
			return nil, err
		}
		if limit > 0 && len(relations) > limit {
			return nil, fmt.Errorf("%w: more than %d blocking relations", ErrLimitExceeded, limit)
		}
		if len(relations) == before {
			// This pass read no relation that an earlier pass had not read,
			// so it has no first-seen time to add.
			continue
		}
		if err := readFirstSeen(ctx, conn, organizationID, chunk, firstSeen); err != nil {
			return nil, err
		}
	}
	if len(relations) == 0 {
		return nil, nil
	}
	keys := make([]relationKey, 0, len(relations))
	for key := range relations {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(left, right int) bool { return keys[left].less(keys[right]) })
	result := make([]workitemmetrics.BlockingRelation, 0, len(keys))
	for _, key := range keys {
		relation := relations[key]
		if seen, stored := firstSeen[key]; stored {
			seen := seen
			relation.FirstSeenAt = &seen
		}
		result = append(result, relation)
	}
	return result, nil
}

// readRelations reads the blocking relations of one chunk of ids (of the
// whole organization when chunk is nil) into relations. Of two reads of one
// relation, the row synced last is kept.
func readRelations(
	ctx context.Context, conn Querier, organizationID string, chunk []string, limit int,
	relations map[relationKey]workitemmetrics.BlockingRelation,
) error {
	query := `
SELECT source_work_item_id, target_work_item_id, relationship_type, relationship_type_raw,
       relationship_semantics_version, last_synced, relation_started_at
FROM work_item_dependencies FINAL
WHERE ` + blockingRelationFilter
	args := []any{organizationID, workitemmetrics.CanonicalBlocksSemantics}
	if chunk != nil {
		query += namingFilter
		args = append(args, chunk, chunk)
	}
	query += `
ORDER BY source_work_item_id, target_work_item_id, relationship_type`
	if limit > 0 {
		query += `
LIMIT ?`
		args = append(args, limit+1)
	}
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("load blocking relations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			relation  workitemmetrics.BlockingRelation
			startedAt *time.Time
		)
		if err := rows.Scan(
			&relation.SourceID, &relation.TargetID, &relation.RelationshipType, &relation.Raw,
			&relation.SemanticsVersion, &relation.LastSynced, &startedAt,
		); err != nil {
			return fmt.Errorf("scan blocking relation: %w", err)
		}
		// last_synced and relation_started_at are DateTime64(3) with no time
		// zone; converting is the rule every reader of this table applies
		// (edges.ReadDependencies).
		relation.LastSynced = relation.LastSynced.UTC()
		if startedAt != nil {
			started := startedAt.UTC()
			relation.StartedAt = &started
		}
		key := relationKey{relation.SourceID, relation.TargetID, relation.RelationshipType}
		if existing, seen := relations[key]; seen && !relation.LastSynced.After(existing.LastSynced) {
			continue
		}
		relations[key] = relation
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate blocking relations: %w", err)
	}
	return nil
}

// readFirstSeen reads, per blocking relation of one chunk (of the whole
// organization when chunk is nil), the first time a sync wrote it:
// min(first_seen_at) of work_item_dependency_first_seen, which a materialized
// view keeps as min(work_item_dependencies.last_synced). The minimum is taken
// HERE, grouped by the key: the table is AggregatingMergeTree and holds one
// row per part until parts merge.
//
// It is a second read, merged in Go, and not a LEFT JOIN: a relation with no
// first-seen row must read as "not stored", and a join would give it the
// type's default time.
//
// The rows are those of the relations readRelations selects, by a subquery
// over the same filter. The first-seen table has no semantics version, so
// read alone it would also return the rows of legacy relations; with the
// subquery this read never returns more rows than the relation read, and
// needs no limit of its own.
func readFirstSeen(
	ctx context.Context, conn Querier, organizationID string, chunk []string,
	firstSeen map[relationKey]time.Time,
) error {
	subquery := `
      SELECT source_work_item_id, target_work_item_id, relationship_type
      FROM work_item_dependencies FINAL
      WHERE ` + blockingRelationFilter
	args := []any{organizationID, organizationID, workitemmetrics.CanonicalBlocksSemantics}
	if chunk != nil {
		subquery += namingFilter
		args = append(args, chunk, chunk)
	}
	query := `
SELECT source_work_item_id, target_work_item_id, relationship_type, min(first_seen_at)
FROM work_item_dependency_first_seen
WHERE org_id = ?
  AND (source_work_item_id, target_work_item_id, relationship_type) IN (` + subquery + `
  )
GROUP BY source_work_item_id, target_work_item_id, relationship_type
ORDER BY source_work_item_id, target_work_item_id, relationship_type`
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("load relation first-seen times: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			key  relationKey
			seen time.Time
		)
		if err := rows.Scan(&key.source, &key.target, &key.relationship, &seen); err != nil {
			return fmt.Errorf("scan relation first-seen time: %w", err)
		}
		seen = seen.UTC()
		if existing, stored := firstSeen[key]; stored && !seen.Before(existing) {
			continue
		}
		firstSeen[key] = seen
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate relation first-seen times: %w", err)
	}
	return nil
}

// endLookup splits the ends that relations name into plain
// work item ids and external issue keys, each sorted and without repeats, so
// the read that follows is a function of the relations alone.
func endLookup(relations []workitemmetrics.BlockingRelation) (ids, keys []string) {
	idSet, keySet := map[string]struct{}{}, map[string]struct{}{}
	for _, relation := range relations {
		for _, end := range []string{relation.SourceID, relation.TargetID} {
			if key, external := workitemmetrics.ExternalKey(end); external {
				keySet[key] = struct{}{}
				continue
			}
			if end != "" && !strings.HasPrefix(end, workitemmetrics.ExternalKeyPrefix) {
				idSet[end] = struct{}{}
			}
		}
	}
	for id := range idSet {
		ids = append(ids, id)
	}
	for key := range keySet {
		keys = append(keys, key)
	}
	sort.Strings(ids)
	sort.Strings(keys)
	return ids, keys
}

// endColumns is the select list of every read of relation ends.
const endColumns = `work_item_id, provider, status, created_at, completed_at, last_synced`

// bareIssueKeySQL is the bare issue key of a work_items row: its id after the
// provider prefix, trimmed and upper-cased, the form
// workitemmetrics.ExternalKey gives the other side of the match.
const bareIssueKeySQL = `upper(trimBoth(substring(work_item_id, position(work_item_id, ':') + 1)))`

// LoadEnds reads the stored work items that the organization's blocking
// relations name, in any repository of the organization: by work item id,
// and -- for an external-key end -- every jira or linear item whose bare
// issue key is the key. Which of those candidates a key resolves to, if any,
// is the rule's decision, not this read's.
//
// The relation ends are found by a SUBQUERY over the relations
// (blockingRelationFilter); they are not sent as a list. An organization has
// as many ends as its relations name, and a list of them in the statement
// text reaches the server's size limit at about 12,000 ids (querybound).
// This statement has the same size for 10 relations and for 100,000.
func LoadEnds(
	ctx context.Context, conn Querier, organizationID string,
) ([]workitemmetrics.RelationEnd, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidRequest
	}
	query := `
WITH relation_ends AS (
    SELECT arrayJoin([source_work_item_id, target_work_item_id]) AS end_id
    FROM work_item_dependencies FINAL
    WHERE ` + blockingRelationFilter + `
)
SELECT ` + endColumns + `
FROM work_items FINAL
WHERE org_id = ?
  AND (
    work_item_id IN (SELECT end_id FROM relation_ends)
    OR (provider IN ('jira', 'linear') AND ` + bareIssueKeySQL + ` IN (
        SELECT upper(trimBoth(substring(end_id, ?)))
        FROM relation_ends
        WHERE startsWith(end_id, ?)
    ))
  )
ORDER BY work_item_id, last_synced`
	ends := endSet{}
	if err := readEnds(ctx, conn, query, []any{
		organizationID, workitemmetrics.CanonicalBlocksSemantics, organizationID,
		len(workitemmetrics.ExternalKeyPrefix) + 1, workitemmetrics.ExternalKeyPrefix,
	}, ends); err != nil {
		return nil, err
	}
	return ends.sorted(), nil
}

// LoadEndsLimited reads the stored work items that the GIVEN relations name,
// and no more than limit of them: the form the sync-time deriver uses. Its
// relations are not all stored (a sync unit merges in the relations it has
// just normalized), so their ends cannot be found by a subquery; they are
// sent as lists, in chunks of at most maxArrayBytes, one list per statement.
// More rows than limit is ErrLimitExceeded.
func LoadEndsLimited(
	ctx context.Context, conn Querier, organizationID string, relations []workitemmetrics.BlockingRelation, limit int,
) ([]workitemmetrics.RelationEnd, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || limit < 1 {
		return nil, ErrInvalidRequest
	}
	ids, keys := endLookup(relations)
	ends := endSet{}
	read := func(predicate string, lookups []string) error {
		for _, chunk := range querybound.ChunkStringsByRenderedBytes(lookups, maxArrayBytes) {
			query := `
SELECT ` + endColumns + `
FROM work_items FINAL
WHERE org_id = ? AND ` + predicate + `
ORDER BY work_item_id, last_synced
LIMIT ?`
			if err := readEnds(ctx, conn, query, []any{organizationID, chunk, limit + 1}, ends); err != nil {
				return err
			}
			if len(ends) > limit {
				return fmt.Errorf("%w: more than %d relation ends", ErrLimitExceeded, limit)
			}
		}
		return nil
	}
	if err := read(`work_item_id IN ?`, ids); err != nil {
		return nil, err
	}
	if err := read(`provider IN ('jira', 'linear') AND `+bareIssueKeySQL+` IN ?`, keys); err != nil {
		return nil, err
	}
	return ends.sorted(), nil
}

// endIdentity is every column of a relation end as comparable values. Two
// statements can return one stored row (an item named by its id and by its
// key); the set keeps it once.
type endIdentity struct {
	workItemID, provider, status string
	createdAt, lastSynced        int64
	completed                    bool
	completedAt                  int64
}

type endSet map[endIdentity]workitemmetrics.RelationEnd

func (ends endSet) add(end workitemmetrics.RelationEnd) {
	identity := endIdentity{
		workItemID: end.WorkItemID, provider: end.Provider, status: end.Status,
		createdAt: end.CreatedAt.UnixNano(), lastSynced: end.LastSynced.UnixNano(),
	}
	if end.CompletedAt != nil {
		identity.completed, identity.completedAt = true, end.CompletedAt.UnixNano()
	}
	ends[identity] = end
}

// sorted returns the ends by work item id, then by sync time, then by the
// other columns, so the result does not depend on the order of the reads.
func (ends endSet) sorted() []workitemmetrics.RelationEnd {
	if len(ends) == 0 {
		return nil
	}
	identities := make([]endIdentity, 0, len(ends))
	for identity := range ends {
		identities = append(identities, identity)
	}
	sort.Slice(identities, func(left, right int) bool {
		a, b := identities[left], identities[right]
		switch {
		case a.workItemID != b.workItemID:
			return a.workItemID < b.workItemID
		case a.lastSynced != b.lastSynced:
			return a.lastSynced < b.lastSynced
		case a.provider != b.provider:
			return a.provider < b.provider
		case a.status != b.status:
			return a.status < b.status
		case a.createdAt != b.createdAt:
			return a.createdAt < b.createdAt
		case a.completed != b.completed:
			return !a.completed
		}
		return a.completedAt < b.completedAt
	})
	result := make([]workitemmetrics.RelationEnd, 0, len(identities))
	for _, identity := range identities {
		result = append(result, ends[identity])
	}
	return result
}

// readEnds runs one read of relation ends into ends.
func readEnds(ctx context.Context, conn Querier, query string, args []any, ends endSet) error {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("load relation ends: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			end         workitemmetrics.RelationEnd
			completedAt *time.Time
		)
		if err := rows.Scan(
			&end.WorkItemID, &end.Provider, &end.Status, &end.CreatedAt, &completedAt, &end.LastSynced,
		); err != nil {
			return fmt.Errorf("scan relation end: %w", err)
		}
		end.CreatedAt, end.LastSynced = end.CreatedAt.UTC(), end.LastSynced.UTC()
		if completedAt != nil {
			completed := completedAt.UTC()
			end.CompletedAt = &completed
		}
		ends.add(end)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate relation ends: %w", err)
	}
	return nil
}

// LoadBlockedIntervals returns, per blocked work item id, the spans in
// which the item has an open blocker: the organization's blocking relations,
// resolved against the stored items they name by the ONE rule both writers of
// work_item_state_durations_daily share.
func LoadBlockedIntervals(
	ctx context.Context, conn Querier, organizationID string,
) (map[string][]workitemmetrics.BlockedInterval, error) {
	relations, err := LoadRelations(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	if len(relations) == 0 {
		return nil, nil
	}
	ends, err := LoadEnds(ctx, conn, organizationID)
	if err != nil {
		return nil, err
	}
	return workitemmetrics.BlockedIntervalsByItem(relations, ends), nil
}
