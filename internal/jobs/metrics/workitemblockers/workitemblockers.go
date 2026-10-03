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
package workitemblockers

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

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

// LoadRelationsNaming reads the blocking relations that name one of itemIDs
// at EITHER end, and no more than limit of them: the keyed, bounded form the
// sync-time deriver uses for the items of one sync unit. Both ends are
// matched because the two relation forms put the blocked item on different
// sides ("blocks": the target; "blocked_by": the source). More rows than
// limit is ErrLimitExceeded, never a truncated answer. No item id reads
// nothing.
func LoadRelationsNaming(
	ctx context.Context, conn Querier, organizationID string, itemIDs []string, limit int,
) ([]workitemmetrics.BlockingRelation, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" || limit < 1 {
		return nil, ErrInvalidRequest
	}
	if len(itemIDs) == 0 {
		return nil, nil
	}
	return loadRelations(ctx, conn, organizationID, itemIDs, limit)
}

func loadRelations(
	ctx context.Context, conn Querier, organizationID string, naming []string, limit int,
) ([]workitemmetrics.BlockingRelation, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidRequest
	}
	query := `
SELECT source_work_item_id, target_work_item_id, relationship_type, relationship_semantics_version, last_synced
FROM work_item_dependencies FINAL
WHERE org_id = ?
  AND relationship_type IN ('blocks', 'blocked_by')
  AND relationship_semantics_version = ?`
	args := []any{organizationID, workitemmetrics.CanonicalBlocksSemantics}
	if len(naming) > 0 {
		query += `
  AND (has(?, source_work_item_id) OR has(?, target_work_item_id))`
		args = append(args, naming, naming)
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
		return nil, fmt.Errorf("load blocking relations: %w", err)
	}
	defer rows.Close()

	var relations []workitemmetrics.BlockingRelation
	for rows.Next() {
		if limit > 0 && len(relations) >= limit {
			return nil, fmt.Errorf("%w: more than %d blocking relations", ErrLimitExceeded, limit)
		}
		var relation workitemmetrics.BlockingRelation
		if err := rows.Scan(
			&relation.SourceID, &relation.TargetID, &relation.RelationshipType,
			&relation.SemanticsVersion, &relation.LastSynced,
		); err != nil {
			return nil, fmt.Errorf("scan blocking relation: %w", err)
		}
		// last_synced is DateTime64(3) with no time zone; converting is the
		// rule every reader of this table applies (edges.ReadDependencies).
		relation.LastSynced = relation.LastSynced.UTC()
		relations = append(relations, relation)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate blocking relations: %w", err)
	}
	return relations, nil
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

// LoadEnds reads the stored work items that the given
// blocking relations name, in any repository of the organization: by work
// item id, and -- for an external-key end -- every jira or linear item whose
// bare issue key (the id after its provider prefix, trimmed and upper-cased,
// the form workitemmetrics.ExternalKey produces) is one of the keys. Which of
// those candidates a key resolves to, if any, is the rule's decision, not
// this read's.
func LoadEnds(
	ctx context.Context, conn Querier, organizationID string, relations []workitemmetrics.BlockingRelation,
) ([]workitemmetrics.RelationEnd, error) {
	return loadEnds(ctx, conn, organizationID, relations, 0)
}

// LoadEndsLimited is LoadEnds bounded to limit rows, for the sync-time
// deriver. More rows than limit is ErrLimitExceeded.
func LoadEndsLimited(
	ctx context.Context, conn Querier, organizationID string, relations []workitemmetrics.BlockingRelation, limit int,
) ([]workitemmetrics.RelationEnd, error) {
	if limit < 1 {
		return nil, ErrInvalidRequest
	}
	return loadEnds(ctx, conn, organizationID, relations, limit)
}

func loadEnds(
	ctx context.Context, conn Querier, organizationID string, relations []workitemmetrics.BlockingRelation, limit int,
) ([]workitemmetrics.RelationEnd, error) {
	if conn == nil || strings.TrimSpace(organizationID) == "" {
		return nil, ErrInvalidRequest
	}
	ids, keys := endLookup(relations)
	if len(ids) == 0 && len(keys) == 0 {
		return nil, nil
	}
	// An empty list is not sent as `IN ()`: the predicate it belongs to is
	// left out instead.
	var predicates []string
	args := []any{organizationID}
	if len(ids) > 0 {
		predicates = append(predicates, "work_item_id IN ?")
		args = append(args, ids)
	}
	if len(keys) > 0 {
		predicates = append(predicates, "(provider IN ('jira', 'linear') AND upper(trimBoth(substring(work_item_id, position(work_item_id, ':') + 1))) IN ?)")
		args = append(args, keys)
	}
	query := `
SELECT work_item_id, provider, status, created_at, completed_at, last_synced
FROM work_items FINAL
WHERE org_id = ? AND (` + strings.Join(predicates, " OR ") + `)
ORDER BY work_item_id, last_synced`
	if limit > 0 {
		query += `
LIMIT ?`
		args = append(args, limit+1)
	}
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("load relation ends: %w", err)
	}
	defer rows.Close()

	var ends []workitemmetrics.RelationEnd
	for rows.Next() {
		if limit > 0 && len(ends) >= limit {
			return nil, fmt.Errorf("%w: more than %d relation ends", ErrLimitExceeded, limit)
		}
		var (
			end         workitemmetrics.RelationEnd
			completedAt *time.Time
		)
		if err := rows.Scan(
			&end.WorkItemID, &end.Provider, &end.Status, &end.CreatedAt, &completedAt, &end.LastSynced,
		); err != nil {
			return nil, fmt.Errorf("scan relation end: %w", err)
		}
		end.CreatedAt, end.LastSynced = end.CreatedAt.UTC(), end.LastSynced.UTC()
		if completedAt != nil {
			completed := completedAt.UTC()
			end.CompletedAt = &completed
		}
		ends = append(ends, end)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate relation ends: %w", err)
	}
	return ends, nil
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
	ends, err := LoadEnds(ctx, conn, organizationID, relations)
	if err != nil {
		return nil, err
	}
	return workitemmetrics.BlockedIntervalsByItem(relations, ends), nil
}
