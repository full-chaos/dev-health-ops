package edges

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// # RETIRING AN EDGE IS AN APPEND, NEVER A MUTATION
//
// work_graph_edges is ReplacingMergeTree(last_synced) keyed on
// (org_id, source_type, source_id, edge_type, target_type, target_id). An edge
// is retired by inserting a copy of its latest version with is_deleted = 1 and
// a last_synced strictly newer than every stored version of that identity.
// Every reader drops an identity whose latest version is a tombstone. This is
// the convention the operational_* tables already use (a plain is_deleted
// UInt8 beside the version column, filtered after the dedup), and it is why
// the build issues no ALTER TABLE ... DELETE against this table.
//
// # WHY THE VERSION IS BUMPED, NOT TAKEN FROM THE CLOCK
//
// A delete removed every stored version, so whatever the build wrote next won
// regardless of the clock. A tombstone only wins if it is newer than what is
// stored, and a re-created edge only beats its tombstone if it is newer again.
// versionAfter makes both hold even when the build clock is behind a stored
// last_synced, so a clock regression can neither keep a retired edge alive nor
// keep a re-created edge retired.

// EdgeVersion is the latest stored version of one edge identity, plus the
// newest last_synced across all of its stored versions.
type EdgeVersion struct {
	Latest        Row
	IsDeleted     bool
	MaxLastSynced time.Time
}

// edgeIdentity is the ReplacingMergeTree key minus org_id, which is constant
// within one EdgeVersions read.
type edgeIdentity struct {
	sourceType, sourceID, edgeType, targetType, targetID string
}

func identityOf(row Row) edgeIdentity {
	return edgeIdentity{row.SourceType, row.SourceID, row.EdgeType, row.TargetType, row.TargetID}
}

// EdgeVersions indexes one organization's stored edge versions by identity.
type EdgeVersions map[edgeIdentity]EdgeVersion

// versionAfter is the last_synced a write needs to supersede prior at
// DateTime64(3) precision: the clock when it is already newer, else one
// millisecond past prior.
func versionAfter(clock, prior time.Time) time.Time {
	floor := prior.UTC().Truncate(time.Millisecond).Add(time.Millisecond)
	if clock.UTC().Truncate(time.Millisecond).Before(floor) {
		return floor
	}
	return clock
}

// edgeVersionsSQL reads the latest version of every issue<->issue identity for
// an organization. One tupled argMax, so the winner is one physical row even
// when two versions tie on last_synced.
const edgeVersionsSQL = `
        SELECT
            source_type, source_id, edge_type, target_type, target_id,
            winner.1, winner.2, winner.3, winner.4, winner.5, winner.6,
            winner.7, winner.8, winner.9, winner.10, max_last_synced
        FROM (
            SELECT
                source_type, source_id, edge_type, target_type, target_id,
                argMax(tuple(edge_id, repo_id, provider, provenance, confidence, evidence,
                    discovered_at, event_ts, day, is_deleted), last_synced) AS winner,
                max(last_synced) AS max_last_synced
            FROM work_graph_edges
            WHERE org_id = {org_id:String}
              AND source_type = 'issue'
              AND target_type = 'issue'
              %s
            GROUP BY source_type, source_id, edge_type, target_type, target_id
        )
`

// stalePRDependencyIdentitySQL narrows the version read to the key-column half
// of the stale PR-dependency shape; isStalePRDependency applies the rest.
const stalePRDependencyIdentitySQL = `AND startsWith(target_id, 'linear:')
              AND (startsWith(source_id, 'ghpr:') OR startsWith(source_id, 'gitlab:'))`

// ReadIssueIssueEdgeVersions loads the latest version of every issue<->issue
// edge identity for an organization, tombstones included.
func ReadIssueIssueEdgeVersions(ctx context.Context, conn driver.Conn, organizationID string) (EdgeVersions, error) {
	return readEdgeVersions(ctx, conn, organizationID, "")
}

func readEdgeVersions(
	ctx context.Context, conn driver.Conn, organizationID, narrowing string,
) (EdgeVersions, error) {
	if err := requireEdgeScope(organizationID); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, fmt.Sprintf(edgeVersionsSQL, narrowing),
		clickhouse.Named("org_id", organizationID))
	if err != nil {
		return nil, fmt.Errorf("read work_graph_edges versions: %w", err)
	}
	defer rows.Close()

	versions := make(EdgeVersions)
	for rows.Next() {
		var (
			latest        Row
			repoID        *uuid.UUID
			provider      *string
			isDeleted     uint8
			maxLastSynced time.Time
		)
		if err := rows.Scan(
			&latest.SourceType, &latest.SourceID, &latest.EdgeType, &latest.TargetType, &latest.TargetID,
			&latest.EdgeID, &repoID, &provider, &latest.Provenance, &latest.Confidence, &latest.Evidence,
			&latest.DiscoveredAt, &latest.EventTs, &latest.Day, &isDeleted, &maxLastSynced,
		); err != nil {
			return nil, fmt.Errorf("scan work_graph_edges version: %w", err)
		}
		latest.OrgID = organizationID
		latest.RepoID = repoID
		latest.Provider = provider
		latest.LastSynced = maxLastSynced
		versions[identityOf(latest)] = EdgeVersion{
			Latest: latest, IsDeleted: isDeleted != 0, MaxLastSynced: maxLastSynced.UTC(),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate work_graph_edges versions: %w", err)
	}
	return versions, nil
}

// tombstoneOf is the row that retires version's identity.
func tombstoneOf(version EdgeVersion, clock time.Time) Row {
	row := version.Latest
	row.LastSynced = versionAfter(clock, version.MaxLastSynced)
	return row
}

func sortTombstones(rows []Row) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].EdgeID != rows[j].EdgeID {
			return rows[i].EdgeID < rows[j].EdgeID
		}
		a, b := identityOf(rows[i]), identityOf(rows[j])
		return strings.Join([]string{a.sourceType, a.sourceID, a.edgeType, a.targetType, a.targetID}, "\x00") <
			strings.Join([]string{b.sourceType, b.sourceID, b.edgeType, b.targetType, b.targetID}, "\x00")
	})
}

func candidateSet(plan CleanupPlan) map[string]struct{} {
	candidates := make(map[string]struct{}, len(plan.CandidateIDs))
	for _, id := range plan.CandidateIDs {
		candidates[id] = struct{}{}
	}
	return candidates
}

// PlanCleanupTombstones is the tombstone equivalent of deleting every plan
// candidate by edge_id: one tombstone per live identity whose latest edge_id is
// a candidate. An identity this run re-writes gets none -- the delete it
// replaces was immediately undone by that write, and StampRewrites makes the
// write supersede what is stored instead.
func PlanCleanupTombstones(plan CleanupPlan, versions EdgeVersions, rewrites []Row, clock time.Time) []Row {
	candidates := candidateSet(plan)
	rewritten := make(map[edgeIdentity]struct{}, len(rewrites))
	for _, row := range rewrites {
		rewritten[identityOf(row)] = struct{}{}
	}
	var tombstones []Row
	for identity, version := range versions {
		if version.IsDeleted {
			continue
		}
		if _, isCandidate := candidates[version.Latest.EdgeID]; !isCandidate {
			continue
		}
		if _, isRewritten := rewritten[identity]; isRewritten {
			continue
		}
		tombstones = append(tombstones, tombstoneOf(version, clock))
	}
	sortTombstones(tombstones)
	return tombstones
}

// StampRewrites returns rows with last_synced raised past every stored version
// for each identity a delete would have cleared first: a cleanup candidate, or
// an identity whose latest version is a tombstone. Every other row is returned
// unchanged, so it competes with stored versions exactly as before.
func StampRewrites(rows []Row, plan CleanupPlan, versions EdgeVersions) []Row {
	candidates := candidateSet(plan)
	stamped := make([]Row, len(rows))
	copy(stamped, rows)
	for index := range stamped {
		version, stored := versions[identityOf(stamped[index])]
		if !stored {
			continue
		}
		_, rowIsCandidate := candidates[stamped[index].EdgeID]
		_, storedIsCandidate := candidates[version.Latest.EdgeID]
		if !version.IsDeleted && !rowIsCandidate && !storedIsCandidate {
			continue
		}
		stamped[index].LastSynced = versionAfter(stamped[index].LastSynced, version.MaxLastSynced)
	}
	return stamped
}

// DeleteEdgesByID retires a CleanupPlan's candidates with tombstones.
func DeleteEdgesByID(
	ctx context.Context, conn driver.Conn, organizationID string,
	plan CleanupPlan, versions EdgeVersions, rewrites []Row, clock time.Time,
) error {
	if err := requireEdgeScope(organizationID); err != nil {
		return err
	}
	if _, err := WriteTombstones(ctx, conn, organizationID,
		PlanCleanupTombstones(plan, versions, rewrites, clock)); err != nil {
		return fmt.Errorf("retire blocker-family edges: %w", err)
	}
	return nil
}

// isStalePRDependency is the stale shape Python's cleanup deleted: a PR-sourced
// edge mislabelled source_type='issue' (source_id keeps its ghpr:/gitlab:
// prefix) pointing at a Linear issue through a linear_attachment tag. Judged on
// the latest version of the identity.
func isStalePRDependency(row Row) bool {
	return row.SourceType == NodeTypeIssue && row.TargetType == NodeTypeIssue &&
		row.Evidence == "linear_attachment" &&
		strings.HasPrefix(row.TargetID, "linear:") &&
		(strings.HasPrefix(row.SourceID, "ghpr:") || strings.HasPrefix(row.SourceID, "gitlab:"))
}

// PlanStalePRDependencyTombstones is one tombstone per live identity in the
// stale PR-dependency shape.
func PlanStalePRDependencyTombstones(versions EdgeVersions, clock time.Time) []Row {
	var tombstones []Row
	for _, version := range versions {
		if version.IsDeleted || !isStalePRDependency(version.Latest) {
			continue
		}
		tombstones = append(tombstones, tombstoneOf(version, clock))
	}
	sortTombstones(tombstones)
	return tombstones
}

// DeleteStalePRDependencyIssueEdges runs the stale-edge cleanup
// `_delete_stale_pr_dependency_issue_edges` used to run as the FIRST action
// inside Python's `build()`, before any other stage, as tombstones. Refuses an
// unscoped call rather than replicating Python's silent no-op on an empty
// org_id: an unscoped cleanup would target every tenant's stale rows at once,
// which is never what a per-org build request means.
func DeleteStalePRDependencyIssueEdges(
	ctx context.Context, conn driver.Conn, organizationID string, clock time.Time,
) error {
	versions, err := readEdgeVersions(ctx, conn, organizationID, stalePRDependencyIdentitySQL)
	if err != nil {
		return fmt.Errorf("read stale PR-dependency issue edges: %w", err)
	}
	if _, err := WriteTombstones(ctx, conn, organizationID,
		PlanStalePRDependencyTombstones(versions, clock)); err != nil {
		return fmt.Errorf("retire stale PR-dependency issue edges: %w", err)
	}
	return nil
}

// WriteTombstones inserts rows with is_deleted = 1. Confidence is copied from a
// stored version rather than minted, so it is not re-validated: a tombstone is
// never grouped.
func WriteTombstones(ctx context.Context, conn driver.Conn, organizationID string, rows []Row) (int, error) {
	if err := requireEdgeScope(organizationID); err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := conn.PrepareBatch(ctx,
		"INSERT INTO work_graph_edges ("+
			"edge_id, source_type, source_id, target_type, target_id, edge_type, "+
			"repo_id, provider, "+
			"provenance, confidence, evidence, discovered_at, last_synced, event_ts, day, org_id, is_deleted)")
	if err != nil {
		return 0, fmt.Errorf("prepare work_graph_edges tombstone batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(
			row.EdgeID, row.SourceType, row.SourceID, row.TargetType, row.TargetID,
			row.EdgeType, row.RepoID, row.Provider,
			row.Provenance, row.Confidence, row.Evidence,
			row.DiscoveredAt, row.LastSynced, row.EventTs, row.Day, organizationID, uint8(1),
		); err != nil {
			_ = batch.Abort()
			return 0, fmt.Errorf("append tombstone %s: %w", row.EdgeID, err)
		}
	}
	if err := batch.Send(); err != nil {
		return 0, fmt.Errorf("send work_graph_edges tombstone batch: %w", err)
	}
	return len(rows), nil
}
