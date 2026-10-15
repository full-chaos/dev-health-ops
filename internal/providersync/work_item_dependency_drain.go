package providersync

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemblockers"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// CHAOS-9706: drain of the blocking relations that the provider no longer
// reports.
//
// A link removed at the provider leaves its work_item_dependencies row behind
// (the table is insert-only; a re-sync replaces a row, it never removes one).
// The end rule (workitemmetrics.EndedBlockingRelations, the rule the blocked
// intervals use) says which rows are gone. This tool deletes the gone rows of
// the two blocking types that are older than a retention, and nothing else:
//
//   - a CURRENT relation, a relation the rule cannot decide (an end that is no
//     stored item, a row of another semantics version) and every row of
//     another relationship type stay. Blocking relations never drive team
//     attribution (teamRepoOwnershipInheritableRelationshipTypes), so no
//     attribution reader sees a deleted row.
//   - a removed row inside the retention stays. The retention is the time the
//     graph projector needs to retract the edge of a removed link and the
//     time the daily blocked-hours recompute still reads, and a removed row
//     is the only record of both.
//   - work_item_dependency_first_seen and work_item_relations_read are not
//     touched. A link that returns is a new row with a new last_synced, and
//     the delete names the exact version that was read, so a re-sync that
//     lands between the read and the delete survives.
//
// Before it deletes, the tool proves that every interval a reader can still
// see is unchanged: the blocked intervals of every item, clipped to the
// retention window, are the same with and without the rows it will delete.

const (
	// WorkItemDependencyDrainTable is the table the tool drains.
	WorkItemDependencyDrainTable = "work_item_dependencies"

	workItemDependencyDrainBackup = "work_item_dependencies_drain_bak"

	// WorkItemDependencyDrainMinRetainDays is the shortest retention accepted.
	WorkItemDependencyDrainMinRetainDays = 30
	// WorkItemDependencyDrainDefaultRetainDays is the retention used when none is given.
	WorkItemDependencyDrainDefaultRetainDays = 90

	workItemDependencyDrainDeleteVersionsPerMutation = 2000
	workItemDependencyDrainMutationTimeout           = 10 * time.Minute
)

// ErrWorkItemDependencyDrainRetention means the retention is below the floor.
var ErrWorkItemDependencyDrainRetention = errors.New("work item dependency drain: retention is below the floor")

// ErrWorkItemDependencyDrainNotPaused means a write was asked for without the statement that the workers are paused.
var ErrWorkItemDependencyDrainNotPaused = errors.New("work item dependency drain: a write needs the statement that the sync workers are paused")

// ErrWorkItemDependencyDrainWritesActive means the tables were written within the quiet window. Nothing is written.
var ErrWorkItemDependencyDrainWritesActive = errors.New("work item dependency drain: the dependency tables were written within the quiet window")

// ErrWorkItemDependencyDrainDifferential means a reader would see another interval after the drain. Nothing is deleted.
var ErrWorkItemDependencyDrainDifferential = errors.New("work item dependency drain: interval differential failed")

// ErrWorkItemDependencyDrainBackup means the rollback copy is missing or has the wrong row count. Nothing is deleted.
var ErrWorkItemDependencyDrainBackup = errors.New("work item dependency drain: backup table check failed")

// ErrWorkItemDependencyDrainMutation means the delete failed or left rows behind. The backup table stays.
var ErrWorkItemDependencyDrainMutation = errors.New("work item dependency drain: delete mutation failed")

// ErrWorkItemDependencyDrainReadback means the relations read after the delete are not the relations expected.
var ErrWorkItemDependencyDrainReadback = errors.New("work item dependency drain: readback after delete differs")

// ErrWorkItemDependencyDrainTable means the table is not the table the tool was written for.
var ErrWorkItemDependencyDrainTable = errors.New("work item dependency drain: table engine or sorting key is not the expected one")

// WorkItemDependencyDrainOptions drives DrainWorkItemDependencies.
type WorkItemDependencyDrainOptions struct {
	// Orgs is the organizations to drain; AllOrgs takes every organization that holds a blocking row. Exactly one.
	Orgs    []string
	AllOrgs bool
	// RetainDays keeps every removed row whose last_synced is newer than this many days (floor 30, default 90).
	RetainDays int
	// Apply writes. Without it the tool reads and reports only.
	Apply bool
	// WorkersPaused is the operator's statement that the sync workers that write the dependency rows are paused.
	WorkersPaused bool
	// QuietSeconds, above zero, refuses a write while a part of work_item_dependencies or work_items was modified
	// within that many seconds by the server clock. Zero turns the check off.
	QuietSeconds int
	// Now is the clock of the retention, the backup stamp and the run id (default time.Now).
	Now func() time.Time

	// afterBackup runs after the backup is written and before the delete; a test writes a row the way a sync does.
	afterBackup func(orgID string)
	// planned lets a test change the drain list after the plan, the way a defect in the plan would.
	planned func(orgID string, drain []workitemmetrics.BlockingRelation) []workitemmetrics.BlockingRelation
}

// WorkItemDependencyDrainOrg is the counts-only result for one organization.
type WorkItemDependencyDrainOrg struct {
	OrgOrdinal int `json:"org_ordinal"`
	// Relations is every blocking row of the canonical semantics version.
	Relations int `json:"relations"`
	Current   int `json:"current"`
	Undecided int `json:"undecided"`
	// Removed is the rows the end rule calls gone; Drainable is the part older than the retention.
	Removed         int `json:"removed"`
	Drainable       int `json:"drainable"`
	KeptInRetention int `json:"kept_in_retention"`
	// OldestDrainableDays and NewestDrainableDays are the age of the oldest and newest drainable row, in whole days.
	OldestDrainableDays int `json:"oldest_drainable_days"`
	NewestDrainableDays int `json:"newest_drainable_days"`
	// Differential: the items whose intervals were compared, and the number that differ (always 0 when it returns).
	DifferentialItems      int `json:"differential_items"`
	DifferentialMismatches int `json:"differential_mismatches"`
	BackupRows             int `json:"backup_rows"`
	// Deleted counts relations (keys); BackupRows counts the physical versions saved, which is more when a key held
	// older versions no merge had folded.
	Deleted                int `json:"deleted"`
	// SurvivedTheDelete is drainable versions that are still stored after the delete. Always 0 when it returns.
	SurvivedTheDelete int   `json:"survived_the_delete"`
	Restored          int   `json:"restored"`
	ElapsedMS         int64 `json:"elapsed_ms"`
}

// WorkItemDependencyDrainOutcome is the counts-only result of a run.
type WorkItemDependencyDrainOutcome struct {
	RunID      string                       `json:"run_id"`
	Apply      bool                         `json:"apply"`
	Restore    bool                         `json:"restore"`
	RetainDays int                          `json:"retain_days"`
	Orgs       []WorkItemDependencyDrainOrg `json:"orgs"`
	Totals     WorkItemDependencyDrainOrg   `json:"totals"`
}

func (org *WorkItemDependencyDrainOrg) add(other WorkItemDependencyDrainOrg) {
	org.Relations += other.Relations
	org.Current += other.Current
	org.Undecided += other.Undecided
	org.Removed += other.Removed
	org.Drainable += other.Drainable
	org.KeptInRetention += other.KeptInRetention
	org.DifferentialItems += other.DifferentialItems
	org.DifferentialMismatches += other.DifferentialMismatches
	org.BackupRows += other.BackupRows
	org.Deleted += other.Deleted
	org.SurvivedTheDelete += other.SurvivedTheDelete
	org.Restored += other.Restored
	org.ElapsedMS += other.ElapsedMS
	if other.OldestDrainableDays > org.OldestDrainableDays {
		org.OldestDrainableDays = other.OldestDrainableDays
	}
	if other.Drainable > 0 && (org.NewestDrainableDays == 0 || other.NewestDrainableDays < org.NewestDrainableDays) {
		org.NewestDrainableDays = other.NewestDrainableDays
	}
}

// dependencyVersion is one physical version of a relation: its key and the last_synced of the row.
type dependencyVersion struct {
	source, target, relationship string
	lastSynced                   time.Time
}

func (version dependencyVersion) token() string {
	return version.source + "\x00" + version.target + "\x00" + version.relationship
}

func normalizeDrainRetention(days int) (int, error) {
	if days == 0 {
		return WorkItemDependencyDrainDefaultRetainDays, nil
	}
	if days < WorkItemDependencyDrainMinRetainDays {
		return 0, ErrWorkItemDependencyDrainRetention
	}
	return days, nil
}

func verifyDependencyTableShape(ctx context.Context, conn driver.Conn) error {
	var engineFull, sortingKey string
	if err := conn.QueryRow(ctx, "SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = {table:String}",
		clickhouse.Named("table", WorkItemDependencyDrainTable)).Scan(&engineFull, &sortingKey); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainTable, err)
	}
	if !strings.HasPrefix(engineFull, "ReplacingMergeTree(last_synced)") ||
		sortingKey != "org_id, source_work_item_id, target_work_item_id, relationship_type" {
		return ErrWorkItemDependencyDrainTable
	}
	return nil
}

func requireQuietDependencies(ctx context.Context, conn driver.Conn, quietSeconds int) error {
	if quietSeconds <= 0 {
		return nil
	}
	// An insert makes a new part, so an active part modified within the window is a write (or the merge of one),
	// of any organization. Provider timestamps are not monotonic, so a row's last_synced cannot say when it was
	// written; the server's part time can.
	var parts uint64
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM system.parts WHERE database = currentDatabase() AND active AND table IN ('work_item_dependencies', 'work_items') AND modification_time > now() - toIntervalSecond(%d)", quietSeconds)).Scan(&parts); err != nil {
		return fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainWritesActive, err)
	}
	if parts != 0 {
		return fmt.Errorf("%w: %d parts", ErrWorkItemDependencyDrainWritesActive, parts)
	}
	return nil
}

func listDependencyDrainOrgs(ctx context.Context, conn driver.Conn) ([]string, error) {
	rows, err := conn.Query(ctx, "SELECT DISTINCT org_id FROM work_item_dependencies WHERE relationship_type IN ('blocks', 'blocked_by') ORDER BY org_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orgs []string
	for rows.Next() {
		var org string
		if err := rows.Scan(&org); err != nil {
			return nil, err
		}
		orgs = append(orgs, org)
	}
	return orgs, rows.Err()
}

func drainOrgList(ctx context.Context, conn driver.Conn, opts WorkItemDependencyDrainOptions) ([]string, error) {
	if opts.AllOrgs == (len(opts.Orgs) > 0) {
		return nil, fmt.Errorf("work item dependency drain: give organizations or all organizations, not both and not neither")
	}
	if !opts.AllOrgs {
		return opts.Orgs, nil
	}
	return listDependencyDrainOrgs(ctx, conn)
}

// planDependencyDrain reads one organization and splits its blocking relations.
func planDependencyDrain(
	ctx context.Context, conn driver.Conn, orgID string, cutoff, now time.Time,
) (report WorkItemDependencyDrainOrg, relations []workitemmetrics.BlockingRelation, ends []workitemmetrics.RelationEnd,
	drain []workitemmetrics.BlockingRelation, err error) {
	relations, err = workitemblockers.LoadRelations(ctx, conn, orgID)
	if err != nil {
		return report, nil, nil, nil, fmt.Errorf("work item dependency drain: read relations: %w", err)
	}
	report.Relations = len(relations)
	if len(relations) == 0 {
		return report, nil, nil, nil, nil
	}
	ends, err = workitemblockers.LoadEnds(ctx, conn, orgID)
	if err != nil {
		return report, nil, nil, nil, fmt.Errorf("work item dependency drain: read items: %w", err)
	}
	ended, current, undecided := workitemmetrics.EndedBlockingRelations(relations, ends)
	report.Current, report.Undecided, report.Removed = current, undecided, len(ended)
	for _, relation := range ended {
		if relation.LastSynced.UTC().Before(cutoff) {
			drain = append(drain, relation)
			age := int(now.Sub(relation.LastSynced.UTC()).Hours() / 24)
			if age > report.OldestDrainableDays {
				report.OldestDrainableDays = age
			}
			if report.NewestDrainableDays == 0 || age < report.NewestDrainableDays {
				report.NewestDrainableDays = age
			}
		}
	}
	report.Drainable = len(drain)
	report.KeptInRetention = report.Removed - report.Drainable
	return report, relations, ends, drain, nil
}

func relationKeyToken(relation workitemmetrics.BlockingRelation) string {
	return relation.SourceID + "\x00" + relation.TargetID + "\x00" + relation.RelationshipType
}

// clipIntervals keeps the part of every interval at or after the cutoff, as text, so two sets compare by value.
func clipIntervals(intervals map[string][]workitemmetrics.BlockedInterval, cutoff time.Time) map[string]string {
	out := map[string]string{}
	for item, list := range intervals {
		var parts []string
		for _, interval := range list {
			start := interval.Start.UTC()
			if interval.End != nil && !interval.End.UTC().After(cutoff) {
				continue
			}
			if start.Before(cutoff) {
				start = cutoff
			}
			end := "open"
			if interval.End != nil {
				end = interval.End.UTC().Format(time.RFC3339Nano)
			}
			parts = append(parts, start.Format(time.RFC3339Nano)+"/"+end)
		}
		if len(parts) > 0 {
			sort.Strings(parts)
			out[item] = strings.Join(parts, ",")
		}
	}
	return out
}

// dependencyDrainDifferential compares the blocked intervals with and without the rows that will be deleted,
// clipped to the retention window. A removed row older than the retention ended before the window starts, so it
// must add nothing inside it.
func dependencyDrainDifferential(
	relations []workitemmetrics.BlockingRelation, ends []workitemmetrics.RelationEnd,
	drain []workitemmetrics.BlockingRelation, cutoff time.Time,
) (items, mismatches int) {
	gone := make(map[string]bool, len(drain))
	for _, relation := range drain {
		gone[relationKeyToken(relation)] = true
	}
	kept := make([]workitemmetrics.BlockingRelation, 0, len(relations)-len(drain))
	for _, relation := range relations {
		if !gone[relationKeyToken(relation)] {
			kept = append(kept, relation)
		}
	}
	before := clipIntervals(workitemmetrics.BlockedIntervalsByItem(relations, ends), cutoff)
	after := clipIntervals(workitemmetrics.BlockedIntervalsByItem(kept, ends), cutoff)
	seen := map[string]bool{}
	for item, value := range before {
		seen[item] = true
		if after[item] != value {
			mismatches++
		}
	}
	for item := range after {
		if !seen[item] {
			mismatches++
		}
	}
	return len(seen), mismatches
}

func newDependencyDrainRunID(now time.Time) string {
	return "drain-" + strings.TrimPrefix(newCompactionRunID(now), "compact-")
}

// DrainWorkItemDependencies plans and, with Apply, runs the drain. It returns the report so far together with
// the first error: a failed run still tells the operator how far it got. It plans every organization first and
// writes only after every plan and differential finished.
func DrainWorkItemDependencies(ctx context.Context, conn driver.Conn, opts WorkItemDependencyDrainOptions) (WorkItemDependencyDrainOutcome, error) {
	if conn == nil {
		return WorkItemDependencyDrainOutcome{}, ErrInvalidConfiguration
	}
	retain, err := normalizeDrainRetention(opts.RetainDays)
	if err != nil {
		return WorkItemDependencyDrainOutcome{}, err
	}
	if opts.Apply && !opts.WorkersPaused {
		return WorkItemDependencyDrainOutcome{}, ErrWorkItemDependencyDrainNotPaused
	}
	clock := opts.Now
	if clock == nil {
		clock = time.Now
	}
	now := clock().UTC()
	cutoff := now.AddDate(0, 0, -retain)
	outcome := WorkItemDependencyDrainOutcome{Apply: opts.Apply, RetainDays: retain}
	if err := verifyDependencyTableShape(ctx, conn); err != nil {
		return outcome, err
	}
	orgs, err := drainOrgList(ctx, conn, opts)
	if err != nil {
		return outcome, err
	}
	type planned struct {
		orgID  string
		report WorkItemDependencyDrainOrg
		drain  []workitemmetrics.BlockingRelation
		before []workitemmetrics.BlockingRelation
	}
	var plans []planned
	for index, orgID := range orgs {
		started := time.Now()
		report, relations, ends, drain, err := planDependencyDrain(ctx, conn, orgID, cutoff, now)
		report.OrgOrdinal = index + 1
		if err != nil {
			outcome.Orgs = append(outcome.Orgs, report)
			return outcome, err
		}
		if opts.planned != nil {
			drain = opts.planned(orgID, drain)
		}
		report.DifferentialItems, report.DifferentialMismatches = dependencyDrainDifferential(relations, ends, drain, cutoff)
		report.ElapsedMS = time.Since(started).Milliseconds()
		outcome.Orgs = append(outcome.Orgs, report)
		outcome.Totals.add(report)
		if report.DifferentialMismatches != 0 {
			return outcome, ErrWorkItemDependencyDrainDifferential
		}
		plans = append(plans, planned{orgID: orgID, report: report, drain: drain, before: relations})
	}
	if !opts.Apply {
		logDependencyDrain(ctx, outcome)
		return outcome, nil
	}
	runID := newDependencyDrainRunID(now)
	outcome.RunID = runID
	outcome.Totals = WorkItemDependencyDrainOrg{}
	// The quiet check reads the server's part times of the whole table, so it runs ONCE, before the first write:
	// run per organization it would see the parts of the run's own delete and refuse the second organization after
	// the first was changed.
	anyToDrain := false
	for _, plan := range plans {
		anyToDrain = anyToDrain || len(plan.drain) > 0
	}
	if anyToDrain {
		if err := requireQuietDependencies(ctx, conn, opts.QuietSeconds); err != nil {
			return outcome, err
		}
	}
	for index, plan := range plans {
		if len(plan.drain) == 0 {
			outcome.Totals.add(plan.report)
			continue
		}
		started := time.Now()
		report, err := applyDependencyDrain(ctx, conn, plan.orgID, runID, now, plan.report, plan.before, plan.drain, opts)
		report.ElapsedMS += time.Since(started).Milliseconds()
		outcome.Orgs[index] = report
		outcome.Totals.add(report)
		if err != nil {
			return outcome, err
		}
	}
	logDependencyDrain(ctx, outcome)
	return outcome, nil
}

func logDependencyDrain(ctx context.Context, outcome WorkItemDependencyDrainOutcome) {
	slog.Default().InfoContext(ctx, "work_item_dependency_drain.done", "run_id", outcome.RunID, "apply", outcome.Apply,
		"retain_days", outcome.RetainDays, "orgs", len(outcome.Orgs), "relations", outcome.Totals.Relations,
		"removed", outcome.Totals.Removed, "drainable", outcome.Totals.Drainable, "deleted", outcome.Totals.Deleted)
}

func dependencyColumns(ctx context.Context, conn driver.Conn, table string) ([]string, error) {
	rows, err := conn.Query(ctx, "SELECT name FROM system.columns WHERE database = currentDatabase() AND table = {table:String} ORDER BY position",
		clickhouse.Named("table", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}
	return columns, rows.Err()
}

func dependencyVersionsArgs(orgID string, versions []dependencyVersion) []any {
	sources := make([]string, 0, len(versions))
	targets := make([]string, 0, len(versions))
	types := make([]string, 0, len(versions))
	stamps := make([]string, 0, len(versions))
	tokens := make([]string, 0, len(versions))
	for _, version := range versions {
		tokens = append(tokens, version.source+"\x1f"+version.target+"\x1f"+version.relationship)
		sources = append(sources, version.source)
		targets = append(targets, version.target)
		types = append(types, version.relationship)
		stamps = append(stamps, strconv.FormatInt(version.lastSynced.UnixMilli(), 10))
	}
	return []any{
		clickhouse.Named("org_id", orgID), clickhouse.Named("sources", sources), clickhouse.Named("targets", targets),
		clickhouse.Named("types", types), clickhouse.Named("tokens", tokens), clickhouse.Named("stamps", stamps),
	}
}

// dependencyVersionsMatch matches the physical rows of the keys read whose last_synced (milliseconds) is at or before
// the stamp read for the key. An older version that no merge has folded yet is part of the same relation: left
// behind, it would show under FINAL once the newest is gone. A row a sync writes after the read has a later stamp
// and survives.
const dependencyVersionsMatch = `org_id = {org_id:String} AND (source_work_item_id, target_work_item_id, relationship_type) IN (` +
	`SELECT z.1, z.2, z.3 FROM (SELECT arrayJoin(arrayZip({sources:Array(String)}, {targets:Array(String)}, {types:Array(String)})) AS z)) ` +
	`AND toUnixTimestamp64Milli(last_synced) <= mapFromArrays({tokens:Array(String)}, arrayMap(x -> toInt64(x), {stamps:Array(String)}))[concat(source_work_item_id, char(31), target_work_item_id, char(31), relationship_type)]`

func applyDependencyDrain(
	ctx context.Context, conn driver.Conn, orgID, runID string, now time.Time, report WorkItemDependencyDrainOrg,
	before []workitemmetrics.BlockingRelation,
	drain []workitemmetrics.BlockingRelation, opts WorkItemDependencyDrainOptions,
) (WorkItemDependencyDrainOrg, error) {
	versions := make([]dependencyVersion, 0, len(drain))
	for _, relation := range drain {
		versions = append(versions, dependencyVersion{
			source: relation.SourceID, target: relation.TargetID, relationship: relation.RelationshipType,
			lastSynced: relation.LastSynced.UTC(),
		})
	}
	sort.Slice(versions, func(a, b int) bool { return versions[a].token() < versions[b].token() })

	columns, err := dependencyColumns(ctx, conn, WorkItemDependencyDrainTable)
	if err != nil || len(columns) == 0 {
		return report, fmt.Errorf("%w: columns", ErrWorkItemDependencyDrainBackup)
	}
	if err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS "+workItemDependencyDrainBackup+
		" AS "+WorkItemDependencyDrainTable+" ENGINE = MergeTree ORDER BY (org_id, source_work_item_id, target_work_item_id, relationship_type, last_synced)"); err != nil {
		return report, fmt.Errorf("%w: create: %v", ErrWorkItemDependencyDrainBackup, err)
	}
	for _, column := range []string{"run_id String", "backed_up_at DateTime64(3, 'UTC')"} {
		if err := conn.Exec(ctx, "ALTER TABLE "+workItemDependencyDrainBackup+" ADD COLUMN IF NOT EXISTS "+column); err != nil {
			return report, fmt.Errorf("%w: column: %v", ErrWorkItemDependencyDrainBackup, err)
		}
	}
	list := strings.Join(columns, ", ")
	for start := 0; start < len(versions); start += workItemDependencyDrainDeleteVersionsPerMutation {
		chunk := versions[start:min(start+workItemDependencyDrainDeleteVersionsPerMutation, len(versions))]
		args := append(dependencyVersionsArgs(orgID, chunk), clickhouse.Named("run_id", runID), clickhouse.Named("stamp", now.Format("2006-01-02 15:04:05.000")))
		if err := conn.Exec(ctx, "INSERT INTO "+workItemDependencyDrainBackup+" ("+list+", run_id, backed_up_at) SELECT "+list+
			", {run_id:String}, toDateTime64({stamp:String}, 3, 'UTC') FROM "+WorkItemDependencyDrainTable+" WHERE "+dependencyVersionsMatch, args...); err != nil {
			return report, fmt.Errorf("%w: copy: %v", ErrWorkItemDependencyDrainBackup, err)
		}
	}
	var stored uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+workItemDependencyDrainBackup+" WHERE run_id = {run_id:String} AND org_id = {org_id:String}",
		clickhouse.Named("run_id", runID), clickhouse.Named("org_id", orgID)).Scan(&stored); err != nil {
		return report, fmt.Errorf("%w: count: %v", ErrWorkItemDependencyDrainBackup, err)
	}
	// Older unmerged versions of a key are copied too, so more rows than keys is normal. A sync that wrote a key
	// after the read may have merged the version read away, so fewer is allowed; none is not.
	if stored == 0 {
		return report, fmt.Errorf("%w: no row copied for %d keys", ErrWorkItemDependencyDrainBackup, len(versions))
	}
	report.BackupRows = int(stored)
	if opts.afterBackup != nil {
		opts.afterBackup(orgID)
	}

	for start := 0; start < len(versions); start += workItemDependencyDrainDeleteVersionsPerMutation {
		chunk := versions[start:min(start+workItemDependencyDrainDeleteVersionsPerMutation, len(versions))]
		args := dependencyVersionsArgs(orgID, chunk)
		mutationCtx, cancel := context.WithTimeout(ctx, workItemDependencyDrainMutationTimeout)
		syncCtx := clickhouse.Context(mutationCtx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": "2"}))
		err := conn.Exec(syncCtx, "ALTER TABLE "+WorkItemDependencyDrainTable+" DELETE WHERE "+dependencyVersionsMatch, args...)
		cancel()
		if err != nil {
			return report, fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainMutation, err)
		}
		var left uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+WorkItemDependencyDrainTable+" WHERE "+dependencyVersionsMatch, args...).Scan(&left); err != nil {
			return report, fmt.Errorf("%w: count remaining: %v", ErrWorkItemDependencyDrainMutation, err)
		}
		report.SurvivedTheDelete += int(left)
		if left != 0 {
			return report, fmt.Errorf("%w: %d rows remain", ErrWorkItemDependencyDrainMutation, left)
		}
		report.Deleted += len(chunk)
	}

	// Readback: the relations the readers still see are the relations before, less exactly the drained keys. A row
	// a sync wrote after the read is allowed to be newer; a row that is gone and was not drained is not.
	after, err := workitemblockers.LoadRelations(ctx, conn, orgID)
	if err != nil {
		return report, fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainReadback, err)
	}
	gone := make(map[string]bool, len(drain))
	for _, relation := range drain {
		gone[relationKeyToken(relation)] = true
	}
	got := make(map[string]time.Time, len(after))
	for _, relation := range after {
		got[relationKeyToken(relation)] = relation.LastSynced.UTC()
	}
	for _, relation := range before {
		key := relationKeyToken(relation)
		have, present := got[key]
		if gone[key] {
			// A drained key is back only when a sync wrote it again after the read: its stamp is then newer.
			if present && !have.After(relation.LastSynced.UTC()) {
				return report, fmt.Errorf("%w: a drained relation is still there", ErrWorkItemDependencyDrainReadback)
			}
			continue
		}
		if !present || have.Before(relation.LastSynced.UTC()) {
			return report, fmt.Errorf("%w: a kept relation is missing", ErrWorkItemDependencyDrainReadback)
		}
	}
	slog.Default().InfoContext(ctx, "work_item_dependency_drain.applied", "run_id", runID, "backup_rows", report.BackupRows, "deleted", report.Deleted)
	return report, nil
}

// RestoreWorkItemDependencyDrain puts the rows of an earlier run back from the backup table. A key that a sync has
// written since with a newer last_synced is left as the sync made it.
func RestoreWorkItemDependencyDrain(ctx context.Context, conn driver.Conn, runID string, opts WorkItemDependencyDrainOptions) (WorkItemDependencyDrainOutcome, error) {
	if conn == nil || strings.TrimSpace(runID) == "" {
		return WorkItemDependencyDrainOutcome{}, ErrInvalidConfiguration
	}
	if opts.AllOrgs == (len(opts.Orgs) > 0) {
		return WorkItemDependencyDrainOutcome{}, fmt.Errorf("work item dependency drain: give organizations or all organizations, not both and not neither")
	}
	if opts.Apply && !opts.WorkersPaused {
		return WorkItemDependencyDrainOutcome{}, ErrWorkItemDependencyDrainNotPaused
	}
	outcome := WorkItemDependencyDrainOutcome{RunID: runID, Apply: opts.Apply, Restore: true}
	var exists uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = {name:String}",
		clickhouse.Named("name", workItemDependencyDrainBackup)).Scan(&exists); err != nil || exists == 0 {
		return outcome, fmt.Errorf("%w: no backup table", ErrWorkItemDependencyDrainBackup)
	}
	orgs := opts.Orgs
	if opts.AllOrgs {
		rows, err := conn.Query(ctx, "SELECT DISTINCT org_id FROM "+workItemDependencyDrainBackup+" WHERE run_id = {run_id:String} ORDER BY org_id", clickhouse.Named("run_id", runID))
		if err != nil {
			return outcome, fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainBackup, err)
		}
		for rows.Next() {
			var org string
			if err := rows.Scan(&org); err != nil {
				rows.Close()
				return outcome, err
			}
			orgs = append(orgs, org)
		}
		rows.Close()
	}
	columns, err := dependencyColumns(ctx, conn, WorkItemDependencyDrainTable)
	if err != nil || len(columns) == 0 {
		return outcome, fmt.Errorf("%w: columns", ErrWorkItemDependencyDrainBackup)
	}
	list := strings.Join(columns, ", ")
	prefixed := "b." + strings.Join(columns, ", b.")
	// Once per run, before the first write: the check reads the part times of the whole table, so a later check
	// would see this run's own insert.
	quietChecked := false
	for index, orgID := range orgs {
		report := WorkItemDependencyDrainOrg{OrgOrdinal: index + 1}
		var saved, missing uint64
		args := []any{clickhouse.Named("run_id", runID), clickhouse.Named("org_id", orgID)}
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+workItemDependencyDrainBackup+" WHERE run_id = {run_id:String} AND org_id = {org_id:String}", args...).Scan(&saved); err != nil {
			return outcome, fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainBackup, err)
		}
		if saved == 0 {
			return outcome, fmt.Errorf("%w: no backup rows for this run id", ErrWorkItemDependencyDrainBackup)
		}
		report.BackupRows = int(saved)
		// Rows to put back: no row of the key now holds a last_synced at or after the saved one.
		missingSQL := "FROM " + workItemDependencyDrainBackup + " AS b LEFT JOIN (SELECT org_id, source_work_item_id, target_work_item_id, relationship_type, max(last_synced) AS newest " +
			"FROM " + WorkItemDependencyDrainTable + " WHERE org_id = {org_id:String} GROUP BY org_id, source_work_item_id, target_work_item_id, relationship_type) AS t " +
			"ON b.org_id = t.org_id AND b.source_work_item_id = t.source_work_item_id AND b.target_work_item_id = t.target_work_item_id AND b.relationship_type = t.relationship_type " +
			"WHERE b.run_id = {run_id:String} AND b.org_id = {org_id:String} AND t.newest < b.last_synced"
		if err := conn.QueryRow(ctx, "SELECT count() "+missingSQL, args...).Scan(&missing); err != nil {
			return outcome, fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainBackup, err)
		}
		report.Restored = int(missing)
		if opts.Apply && missing > 0 {
			if !quietChecked {
				if err := requireQuietDependencies(ctx, conn, opts.QuietSeconds); err != nil {
					return outcome, err
				}
				quietChecked = true
			}
			if err := conn.Exec(ctx, "INSERT INTO "+WorkItemDependencyDrainTable+" ("+list+") SELECT "+prefixed+" "+missingSQL, args...); err != nil {
				return outcome, fmt.Errorf("%w: %v", ErrWorkItemDependencyDrainBackup, err)
			}
		}
		outcome.Orgs = append(outcome.Orgs, report)
		outcome.Totals.add(report)
	}
	slog.Default().InfoContext(ctx, "work_item_dependency_drain.restore", "run_id", runID, "apply", opts.Apply, "restored", outcome.Totals.Restored)
	return outcome, nil
}
