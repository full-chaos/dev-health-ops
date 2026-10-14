package providersync

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// CHAOS-9556: one-time compaction of the closed ownership intervals that the
// old per-sync valid_from writer left behind.
//
// The old writer closed the open row of an UNCHANGED fact at every sync and
// opened a new one, so a key holds a run of adjacent closed intervals with one
// shape. This file merges each such run into one interval (first valid_from,
// last valid_to) and drops the supersede tombstones that cover no instant. The
// ownership TIMELINE stays exact: for every key and every instant T, the rows
// valid at T (valid_from <= T < valid_to, or open) have the same shape before
// and after. The readers ask exactly that (internal/teamattribution
// LoadRepos/LoadProjects, internal/teamownership), and the tool proves it
// before it deletes anything.
//
// How a run is rewritten (no mutation for the merge):
//
//	phase A, ONE insert block per batch: the first row of a run again under its
//	  own sort key with valid_to = the last valid_to, and every other row of the
//	  run again under its own sort key as a tombstone (valid_to = valid_from).
//	  Readers read FINAL and filter valid_to > T, so the merged row appears and
//	  the inner rows vanish in one step: no gap and no overlap window.
//	phase B, ALTER TABLE ... DELETE of the tombstones. It removes rows that no
//	  reader sees, so it cannot change an answer. It runs only after the rows
//	  of phase A are read back and the differential holds.
//
// updated_at never becomes now(): the work-item attribution scope check reads
// max(updated_at, valid_*) per repository/project, and a fresh stamp would
// queue every compacted repository for a recompute. A rewritten row takes the
// newest updated_at of its run (the first row: at least its own + 1 ms).

const (
	// OwnershipCompactionProjectTable and OwnershipCompactionRepoTable are the
	// two tables the tool compacts.
	OwnershipCompactionProjectTable = "team_project_ownership"
	OwnershipCompactionRepoTable    = "team_repo_ownership"

	ownershipCompactionBackupSuffix = "_compaction_bak"

	// OwnershipCompactionDefaultBatchSize is the number of runs one insert
	// block carries.
	OwnershipCompactionDefaultBatchSize = 2000

	ownershipCompactionDeleteKeysPerMutation = 5000
	ownershipCompactionMutationTimeout       = 10 * time.Minute
)

// ErrOwnershipCompactionDifferential means the owner-at-T answers would differ
// after the compaction. Nothing is deleted when it is returned.
var ErrOwnershipCompactionDifferential = errors.New("ownership compaction: owner-at-T differential failed")

// ErrOwnershipCompactionBackup means the rollback copy is missing or has the
// wrong row count. Nothing is rewritten when it is returned.
var ErrOwnershipCompactionBackup = errors.New("ownership compaction: backup table check failed")

// ErrOwnershipCompactionReadback means rows read back after the insert are not
// the rows that were written. Nothing is deleted when it is returned.
var ErrOwnershipCompactionReadback = errors.New("ownership compaction: readback after insert differs")

// ErrOwnershipCompactionMutation means the delete mutation failed, timed out or
// left rows behind. The backup table stays.
var ErrOwnershipCompactionMutation = errors.New("ownership compaction: delete mutation failed")

type ownershipCompactionSpec struct {
	table   string
	source  string
	natural string
	repo    bool
}

var ownershipCompactionSpecs = []ownershipCompactionSpec{
	{table: OwnershipCompactionProjectTable, source: "native", natural: "project_id"},
	{table: OwnershipCompactionRepoTable, source: "inferred", natural: "repo_full_name", repo: true},
}

// OwnershipCompactionOptions drives CompactOwnershipIntervals.
type OwnershipCompactionOptions struct {
	// Orgs is the organizations to compact; AllOrgs takes every organization
	// that holds a row of the compacted source. Exactly one of the two.
	Orgs    []string
	AllOrgs bool
	// Table limits the run to one table ("" = both).
	Table string
	// BatchSize is the number of runs per insert block (default 2000).
	BatchSize int
	// Apply writes. Without it the tool reads and reports only.
	Apply bool
	// Now is the clock of the backup stamp and the run id (default time.Now).
	Now func() time.Time

	// beforeBatch runs after the plan is read and before the batch is
	// re-checked; a test uses it to write a row the way a concurrent sync does.
	beforeBatch func(table string, batch int)
	// afterInsert runs after the last insert and before the readback.
	afterInsert func(table string)
}

// OwnershipCompactionDifferential is the machine-readable owner-at-T result.
type OwnershipCompactionDifferential struct {
	GroupsTotal     int `json:"groups_total"`
	GroupsChecked   int `json:"groups_checked"`
	InstantsChecked int `json:"instants_checked"`
	Mismatches      int `json:"mismatches"`
}

// OwnershipCompactionOrg is the report of one table in one organization. The
// same struct is the per-table total (OrgOrdinal 0).
type OwnershipCompactionOrg struct {
	OrgOrdinal int `json:"org_ordinal"`

	RowsBefore             int `json:"rows_before"`
	OpenBefore             int `json:"open_before"`
	ClosedBefore           int `json:"closed_before"`
	TombstonesBefore       int `json:"tombstones_before"`
	CloseBeforeOpenBefore  int `json:"close_before_open_before"`
	RowsAfter              int `json:"rows_after"`
	OpenAfter              int `json:"open_after"`
	ClosedAfter            int `json:"closed_after"`
	TombstonesAfter        int `json:"tombstones_after"`
	CloseBeforeOpenAfter   int `json:"close_before_open_after"`
	RunsFound              int `json:"runs_found"`
	RunsMerged             int `json:"runs_merged"`
	RowsMerged             int `json:"rows_merged"`
	RowsKept               int `json:"rows_kept"`
	RunsKeptMultiSource    int `json:"runs_kept_multi_source"`
	RunsChangedDuringRun   int `json:"runs_changed_during_run"`
	Gaps                   int `json:"gaps"`
	Overlaps               int `json:"overlaps"`
	TombstonesFound        int `json:"tombstones_found"`
	TombstonesDropped      int `json:"tombstones_dropped"`
	TombstonesKeptAnchor   int `json:"tombstones_kept_anchor"`
	TombstonesKeptLatest   int `json:"tombstones_kept_latest"`
	TombstonesKeptStamp    int `json:"tombstones_kept_stamp"`
	BackupRows             int `json:"backup_rows"`
	RowsAppearedDuringRun  int `json:"rows_appeared_during_run"`
	GroupsDriftedDuringRun int `json:"groups_drifted_during_run"`
	RowsNewerThanTheDelete int `json:"rows_newer_than_the_delete"`
	RowsChangedSinceTheRun int `json:"rows_changed_since_the_run"`

	Differential OwnershipCompactionDifferential `json:"differential"`
	ElapsedMS    int64                           `json:"elapsed_ms"`
}

// OwnershipCompactionTable is the report of one table.
type OwnershipCompactionTable struct {
	Table  string                   `json:"table"`
	Source string                   `json:"source"`
	Orgs   []OwnershipCompactionOrg `json:"orgs"`
	Totals OwnershipCompactionOrg   `json:"totals"`
}

// OwnershipCompactionOutcome is the report of a run. It holds counts only: no
// organization id, team id or repository name.
type OwnershipCompactionOutcome struct {
	RunID   string                     `json:"run_id"`
	Apply   bool                       `json:"apply"`
	Restore bool                       `json:"restore"`
	Tables  []OwnershipCompactionTable `json:"tables"`
}

// ownershipIntervalRow is one FINAL row of an ownership table.
type ownershipIntervalRow struct {
	Provider, Natural, TeamID, Source string
	ProjectKey                        *string
	RepoID                            *uuid.UUID
	MatchType                         string
	IsPrimary                         uint8
	Specificity                       uint16
	Priority                          int32
	Share                             *float64
	ValidFrom                         time.Time
	ValidTo                           *time.Time
	UpdatedAt                         time.Time
	// LastSynced is read from team_project_ownership only, to keep it in the backup. The rewritten row
	// does not carry it: the server stamps it (ruled: the content changed, so the fresh stamp stays).
	LastSynced time.Time
}

// shape is every column of the row besides its key (provider, natural, team,
// source), its valid_from, its valid_to and its version updated_at. Nothing
// else is ignored: a column that is not named here would be a reason to not
// merge, never a reason to merge.
func (row ownershipIntervalRow) shape() string {
	var out strings.Builder
	if row.ProjectKey == nil {
		out.WriteString("\x00null")
	} else {
		out.WriteString("\x01" + *row.ProjectKey)
	}
	if row.RepoID == nil {
		out.WriteString("\x00null")
	} else {
		out.WriteString("\x01" + row.RepoID.String())
	}
	out.WriteString("\x00" + row.MatchType)
	out.WriteString("\x00" + strconv.Itoa(int(row.IsPrimary)))
	out.WriteString("\x00" + strconv.Itoa(int(row.Specificity)))
	out.WriteString("\x00" + strconv.Itoa(int(row.Priority)))
	if row.Share == nil {
		out.WriteString("\x00null")
	} else {
		out.WriteString("\x01" + strconv.FormatFloat(*row.Share, 'g', -1, 64))
	}
	return out.String()
}

func (row ownershipIntervalRow) keyOf() string {
	return row.Provider + "\x00" + row.Natural + "\x00" + row.TeamID + "\x00" + row.Source
}

func (row ownershipIntervalRow) groupOf() string { return row.Provider + "\x00" + row.Natural }

func (row ownershipIntervalRow) sortKey() string {
	return row.keyOf() + "\x00" + strconv.FormatInt(row.ValidFrom.UnixMilli(), 10)
}

func (row ownershipIntervalRow) isOpen() bool { return row.ValidTo == nil }

func (row ownershipIntervalRow) isTombstone() bool {
	return row.ValidTo != nil && row.ValidTo.Equal(row.ValidFrom)
}

func (row ownershipIntervalRow) isCloseBeforeOpen() bool {
	return row.ValidTo != nil && row.ValidTo.Before(row.ValidFrom)
}

func (row ownershipIntervalRow) validAt(at time.Time) bool {
	return !row.ValidFrom.After(at) && (row.ValidTo == nil || row.ValidTo.After(at))
}

// token is the key of the row for a server-side match. Hex needs no escaping
// for any byte of any id (the same device as internal/queryapi/chpairliteral).
func (row ownershipIntervalRow) token() string {
	return hex.EncodeToString([]byte(row.Provider)) + ":" + hex.EncodeToString([]byte(row.Natural)) + ":" +
		hex.EncodeToString([]byte(row.TeamID)) + ":" + hex.EncodeToString([]byte(row.Source)) + ":" +
		strconv.FormatInt(row.ValidFrom.UnixMilli(), 10)
}

func sameInstant(a, b *time.Time) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Equal(*b)
}

func (spec ownershipCompactionSpec) columns() string {
	if spec.repo {
		return "org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, valid_to, updated_at, share"
	}
	return "org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, valid_to, updated_at"
}

func (spec ownershipCompactionSpec) appendArgs(orgID string, row ownershipIntervalRow) []any {
	if spec.repo {
		return []any{orgID, row.Provider, row.TeamID, row.RepoID, row.Natural, row.MatchType, row.Source,
			row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt, row.Share}
	}
	return []any{orgID, row.Provider, row.TeamID, row.Natural, row.ProjectKey, row.Source,
		row.IsPrimary, row.Specificity, row.Priority, row.ValidFrom, row.ValidTo, row.UpdatedAt}
}

func (spec ownershipCompactionSpec) backupTable() string {
	return spec.table + ownershipCompactionBackupSuffix
}

func (spec ownershipCompactionSpec) backupDDL() string {
	if spec.repo {
		return "CREATE TABLE IF NOT EXISTS " + spec.backupTable() + " (" +
			"run_id String, backed_up_at DateTime64(3, 'UTC'), org_id String, provider String, team_id String, " +
			"repo_id Nullable(UUID), repo_full_name String, match_type String, source String, is_primary UInt8, " +
			"specificity UInt16, priority Int32, valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')), " +
			"updated_at DateTime64(3, 'UTC'), share Nullable(Float64)) " +
			"ENGINE = MergeTree ORDER BY (run_id, org_id, provider, repo_full_name, team_id, source, valid_from)"
	}
	return "CREATE TABLE IF NOT EXISTS " + spec.backupTable() + " (" +
		"run_id String, backed_up_at DateTime64(3, 'UTC'), org_id String, provider String, team_id String, " +
		"project_id String, project_key Nullable(String), source String, is_primary UInt8, " +
		"specificity UInt16, priority Int32, valid_from DateTime64(3, 'UTC'), valid_to Nullable(DateTime64(3, 'UTC')), " +
		"updated_at DateTime64(3, 'UTC'), last_synced DateTime64(3, 'UTC')) " +
		"ENGINE = MergeTree ORDER BY (run_id, org_id, provider, project_id, team_id, source, valid_from)"
}

func (spec ownershipCompactionSpec) backupInsert() string {
	cols := strings.Replace(spec.columns(), "org_id,", "run_id, backed_up_at, org_id,", 1)
	if !spec.repo {
		cols += ", last_synced"
	}
	return "INSERT INTO " + spec.backupTable() + " (" + cols + ")"
}

func (spec ownershipCompactionSpec) selectColumns() string {
	if spec.repo {
		return "provider, repo_full_name, team_id, toString(source), repo_id, toString(match_type), is_primary, specificity, priority, share, valid_from, valid_to, updated_at"
	}
	return "provider, project_id, team_id, toString(source), project_key, is_primary, specificity, priority, valid_from, valid_to, updated_at, last_synced"
}

func (spec ownershipCompactionSpec) scan(rows driver.Rows) (ownershipIntervalRow, error) {
	var row ownershipIntervalRow
	var err error
	if spec.repo {
		err = rows.Scan(&row.Provider, &row.Natural, &row.TeamID, &row.Source, &row.RepoID, &row.MatchType,
			&row.IsPrimary, &row.Specificity, &row.Priority, &row.Share, &row.ValidFrom, &row.ValidTo, &row.UpdatedAt)
	} else {
		err = rows.Scan(&row.Provider, &row.Natural, &row.TeamID, &row.Source, &row.ProjectKey,
			&row.IsPrimary, &row.Specificity, &row.Priority, &row.ValidFrom, &row.ValidTo, &row.UpdatedAt, &row.LastSynced)
	}
	if err != nil {
		return row, err
	}
	row.ValidFrom = row.ValidFrom.UTC()
	row.UpdatedAt = row.UpdatedAt.UTC()
	if row.ValidTo != nil {
		to := row.ValidTo.UTC()
		row.ValidTo = &to
	}
	return row, nil
}

// readOwnershipRows reads every FINAL row of one organization, of every source:
// the rows of the compacted source are planned, the rows of the other sources
// are only read, for the winner check.
func readOwnershipRows(ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec, orgID string) ([]ownershipIntervalRow, error) {
	rows, err := conn.Query(ctx, "SELECT "+spec.selectColumns()+" FROM "+spec.table+" FINAL WHERE org_id = {org_id:String}",
		clickhouse.Named("org_id", orgID))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ownershipIntervalRow
	for rows.Next() {
		row, err := spec.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// compactionRun is a run of adjacent closed rows of one key and one shape.
type compactionRun struct {
	rows   []ownershipIntervalRow
	merged ownershipIntervalRow
}

func (run compactionRun) inner() []ownershipIntervalRow { return run.rows[1:] }

// tombstoneOf is the row again under its own sort key as a tombstone.
func tombstoneOf(row ownershipIntervalRow) ownershipIntervalRow {
	out := row
	from := row.ValidFrom
	out.ValidTo = &from
	out.UpdatedAt = row.UpdatedAt.Add(time.Millisecond)
	return out
}

// mergedRow is the first row of a run with the valid_to of the last.
func mergedRow(rows []ownershipIntervalRow) ownershipIntervalRow {
	out := rows[0]
	newest := rows[0].UpdatedAt
	for _, row := range rows {
		if row.UpdatedAt.After(newest) {
			newest = row.UpdatedAt
		}
	}
	if !newest.After(rows[0].UpdatedAt) {
		newest = rows[0].UpdatedAt.Add(time.Millisecond)
	}
	to := *rows[len(rows)-1].ValidTo
	out.ValidTo = &to
	out.UpdatedAt = newest
	return out
}

type ownershipCompactionPlan struct {
	runs      []compactionRun
	drops     []ownershipIntervalRow
	org       OwnershipCompactionOrg
	byGroup   map[string][]ownershipIntervalRow
	compacted ownershipCompactionSpec
}

// planOwnershipCompaction finds the runs and the droppable tombstones of the
// compacted source. It reads rows and writes nothing.
func planOwnershipCompaction(spec ownershipCompactionSpec, rows []ownershipIntervalRow) ownershipCompactionPlan {
	plan := ownershipCompactionPlan{byGroup: map[string][]ownershipIntervalRow{}, compacted: spec}
	byKey := map[string][]ownershipIntervalRow{}
	for _, row := range rows {
		plan.byGroup[row.groupOf()] = append(plan.byGroup[row.groupOf()], row)
		if row.Source != spec.source {
			continue
		}
		byKey[row.keyOf()] = append(byKey[row.keyOf()], row)
		plan.org.RowsBefore++
		switch {
		case row.isOpen():
			plan.org.OpenBefore++
		case row.isTombstone():
			plan.org.TombstonesBefore++
		case row.isCloseBeforeOpen():
			plan.org.CloseBeforeOpenBefore++
		default:
			plan.org.ClosedBefore++
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		keyRows := byKey[key]
		sort.Slice(keyRows, func(i, j int) bool { return keyRows[i].ValidFrom.Before(keyRows[j].ValidFrom) })
		plan.planKey(keyRows)
	}
	return plan
}

func (plan *ownershipCompactionPlan) planKey(keyRows []ownershipIntervalRow) {
	var live []ownershipIntervalRow
	var tombstones []ownershipIntervalRow
	var minLive, maxEnd *time.Time
	var maxLiveStamp time.Time
	hasOpen := false
	for _, row := range keyRows {
		if row.isTombstone() {
			tombstones = append(tombstones, row)
			continue
		}
		if minLive == nil {
			from := row.ValidFrom
			minLive = &from
		}
		if row.UpdatedAt.After(maxLiveStamp) {
			maxLiveStamp = row.UpdatedAt
		}
		if row.isOpen() {
			hasOpen = true
		} else if maxEnd == nil || row.ValidTo.After(*maxEnd) {
			end := *row.ValidTo
			maxEnd = &end
		}
		if row.isCloseBeforeOpen() {
			continue
		}
		live = append(live, row)
	}
	// A tombstone covers no instant, and is dropped unless a reader still
	// keys on it:
	//   - it is the earliest row of its key: team_id_carry reads
	//     min(valid_from) of a key as the "since when" of the fact, and the
	//     acr edge reads it as first_valid_from;
	//   - it is the LATEST row of its key by the acr ownershipFactOrder (no
	//     open row, and no other row ends strictly after it): the edge takes
	//     latest_valid_to and the latest row's columns from it, so dropping it
	//     would change both;
	//   - it holds the key's newest updated_at (more than 1 ms above every other
	//     row; the 1 ms is the stamp a half-finished run of this tool leaves on
	//     its own tombstones): the acr edge pages on max(updated_at) of the key
	//     and the work-item scope check reads it, so a key whose maximum FELL
	//     could slip behind a cursor that has not read it yet. A merged row
	//     takes the newest updated_at of its run, so merging never lowers it.
	//     DO NOT WEAKEN THIS CLAUSE: a falling max(updated_at) is SILENT for
	//     the acr edges (the group is simply not read again, nothing fails), so
	//     this clause and TestCompactOwnershipIntervalsNeverLowersAKeysNewestStamp
	//     are the only protection a consumer has.
	for _, tombstone := range tombstones {
		plan.org.TombstonesFound++
		switch {
		case minLive == nil || !minLive.Before(tombstone.ValidFrom):
			plan.org.TombstonesKeptAnchor++
		case !hasOpen && (maxEnd == nil || !maxEnd.After(tombstone.ValidFrom)):
			plan.org.TombstonesKeptLatest++
		case tombstone.UpdatedAt.After(maxLiveStamp.Add(time.Millisecond)):
			plan.org.TombstonesKeptStamp++
		default:
			plan.drops = append(plan.drops, tombstone)
			plan.org.TombstonesDropped++
		}
	}
	var run []ownershipIntervalRow
	flush := func() {
		if len(run) >= 2 {
			plan.consider(run)
		}
		run = nil
	}
	for index, row := range live {
		if index > 0 {
			previous := live[index-1]
			switch {
			case previous.ValidTo == nil || row.ValidFrom.Before(*previous.ValidTo):
				plan.org.Overlaps++
			case row.ValidFrom.After(*previous.ValidTo):
				plan.org.Gaps++
			}
		}
		if row.isOpen() {
			flush()
			continue
		}
		if len(run) > 0 {
			last := run[len(run)-1]
			if last.ValidTo.Equal(row.ValidFrom) && last.shape() == row.shape() {
				run = append(run, row)
				continue
			}
			flush()
		}
		run = append(run, row)
	}
	flush()
}

// consider accepts a run when the winner of its (provider, natural, team)
// group is the same at every instant of the run before and after the merge.
// The winner is what the cascade reads: among the rows valid at T of every
// source, the newest by (updated_at, valid_from). Two sources on one key can
// disagree on it; the run then stays as it is.
func (plan *ownershipCompactionPlan) consider(run []ownershipIntervalRow) {
	plan.org.RunsFound++
	merged := mergedRow(run)
	first := run[0]
	groupRows := plan.byGroup[first.groupOf()]
	var pre, post []ownershipIntervalRow
	inRun := map[string]bool{}
	for _, row := range run {
		inRun[row.sortKey()] = true
	}
	for _, row := range groupRows {
		if row.TeamID != first.TeamID {
			continue
		}
		pre = append(pre, row)
		if inRun[row.sortKey()] {
			continue
		}
		post = append(post, row)
	}
	post = append(post, merged)
	from, to := first.ValidFrom, *run[len(run)-1].ValidTo
	if _, mismatches := compareOwnerAnswers(pre, post, &from, &to); mismatches > 0 {
		plan.org.RunsKeptMultiSource++
		return
	}
	plan.runs = append(plan.runs, compactionRun{rows: run, merged: merged})
	plan.org.RunsMerged++
	plan.org.RowsMerged += len(run) - 1
}

// final is the state the rows end in: merged rows in, inner rows and dropped
// tombstones out.
func (plan ownershipCompactionPlan) final(rows []ownershipIntervalRow) []ownershipIntervalRow {
	replaced := map[string]*ownershipIntervalRow{}
	removed := map[string]bool{}
	for index := range plan.runs {
		run := plan.runs[index]
		replaced[run.rows[0].sortKey()] = &run.merged
		for _, row := range run.inner() {
			removed[row.sortKey()] = true
		}
	}
	for _, row := range plan.drops {
		removed[row.sortKey()] = true
	}
	out := make([]ownershipIntervalRow, 0, len(rows))
	for _, row := range rows {
		key := row.sortKey()
		if removed[key] {
			continue
		}
		if merged, ok := replaced[key]; ok {
			out = append(out, *merged)
			continue
		}
		out = append(out, row)
	}
	return out
}

// answerAt is what a reader that asks "who owns this at T" sees for one group:
// the multiset of (team, source, shape) valid at T, and per team the winner.
func answerAt(rows []ownershipIntervalRow, at time.Time) string {
	var valid []string
	winners := map[string]ownershipIntervalRow{}
	for _, row := range rows {
		if !row.validAt(at) {
			continue
		}
		valid = append(valid, row.TeamID+"\x01"+row.Source+"\x01"+row.shape())
		best, ok := winners[row.TeamID]
		if !ok || row.UpdatedAt.After(best.UpdatedAt) || (row.UpdatedAt.Equal(best.UpdatedAt) && row.ValidFrom.After(best.ValidFrom)) {
			winners[row.TeamID] = row
		}
	}
	sort.Strings(valid)
	teams := make([]string, 0, len(winners))
	for team := range winners {
		teams = append(teams, team)
	}
	sort.Strings(teams)
	var out strings.Builder
	out.WriteString(strings.Join(valid, "\x02"))
	out.WriteString("\x03")
	for _, team := range teams {
		out.WriteString(team + "\x01" + winners[team].shape() + "\x02")
	}
	return out.String()
}

// instantsOf lists every instant worth asking about for two row sets: each
// boundary, one millisecond before it, the midpoint between neighbours, and the
// instants just outside. Limited to [from, to] when given.
func instantsOf(from, to *time.Time, sets ...[]ownershipIntervalRow) []time.Time {
	seen := map[int64]bool{}
	add := func(t time.Time) { seen[t.UnixMilli()] = true }
	for _, rows := range sets {
		for _, row := range rows {
			add(row.ValidFrom)
			add(row.ValidFrom.Add(-time.Millisecond))
			if row.ValidTo != nil {
				add(*row.ValidTo)
				add(row.ValidTo.Add(-time.Millisecond))
				add(row.ValidTo.Add(time.Millisecond))
			}
		}
	}
	millis := make([]int64, 0, len(seen))
	for m := range seen {
		millis = append(millis, m)
	}
	sort.Slice(millis, func(i, j int) bool { return millis[i] < millis[j] })
	var out []time.Time
	for index, m := range millis {
		out = append(out, time.UnixMilli(m).UTC())
		if index+1 < len(millis) {
			out = append(out, time.UnixMilli(m+(millis[index+1]-m)/2).UTC())
		}
	}
	if len(millis) > 0 {
		out = append(out, time.UnixMilli(millis[len(millis)-1]).UTC().Add(24*time.Hour*365*100))
	}
	if from == nil && to == nil {
		return out
	}
	limited := out[:0:0]
	for _, at := range out {
		if from != nil && at.Before(from.Add(-time.Millisecond)) {
			continue
		}
		if to != nil && at.After(to.Add(time.Millisecond)) {
			continue
		}
		limited = append(limited, at)
	}
	return limited
}

func compareOwnerAnswers(pre, post []ownershipIntervalRow, from, to *time.Time) (instants, mismatches int) {
	for _, at := range instantsOf(from, to, pre, post) {
		instants++
		if answerAt(pre, at) != answerAt(post, at) {
			mismatches++
		}
	}
	return instants, mismatches
}

// ownershipDifferential asks the owner-at-T question of every group that the
// change touches, for both row sets, at every boundary and midpoint.
func ownershipDifferential(pre, post []ownershipIntervalRow) OwnershipCompactionDifferential {
	preGroups, postGroups := map[string][]ownershipIntervalRow{}, map[string][]ownershipIntervalRow{}
	for _, row := range pre {
		preGroups[row.groupOf()] = append(preGroups[row.groupOf()], row)
	}
	for _, row := range post {
		postGroups[row.groupOf()] = append(postGroups[row.groupOf()], row)
	}
	groups := map[string]bool{}
	for group := range preGroups {
		groups[group] = true
	}
	for group := range postGroups {
		groups[group] = true
	}
	result := OwnershipCompactionDifferential{GroupsTotal: len(groups)}
	names := make([]string, 0, len(groups))
	for group := range groups {
		names = append(names, group)
	}
	sort.Strings(names)
	for _, group := range names {
		if sameRowSet(preGroups[group], postGroups[group]) {
			continue
		}
		result.GroupsChecked++
		instants, mismatches := compareOwnerAnswers(preGroups[group], postGroups[group], nil, nil)
		result.InstantsChecked += instants
		result.Mismatches += mismatches
	}
	return result
}

func rowSignature(row ownershipIntervalRow) string {
	to := "open"
	if row.ValidTo != nil {
		to = strconv.FormatInt(row.ValidTo.UnixMilli(), 10)
	}
	return row.sortKey() + "\x00" + to + "\x00" + strconv.FormatInt(row.UpdatedAt.UnixMilli(), 10) + "\x00" + row.shape()
}

func sameRowSet(a, b []ownershipIntervalRow) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, row := range a {
		seen[rowSignature(row)]++
	}
	for _, row := range b {
		seen[rowSignature(row)]--
	}
	for _, count := range seen {
		if count != 0 {
			return false
		}
	}
	return true
}

func countClasses(rows []ownershipIntervalRow, source string) (total, open, closed, tombstones, closeBeforeOpen int) {
	for _, row := range rows {
		if row.Source != source {
			continue
		}
		total++
		switch {
		case row.isOpen():
			open++
		case row.isTombstone():
			tombstones++
		case row.isCloseBeforeOpen():
			closeBeforeOpen++
		default:
			closed++
		}
	}
	return
}

func (org *OwnershipCompactionOrg) setAfter(rows []ownershipIntervalRow, source string) {
	org.RowsAfter, org.OpenAfter, org.ClosedAfter, org.TombstonesAfter, org.CloseBeforeOpenAfter = countClasses(rows, source)
	org.RowsKept = org.ClosedAfter
}

func (org *OwnershipCompactionOrg) add(other OwnershipCompactionOrg) {
	org.RowsBefore += other.RowsBefore
	org.OpenBefore += other.OpenBefore
	org.ClosedBefore += other.ClosedBefore
	org.TombstonesBefore += other.TombstonesBefore
	org.CloseBeforeOpenBefore += other.CloseBeforeOpenBefore
	org.RowsAfter += other.RowsAfter
	org.OpenAfter += other.OpenAfter
	org.ClosedAfter += other.ClosedAfter
	org.TombstonesAfter += other.TombstonesAfter
	org.CloseBeforeOpenAfter += other.CloseBeforeOpenAfter
	org.RunsFound += other.RunsFound
	org.RunsMerged += other.RunsMerged
	org.RowsMerged += other.RowsMerged
	org.RowsKept += other.RowsKept
	org.RunsKeptMultiSource += other.RunsKeptMultiSource
	org.RunsChangedDuringRun += other.RunsChangedDuringRun
	org.Gaps += other.Gaps
	org.Overlaps += other.Overlaps
	org.TombstonesFound += other.TombstonesFound
	org.TombstonesDropped += other.TombstonesDropped
	org.TombstonesKeptAnchor += other.TombstonesKeptAnchor
	org.TombstonesKeptLatest += other.TombstonesKeptLatest
	org.TombstonesKeptStamp += other.TombstonesKeptStamp
	org.BackupRows += other.BackupRows
	org.RowsAppearedDuringRun += other.RowsAppearedDuringRun
	org.GroupsDriftedDuringRun += other.GroupsDriftedDuringRun
	org.RowsNewerThanTheDelete += other.RowsNewerThanTheDelete
	org.RowsChangedSinceTheRun += other.RowsChangedSinceTheRun
	org.Differential.GroupsTotal += other.Differential.GroupsTotal
	org.Differential.GroupsChecked += other.Differential.GroupsChecked
	org.Differential.InstantsChecked += other.Differential.InstantsChecked
	org.Differential.Mismatches += other.Differential.Mismatches
	org.ElapsedMS += other.ElapsedMS
}

func newCompactionRunID(now time.Time) string {
	suffix := make([]byte, 4)
	_, _ = rand.Read(suffix)
	return "compact-" + now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(suffix)
}

func pickSpecs(table string) ([]ownershipCompactionSpec, error) {
	if table == "" {
		return ownershipCompactionSpecs, nil
	}
	for _, spec := range ownershipCompactionSpecs {
		if spec.table == table {
			return []ownershipCompactionSpec{spec}, nil
		}
	}
	return nil, fmt.Errorf("ownership compaction: unknown table")
}

// ErrOwnershipCompactionTable means the table is not the table the tool was
// written for. The tool reads the sort key and the version column from the
// server and refuses anything else, before it reads a row.
var ErrOwnershipCompactionTable = errors.New("ownership compaction: table engine or sorting key is not the expected one")

func verifyOwnershipTableShape(ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec) error {
	var engineFull, sortingKey string
	if err := conn.QueryRow(ctx, "SELECT engine_full, sorting_key FROM system.tables WHERE database = currentDatabase() AND name = {table:String}",
		clickhouse.Named("table", spec.table)).Scan(&engineFull, &sortingKey); err != nil {
		return fmt.Errorf("%w: %v", ErrOwnershipCompactionTable, err)
	}
	wantKey := "org_id, provider, " + spec.natural + ", team_id, source, valid_from"
	if !strings.HasPrefix(engineFull, "ReplacingMergeTree(updated_at)") || sortingKey != wantKey {
		return ErrOwnershipCompactionTable
	}
	return nil
}

func listOwnershipOrgs(ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec) ([]string, error) {
	rows, err := conn.Query(ctx, "SELECT DISTINCT org_id FROM "+spec.table+" WHERE source = '"+spec.source+"' ORDER BY org_id")
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

// CompactOwnershipIntervals plans and, with Apply, runs the compaction. It
// returns the report so far together with the first error: a failed run still
// tells the operator how far it got.
func CompactOwnershipIntervals(ctx context.Context, conn driver.Conn, opts OwnershipCompactionOptions) (OwnershipCompactionOutcome, error) {
	if conn == nil {
		return OwnershipCompactionOutcome{}, ErrInvalidConfiguration
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = OwnershipCompactionDefaultBatchSize
	}
	if opts.AllOrgs == (len(opts.Orgs) > 0) {
		return OwnershipCompactionOutcome{}, fmt.Errorf("ownership compaction: give organizations or all organizations, not both and not neither")
	}
	specs, err := pickSpecs(opts.Table)
	if err != nil {
		return OwnershipCompactionOutcome{}, err
	}
	outcome := OwnershipCompactionOutcome{RunID: newCompactionRunID(opts.Now()), Apply: opts.Apply}
	for _, spec := range specs {
		if err := verifyOwnershipTableShape(ctx, conn, spec); err != nil {
			return outcome, err
		}
		table := OwnershipCompactionTable{Table: spec.table, Source: spec.source}
		orgs := opts.Orgs
		if opts.AllOrgs {
			if orgs, err = listOwnershipOrgs(ctx, conn, spec); err != nil {
				return outcome, err
			}
		}
		for index, orgID := range orgs {
			report, err := compactOwnershipOrg(ctx, conn, spec, orgID, index+1, opts, outcome.RunID)
			table.Orgs = append(table.Orgs, report)
			table.Totals.add(report)
			if err != nil {
				outcome.Tables = append(outcome.Tables, table)
				return outcome, err
			}
		}
		outcome.Tables = append(outcome.Tables, table)
	}
	return outcome, nil
}

func compactOwnershipOrg(
	ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec, orgID string, ordinal int,
	opts OwnershipCompactionOptions, runID string,
) (OwnershipCompactionOrg, error) {
	started := time.Now()
	logger := slog.Default().With("table", spec.table, "source", spec.source, "org_ordinal", ordinal, "run_id", runID, "apply", opts.Apply)
	rows, err := readOwnershipRows(ctx, conn, spec, orgID)
	if err != nil {
		return OwnershipCompactionOrg{OrgOrdinal: ordinal}, fmt.Errorf("ownership compaction: read %s: %w", spec.table, err)
	}
	plan := planOwnershipCompaction(spec, rows)
	report := plan.org
	report.OrgOrdinal = ordinal
	final := plan.final(rows)
	report.Differential = ownershipDifferential(rows, final)
	report.setAfter(final, spec.source)
	finish := func(err error) (OwnershipCompactionOrg, error) {
		report.ElapsedMS = time.Since(started).Milliseconds()
		failed := err != nil
		logger.InfoContext(ctx, "ownership_interval_compaction.table",
			"rows_before", report.RowsBefore, "closed_before", report.ClosedBefore, "closed_after", report.ClosedAfter,
			"runs_found", report.RunsFound, "runs_merged", report.RunsMerged, "rows_merged", report.RowsMerged,
			"rows_kept", report.RowsKept, "runs_kept_multi_source", report.RunsKeptMultiSource,
			"runs_changed_during_run", report.RunsChangedDuringRun, "tombstones_found", report.TombstonesFound,
			"tombstones_dropped", report.TombstonesDropped, "tombstones_kept_anchor", report.TombstonesKeptAnchor, "tombstones_kept_latest", report.TombstonesKeptLatest, "tombstones_kept_stamp", report.TombstonesKeptStamp,
			"differential_mismatches", report.Differential.Mismatches, "elapsed_ms", report.ElapsedMS, "failed", failed)
		return report, err
	}
	if report.Differential.Mismatches > 0 {
		return finish(fmt.Errorf("%w: %d instants", ErrOwnershipCompactionDifferential, report.Differential.Mismatches))
	}
	if !opts.Apply || (len(plan.runs) == 0 && len(plan.drops) == 0) {
		return finish(nil)
	}

	backup := plan.backupRows()
	if err := writeOwnershipBackup(ctx, conn, spec, orgID, runID, opts.Now(), backup); err != nil {
		return finish(err)
	}
	report.BackupRows = len(backup)
	logger.InfoContext(ctx, "ownership_interval_compaction.backup_written", "backup_table", spec.backupTable(), "backup_rows", len(backup))

	applied, changed, err := plan.applyRuns(ctx, conn, orgID, opts)
	report.RunsChangedDuringRun = changed
	if err != nil {
		return finish(err)
	}
	if opts.afterInsert != nil {
		opts.afterInsert(spec.table)
	}
	post, err := readOwnershipRows(ctx, conn, spec, orgID)
	if err != nil {
		return finish(fmt.Errorf("ownership compaction: read back %s: %w", spec.table, err))
	}
	if err := plan.verifyReadback(rows, post, applied, &report); err != nil {
		return finish(err)
	}

	// The rows to delete, each as the TOMBSTONE this run expects to find: a pre-existing tombstone as it is, an
	// inner row as phase A wrote it. A physical version newer than that stamp is a sync's and must survive.
	deletes := append([]ownershipIntervalRow(nil), plan.drops...)
	for _, run := range applied {
		for _, inner := range run.inner() {
			deletes = append(deletes, tombstoneOf(inner))
		}
	}
	survivors, err := deleteOwnershipTombstones(ctx, conn, spec, orgID, deletes)
	report.RowsNewerThanTheDelete = survivors
	if err != nil {
		return finish(err)
	}
	after, err := readOwnershipRows(ctx, conn, spec, orgID)
	if err != nil {
		return finish(fmt.Errorf("ownership compaction: recount %s: %w", spec.table, err))
	}
	report.setAfter(after, spec.source)
	return finish(nil)
}

// backupRows is every pre-state row that the run rewrites or drops.
func (plan ownershipCompactionPlan) backupRows() []ownershipIntervalRow {
	var out []ownershipIntervalRow
	for _, run := range plan.runs {
		out = append(out, run.rows...)
	}
	return append(out, plan.drops...)
}

func writeOwnershipBackup(
	ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec, orgID, runID string, now time.Time,
	rows []ownershipIntervalRow,
) error {
	if len(rows) == 0 {
		return fmt.Errorf("%w: nothing to back up", ErrOwnershipCompactionBackup)
	}
	if err := conn.Exec(ctx, spec.backupDDL()); err != nil {
		return fmt.Errorf("%w: create: %v", ErrOwnershipCompactionBackup, err)
	}
	batch, err := conn.PrepareBatch(ctx, spec.backupInsert())
	if err != nil {
		return fmt.Errorf("%w: prepare: %v", ErrOwnershipCompactionBackup, err)
	}
	stamp := now.UTC()
	for _, row := range rows {
		args := append([]any{runID, stamp}, spec.appendArgs(orgID, row)...)
		if !spec.repo {
			args = append(args, row.LastSynced)
		}
		if err := batch.Append(args...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("%w: append: %v", ErrOwnershipCompactionBackup, err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("%w: send: %v", ErrOwnershipCompactionBackup, err)
	}
	var stored uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+spec.backupTable()+" WHERE run_id = {run_id:String} AND org_id = {org_id:String}",
		clickhouse.Named("run_id", runID), clickhouse.Named("org_id", orgID)).Scan(&stored); err != nil {
		return fmt.Errorf("%w: count: %v", ErrOwnershipCompactionBackup, err)
	}
	if stored == 0 || stored != uint64(len(rows)) {
		return fmt.Errorf("%w: wrote %d rows, found %d", ErrOwnershipCompactionBackup, len(rows), stored)
	}
	return nil
}

// applyRuns is phase A. Before each batch it reads the organization again and
// leaves out every run whose rows are no longer what the plan read: a sync
// wrote the key in between, and a replacement stamped from the old version
// could lose to the newer one.
func (plan ownershipCompactionPlan) applyRuns(
	ctx context.Context, conn driver.Conn, orgID string, opts OwnershipCompactionOptions,
) (applied []compactionRun, changed int, err error) {
	spec := plan.compacted
	for start, batchNumber := 0, 0; start < len(plan.runs); start, batchNumber = start+opts.BatchSize, batchNumber+1 {
		end := min(start+opts.BatchSize, len(plan.runs))
		if opts.beforeBatch != nil {
			opts.beforeBatch(spec.table, batchNumber)
		}
		fresh, err := readOwnershipRows(ctx, conn, spec, orgID)
		if err != nil {
			return applied, changed, fmt.Errorf("ownership compaction: recheck %s: %w", spec.table, err)
		}
		current := map[string]string{}
		for _, row := range fresh {
			current[row.sortKey()] = rowSignature(row)
		}
		var batchRuns []compactionRun
		for _, run := range plan.runs[start:end] {
			intact := true
			for _, row := range run.rows {
				if current[row.sortKey()] != rowSignature(row) {
					intact = false
					break
				}
			}
			if intact {
				batchRuns = append(batchRuns, run)
			} else {
				changed++
			}
		}
		if len(batchRuns) == 0 {
			continue
		}
		insert, err := conn.PrepareBatch(ctx, "INSERT INTO "+spec.table+" ("+spec.columns()+")")
		if err != nil {
			return applied, changed, fmt.Errorf("ownership compaction: prepare insert %s: %w", spec.table, err)
		}
		for _, run := range batchRuns {
			rows := []ownershipIntervalRow{run.merged}
			for _, inner := range run.inner() {
				rows = append(rows, tombstoneOf(inner))
			}
			for _, row := range rows {
				if err := insert.Append(spec.appendArgs(orgID, row)...); err != nil {
					_ = insert.Abort()
					return applied, changed, fmt.Errorf("ownership compaction: append %s: %w", spec.table, err)
				}
			}
		}
		if err := insert.Send(); err != nil {
			return applied, changed, fmt.Errorf("ownership compaction: insert %s: %w", spec.table, err)
		}
		applied = append(applied, batchRuns...)
		slog.Default().InfoContext(ctx, "ownership_interval_compaction.batch_inserted", "table", spec.table, "batch", batchNumber,
			"runs", len(batchRuns), "runs_skipped_changed", len(plan.runs[start:end])-len(batchRuns))
	}
	return applied, changed, nil
}

// verifyReadback checks, before anything is deleted, that every row phase A
// wrote is the FINAL row now, and that the owner-at-T answers of the groups it
// touched are still the answers of the rows read before. A group that a sync
// changed in between is left out of the answer check and counted.
func (plan ownershipCompactionPlan) verifyReadback(
	pre, post []ownershipIntervalRow, applied []compactionRun, report *OwnershipCompactionOrg,
) error {
	actual := map[string]string{}
	for _, row := range post {
		actual[row.sortKey()] = rowSignature(row)
	}
	touched := map[string]bool{}
	expected := map[string]bool{}
	for _, run := range applied {
		touched[run.merged.groupOf()] = true
		rows := []ownershipIntervalRow{run.merged}
		for _, inner := range run.inner() {
			rows = append(rows, tombstoneOf(inner))
		}
		for _, row := range rows {
			expected[row.sortKey()] = true
			if actual[row.sortKey()] != rowSignature(row) {
				return fmt.Errorf("%w: a written row is not the FINAL row", ErrOwnershipCompactionReadback)
			}
		}
	}
	preSet := map[string]bool{}
	for _, row := range pre {
		preSet[row.sortKey()] = true
	}
	for _, row := range post {
		if !preSet[row.sortKey()] {
			report.RowsAppearedDuringRun++
		}
	}
	preGroups, postGroups := map[string][]ownershipIntervalRow{}, map[string][]ownershipIntervalRow{}
	for _, row := range pre {
		if touched[row.groupOf()] {
			preGroups[row.groupOf()] = append(preGroups[row.groupOf()], row)
		}
	}
	for _, row := range post {
		if touched[row.groupOf()] {
			postGroups[row.groupOf()] = append(postGroups[row.groupOf()], row)
		}
	}
	var preKept, postKept []ownershipIntervalRow
	for group := range touched {
		if groupDrifted(preGroups[group], postGroups[group], expected) {
			report.GroupsDriftedDuringRun++
			continue
		}
		preKept = append(preKept, preGroups[group]...)
		postKept = append(postKept, postGroups[group]...)
	}
	differential := ownershipDifferential(preKept, postKept)
	report.Differential.GroupsChecked = max(report.Differential.GroupsChecked, differential.GroupsChecked)
	report.Differential.InstantsChecked += differential.InstantsChecked
	report.Differential.Mismatches += differential.Mismatches
	if differential.Mismatches > 0 {
		return fmt.Errorf("%w: %d instants after the insert", ErrOwnershipCompactionDifferential, differential.Mismatches)
	}
	return nil
}

// groupDrifted is true when the group holds a row that is neither a row read
// before nor a row written by phase A: a sync wrote it in between.
func groupDrifted(pre, post []ownershipIntervalRow, written map[string]bool) bool {
	known := map[string]string{}
	for _, row := range pre {
		known[row.sortKey()] = rowSignature(row)
	}
	for _, row := range post {
		key := row.sortKey()
		if written[key] {
			continue
		}
		if signature, ok := known[key]; !ok || signature != rowSignature(row) {
			return true
		}
	}
	return false
}

const ownershipCompactionKeysMatch = `(provider, %s, team_id, toString(source), valid_from) IN (` +
	`SELECT unhex(parts[1]), unhex(parts[2]), unhex(parts[3]), unhex(parts[4]), fromUnixTimestamp64Milli(toInt64(parts[5]), 'UTC') ` +
	`FROM (SELECT splitByChar(':', p) AS parts FROM (SELECT arrayJoin({keys:Array(String)}) AS p)))`

// deleteOwnershipTombstones is phase B. A key is deleted only while its
// newest version is a tombstone; every physical version of the key goes, or an
// older version would come back through FINAL.
func deleteOwnershipTombstones(
	ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec, orgID string, rows []ownershipIntervalRow,
) (survivors int, err error) {
	match := fmt.Sprintf(ownershipCompactionKeysMatch, spec.natural)
	for start := 0; start < len(rows); start += ownershipCompactionDeleteKeysPerMutation {
		end := min(start+ownershipCompactionDeleteKeysPerMutation, len(rows))
		tokens := make([]string, 0, end-start)
		for _, row := range rows[start:end] {
			tokens = append(tokens, row.token())
		}
		args := []any{clickhouse.Named("org_id", orgID), clickhouse.Named("keys", tokens)}
		scope := "org_id = {org_id:String} AND source = '" + spec.source + "' AND " + match
		var tombstoned uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+spec.table+" FINAL WHERE valid_to = valid_from AND "+scope, args...).Scan(&tombstoned); err != nil {
			return survivors, fmt.Errorf("%w: check tombstones: %v", ErrOwnershipCompactionMutation, err)
		}
		if tombstoned != uint64(len(tokens)) {
			return survivors, fmt.Errorf("%w: %d of %d keys are tombstones", ErrOwnershipCompactionMutation, tombstoned, len(tokens))
		}
		// The delete names the physical VERSIONS (key and updated_at) that are on disk now, not the keys: a
		// version that a sync inserts after this read has another updated_at and survives the mutation.
		expected := make(map[string]int64, end-start)
		for _, row := range rows[start:end] {
			expected[row.token()] = row.UpdatedAt.UnixMilli()
		}
		versions, err := physicalVersionTokens(ctx, conn, spec, scope, args, expected)
		if err != nil {
			return survivors, err
		}
		versionArgs := []any{clickhouse.Named("org_id", orgID), clickhouse.Named("versions", versions)}
		versionScope := "org_id = {org_id:String} AND source = '" + spec.source + "' AND " + fmt.Sprintf(ownershipCompactionVersionsMatch, spec.natural)
		mutationCtx, cancel := context.WithTimeout(ctx, ownershipCompactionMutationTimeout)
		syncCtx := clickhouse.Context(mutationCtx, clickhouse.WithSettings(clickhouse.Settings{"mutations_sync": "2"}))
		err = conn.Exec(syncCtx, "ALTER TABLE "+spec.table+" DELETE WHERE "+versionScope, versionArgs...)
		cancel()
		if err != nil {
			return survivors, fmt.Errorf("%w: %v", ErrOwnershipCompactionMutation, err)
		}
		var remaining, left uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+spec.table+" WHERE "+versionScope, versionArgs...).Scan(&remaining); err != nil {
			return survivors, fmt.Errorf("%w: count remaining: %v", ErrOwnershipCompactionMutation, err)
		}
		if remaining != 0 {
			return survivors, fmt.Errorf("%w: %d rows remain", ErrOwnershipCompactionMutation, remaining)
		}
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+spec.table+" WHERE "+scope, args...).Scan(&left); err != nil {
			return survivors, fmt.Errorf("%w: count survivors: %v", ErrOwnershipCompactionMutation, err)
		}
		survivors += int(left)
		slog.Default().InfoContext(ctx, "ownership_interval_compaction.tombstones_deleted", "table", spec.table, "keys", len(tokens),
			"versions", len(versions), "rows_newer_than_the_delete", left)
		var failed uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = {table:String} AND NOT is_done AND latest_fail_reason != ''",
			clickhouse.Named("table", spec.table)).Scan(&failed); err != nil {
			slog.Default().WarnContext(ctx, "ownership_interval_compaction.mutation_state_unreadable", "table", spec.table)
		} else if failed != 0 {
			return survivors, fmt.Errorf("%w: %d mutations report a failure", ErrOwnershipCompactionMutation, failed)
		}
	}
	return survivors, nil
}

// ownershipCompactionVersionsMatch matches physical rows by key AND updated_at (milliseconds).
const ownershipCompactionVersionsMatch = `(provider, %s, team_id, toString(source), valid_from, toUnixTimestamp64Milli(updated_at)) IN (` +
	`SELECT unhex(parts[1]), unhex(parts[2]), unhex(parts[3]), unhex(parts[4]), fromUnixTimestamp64Milli(toInt64(parts[5]), 'UTC'), toInt64(parts[6]) ` +
	`FROM (SELECT splitByChar(':', p) AS parts FROM (SELECT arrayJoin({versions:Array(String)}) AS p)))`

// physicalVersionTokens lists every physical row (not FINAL) of the keys as key token + updated_at.
// A version newer than the stamp the run expects for its key (expected, in ms) is left out: it is a sync's write.
func physicalVersionTokens(ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec, scope string, args []any, expected map[string]int64) ([]string, error) {
	rows, err := conn.Query(ctx, "SELECT provider, "+spec.natural+", team_id, toString(source), valid_from, toUnixTimestamp64Milli(updated_at) FROM "+spec.table+" WHERE "+scope, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: list versions: %v", ErrOwnershipCompactionMutation, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var provider, natural, team, source string
		var from time.Time
		var stamp int64
		if err := rows.Scan(&provider, &natural, &team, &source, &from, &stamp); err != nil {
			return nil, fmt.Errorf("%w: list versions: %v", ErrOwnershipCompactionMutation, err)
		}
		row := ownershipIntervalRow{Provider: provider, Natural: natural, TeamID: team, Source: source, ValidFrom: from.UTC()}
		if bound, known := expected[row.token()]; known && stamp > bound {
			continue
		}
		out = append(out, row.token()+":"+strconv.FormatInt(stamp, 10))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: list versions: %v", ErrOwnershipCompactionMutation, err)
	}
	return out, nil
}

// RestoreOwnershipCompaction puts the rows of one run back from the backup
// table. A restored row is written with an updated_at above the current
// version of its key, so it wins. Without Apply it counts only.
func RestoreOwnershipCompaction(ctx context.Context, conn driver.Conn, runID string, opts OwnershipCompactionOptions) (OwnershipCompactionOutcome, error) {
	if conn == nil || strings.TrimSpace(runID) == "" {
		return OwnershipCompactionOutcome{}, ErrInvalidConfiguration
	}
	if opts.AllOrgs == (len(opts.Orgs) > 0) {
		return OwnershipCompactionOutcome{}, fmt.Errorf("ownership compaction: give organizations or all organizations, not both and not neither")
	}
	specs, err := pickSpecs(opts.Table)
	if err != nil {
		return OwnershipCompactionOutcome{}, err
	}
	outcome := OwnershipCompactionOutcome{RunID: runID, Apply: opts.Apply, Restore: true}
	saved, err := backupRowsOfRun(ctx, conn, specs, runID)
	if err != nil {
		return outcome, err
	}
	if saved == 0 {
		return outcome, fmt.Errorf("%w: no backup rows for this run id", ErrOwnershipCompactionBackup)
	}
	for _, spec := range specs {
		if err := verifyOwnershipTableShape(ctx, conn, spec); err != nil {
			return outcome, err
		}
		table := OwnershipCompactionTable{Table: spec.table, Source: spec.source}
		orgs := opts.Orgs
		if opts.AllOrgs {
			rows, err := conn.Query(ctx, "SELECT DISTINCT org_id FROM "+spec.backupTable()+" WHERE run_id = {run_id:String} ORDER BY org_id", clickhouse.Named("run_id", runID))
			if err != nil {
				return outcome, fmt.Errorf("ownership compaction: list backup orgs: %w", err)
			}
			orgs = nil
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
		for index, orgID := range orgs {
			report, err := restoreOwnershipOrg(ctx, conn, spec, orgID, index+1, runID, opts.Apply)
			table.Orgs = append(table.Orgs, report)
			table.Totals.add(report)
			if err != nil {
				outcome.Tables = append(outcome.Tables, table)
				return outcome, err
			}
		}
		outcome.Tables = append(outcome.Tables, table)
	}
	return outcome, nil
}

// backupRowsOfRun counts the rows the run saved, over the backup tables that exist (a run that touched one table
// created only that table's backup).
func backupRowsOfRun(ctx context.Context, conn driver.Conn, specs []ownershipCompactionSpec, runID string) (uint64, error) {
	var total uint64
	for _, spec := range specs {
		var exists uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = {name:String}",
			clickhouse.Named("name", spec.backupTable())).Scan(&exists); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrOwnershipCompactionBackup, err)
		}
		if exists == 0 {
			continue
		}
		var count uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+spec.backupTable()+" WHERE run_id = {run_id:String}",
			clickhouse.Named("run_id", runID)).Scan(&count); err != nil {
			return 0, fmt.Errorf("%w: %v", ErrOwnershipCompactionBackup, err)
		}
		total += count
	}
	return total, nil
}

func restoreOwnershipOrg(
	ctx context.Context, conn driver.Conn, spec ownershipCompactionSpec, orgID string, ordinal int, runID string, apply bool,
) (OwnershipCompactionOrg, error) {
	started := time.Now()
	report := OwnershipCompactionOrg{OrgOrdinal: ordinal}
	rows, err := conn.Query(ctx, "SELECT "+spec.selectColumns()+" FROM "+spec.backupTable()+" WHERE run_id = {run_id:String} AND org_id = {org_id:String}",
		clickhouse.Named("run_id", runID), clickhouse.Named("org_id", orgID))
	if err != nil {
		return report, fmt.Errorf("ownership compaction: read backup: %w", err)
	}
	var saved []ownershipIntervalRow
	for rows.Next() {
		row, err := spec.scan(rows)
		if err != nil {
			rows.Close()
			return report, err
		}
		saved = append(saved, row)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return report, err
	}
	current, err := readOwnershipRows(ctx, conn, spec, orgID)
	if err != nil {
		return report, fmt.Errorf("ownership compaction: read %s: %w", spec.table, err)
	}
	now := map[string]ownershipIntervalRow{}
	for _, row := range current {
		now[row.sortKey()] = row
	}
	// A key that a sync has written since the run (a current version newer than the newest version the run
	// saw, plus the 1 ms the run itself adds) is left as the sync made it: restoring over it would put the old
	// shape back on top of the newer fact. All rows of that key are skipped, so the key is never half restored.
	newest := map[string]time.Time{}
	for _, row := range saved {
		if row.UpdatedAt.After(newest[row.keyOf()]) {
			newest[row.keyOf()] = row.UpdatedAt
		}
	}
	changedSince := map[string]bool{}
	for _, row := range saved {
		if have, ok := now[row.sortKey()]; ok && have.UpdatedAt.After(newest[row.keyOf()].Add(time.Millisecond)) {
			changedSince[row.keyOf()] = true
		}
	}
	var restore []ownershipIntervalRow
	for _, row := range saved {
		if changedSince[row.keyOf()] {
			report.RowsChangedSinceTheRun++
			continue
		}
		if have, ok := now[row.sortKey()]; ok && sameInstant(have.ValidTo, row.ValidTo) && have.shape() == row.shape() {
			continue
		}
		if have, ok := now[row.sortKey()]; ok && !have.UpdatedAt.Before(row.UpdatedAt) {
			row.UpdatedAt = have.UpdatedAt.Add(time.Millisecond)
		}
		restore = append(restore, row)
	}
	report.BackupRows = len(saved)
	report.RowsMerged = len(restore)
	if apply && len(restore) > 0 {
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO "+spec.table+" ("+spec.columns()+")")
		if err != nil {
			return report, err
		}
		for _, row := range restore {
			if err := batch.Append(spec.appendArgs(orgID, row)...); err != nil {
				_ = batch.Abort()
				return report, err
			}
		}
		if err := batch.Send(); err != nil {
			return report, err
		}
	}
	report.ElapsedMS = time.Since(started).Milliseconds()
	slog.Default().InfoContext(ctx, "ownership_interval_compaction.restore", "table", spec.table, "org_ordinal", ordinal,
		"run_id", runID, "apply", apply, "backup_rows", len(saved), "rows_restored", len(restore), "elapsed_ms", report.ElapsedMS)
	return report, nil
}
