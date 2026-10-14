//go:build integration

package providersync

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/full-chaos/dev-health-ops/internal/teamownership"
)

// CHAOS-9556 real-store tests. The differential here asks the PRODUCTION readers
// (teamattribution LoadRepos/LoadProjects, teamownership OwnerSharesByRepo and
// OwnedRepoIDs, the writer's own tenure read, and the raw valid-at-T SQL) the
// owner-at-T question before and after the compaction, at every boundary and
// midpoint of the fixture.

var (
	cmpBase  = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cmpTeam1 = "gh:alpha"
	cmpTeam2 = "gh:beta"
	cmpTeam3 = "gh:gamma"
	cmpRepo1 = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	cmpRepo2 = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	cmpRepo3 = uuid.MustParse("33333333-3333-4333-8333-333333333333")
)

func cmpDay(n int) time.Time { return cmpBase.AddDate(0, 0, n) }

type cmpFixture struct {
	t     *testing.T
	ctx   context.Context
	conn  driver.Conn
	orgA  string
	orgB  string
	rowsA []ownershipIntervalRow
}

func (f cmpFixture) insert(spec ownershipCompactionSpec, org string, rows []ownershipIntervalRow) {
	f.t.Helper()
	batch, err := f.conn.PrepareBatch(f.ctx, "INSERT INTO "+spec.table+" ("+spec.columns()+")")
	if err != nil {
		f.t.Fatalf("prepare: %v", err)
	}
	for _, row := range rows {
		if err := batch.Append(spec.appendArgs(org, row)...); err != nil {
			f.t.Fatalf("append: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		f.t.Fatalf("send: %v", err)
	}
}

func cmpRepoRow(team string, repoID uuid.UUID, repo string, from, to int, share float64, primary uint8) ownershipIntervalRow {
	id := repoID
	row := ownershipIntervalRow{
		Provider: "github", Natural: repo, TeamID: team, Source: "inferred", RepoID: &id, MatchType: "exact",
		IsPrimary: primary, Specificity: 100, Priority: 10, ValidFrom: cmpDay(from), UpdatedAt: cmpDay(from),
	}
	row.Share = &share
	if to >= 0 {
		end := cmpDay(to)
		row.ValidTo = &end
		row.UpdatedAt = end
	}
	return row
}

func cmpProjectRow(team, project string, key *string, from, to int, priority int32, source string) ownershipIntervalRow {
	row := ownershipIntervalRow{
		Provider: "linear", Natural: project, TeamID: team, Source: source, ProjectKey: key,
		IsPrimary: 1, Specificity: 100, Priority: priority, ValidFrom: cmpDay(from), UpdatedAt: cmpDay(from),
	}
	if to >= 0 {
		end := cmpDay(to)
		row.ValidTo = &end
		row.UpdatedAt = end
	}
	return row
}

func cmpChain(build func(from, to int) ownershipIntervalRow, from, until int) []ownershipIntervalRow {
	var rows []ownershipIntervalRow
	for day := from; day < until; day++ {
		rows = append(rows, build(day, day+1))
	}
	return rows
}

// newCmpFixture builds the fixture of the plan: unchanged runs, a real change
// in the middle of a run, a gap, an open interval at the end, tombstones (an
// anchor and droppable ones), a close-before-open row, two sources on one key,
// and a second organization that must not move.
func newCmpFixture(t *testing.T) cmpFixture {
	t.Helper()
	ctx, conn := newWorkItemEffectsConn(t)
	f := cmpFixture{t: t, ctx: ctx, conn: conn, orgA: uuid.NewString(), orgB: uuid.NewString()}
	repo := repoSpec()
	project := projectSpec()

	var repoRows []ownershipIntervalRow
	// R1, team 1: an unchanged run of 5, a share change, a run of 3, a gap, a run of 3, then open.
	repoRows = append(repoRows, cmpChain(func(a, b int) ownershipIntervalRow { return cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", a, b, 1.0, 1) }, 0, 5)...)
	repoRows = append(repoRows, cmpChain(func(a, b int) ownershipIntervalRow { return cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", a, b, 0.6, 1) }, 5, 8)...)
	repoRows = append(repoRows, cmpChain(func(a, b int) ownershipIntervalRow { return cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", a, b, 0.6, 1) }, 9, 12)...)
	repoRows = append(repoRows, cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", 12, -1, 0.6, 1))
	// R1, team 2 (co-owner): a run of 2, then open.
	repoRows = append(repoRows, cmpChain(func(a, b int) ownershipIntervalRow { return cmpRepoRow(cmpTeam2, cmpRepo1, "acme/api", a, b, 0.4, 2) }, 0, 4)[:2]...)
	repoRows = append(repoRows, cmpRepoRow(cmpTeam2, cmpRepo1, "acme/api", 4, -1, 0.4, 2))
	// R2: tombstone anchor (team 1), a droppable tombstone inside a run and one after (team 2), a close-before-open row (team 3).
	repoRows = append(repoRows, cmpRepoRow(cmpTeam1, cmpRepo2, "acme/web", 3, 3, 1, 1))
	repoRows = append(repoRows, cmpRepoRow(cmpTeam1, cmpRepo2, "acme/web", 4, 6, 1, 1), cmpRepoRow(cmpTeam1, cmpRepo2, "acme/web", 6, 8, 1, 1))
	repoRows = append(repoRows, cmpRepoRow(cmpTeam2, cmpRepo2, "acme/web", 0, 2, 1, 1), cmpRepoRow(cmpTeam2, cmpRepo2, "acme/web", 2, 4, 1, 1))
	repoRows = append(repoRows, cmpRepoRow(cmpTeam2, cmpRepo2, "acme/web", 3, 3, 1, 1), cmpRepoRow(cmpTeam2, cmpRepo2, "acme/web", 9, 9, 1, 1))
	repoRows = append(repoRows, cmpRepoRow(cmpTeam3, cmpRepo2, "acme/web", 5, 3, 1, 1))
	// R3: two sources on one key whose winner by updated_at would flip.
	shared := cmpChain(func(a, b int) ownershipIntervalRow { return cmpRepoRow(cmpTeam1, cmpRepo3, "acme/shared", a, b, 1, 1) }, 0, 3)
	shared[2].UpdatedAt = cmpDay(100)
	repoRows = append(repoRows, shared...)
	access := cmpRepoRow(cmpTeam1, cmpRepo3, "acme/shared", 1, 2, 0.25, 2)
	access.Source, access.UpdatedAt = "provider_access", cmpDay(50)
	repoRows = append(repoRows, access)

	var projectRows []ownershipIntervalRow
	eng := "ENG"
	projectRows = append(projectRows, cmpChain(func(a, b int) ownershipIntervalRow {
		return cmpProjectRow(cmpTeam1, "proj-1", &eng, a, b, 10, "native")
	}, 0, 5)...)
	projectRows = append(projectRows, cmpChain(func(a, b int) ownershipIntervalRow {
		return cmpProjectRow(cmpTeam1, "proj-1", &eng, a, b, 20, "native")
	}, 5, 7)...)
	projectRows = append(projectRows, cmpProjectRow(cmpTeam1, "proj-1", &eng, 7, -1, 20, "native"))
	projectRows = append(projectRows, cmpChain(func(a, b int) ownershipIntervalRow { return cmpProjectRow(cmpTeam2, "proj-2", nil, a, b, 10, "native") }, 0, 3)...)
	projectRows = append(projectRows, cmpProjectRow(cmpTeam2, "proj-2", nil, 4, 5, 10, "native"))
	projectRows = append(projectRows, cmpChain(func(a, b int) ownershipIntervalRow {
		return cmpProjectRow(cmpTeam2, "proj-3", nil, a, b, 10, "jira_legacy")
	}, 0, 3)...)

	f.rowsA = repoRows
	f.insert(repo, f.orgA, repoRows)
	f.insert(project, f.orgA, projectRows)
	// Older physical versions of some keys: FINAL must hide them, the delete must remove them all.
	stale := cmpChain(func(a, b int) ownershipIntervalRow { return cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", a, b, 1.0, 1) }, 0, 3)
	for index := range stale {
		stale[index].ValidTo = nil
		stale[index].UpdatedAt = cmpDay(-10)
	}
	f.insert(repo, f.orgA, stale)

	otherRepo := cmpChain(func(a, b int) ownershipIntervalRow { return cmpRepoRow(cmpTeam1, cmpRepo1, "other/repo", a, b, 1, 1) }, 0, 3)
	f.insert(repo, f.orgB, otherRepo)
	f.insert(project, f.orgB, cmpChain(func(a, b int) ownershipIntervalRow { return cmpProjectRow(cmpTeam1, "proj-b", nil, a, b, 10, "native") }, 0, 3))
	return f
}

func (f cmpFixture) rows(spec ownershipCompactionSpec, org string) []ownershipIntervalRow {
	f.t.Helper()
	rows, err := readOwnershipRows(f.ctx, f.conn, spec, org)
	if err != nil {
		f.t.Fatalf("read %s: %v", spec.table, err)
	}
	return rows
}

func (f cmpFixture) physicalCount(table, org string) uint64 {
	f.t.Helper()
	return countRows(f.t, f.ctx, f.conn, "SELECT count() FROM "+table+" WHERE org_id = ?", org)
}

func cmpLiteral(at time.Time) string { return at.UTC().Format("2006-01-02 15:04:05.000") }

// readerSnapshot is the answer of every reader of the fixture at one instant.
// UpdatedAt is left out: it is a version, not an owner.
func (f cmpFixture) readerSnapshot(org string, at time.Time) string {
	f.t.Helper()
	var out []string
	source := teamattribution.ClickHouseFactSource{Conn: f.conn}
	repos, err := source.LoadRepos(f.ctx, org, at)
	if err != nil {
		f.t.Fatalf("LoadRepos: %v", err)
	}
	for _, fact := range repos {
		repoID := "nil"
		if fact.RepoID != nil {
			repoID = *fact.RepoID
		}
		out = append(out, fmt.Sprintf("repo|%s|%s|%s|%s|%d|%d|%d", fact.Provider, fact.TeamID, repoID, fact.RepoFullName, fact.IsPrimary, fact.Specificity, fact.Priority))
	}
	projects, err := source.LoadProjects(f.ctx, org, at)
	if err != nil {
		f.t.Fatalf("LoadProjects: %v", err)
	}
	for _, fact := range projects {
		key := "nil"
		if fact.ProjectKey != nil {
			key = *fact.ProjectKey
		}
		out = append(out, fmt.Sprintf("project|%s|%s|%s|%s|%d|%d|%d", fact.Provider, fact.TeamID, fact.ProjectID, key, fact.IsPrimary, fact.Specificity, fact.Priority))
	}
	shares, err := teamownership.OwnerSharesByRepo(f.ctx, f.conn, org, at)
	if err != nil {
		f.t.Fatalf("OwnerSharesByRepo: %v", err)
	}
	for repoID, owners := range shares {
		for _, owner := range owners {
			out = append(out, fmt.Sprintf("share|%s|%s|%t|%v", repoID, owner.TeamID, owner.Main, owner.Share))
		}
	}
	for _, team := range []string{cmpTeam1, cmpTeam2, cmpTeam3} {
		owned, err := teamownership.OwnedRepoIDs(f.ctx, f.conn, org, team, at)
		if err != nil {
			f.t.Fatalf("OwnedRepoIDs: %v", err)
		}
		for _, id := range owned {
			out = append(out, fmt.Sprintf("owned|%s|%s", team, id))
		}
	}
	for _, query := range []string{
		"SELECT concat('rawrepo|', provider, '|', repo_full_name, '|', team_id, '|', toString(source), '|', toString(repo_id), '|', toString(match_type), '|', toString(is_primary), '|', toString(specificity), '|', toString(priority), '|', toString(share)) FROM team_repo_ownership FINAL WHERE org_id = {org:String} AND valid_from <= {at:DateTime64(3, 'UTC')} AND (valid_to IS NULL OR valid_to > {at:DateTime64(3, 'UTC')})",
		"SELECT concat('rawproject|', provider, '|', project_id, '|', team_id, '|', toString(source), '|', toString(project_key), '|', toString(is_primary), '|', toString(specificity), '|', toString(priority)) FROM team_project_ownership FINAL WHERE org_id = {org:String} AND valid_from <= {at:DateTime64(3, 'UTC')} AND (valid_to IS NULL OR valid_to > {at:DateTime64(3, 'UTC')})",
	} {
		rows, err := f.conn.Query(f.ctx, query, clickhouse.Named("org", org), clickhouse.Named("at", cmpLiteral(at)))
		if err != nil {
			f.t.Fatalf("raw valid-at-T: %v", err)
		}
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				f.t.Fatalf("scan: %v", err)
			}
			out = append(out, line)
		}
		rows.Close()
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// writerInputs is what the derivation writer reads back from closed rows: the
// tenure ends of each claim. Compaction keeps the last valid_to of a run.
func (f cmpFixture) writerInputs(org string) string {
	f.t.Helper()
	tenure, err := loadTeamRepoOwnershipRetractedUntil(f.ctx, f.conn, org)
	if err != nil {
		f.t.Fatalf("tenure: %v", err)
	}
	var out []string
	for claim, until := range tenure.retractedUntil {
		out = append(out, fmt.Sprintf("%v|%s|%s", claim, until.UTC(), tenure.mainUntil[claim].UTC()))
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// instants are the boundaries and midpoints of the fixture, and the instants
// around them.
func (f cmpFixture) instants() []time.Time {
	f.t.Helper()
	var all []ownershipIntervalRow
	for _, spec := range ownershipCompactionSpecs {
		all = append(all, f.rows(spec, f.orgA)...)
	}
	return instantsOf(nil, nil, all)
}

func (f cmpFixture) snapshots(org string, instants []time.Time) map[time.Time]string {
	f.t.Helper()
	out := map[time.Time]string{}
	for _, at := range instants {
		out[at] = f.readerSnapshot(org, at)
	}
	return out
}

func (f cmpFixture) assertSameAnswers(org string, instants []time.Time, before map[time.Time]string, where string) {
	f.t.Helper()
	for _, at := range instants {
		if after := f.readerSnapshot(org, at); after != before[at] {
			f.t.Fatalf("%s: the owner answers differ at %s\nbefore:\n%s\nafter:\n%s", where, at, before[at], after)
		}
	}
}

func cmpOptions(org string) OwnershipCompactionOptions {
	return OwnershipCompactionOptions{Orgs: []string{org}, Apply: true, BatchSize: 3}
}

func cmpTable(t *testing.T, outcome OwnershipCompactionOutcome, table string) OwnershipCompactionOrg {
	t.Helper()
	for _, entry := range outcome.Tables {
		if entry.Table == table {
			return entry.Totals
		}
	}
	t.Fatalf("no report for %s", table)
	return OwnershipCompactionOrg{}
}

func cmpMutations(t *testing.T, ctx context.Context, conn driver.Conn, table string) uint64 {
	t.Helper()
	return countRows(t, ctx, conn, "SELECT count() FROM system.mutations WHERE database = currentDatabase() AND table = ?", table)
}

func TestCompactOwnershipIntervalsDryRunWritesNothing(t *testing.T) {
	f := newCmpFixture(t)
	physicalBefore := f.physicalCount(OwnershipCompactionRepoTable, f.orgA)
	opts := cmpOptions(f.orgA)
	opts.Apply = false
	outcome, err := CompactOwnershipIntervals(f.ctx, f.conn, opts)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	repo := cmpTable(t, outcome, OwnershipCompactionRepoTable)
	if repo.RunsMerged != 6 || repo.ClosedBefore != 20 || repo.ClosedAfter != 9 {
		t.Fatalf("dry run report %+v", repo)
	}
	if f.physicalCount(OwnershipCompactionRepoTable, f.orgA) != physicalBefore {
		t.Fatal("a dry run wrote rows")
	}
	if got := countRows(t, f.ctx, f.conn, "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name LIKE '%compaction_bak'"); got != 0 {
		t.Fatalf("a dry run created a backup table (%d)", got)
	}
	if cmpMutations(t, f.ctx, f.conn, OwnershipCompactionRepoTable) != 0 {
		t.Fatal("a dry run ran a mutation")
	}
}

func TestCompactOwnershipIntervalsKeepsEveryOwnerAnswer(t *testing.T) {
	f := newCmpFixture(t)
	instants := f.instants()
	before := f.snapshots(f.orgA, instants)
	beforeWriter := f.writerInputs(f.orgA)
	orgBBefore := f.snapshots(f.orgB, instants)
	repoBefore := f.rows(repoSpec(), f.orgB)

	outcome, err := CompactOwnershipIntervals(f.ctx, f.conn, cmpOptions(f.orgA))
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	f.assertSameAnswers(f.orgA, instants, before, "after the compaction")
	if after := f.writerInputs(f.orgA); after != beforeWriter {
		t.Fatalf("the tenure ends of the writer changed:\n%s\n--\n%s", beforeWriter, after)
	}
	f.assertSameAnswers(f.orgB, instants, orgBBefore, "other organization")
	if !sameRowSet(repoBefore, f.rows(repoSpec(), f.orgB)) {
		t.Fatal("another organization's rows moved")
	}

	repo := cmpTable(t, outcome, OwnershipCompactionRepoTable)
	project := cmpTable(t, outcome, OwnershipCompactionProjectTable)
	if repo.RunsMerged != 6 || repo.RunsKeptMultiSource != 1 || repo.RowsMerged != 11 {
		t.Fatalf("repo report %+v", repo)
	}
	if repo.RowsBefore != 26 || repo.ClosedBefore != 20 || repo.ClosedAfter != 9 || repo.RowsAfter != 13 || repo.OpenAfter != 2 {
		t.Fatalf("repo counts %+v", repo)
	}
	if repo.TombstonesFound != 3 || repo.TombstonesDropped != 2 || repo.TombstonesKeptAnchor != 1 || repo.TombstonesAfter != 1 || repo.CloseBeforeOpenAfter != 1 {
		t.Fatalf("repo tombstones %+v", repo)
	}
	if project.RunsMerged != 3 || project.ClosedBefore != 11 || project.ClosedAfter != 4 {
		t.Fatalf("project report %+v", project)
	}
	if repo.Differential.Mismatches != 0 || repo.Differential.InstantsChecked == 0 || repo.Differential.GroupsChecked == 0 {
		t.Fatalf("differential %+v", repo.Differential)
	}
	if repo.BackupRows != 17+2 || repo.RunsChangedDuringRun != 0 {
		t.Fatalf("backup %d, changed %d", repo.BackupRows, repo.RunsChangedDuringRun)
	}
	// The physical rows are gone, every version of them (the stale open copies too).
	if got := countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership WHERE org_id = ? AND repo_full_name = 'acme/api' AND team_id = ? AND valid_from IN (?, ?, ?)",
		f.orgA, cmpTeam1, cmpDay(1), cmpDay(2), cmpDay(3)); got != 0 {
		t.Fatalf("%d physical rows of a merged run remain (a stale version would resurrect through FINAL)", got)
	}
	// The merged row keeps the first valid_from and the last valid_to.
	if got := countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND repo_full_name = 'acme/api' AND team_id = ? AND valid_from = ? AND valid_to = ?",
		f.orgA, cmpTeam1, cmpDay(0), cmpDay(5)); got != 1 {
		t.Fatalf("merged interval rows: %d", got)
	}
	// The backup holds the rows before the rewrite.
	if got := countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership_compaction_bak WHERE org_id = ?", f.orgA); got != 19 {
		t.Fatalf("backup rows %d", got)
	}
}

func TestCompactOwnershipIntervalsSecondRunIsANoOp(t *testing.T) {
	f := newCmpFixture(t)
	if _, err := CompactOwnershipIntervals(f.ctx, f.conn, cmpOptions(f.orgA)); err != nil {
		t.Fatal(err)
	}
	snapshot := f.rows(repoSpec(), f.orgA)
	projectSnapshot := f.rows(projectSpec(), f.orgA)
	mutations := cmpMutations(t, f.ctx, f.conn, OwnershipCompactionRepoTable) + cmpMutations(t, f.ctx, f.conn, OwnershipCompactionProjectTable)
	backups := countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership_compaction_bak")
	physical := f.physicalCount(OwnershipCompactionRepoTable, f.orgA)

	outcome, err := CompactOwnershipIntervals(f.ctx, f.conn, cmpOptions(f.orgA))
	if err != nil {
		t.Fatalf("second run: %v", err)
	}
	repo := cmpTable(t, outcome, OwnershipCompactionRepoTable)
	// The multi-source run is still found and still kept; nothing else is.
	if repo.RunsMerged != 0 || repo.TombstonesDropped != 0 || repo.BackupRows != 0 || repo.RowsMerged != 0 {
		t.Fatalf("second run is not a no-op: %+v", repo)
	}
	if got := cmpMutations(t, f.ctx, f.conn, OwnershipCompactionRepoTable) + cmpMutations(t, f.ctx, f.conn, OwnershipCompactionProjectTable); got != mutations {
		t.Fatalf("second run ran a mutation (%d -> %d)", mutations, got)
	}
	if countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership_compaction_bak") != backups || f.physicalCount(OwnershipCompactionRepoTable, f.orgA) != physical {
		t.Fatal("second run wrote rows")
	}
	if !sameRowSet(snapshot, f.rows(repoSpec(), f.orgA)) || !sameRowSet(projectSnapshot, f.rows(projectSpec(), f.orgA)) {
		t.Fatal("second run changed rows")
	}
}

func TestCompactOwnershipIntervalsRestoreBringsBackTheRows(t *testing.T) {
	f := newCmpFixture(t)
	instants := f.instants()
	before := f.snapshots(f.orgA, instants)
	original := f.rows(repoSpec(), f.orgA)
	originalProjects := f.rows(projectSpec(), f.orgA)
	outcome, err := CompactOwnershipIntervals(f.ctx, f.conn, cmpOptions(f.orgA))
	if err != nil {
		t.Fatal(err)
	}
	restoreOpts := cmpOptions(f.orgA)
	restoreOpts.Apply = false
	dry, err := RestoreOwnershipCompaction(f.ctx, f.conn, outcome.RunID, restoreOpts)
	if err != nil {
		t.Fatal(err)
	}
	if got := cmpTable(t, dry, OwnershipCompactionRepoTable); got.RowsMerged == 0 || got.BackupRows == 0 {
		t.Fatalf("restore dry run %+v", got)
	}
	if sameRowSet(original, f.rows(repoSpec(), f.orgA)) {
		t.Fatal("a restore dry run changed the rows")
	}
	restoreOpts.Apply = true
	if _, err := RestoreOwnershipCompaction(f.ctx, f.conn, outcome.RunID, restoreOpts); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored := map[string]ownershipIntervalRow{}
	for _, row := range f.rows(repoSpec(), f.orgA) {
		restored[row.sortKey()] = row
	}
	for _, row := range original {
		got, ok := restored[row.sortKey()]
		if !ok || !sameInstant(got.ValidTo, row.ValidTo) || got.shape() != row.shape() {
			t.Fatalf("row %s not restored: %+v", row.sortKey(), got)
		}
	}
	for _, row := range f.rows(projectSpec(), f.orgA) {
		_ = row
	}
	restoredProjects := map[string]ownershipIntervalRow{}
	for _, row := range f.rows(projectSpec(), f.orgA) {
		restoredProjects[row.sortKey()] = row
	}
	for _, row := range originalProjects {
		got, ok := restoredProjects[row.sortKey()]
		if !ok || !sameInstant(got.ValidTo, row.ValidTo) || got.shape() != row.shape() {
			t.Fatalf("project row %s not restored", row.sortKey())
		}
	}
	f.assertSameAnswers(f.orgA, instants, before, "after the restore")
	again, err := RestoreOwnershipCompaction(f.ctx, f.conn, outcome.RunID, restoreOpts)
	if err != nil {
		t.Fatal(err)
	}
	if got := cmpTable(t, again, OwnershipCompactionRepoTable); got.RowsMerged != 0 {
		t.Fatalf("a second restore wrote %d rows", got.RowsMerged)
	}
}

// failingConn lets a test make one statement fail or lie, the way a broken
// server does.
type failingConn struct {
	driver.Conn
	failExec    func(query string) error
	failPrepare func(query string) error
	countRow    func(query string) (uint64, bool)
}

func (c failingConn) Exec(ctx context.Context, query string, args ...any) error {
	if c.failExec != nil {
		if err := c.failExec(query); err != nil {
			return err
		}
	}
	return c.Conn.Exec(ctx, query, args...)
}

func (c failingConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	if c.failPrepare != nil {
		if err := c.failPrepare(query); err != nil {
			return nil, err
		}
	}
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

type fixedRow struct{ value uint64 }

func (r fixedRow) Err() error { return nil }
func (r fixedRow) Scan(dest ...any) error {
	*(dest[0].(*uint64)) = r.value
	return nil
}
func (r fixedRow) ScanStruct(any) error { return errors.New("not supported") }

func (c failingConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if c.countRow != nil {
		if value, ok := c.countRow(query); ok {
			return fixedRow{value: value}
		}
	}
	return c.Conn.QueryRow(ctx, query, args...)
}

func TestCompactOwnershipIntervalsFailedMutationLeavesTheBackupAndTheAnswers(t *testing.T) {
	f := newCmpFixture(t)
	instants := f.instants()
	before := f.snapshots(f.orgA, instants)
	broken := failingConn{Conn: f.conn, failExec: func(query string) error {
		if strings.HasPrefix(query, "ALTER TABLE") {
			return context.DeadlineExceeded
		}
		return nil
	}}
	opts := cmpOptions(f.orgA)
	opts.Table = OwnershipCompactionRepoTable
	outcome, err := CompactOwnershipIntervals(f.ctx, broken, opts)
	if !errors.Is(err, ErrOwnershipCompactionMutation) {
		t.Fatalf("a failed mutation must fail loudly, got %v", err)
	}
	if got := countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership_compaction_bak WHERE run_id = ?", outcome.RunID); got == 0 {
		t.Fatal("the backup is gone")
	}
	// The tombstones are inert: the answers are the answers of the rows before.
	f.assertSameAnswers(f.orgA, instants, before, "between phase A and phase B")
	// A new run finishes the job: it finds the tombstones and the rest.
	if _, err := CompactOwnershipIntervals(f.ctx, f.conn, cmpOptions(f.orgA)); err != nil {
		t.Fatalf("resume: %v", err)
	}
	f.assertSameAnswers(f.orgA, instants, before, "after the resume")
	if got := countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND valid_to = valid_from", f.orgA); got != 1 {
		t.Fatalf("tombstones after the resume: %d, want the one anchor", got)
	}
}

func TestCompactOwnershipIntervalsChecksTheBackupBeforeAnyRewrite(t *testing.T) {
	f := newCmpFixture(t)
	physical := f.physicalCount(OwnershipCompactionRepoTable, f.orgA)
	cases := map[string]failingConn{
		"backup insert fails": {Conn: f.conn, failPrepare: func(query string) error {
			if strings.Contains(query, "_compaction_bak") {
				return errors.New("insert refused")
			}
			return nil
		}},
		"backup holds fewer rows than were sent": {Conn: f.conn, countRow: func(query string) (uint64, bool) {
			return 0, strings.Contains(query, "_compaction_bak")
		}},
		"backup count is short by one": {Conn: f.conn, countRow: func(query string) (uint64, bool) {
			if !strings.Contains(query, "_compaction_bak") {
				return 0, false
			}
			return 12, true
		}},
	}
	for name, conn := range cases {
		opts := cmpOptions(f.orgA)
		opts.Table = OwnershipCompactionRepoTable
		if _, err := CompactOwnershipIntervals(f.ctx, conn, opts); !errors.Is(err, ErrOwnershipCompactionBackup) {
			t.Fatalf("%s: got %v", name, err)
		}
		if got := f.physicalCount(OwnershipCompactionRepoTable, f.orgA); got != physical {
			t.Fatalf("%s: rows were rewritten before the backup was proven (%d -> %d)", name, physical, got)
		}
	}
}

func TestCompactOwnershipIntervalsReadbackFailureStopsBeforeTheDelete(t *testing.T) {
	f := newCmpFixture(t)
	opts := cmpOptions(f.orgA)
	opts.Table = OwnershipCompactionRepoTable
	opts.afterInsert = func(string) {
		// A sync re-stamps an inner row of a merged run with a newer version: the tombstone loses.
		row := cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", 1, 2, 1.0, 1)
		row.UpdatedAt = cmpDay(1000)
		f.insert(repoSpec(), f.orgA, []ownershipIntervalRow{row})
	}
	_, err := CompactOwnershipIntervals(f.ctx, f.conn, opts)
	if !errors.Is(err, ErrOwnershipCompactionReadback) && !errors.Is(err, ErrOwnershipCompactionDifferential) {
		t.Fatalf("got %v", err)
	}
	if cmpMutations(t, f.ctx, f.conn, OwnershipCompactionRepoTable) != 0 {
		t.Fatal("the delete ran after a failed readback")
	}
}

func TestCompactOwnershipIntervalsSkipsAKeyAConcurrentSyncWrote(t *testing.T) {
	f := newCmpFixture(t)
	instants := f.instants()
	opts := cmpOptions(f.orgA)
	opts.Table = OwnershipCompactionRepoTable
	opts.BatchSize = 100
	opts.beforeBatch = func(table string, batch int) {
		// A sync re-stamps one closed row of the first run (same key, newer version, closed 1 ms later),
		// and opens a new row for another key after every fixture time.
		moved := cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", 2, 3, 1.0, 1)
		end := moved.ValidTo.Add(time.Millisecond)
		moved.ValidTo, moved.UpdatedAt = &end, cmpDay(2000)
		f.insert(repoSpec(), f.orgA, []ownershipIntervalRow{moved, cmpRepoRow(cmpTeam2, cmpRepo3, "acme/late", 500, -1, 1, 1)})
	}
	before := f.snapshots(f.orgA, instants)
	_ = before
	outcome, err := CompactOwnershipIntervals(f.ctx, f.conn, opts)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	repo := cmpTable(t, outcome, OwnershipCompactionRepoTable)
	if repo.RunsChangedDuringRun != 1 {
		t.Fatalf("a run changed during the run must be skipped and reported: %+v", repo)
	}
	// The skipped key still has its closed rows, the sync's version wins, and no merged row spans it.
	if got := countRows(t, f.ctx, f.conn, "SELECT count() FROM team_repo_ownership FINAL WHERE org_id = ? AND team_id = ? AND repo_full_name = 'acme/api' AND valid_from = ? AND valid_to > ?",
		f.orgA, cmpTeam1, cmpDay(0), cmpDay(1)); got != 0 {
		t.Fatalf("the skipped run was merged anyway (%d)", got)
	}
	// Answers at every fixture instant are unchanged except where the sync's own 1 ms change applies.
	for _, at := range instants {
		if at.Equal(cmpDay(3)) {
			continue
		}
		if f.readerSnapshot(f.orgA, at) != before[at] && !at.After(cmpDay(2)) {
			t.Fatalf("the answer changed at %s", at)
		}
	}
	// A second run plans from what the sync left; it never half applies a key.
	opts.beforeBatch = nil
	if _, err := CompactOwnershipIntervals(f.ctx, f.conn, opts); err != nil {
		t.Fatalf("second run: %v", err)
	}
}

// A sync that extends the key while the tool runs (a new row after the run) does
// not stop the merge and does not change any answer at the fixture's instants.
func TestCompactOwnershipIntervalsKeyExtendedByASyncKeepsTheAnswers(t *testing.T) {
	f := newCmpFixture(t)
	instants := f.instants()
	before := f.snapshots(f.orgA, instants)
	opts := cmpOptions(f.orgA)
	opts.Table = OwnershipCompactionRepoTable
	opts.BatchSize = 100
	opts.beforeBatch = func(string, int) {
		f.insert(repoSpec(), f.orgA, []ownershipIntervalRow{
			cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", 100, 101, 0.6, 1),
			cmpRepoRow(cmpTeam1, cmpRepo1, "acme/api", 101, -1, 0.6, 1),
		})
	}
	outcome, err := CompactOwnershipIntervals(f.ctx, f.conn, opts)
	if err != nil {
		t.Fatal(err)
	}
	repo := cmpTable(t, outcome, OwnershipCompactionRepoTable)
	if repo.RunsChangedDuringRun != 0 || repo.RunsMerged != 6 || repo.RowsAppearedDuringRun != 2 {
		t.Fatalf("report %+v", repo)
	}
	var early []time.Time
	for _, at := range instants {
		if at.Before(cmpDay(50)) {
			early = append(early, at)
		}
	}
	f.assertSameAnswers(f.orgA, early, before, "key extended by a sync")
	again, err := CompactOwnershipIntervals(f.ctx, f.conn, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got := cmpTable(t, again, OwnershipCompactionRepoTable); got.RunsMerged != 0 {
		t.Fatalf("second run %+v", got)
	}
}

func TestCompactOwnershipIntervalsAllOrgsAndTableFilter(t *testing.T) {
	f := newCmpFixture(t)
	opts := OwnershipCompactionOptions{AllOrgs: true, Apply: true, Table: OwnershipCompactionProjectTable}
	outcome, err := CompactOwnershipIntervals(f.ctx, f.conn, opts)
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.Tables) != 1 || len(outcome.Tables[0].Orgs) != 2 {
		t.Fatalf("outcome %+v", outcome)
	}
	if f.physicalCount(OwnershipCompactionRepoTable, f.orgA) == 0 || cmpTable(t, outcome, OwnershipCompactionProjectTable).RunsMerged != 4 {
		t.Fatalf("project totals %+v", outcome.Tables[0].Totals)
	}
	for _, org := range outcome.Tables[0].Orgs {
		if org.OrgOrdinal == 0 {
			t.Fatal("the report must number the organizations")
		}
	}
	if _, err := CompactOwnershipIntervals(f.ctx, f.conn, OwnershipCompactionOptions{Orgs: []string{f.orgA}, AllOrgs: true}); err == nil {
		t.Fatal("organizations and all organizations together must be refused")
	}
	if _, err := CompactOwnershipIntervals(f.ctx, f.conn, OwnershipCompactionOptions{Table: "teams", AllOrgs: true}); err == nil {
		t.Fatal("an unknown table must be refused")
	}
}

// TestCompactOwnershipIntervalsAtProdSize is the timing rehearsal: the closed
// interval counts of prod (67,462 native project, 44,367 inferred repo) as runs
// of 11 with a real change in every fourth key. Set OWNERSHIP_COMPACTION_SCALE=1
// to run it.
func TestCompactOwnershipIntervalsAtProdSize(t *testing.T) {
	if os.Getenv("OWNERSHIP_COMPACTION_SCALE") != "1" {
		t.Skip("set OWNERSHIP_COMPACTION_SCALE=1 for the prod-size timing run")
	}
	ctx, conn := newWorkItemEffectsConn(t)
	seed := func(table, natural, extraColumns, extraValues string, keys, orgs int, source string) {
		query := fmt.Sprintf("INSERT INTO %s (org_id, provider, team_id, %s, source, is_primary, specificity, priority, valid_from, valid_to, updated_at%s) "+
			"SELECT concat('org-', toString(k %% %d)), 'github', concat('gh:team-', toString(k %% 50)), concat('key-', toString(k)), '%s', 1, 100, "+
			"if(k %% 4 = 0 AND i >= 5, 20, 10), "+
			"toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + toIntervalDay(i), "+
			"if(i < 11, toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + toIntervalDay(i + 1), NULL), "+
			"toDateTime64('2026-01-01 00:00:00', 3, 'UTC') + toIntervalDay(i + 1)%s "+
			"FROM (SELECT number AS k FROM numbers(%d)) AS keys ARRAY JOIN range(12) AS i",
			table, natural, extraColumns, orgs, source, extraValues, keys)
		if err := conn.Exec(ctx, query); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
	seed(OwnershipCompactionProjectTable, "project_id", "", "", 6133, 6, "native")
	seed(OwnershipCompactionRepoTable, "repo_full_name", ", match_type, share", ", 'exact', if(k % 4 = 0 AND i >= 5, 0.5, 1.0)", 4033, 5, "inferred")
	count := func(table string) uint64 {
		return countRows(t, ctx, conn, "SELECT count() FROM "+table+" FINAL WHERE valid_to IS NOT NULL")
	}
	t.Logf("closed intervals before: project %d, repo %d", count(OwnershipCompactionProjectTable), count(OwnershipCompactionRepoTable))
	// The production readers answer at every boundary and midpoint of two
	// organizations before and after.
	probe := cmpFixture{t: t, ctx: ctx, conn: conn}
	probeOrgs := []string{"org-0", "org-1"}
	probeInstants := map[string][]time.Time{}
	probeBefore := map[string]map[time.Time]string{}
	for _, org := range probeOrgs {
		var rows []ownershipIntervalRow
		for _, spec := range ownershipCompactionSpecs {
			rows = append(rows, probe.rows(spec, org)...)
		}
		probeInstants[org] = instantsOf(nil, nil, rows)
		probeBefore[org] = probe.snapshots(org, probeInstants[org])
	}
	started := time.Now()
	dry, err := CompactOwnershipIntervals(ctx, conn, OwnershipCompactionOptions{AllOrgs: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("dry run: %s", time.Since(started))
	started = time.Now()
	outcome, err := CompactOwnershipIntervals(ctx, conn, OwnershipCompactionOptions{AllOrgs: true, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("apply: %s", time.Since(started))
	dryClosedAfter := map[string]int{}
	for _, table := range dry.Tables {
		dryClosedAfter[table.Table] = table.Totals.ClosedAfter
	}
	for _, table := range outcome.Tables {
		t.Logf("%s: closed %d -> %d, runs merged %d, rows merged %d, differential %+v, planned closed after (dry) %d",
			table.Table, table.Totals.ClosedBefore, table.Totals.ClosedAfter, table.Totals.RunsMerged, table.Totals.RowsMerged,
			table.Totals.Differential, dryClosedAfter[table.Table])
		if table.Totals.Differential.Mismatches != 0 {
			t.Fatalf("differential %+v", table.Totals.Differential)
		}
	}
	t.Logf("closed intervals after: project %d, repo %d", count(OwnershipCompactionProjectTable), count(OwnershipCompactionRepoTable))
	for _, org := range probeOrgs {
		probe.assertSameAnswers(org, probeInstants[org], probeBefore[org], "prod-size sample "+org)
		t.Logf("production readers: %d instants of one organization identical before and after", len(probeInstants[org]))
	}
	started = time.Now()
	again, err := CompactOwnershipIntervals(ctx, conn, OwnershipCompactionOptions{AllOrgs: true, Apply: true})
	if err != nil {
		t.Fatal(err)
	}
	if again.Tables[0].Totals.RunsMerged != 0 || again.Tables[1].Totals.RunsMerged != 0 {
		t.Fatalf("second run merged runs: %+v", again.Tables)
	}
	t.Logf("second run: %s", time.Since(started))
}
