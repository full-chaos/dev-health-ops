//go:build integration

package chmigrate_test

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/chmigrate"
)

const (
	groupsRefreshView = "project_team_ownership_groups_refresh"
	groupsOrgA        = "org-147-a"
	groupsOrgB        = "org-147-b"
)

// groupsRowKeySQL is the reader's row key over a group's provider, project,
// team and source: each escaped '%' then ':', joined with ':'.
const groupsRowKeySQL = `concat(replaceAll(replaceAll(provider, '%', '%25'), ':', '%3A'), ':',
       replaceAll(replaceAll(project_id, '%', '%25'), ':', '%3A'), ':',
       replaceAll(replaceAll(team_id, '%', '%25'), ':', '%3A'), ':',
       replaceAll(replaceAll(source_name, '%', '%25'), ':', '%3A'))`

// groupsContentSQL renders one group's content, the columns the reader scans.
// conflict_identities is a set: sorted on both sides.
const groupsContentSQL = `concat(project_id, '|', team_id, '|', source_name, '|', provider, '|',
       toString(toDateTime64(first_valid_from, 3, 'UTC')), '|', toString(toUInt8(latest_is_open)), '|',
       toString(toDateTime64(latest_valid_to, 3, 'UTC')), '|', toString(toDateTime64(observed_at, 3, 'UTC')), '|',
       toString(toUInt8(edge_suppressed)), '|', toString(arraySort(conflict_identities)), '|', toString(toUInt8(conflicting_identity_present)))`

func groupsExec(ctx context.Context, t *testing.T, conn driver.Conn, query string, args ...any) {
	t.Helper()
	if err := conn.Exec(ctx, query, args...); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func groupsMap(ctx context.Context, t *testing.T, conn driver.Conn, query string) map[string]string {
	t.Helper()
	rows, err := conn.Query(ctx, query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var key, content string
		if err := rows.Scan(&key, &content); err != nil {
			t.Fatal(err)
		}
		if _, dup := out[key]; dup {
			t.Fatalf("row key %q twice in %s", key, query)
		}
		out[key] = content
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// oracleGroups runs the reader's own statement for one organization.
func oracleGroups(ctx context.Context, t *testing.T, conn driver.Conn, org string) map[string]string {
	t.Helper()
	statement := strings.ReplaceAll(acrProjectTeamGroupsSQL, "{org_id:String}", "'"+org+"'")
	return groupsMap(ctx, t, conn, "SELECT "+groupsRowKeySQL+", "+groupsContentSQL+" FROM ("+statement+")")
}

// storedGroups reads the current, not deleted groups of one organization.
func storedGroups(ctx context.Context, t *testing.T, conn driver.Conn, org string) map[string]string {
	t.Helper()
	return groupsMap(ctx, t, conn, "SELECT row_key, "+groupsContentSQL+" FROM project_team_ownership_groups_current FINAL WHERE org_id = '"+org+"' AND is_deleted = 0")
}

type appendedGroup struct {
	org, rowKey string
	deleted     uint8
}

// appendedSince reads the log rows stored after since. The mark is bound in
// microseconds: clickhouse-go renders a positional time.Time argument as a
// whole-second toDateTime, which would count an earlier refresh of the same
// second as appended after the mark.
func appendedSince(ctx context.Context, t *testing.T, conn driver.Conn, since time.Time) []appendedGroup {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT org_id, row_key, is_deleted FROM project_team_ownership_groups_log WHERE stored_at > fromUnixTimestamp64Micro(?, 'UTC') ORDER BY org_id, row_key", since.UnixMicro())
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []appendedGroup
	for rows.Next() {
		var g appendedGroup
		if err := rows.Scan(&g.org, &g.rowKey, &g.deleted); err != nil {
			t.Fatal(err)
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func serverNow(ctx context.Context, t *testing.T, conn driver.Conn) time.Time {
	t.Helper()
	var now time.Time
	if err := conn.QueryRow(ctx, "SELECT now64(6, 'UTC')").Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}

func refreshGroups(ctx context.Context, t *testing.T, conn driver.Conn) {
	t.Helper()
	groupsExec(ctx, t, conn, "SYSTEM REFRESH VIEW "+groupsRefreshView)
	groupsExec(ctx, t, conn, "SYSTEM WAIT VIEW "+groupsRefreshView)
}

// changedGroups is every group of an organization whose oracle content differs
// between before and after, new and gone included, with gone marked deleted.
func changedGroups(org string, before, after map[string]string) []appendedGroup {
	var out []appendedGroup
	for key, content := range after {
		if old, ok := before[key]; !ok || old != content {
			out = append(out, appendedGroup{org: org, rowKey: key})
		}
	}
	for key := range before {
		if _, ok := after[key]; !ok {
			out = append(out, appendedGroup{org: org, rowKey: key, deleted: 1})
		}
	}
	return out
}

func sortAppended(groups []appendedGroup) []appendedGroup {
	sort.Slice(groups, func(i, j int) bool {
		if groups[i].org != groups[j].org {
			return groups[i].org < groups[j].org
		}
		return groups[i].rowKey < groups[j].rowKey
	})
	return groups
}

// seedGroupsOrg writes one organization's projects, teams and ownership: an id
// match, a key held in project_id, an ambiguous key (two projects), an identity
// conflict (project_id and project_key resolve to different projects), an open
// assertion closed by a later one, the same project id under a second provider,
// an inactive team and a NULL project key. Org B gets the same rows minus one.
func seedGroupsOrg(ctx context.Context, t *testing.T, conn driver.Conn, org string, full bool) {
	t.Helper()
	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	project := func(provider, id, key string, updated time.Time) {
		keyValue := any(key)
		if key == "" {
			keyValue = nil
		}
		groupsExec(ctx, t, conn, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, url, updated_at, last_synced) VALUES (?, ?, ?, ?, ?, 1, 'started', '', ?, ?)`,
			id, org, provider, keyValue, id, updated, updated)
	}
	team := func(id string, active uint8) {
		groupsExec(ctx, t, conn, `INSERT INTO teams (id, team_uuid, name, updated_at, last_synced, org_id, provider, is_active) VALUES (?, generateUUIDv4(), ?, ?, ?, ?, 'github', ?)`,
			id, id, at, at, org, active)
	}
	owner := func(provider, teamID, projectRef, key, source string, from time.Time, to *time.Time, updated time.Time) {
		keyValue := any(key)
		if key == "" {
			keyValue = nil
		}
		groupsExec(ctx, t, conn, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, valid_from, valid_to, updated_at, last_synced) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			org, provider, teamID, projectRef, keyValue, source, from, to, updated, updated)
	}
	project("github", "P1", "K1", at)
	project("github", "P2", "K2", at)
	project("github", "P3", "AMB", at)
	project("github", "P4", "AMB", at)
	project("github", "P5", "", at)
	project("gitlab", "P1", "GL1", at)
	team("T1", 1)
	team("T2", 1)
	team("T3", 0)
	closed := at.Add(48 * time.Hour)
	owner("github", "T1", "P1", "K1", "native", at, nil, at.Add(time.Hour))
	owner("github", "T1", "K2", "", "native", at, nil, at.Add(time.Hour))
	owner("github", "T2", "P3", "AMB", "native", at, nil, at.Add(time.Hour))
	owner("github", "T2", "P1", "K2", "manual", at, nil, at.Add(time.Hour))
	owner("github", "T2", "P2", "K2", "native", at, nil, at.Add(time.Hour))
	owner("github", "T2", "P2", "K2", "native", at.Add(24*time.Hour), &closed, at.Add(2*time.Hour))
	owner("github", "T3", "P5", "", "native", at, nil, at.Add(time.Hour))
	owner("gitlab", "T1", "P1", "GL1", "native", at, nil, at.Add(time.Hour))
	if full {
		owner("github", "T1", "P5", "", "inferred", at, nil, at.Add(time.Hour))
	}
}

// groupsHead migrates a scratch database to the head and seeds two
// organizations that share every project and team id.
func groupsHead(ctx context.Context, t *testing.T) driver.Conn {
	t.Helper()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	seedGroupsOrg(ctx, t, conn, groupsOrgA, true)
	seedGroupsOrg(ctx, t, conn, groupsOrgB, false)
	return conn
}

func requireStoredEqualsOracle(ctx context.Context, t *testing.T, conn driver.Conn, when string) map[string]map[string]string {
	t.Helper()
	oracle := map[string]map[string]string{}
	for _, org := range []string{groupsOrgA, groupsOrgB} {
		want, got := oracleGroups(ctx, t, conn, org), storedGroups(ctx, t, conn, org)
		if len(want) == 0 {
			t.Fatalf("%s %s: the oracle returns no group, the comparison proves nothing", when, org)
		}
		for key, content := range want {
			if got[key] != content {
				t.Errorf("%s %s group %s:\nstored %q\noracle %q", when, org, key, got[key], content)
			}
		}
		for key := range got {
			if _, ok := want[key]; !ok {
				t.Errorf("%s %s: stored group %s is not in the oracle", when, org, key)
			}
		}
		oracle[org] = want
	}
	if t.Failed() {
		t.FailNow()
	}
	return oracle
}

// O1: after a refresh, the stored current groups of every organization equal
// the reader's own per-organization statement, row for row and column for
// column. The fixture holds a suppressed (retraction) group and a conflicting
// group, so the retraction and conflict columns are compared, not defaults.
func TestMigration147StoredGroupsEqualTheReadersStatement(t *testing.T) {
	ctx := context.Background()
	conn := groupsHead(ctx, t)
	refreshGroups(ctx, t, conn)
	oracle := requireStoredEqualsOracle(ctx, t, conn, "first refresh")
	suppressed, conflicting := 0, 0
	for _, content := range oracle[groupsOrgA] {
		fields := strings.Split(content, "|")
		if fields[8] == "1" {
			suppressed++
		}
		if fields[10] == "1" {
			conflicting++
		}
	}
	if suppressed == 0 || conflicting == 0 || len(oracle[groupsOrgA]) == len(oracle[groupsOrgB]) {
		t.Fatalf("non-vacuity: suppressed=%d conflicting=%d groups A=%d B=%d -- the fixture must hold a suppressed group, a conflicting group, and differ between the organizations",
			suppressed, conflicting, len(oracle[groupsOrgA]), len(oracle[groupsOrgB]))
	}
	t.Logf("groups A=%d B=%d, suppressed %d, conflicting %d: equal to the oracle", len(oracle[groupsOrgA]), len(oracle[groupsOrgB]), suppressed, conflicting)
}

// O2 + O3: a refresh with no source change appends nothing; each change to an
// input table appends exactly the groups whose oracle content changed (new and
// gone included, gone as is_deleted = 1), in the organization that changed,
// and the current groups equal the oracle again after each.
func TestMigration147RefreshAppendsExactlyTheChangedGroups(t *testing.T) {
	ctx := context.Background()
	conn := groupsHead(ctx, t)
	refreshGroups(ctx, t, conn)
	before := requireStoredEqualsOracle(ctx, t, conn, "first refresh")

	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	steps := []struct {
		name   string
		change func()
		empty  bool
	}{
		{"no change", func() {}, true},
		{"ownership: a later assertion closes T1's P1", func() {
			closed := at.Add(72 * time.Hour)
			groupsExec(ctx, t, conn, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, valid_from, valid_to, updated_at, last_synced) VALUES (?, 'github', 'T1', 'P1', 'K1', 'native', ?, ?, ?, ?)`,
				groupsOrgA, at.Add(48*time.Hour), &closed, at.Add(5*time.Hour), at.Add(5*time.Hour))
		}, false},
		{"projects: P2 updated", func() {
			later := at.Add(6 * time.Hour)
			groupsExec(ctx, t, conn, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, url, updated_at, last_synced) VALUES ('P2', ?, 'github', 'K2', 'P2 renamed', 1, 'started', '', ?, ?)`,
				groupsOrgA, later, later)
		}, false},
		{"teams: T1 deactivated", func() {
			later := at.Add(7 * time.Hour)
			groupsExec(ctx, t, conn, `INSERT INTO teams (id, team_uuid, name, updated_at, last_synced, org_id, provider, is_active) VALUES ('T1', generateUUIDv4(), 'T1', ?, ?, ?, 'github', 0)`,
				later, later, groupsOrgA)
		}, false},
		{"ownership: T2's manual (conflicting) row removed", func() {
			groupsExec(ctx, t, conn, `ALTER TABLE team_project_ownership DELETE WHERE org_id = ? AND team_id = 'T2' AND source = 'manual' SETTINGS mutations_sync = 2`, groupsOrgA)
		}, false},
		{"no change after the removal", func() {}, true},
		{"no change after the removal, again", func() {}, true},
	}
	for _, step := range steps {
		mark := serverNow(ctx, t, conn)
		step.change()
		want := changedGroups(groupsOrgA, before[groupsOrgA], oracleGroups(ctx, t, conn, groupsOrgA))
		want = append(want, changedGroups(groupsOrgB, before[groupsOrgB], oracleGroups(ctx, t, conn, groupsOrgB))...)
		if step.empty != (len(want) == 0) {
			t.Fatalf("%s: the oracle changed %d groups; the step is built to change %v", step.name, len(want), !step.empty)
		}
		refreshGroups(ctx, t, conn)
		got := appendedSince(ctx, t, conn, mark)
		if fmt.Sprint(sortAppended(got)) != fmt.Sprint(sortAppended(want)) {
			dumpGroupKeys(ctx, t, conn, step.name, got, want)
			t.Fatalf("%s: appended %v, want exactly %v", step.name, got, want)
		}
		before = requireStoredEqualsOracle(ctx, t, conn, step.name)
		t.Logf("%s: %d groups appended, equal to the oracle's changes", step.name, len(got))
	}
}

// dumpGroupKeys logs, column by column, every key appended or wanted in one
// set but not the other: its log rows and its current rows in stamp order
// (current with the part each row is in, read without FINAL), current FINAL,
// and the oracle; then the parts of the two tables, the view's refresh state
// and the refresh inserts in query_log. Every line starts with DIAG.
func dumpGroupKeys(ctx context.Context, t *testing.T, conn driver.Conn, label string, got, want []appendedGroup) {
	t.Helper()
	count := map[appendedGroup]int{}
	for _, g := range got {
		count[g]++
	}
	for _, g := range want {
		count[g]--
	}
	keys := map[[2]string]bool{}
	for g, n := range count {
		if n != 0 {
			keys[[2]string{g.org, g.rowKey}] = true
			t.Logf("DIAG %s: %+v appended %+d times more than wanted", label, g, n)
		}
	}
	lines := func(query string, args ...any) []string {
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			return []string{"query failed: " + err.Error()}
		}
		defer rows.Close()
		var out []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				return append(out, "scan failed: "+err.Error())
			}
			out = append(out, line)
		}
		return out
	}
	const row = "concat(toString(stored_at), ' deleted=', toString(is_deleted), ' hash=', toString(content_hash), ' ', " + groupsContentSQL + ")"
	for key := range keys {
		org, rowKey := key[0], key[1]
		t.Logf("DIAG %s key %s %s", label, org, rowKey)
		for _, l := range lines("SELECT "+row+" FROM project_team_ownership_groups_log WHERE org_id = ? AND row_key = ? ORDER BY stored_at", org, rowKey) {
			t.Logf("DIAG   log      %s", l)
		}
		for _, l := range lines("SELECT concat(_part, ' ', "+row+") FROM project_team_ownership_groups_current WHERE org_id = ? AND row_key = ? ORDER BY stored_at", org, rowKey) {
			t.Logf("DIAG   current  %s", l)
		}
		for _, l := range lines("SELECT "+row+" FROM project_team_ownership_groups_current FINAL WHERE org_id = ? AND row_key = ?", org, rowKey) {
			t.Logf("DIAG   final    %s", l)
		}
		t.Logf("DIAG   oracle   %s", oracleGroups(ctx, t, conn, org)[rowKey])
	}
	for _, l := range lines("SELECT concat(table, ' active parts=', toString(count()), ' rows=', toString(sum(rows))) FROM system.parts WHERE database = currentDatabase() AND active AND table LIKE 'project_team_ownership_groups%' GROUP BY table") {
		t.Logf("DIAG parts %s", l)
	}
	for _, l := range lines("SELECT toString(tuple(*)) FROM system.view_refreshes WHERE database = currentDatabase()") {
		t.Logf("DIAG view_refreshes %s", l)
	}
	if err := conn.Exec(ctx, "SYSTEM FLUSH LOGS"); err == nil {
		for _, l := range lines(`SELECT concat(toString(query_start_time_microseconds), ' -> ', toString(event_time_microseconds), ' written=', toString(written_rows), ' ', query_id)
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Insert' AND has(databases, currentDatabase())
  AND position(query, 'project_team_ownership_groups_log') > 0 AND event_time > now() - INTERVAL 15 MINUTE
ORDER BY query_start_time_microseconds`) {
			t.Logf("DIAG refresh insert %s", l)
		}
	}
}

// O2 repeated: with merges of both tables stopped (every refresh's rows stay in
// their own part, so current holds several versions of a group side by side)
// and after three changes that give the same groups new versions, at least
// twenty refreshes with no source change, run until the server clock has passed
// the view's next scheduled refresh so the schedule fires among them, each
// append nothing, and current
// equals the oracle after each. A comparison that reads more than the latest
// row of a group, or a schedule that interleaves with a triggered refresh,
// shows up here as appended rows.
func TestMigration147RepeatedNoChangeRefreshesAppendNothing(t *testing.T) {
	ctx := context.Background()
	conn := groupsHead(ctx, t)
	groupsExec(ctx, t, conn, "SYSTEM STOP MERGES project_team_ownership_groups_current")
	groupsExec(ctx, t, conn, "SYSTEM STOP MERGES project_team_ownership_groups_log")
	refreshGroups(ctx, t, conn)
	before := requireStoredEqualsOracle(ctx, t, conn, "first refresh")

	at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	changes := []struct {
		name   string
		change func()
	}{
		{"projects: P2 updated", func() {
			later := at.Add(6 * time.Hour)
			groupsExec(ctx, t, conn, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, url, updated_at, last_synced) VALUES ('P2', ?, 'github', 'K2', 'P2 renamed', 1, 'started', '', ?, ?)`,
				groupsOrgA, later, later)
		}},
		{"projects: P2 updated again", func() {
			later := at.Add(8 * time.Hour)
			groupsExec(ctx, t, conn, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, url, updated_at, last_synced) VALUES ('P2', ?, 'github', 'K2', 'P2 renamed twice', 1, 'started', '', ?, ?)`,
				groupsOrgA, later, later)
		}},
		{"ownership: a later assertion closes T1's P1", func() {
			closed := at.Add(72 * time.Hour)
			groupsExec(ctx, t, conn, `INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, valid_from, valid_to, updated_at, last_synced) VALUES (?, 'github', 'T1', 'P1', 'K1', 'native', ?, ?, ?, ?)`,
				groupsOrgA, at.Add(48*time.Hour), &closed, at.Add(5*time.Hour), at.Add(5*time.Hour))
		}},
	}
	for _, step := range changes {
		mark := serverNow(ctx, t, conn)
		step.change()
		want := changedGroups(groupsOrgA, before[groupsOrgA], oracleGroups(ctx, t, conn, groupsOrgA))
		if len(want) == 0 {
			t.Fatalf("%s: the oracle changed no group; the step is built to change some", step.name)
		}
		refreshGroups(ctx, t, conn)
		if got := appendedSince(ctx, t, conn, mark); fmt.Sprint(sortAppended(got)) != fmt.Sprint(sortAppended(want)) {
			dumpGroupKeys(ctx, t, conn, step.name, got, want)
			t.Fatalf("%s: appended %v, want exactly %v", step.name, got, want)
		}
		before = requireStoredEqualsOracle(ctx, t, conn, step.name)
	}
	var nextTick time.Time
	if err := conn.QueryRow(ctx, "SELECT next_refresh_time FROM system.view_refreshes WHERE database = currentDatabase() AND view = ?", groupsRefreshView).Scan(&nextTick); err != nil {
		t.Fatal(err)
	}
	start := serverNow(ctx, t, conn)
	crossed := false
	for i := 1; i <= 40 && (i <= 20 || !crossed); i++ {
		mark := serverNow(ctx, t, conn)
		refreshGroups(ctx, t, conn)
		if got := appendedSince(ctx, t, conn, mark); len(got) != 0 {
			label := fmt.Sprintf("no-change refresh %d", i)
			dumpGroupKeys(ctx, t, conn, label, got, nil)
			t.Fatalf("%s appended %v, want nothing", label, got)
		}
		requireStoredEqualsOracle(ctx, t, conn, fmt.Sprintf("no-change refresh %d", i))
		crossed = serverNow(ctx, t, conn).After(nextTick)
		time.Sleep(3500 * time.Millisecond)
	}
	if !crossed {
		t.Fatalf("the no-change refreshes never passed the view's scheduled refresh at %v: the cell did not cross the schedule", nextTick)
	}
	if got := appendedSince(ctx, t, conn, start); len(got) != 0 {
		dumpGroupKeys(ctx, t, conn, "the whole no-change loop", got, nil)
		t.Fatalf("refreshes appended %v during the no-change loop (the schedule included), want nothing", got)
	}
}

// O4: the projects key index is there and a key lookup reads fewer granules
// than the organization holds.
func TestMigration148ProjectKeyIndexSkipsGranules(t *testing.T) {
	ctx := context.Background()
	scratch := scratchDatabase(t)
	conn := openDatabase(t, scratch.URI, "")
	db, _, err := chmigrate.NewConnDB(ctx, conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := chmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	chain, err := chmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := chmigrate.Upgrade(ctx, db, baseline, chain); err != nil {
		t.Fatalf("real chain: %v", err)
	}
	groupsExec(ctx, t, conn, `INSERT INTO projects (id, org_id, provider, project_key, name, is_active, state, url, updated_at)
SELECT concat('P-', leftPad(toString(number), 6, '0')), 'org-148', 'github', concat('KEY-', toString(number)), 'bulk', 1, 'started', '', toDateTime64('2026-09-01 00:00:00', 3, 'UTC')
FROM numbers(40000)`)
	groupsExec(ctx, t, conn, "OPTIMIZE TABLE projects FINAL")
	rows, err := conn.Query(ctx, "EXPLAIN indexes = 1 SELECT count() FROM projects WHERE org_id = 'org-148' AND ifNull(project_key, '') IN ('KEY-31000')")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, strings.TrimSpace(line))
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	selected, total := -1, -1
	for i, line := range plan {
		if line == "Name: project_key_bf" {
			for _, next := range plan[i+1:] {
				if _, err := fmt.Sscanf(next, "Granules: %d/%d", &selected, &total); err == nil {
					break
				}
			}
			break
		}
	}
	if selected < 0 || total <= 1 || selected >= total {
		t.Fatalf("project_key_bf selected %d of %d granules, want fewer than all (and the index in the plan):\n%s", selected, total, strings.Join(plan, "\n"))
	}
	t.Logf("project_key_bf: %d of %d granules", selected, total)
}
