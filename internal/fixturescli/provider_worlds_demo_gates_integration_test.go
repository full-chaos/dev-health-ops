//go:build integration

package fixturescli

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// The demo world's gates: what the six delivery questions asked of a demo organization need the world to
// hold, read from the frozen world's rows (no store is opened). Each gate names the answer gap it closes.
// -demo-gate-world runs them over another world file, so the same tests show a world without the rows
// failing (the red half of each gate's proof).

var demoGateWorldFile = flag.String("demo-gate-world", "", "a frozen demo world file the demo gates read instead of the committed demo world")

// demoGateWorld is the world the gates read and its instant.
func demoGateWorld(t *testing.T) (FrozenWorld, time.Time) {
	t.Helper()
	var world FrozenWorld
	if *demoGateWorldFile == "" {
		loaded, err := LoadFrozenWorld(demoWorld.Params)
		if err != nil {
			t.Fatal(err)
		}
		world = loaded
	} else {
		raw, err := os.ReadFile(*demoGateWorldFile)
		if err != nil {
			t.Fatal(err)
		}
		if world, err = decodeWorld(raw); err != nil {
			t.Fatal(err)
		}
	}
	frozenAt, err := time.Parse(time.RFC3339Nano, world.FrozenAt)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("world %s, frozen at %s", world.Params, world.FrozenAt)
	return world, frozenAt.UTC()
}

// demoGateRows are a table's rows by column name; a table the world does not hold has none.
func demoGateRows(world FrozenWorld, name string) []map[string]any {
	for _, table := range world.Tables {
		if table.Name != name {
			continue
		}
		rows := make([]map[string]any, 0, len(table.Rows))
		for _, row := range table.Rows {
			named := make(map[string]any, len(row))
			for index, column := range table.Columns {
				named[column.Name] = row[index]
			}
			rows = append(rows, named)
		}
		return rows
	}
	return nil
}

func gateText(value any) string {
	text, _ := value.(string)
	return text
}

func gateTime(value any) (time.Time, bool) {
	text, ok := value.(string)
	if !ok || text == "" {
		return time.Time{}, false
	}
	parsed, err := parseWorldTime(text)
	return parsed, err == nil
}

func gateNumber(value any) (float64, bool) {
	number, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	parsed, err := number.Float64()
	return parsed, err == nil
}

func gateDay(at time.Time) string { return at.UTC().Format(time.DateOnly) }

// demoGateWorkItems are the tracker items (no pull or merge request), newest version of each.
func demoGateWorkItems(world FrozenWorld) map[string]map[string]any {
	items := map[string]map[string]any{}
	for _, row := range demoGateRows(world, "work_items") {
		id := gateText(row["work_item_id"])
		if strings.HasPrefix(id, "ghpr:") || strings.Contains(id, "!") {
			continue
		}
		if held, ok := items[id]; ok && gateText(held["last_synced"]) >= gateText(row["last_synced"]) {
			continue
		}
		items[id] = row
	}
	return items
}

// demoGateRepoOwners are each repository's current owning teams (team_repo_ownership rows that are not
// closed), by repository id.
func demoGateRepoOwners(world FrozenWorld) map[string]map[string]bool {
	byName := map[string]string{}
	for _, row := range demoGateRows(world, "repos") {
		byName[gateText(row["repo"])] = gateText(row["id"])
	}
	owners := map[string]map[string]bool{}
	for _, row := range demoGateRows(world, "team_repo_ownership") {
		if row["valid_to"] != nil {
			continue
		}
		repoID := gateText(row["repo_id"])
		if repoID == "" {
			repoID = byName[gateText(row["repo_full_name"])]
		}
		if repoID == "" {
			continue
		}
		if owners[repoID] == nil {
			owners[repoID] = map[string]bool{}
		}
		owners[repoID][gateText(row["team_id"])] = true
	}
	return owners
}

// G1: a capacity forecast needs a completion history longer than its 14-day minimum on every scale the
// question reads (the world's whole window), and "commitments this quarter" need committed dates: the
// Linear projects' target dates (the only committed date a provider sync writes, projects.target_date).
func TestDemoGateG1CapacityHistoryAndCommittedDates(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	weeks := map[string]bool{}
	var earliest time.Time
	for _, item := range demoGateWorkItems(world) {
		completed, ok := gateTime(item["completed_at"])
		if !ok {
			continue
		}
		year, week := completed.ISOWeek()
		weeks[fmt.Sprintf("%d-W%02d", year, week)] = true
		if earliest.IsZero() || completed.Before(earliest) {
			earliest = completed
		}
	}
	if earliest.IsZero() || frozenAt.Sub(earliest) < 150*24*time.Hour {
		t.Errorf("the earliest completed item is %s, less than 150 days before the instant %s: the window holds no half-year of history", earliest, frozenAt)
	}
	if len(weeks) < 20 {
		t.Errorf("items were completed in %d distinct weeks, want at least 20", len(weeks))
	}
	latest := map[string]map[string]any{}
	for _, row := range demoGateRows(world, "projects") {
		if gateText(row["provider"]) != "linear" {
			continue
		}
		id := gateText(row["id"])
		if held, ok := latest[id]; ok && gateText(held["updated_at"]) >= gateText(row["updated_at"]) {
			continue
		}
		latest[id] = row
	}
	if len(latest) == 0 {
		t.Fatal("the world holds no Linear project")
	}
	for id, row := range latest {
		target, ok := gateTime(row["target_date"])
		if !ok {
			t.Errorf("Linear project %s (%s) has no target date", id, gateText(row["name"]))
			continue
		}
		if !target.After(frozenAt) {
			t.Errorf("Linear project %s (%s) targets %s, not after the instant %s: no commitment is still ahead", id, gateText(row["name"]), gateDay(target), frozenAt)
		}
	}
}

// G2: estimates on a realistic share of the items (not none, not all) of every tracker that has an
// estimate field: Jira story points, Linear estimates, GitLab weights. GitHub issues have none.
func TestDemoGateG2EstimatesOnAShareOfItems(t *testing.T) {
	world, _ := demoGateWorld(t)
	total, estimated := map[string]int{}, map[string]int{}
	scopes := map[string]bool{}
	for _, item := range demoGateWorkItems(world) {
		provider := gateText(item["provider"])
		total[provider]++
		if points, ok := gateNumber(item["story_points"]); ok && points > 0 {
			estimated[provider]++
			scopes[gateText(item["project_key"])+"/"+gateText(item["native_team_key"])] = true
		}
	}
	for _, provider := range []string{"jira", "linear", "gitlab"} {
		if total[provider] == 0 {
			t.Errorf("%s: no item", provider)
			continue
		}
		share := float64(estimated[provider]) / float64(total[provider])
		t.Logf("%s: %d of %d items estimated (%.0f%%)", provider, estimated[provider], total[provider], share*100)
		if share < 0.25 || share > 0.9 {
			t.Errorf("%s: %d of %d items carry an estimate (%.0f%%), want between 25%% and 90%%", provider, estimated[provider], total[provider], share*100)
		}
	}
	if len(scopes) < 6 {
		t.Errorf("estimates are in %d project scopes %v, want at least 6", len(scopes), scopes)
	}
}

// G3: blockers in several teams, at least two of them a wait on another team: the blocked item and its
// blocker each reach a repository through a native issue <-> pull request link (work_graph_issue_pr), and
// no team owns both repositories. The Payments chains stay, and Payments still closed nothing in the last
// two weeks.
func TestDemoGateG3BlockersAcrossTeams(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	items := demoGateWorkItems(world)
	owners := demoGateRepoOwners(world)
	native := map[string]map[string]bool{}
	for _, row := range demoGateRows(world, "work_graph_issue_pr") {
		if gateText(row["provenance"]) != "native" {
			continue
		}
		id := gateText(row["work_item_id"])
		if native[id] == nil {
			native[id] = map[string]bool{}
		}
		native[id][gateText(row["repo_id"])] = true
	}
	teamsOf := func(id string) map[string]bool {
		teams := map[string]bool{}
		for repoID := range native[id] {
			for team := range owners[repoID] {
				teams[team] = true
			}
		}
		return teams
	}
	scope := func(id string) string {
		rest := id[strings.Index(id, ":")+1:]
		if cut := strings.LastIndexAny(rest, "-#"); cut > 0 {
			return rest[:cut]
		}
		return rest
	}
	seen := map[[2]string]bool{}
	scopes := map[string]bool{}
	openPayments, crossTeam := 0, 0
	for _, row := range demoGateRows(world, "work_item_dependencies") {
		if gateText(row["relationship_type"]) != "blocks" {
			continue
		}
		pair := [2]string{gateText(row["source_work_item_id"]), gateText(row["target_work_item_id"])}
		if seen[pair] {
			continue
		}
		seen[pair] = true
		blocker, blocked := pair[0], pair[1]
		target, ok := items[blocked]
		if !ok {
			continue
		}
		scopes[scope(blocked)] = true
		open := target["completed_at"] == nil
		if open && strings.HasPrefix(blocked, "jira:PAY-") && strings.HasPrefix(blocker, "jira:PAY-") {
			openPayments++
		}
		blockerTeams, blockedTeams := teamsOf(blocker), teamsOf(blocked)
		if !open || len(blockerTeams) == 0 || len(blockedTeams) == 0 {
			continue
		}
		shared := false
		for team := range blockerTeams {
			shared = shared || blockedTeams[team]
		}
		if !shared {
			crossTeam++
			t.Logf("cross-team wait: %s (teams %v) blocks %s (teams %v)", blocker, gateKeys(blockerTeams), blocked, gateKeys(blockedTeams))
		}
	}
	if len(scopes) < 5 {
		t.Errorf("blocked items are in %d project scopes %v, want at least 5", len(scopes), gateKeys(scopes))
	}
	if crossTeam < 2 {
		t.Errorf("%d open blockers are a wait on another team through native linked pull requests, want at least 2", crossTeam)
	}
	if openPayments < 4 {
		t.Errorf("%d open Payments items are blocked inside Payments, want the chains' at least 4", openPayments)
	}
	for id, item := range items {
		if !strings.HasPrefix(id, "jira:PAY-") {
			continue
		}
		if completed, ok := gateTime(item["completed_at"]); ok && frozenAt.Sub(completed) < 14*24*time.Hour {
			t.Errorf("Payments closed %s on %s, inside the two weeks before the instant: its stalled story is gone", id, gateDay(completed))
		}
	}
}

func gateKeys(set map[string]bool) []string {
	out := make([]string, 0, len(set))
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// G7: a repository's risk score has its review part (time to first review of the pull requests merged in
// the last 30 days) and its complexity part (complexity in both halves of those 30 days) for every
// repository with work in that window.
func TestDemoGateG7RiskInputsForEveryActiveRepository(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	names := map[string]string{}
	for _, row := range demoGateRows(world, "repos") {
		names[gateText(row["id"])] = gateText(row["repo"])
	}
	since := frozenAt.AddDate(0, 0, -30)
	merged, reviewed := map[string]int{}, map[string]int{}
	for _, row := range demoGateRows(world, "git_pull_requests") {
		at, ok := gateTime(row["merged_at"])
		if !ok || at.Before(since) || at.After(frozenAt) {
			continue
		}
		repoID := gateText(row["repo_id"])
		merged[repoID]++
		if _, ok := gateTime(row["first_review_at"]); ok {
			reviewed[repoID]++
		}
	}
	if len(merged) == 0 {
		t.Fatal("no pull request merged in the last 30 days")
	}
	for repoID, count := range merged {
		if reviewed[repoID] == 0 {
			t.Errorf("%s: %d pull request(s) merged in the last 30 days, none with a first review time: its risk score has no review part", names[repoID], count)
		}
	}
	last := frozenAt.Truncate(24 * time.Hour)
	start, midpoint := gateDay(last.AddDate(0, 0, -29)), gateDay(last.AddDate(0, 0, -14))
	first, second := map[string]int{}, map[string]int{}
	for _, row := range demoGateRows(world, "repo_complexity_daily") {
		day := gateText(row["day"])
		switch {
		case day >= midpoint:
			second[gateText(row["repo_id"])]++
		case day >= start:
			first[gateText(row["repo_id"])]++
		}
	}
	for repoID, name := range names {
		if first[repoID] == 0 || second[repoID] == 0 {
			t.Errorf("%s: %d complexity days in the first and %d in the second half of the last 30 days: its risk score has no complexity part", name, first[repoID], second[repoID])
		}
	}
}

// G9: file-level risk: the complexity job's per-file snapshot of every repository, covering the files the
// world's commits change, so a hotspot (churn x complexity, computed by the daily metrics after a load)
// has both parts where the provider writes per-file churn.
func TestDemoGateG9FileComplexityForChangedFiles(t *testing.T) {
	world, _ := demoGateWorld(t)
	names := map[string]string{}
	for _, row := range demoGateRows(world, "repos") {
		names[gateText(row["id"])] = gateText(row["repo"])
	}
	changed := map[string]map[string]bool{}
	for _, row := range demoGateRows(world, "git_commit_stats") {
		path := gateText(row["file_path"])
		if !strings.HasSuffix(path, ".go") {
			continue
		}
		repoID := gateText(row["repo_id"])
		if changed[repoID] == nil {
			changed[repoID] = map[string]bool{}
		}
		changed[repoID][path] = true
	}
	if len(changed) == 0 {
		t.Fatal("the world's commits change no Go file")
	}
	measured := map[string]map[string]bool{}
	for _, row := range demoGateRows(world, "file_complexity_snapshots") {
		cyclomatic, _ := gateNumber(row["cyclomatic_total"])
		if cyclomatic <= 0 {
			continue
		}
		repoID := gateText(row["repo_id"])
		if measured[repoID] == nil {
			measured[repoID] = map[string]bool{}
		}
		measured[repoID][gateText(row["file_path"])] = true
	}
	// Every repository has per-file complexity: a GitLab repository's commit stats are one aggregate row
	// per commit (gitlab_commit_stats_route.go), so its hotspots are complexity-only and the snapshot is
	// all the file-level risk it can have.
	for repoID, name := range names {
		if len(measured[repoID]) == 0 {
			t.Errorf("%s: no file has a complexity snapshot: no file-level risk", name)
		}
	}
	for repoID, paths := range changed {
		both := 0
		for path := range paths {
			if measured[repoID][path] {
				both++
			}
		}
		if both == 0 {
			t.Errorf("%s: none of its %d changed Go files has a complexity snapshot: no file-level risk", names[repoID], len(paths))
		}
	}
}

// G10: deployments and CI run up to the as-of day (the day before it at the latest), the latest CI day
// has passing runs, and nothing happens after the instant.
func TestDemoGateG10DeliveryReachesTheAsOfDay(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	floor := gateDay(frozenAt.AddDate(0, 0, -1))
	latestDeploy := ""
	for _, row := range demoGateRows(world, "deployments") {
		at, ok := gateTime(row["deployed_at"])
		if !ok {
			continue
		}
		if at.After(frozenAt) {
			t.Errorf("deployment %s at %s is after the instant %s", gateText(row["deployment_id"]), at, frozenAt)
		}
		latestDeploy = max(latestDeploy, gateDay(at))
	}
	if latestDeploy < floor {
		t.Errorf("the latest deployment is on %q, before %s: deployments stop before the as-of day", latestDeploy, floor)
	}
	runs, passed := map[string]int{}, map[string]int{}
	for _, row := range demoGateRows(world, "ci_pipeline_runs") {
		at, ok := gateTime(row["finished_at"])
		if !ok {
			continue
		}
		if at.After(frozenAt) {
			t.Errorf("CI run %s finished at %s, after the instant %s", gateText(row["run_id"]), at, frozenAt)
		}
		day := gateDay(at)
		runs[day]++
		if gateText(row["status"]) == "success" {
			passed[day]++
		}
	}
	latestCI := ""
	for day := range runs {
		latestCI = max(latestCI, day)
	}
	if latestCI < floor {
		t.Errorf("the latest CI run finished on %q, before %s: CI stops before the as-of day", latestCI, floor)
	}
	if passed[latestCI] == 0 {
		t.Errorf("the latest CI day %s has %d run(s), none passing", latestCI, runs[latestCI])
	}
	t.Logf("latest deployment %s, latest CI day %s: %d of %d runs passing", latestDeploy, latestCI, passed[latestCI], runs[latestCI])
}
