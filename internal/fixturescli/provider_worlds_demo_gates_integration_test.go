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

// demoGateScope is a work item's project scope from its id: the Jira project or Linear team key
// ("jira:PAY-12" -> PAY), the GitLab project path ("gitlab:chaos-gl/x#3" -> chaos-gl/x), the GitHub
// repository. GitLab items carry no project key column, so the id is the one scope every tracker has.
func demoGateScope(id string) string {
	rest := id[strings.Index(id, ":")+1:]
	if cut := strings.LastIndexAny(rest, "-#"); cut > 0 {
		return rest[:cut]
	}
	return rest
}

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
			scopes[demoGateScope(gateText(item["work_item_id"]))] = true
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
		scopes[demoGateScope(blocked)] = true
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

// demoGateTeamNames are the teams' names by team id.
func demoGateTeamNames(world FrozenWorld) map[string]string {
	names := map[string]string{}
	for _, row := range demoGateRows(world, "teams") {
		names[gateText(row["id"])] = gateText(row["name"])
	}
	return names
}

// demoGateTeamRepos are each team's (by name) currently owned repositories.
func demoGateTeamRepos(world FrozenWorld) map[string][]string {
	names := demoGateTeamNames(world)
	repos := map[string][]string{}
	for repoID, teams := range demoGateRepoOwners(world) {
		for team := range teams {
			repos[names[team]] = append(repos[names[team]], repoID)
		}
	}
	for name := range repos {
		sort.Strings(repos[name])
	}
	return repos
}

// Incidents reach a change failure rate only through a deployment: the daily work-graph edges tie an
// incident of a service's repository to that repository's deployment of the same UTC day, before the
// incident. Several incidents, one of them on a failed deployment, must be tied that way.
func TestDemoGateIncidentsTieToSameDayDeployments(t *testing.T) {
	world, _ := demoGateWorld(t)
	serviceRepo := map[string]string{}
	for _, row := range demoGateRows(world, "operational_service_repository_mappings") {
		if number, ok := gateNumber(row["is_active"]); ok && number == 1 {
			serviceRepo[gateText(row["service_id"])] = gateText(row["repo_id"])
		}
	}
	type deploy struct {
		at     time.Time
		failed bool
	}
	deploys := map[string][]deploy{}
	for _, row := range demoGateRows(world, "deployments") {
		if at, ok := gateTime(row["deployed_at"]); ok {
			status := gateText(row["status"])
			deploys[gateText(row["repo_id"])] = append(deploys[gateText(row["repo_id"])], deploy{at, status == "failed" || status == "failure"})
		}
	}
	tied, onFailed, total := 0, 0, 0
	for _, row := range demoGateRows(world, "operational_incidents") {
		total++
		started, ok := gateTime(row["started_at"])
		repoID := serviceRepo[gateText(row["service_id"])]
		if !ok || repoID == "" {
			continue
		}
		for _, d := range deploys[repoID] {
			if gateDay(d.at) == gateDay(started) && !started.Before(d.at) {
				tied++
				if d.failed {
					onFailed++
				}
				break
			}
		}
	}
	t.Logf("%d of %d incidents start after a same-day deployment of their service's repository, %d of them a failed one", tied, total, onFailed)
	if tied < 5 || onFailed < 1 {
		t.Errorf("%d incidents tie to a same-day deployment (%d to a failed one), want at least 5 (and 1): no change failure rate can be measured", tied, onFailed)
	}
}

// demoGateRepoInputs are a repository's churn on files touched more than once (share of the window's
// churn) and its p90 hours to a first review, over the 30 days before the instant: two of the risk
// score's inputs, computed as the daily metrics compute them, from the world's rows.
func demoGateRepoInputs(world FrozenWorld, frozenAt time.Time) (churn map[string]float64, reviewP90 map[string]float64) {
	since := frozenAt.AddDate(0, 0, -30)
	committed := map[string]bool{}
	for _, row := range demoGateRows(world, "git_commits") {
		if at, ok := gateTime(row["committer_when"]); ok && !at.Before(since) && !at.After(frozenAt) {
			committed[gateText(row["repo_id"])+"/"+gateText(row["hash"])] = true
		}
	}
	touches := map[string]map[string]int{}
	lines := map[string]map[string]float64{}
	for _, row := range demoGateRows(world, "git_commit_stats") {
		repoID := gateText(row["repo_id"])
		if !committed[repoID+"/"+gateText(row["commit_hash"])] {
			continue
		}
		if touches[repoID] == nil {
			touches[repoID], lines[repoID] = map[string]int{}, map[string]float64{}
		}
		path := gateText(row["file_path"])
		added, _ := gateNumber(row["additions"])
		deleted, _ := gateNumber(row["deletions"])
		touches[repoID][path]++
		lines[repoID][path] += added + deleted
	}
	churn = map[string]float64{}
	for repoID, files := range lines {
		var all, again float64
		for path, count := range files {
			all += count
			if touches[repoID][path] > 1 {
				again += count
			}
		}
		if all > 0 {
			churn[repoID] = again / all
		}
	}
	latencies := map[string][]float64{}
	for _, row := range demoGateRows(world, "git_pull_requests") {
		merged, ok := gateTime(row["merged_at"])
		created, okCreated := gateTime(row["created_at"])
		reviewed, okReviewed := gateTime(row["first_review_at"])
		if !ok || !okCreated || !okReviewed || merged.Before(since) || merged.After(frozenAt) {
			continue
		}
		latencies[gateText(row["repo_id"])] = append(latencies[gateText(row["repo_id"])], reviewed.Sub(created).Hours())
	}
	reviewP90 = map[string]float64{}
	for repoID, values := range latencies {
		sort.Float64s(values)
		reviewP90[repoID] = values[min(len(values)-1, int(float64(len(values))*0.9))]
	}
	return churn, reviewP90
}

// Contrast between teams: one team's repositories have the inputs of a low risk score (little churn on
// files touched again, first reviews within hours), and a second team's first reviews are as quick while
// its churn is not (a middling score); the rest keep their stories.
func TestDemoGateTeamsContrast(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	churn, review := demoGateRepoInputs(world, frozenAt)
	teamRepos := demoGateTeamRepos(world)
	mean := func(team string, values map[string]float64) (float64, int) {
		var sum float64
		count := 0
		for _, repoID := range teamRepos[team] {
			if value, ok := values[repoID]; ok {
				sum += value
				count++
			}
		}
		if count == 0 {
			return 0, 0
		}
		return sum / float64(count), count
	}
	for _, team := range []string{"Mobile", "Identity", "Checkout", "Payments"} {
		c, cn := mean(team, churn)
		r, rn := mean(team, review)
		t.Logf("%s: churn on files touched again %.2f (%d repos), first review p90 %.1fh (%d repos)", team, c, cn, r, rn)
	}
	if c, n := mean("Mobile", churn); n == 0 || c > 0.1 {
		t.Errorf("Mobile: churn on files touched again %.2f over %d repositories, want at most 0.10 (a healthy team)", c, n)
	}
	if r, n := mean("Mobile", review); n == 0 || r > 14 {
		t.Errorf("Mobile: first review p90 %.1fh over %d repositories, want at most 14h (a healthy team)", r, n)
	}
	if r, n := mean("Identity", review); n == 0 || r > 14 {
		t.Errorf("Identity: first review p90 %.1fh over %d repositories, want at most 14h (a middling team)", r, n)
	}
	if c, n := mean("Identity", churn); n == 0 || c <= 0.1 {
		t.Errorf("Identity: churn on files touched again %.2f over %d repositories, want above 0.10 (a middling team, not a healthy one)", c, n)
	}
}

// Web is sparse, not stale: its repositories hold commits in the two weeks before the instant, so its
// daily repository metrics, and the health score built on them, reach the as-of day.
func TestDemoGateWebHasRecentWork(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	web := map[string]bool{}
	for _, repoID := range demoGateTeamRepos(world)["Web"] {
		web[repoID] = true
	}
	if len(web) == 0 {
		t.Fatal("the Web team owns no repository")
	}
	latest := time.Time{}
	for _, row := range demoGateRows(world, "git_commits") {
		if at, ok := gateTime(row["committer_when"]); ok && web[gateText(row["repo_id"])] && at.After(latest) {
			latest = at
		}
	}
	if frozenAt.Sub(latest) > 14*24*time.Hour {
		t.Errorf("Web's latest commit is %s, more than two weeks before the instant %s", latest, frozenAt)
	}
}

// No synthetic tells: pull request cycle times vary (no two teams share a median, few changes share a
// duration), and some repository days hold several commits.
func TestDemoGateCycleTimesVary(t *testing.T) {
	world, _ := demoGateWorld(t)
	teamOf := map[string]string{}
	for team, repos := range demoGateTeamRepos(world) {
		for _, repoID := range repos {
			if teamOf[repoID] == "" || team < teamOf[repoID] {
				teamOf[repoID] = team
			}
		}
	}
	durations := map[float64]int{}
	byTeam := map[string][]float64{}
	count := 0
	for _, row := range demoGateRows(world, "git_pull_requests") {
		merged, ok := gateTime(row["merged_at"])
		created, okCreated := gateTime(row["created_at"])
		if !ok || !okCreated {
			continue
		}
		hours := merged.Sub(created).Hours()
		durations[hours]++
		count++
		byTeam[teamOf[gateText(row["repo_id"])]] = append(byTeam[teamOf[gateText(row["repo_id"])]], hours)
	}
	if float64(len(durations)) < 0.9*float64(count) {
		t.Errorf("%d merged pull requests have %d distinct cycle times, want at least 90%% distinct", count, len(durations))
	}
	medians := map[float64][]string{}
	for team, values := range byTeam {
		sort.Float64s(values)
		medians[values[len(values)/2]] = append(medians[values[len(values)/2]], team)
	}
	for median, teams := range medians {
		if len(teams) > 1 {
			sort.Strings(teams)
			t.Errorf("teams %v share the median cycle time %.2fh", teams, median)
		}
	}
	perDay := map[string]int{}
	for _, row := range demoGateRows(world, "git_commits") {
		if at, ok := gateTime(row["committer_when"]); ok {
			perDay[gateText(row["repo_id"])+"/"+gateDay(at)]++
		}
	}
	busy := 0
	for _, commits := range perDay {
		if commits >= 3 {
			busy++
		}
	}
	if busy < 5 {
		t.Errorf("%d repository days hold three commits or more, want at least 5", busy)
	}
}

// Regression and hotfix work is evidence for "shipped fast, now paying": every bug item whose title says
// it is a regression, a hotfix or a revert reaches its pull request through a native or explicit-text
// issue <-> pull request link (work_graph_issue_pr), never only a heuristic one.
func TestDemoGateRegressionItemsLinkToPullRequests(t *testing.T) {
	world, _ := demoGateWorld(t)
	tiers := map[string]map[string]bool{}
	for _, row := range demoGateRows(world, "work_graph_issue_pr") {
		id := gateText(row["work_item_id"])
		if tiers[id] == nil {
			tiers[id] = map[string]bool{}
		}
		tiers[id][gateText(row["provenance"])] = true
	}
	found := 0
	for id, item := range demoGateWorkItems(world) {
		title := gateText(item["title"])
		if gateText(item["type"]) != "bug" || !(strings.Contains(title, "Regression") || strings.Contains(title, "Hotfix") || strings.Contains(title, "Revert")) {
			continue
		}
		found++
		if !tiers[id]["native"] && !tiers[id]["explicit_text"] {
			t.Errorf("%s (%s) has no native or explicit-text link to a pull request (tiers %v)", id, title, gateKeys(tiers[id]))
		}
	}
	if found < 4 {
		t.Errorf("%d regression, hotfix or revert bug items, want at least 4", found)
	}
}

// demoGateTrackedIssues counts each team's (by name) linked issues: tracker items (not pull or merge
// requests) with a work_graph_issue_pr row into a repository the team owns.
func demoGateTrackedIssues(world FrozenWorld) map[string]int {
	owners := demoGateRepoOwners(world)
	names := demoGateTeamNames(world)
	issues := map[string]map[string]bool{}
	for _, row := range demoGateRows(world, "work_graph_issue_pr") {
		id := gateText(row["work_item_id"])
		if strings.HasPrefix(id, "ghpr:") || strings.Contains(id, "!") {
			continue
		}
		for team := range owners[gateText(row["repo_id"])] {
			if issues[names[team]] == nil {
				issues[names[team]] = map[string]bool{}
			}
			issues[names[team]][id] = true
		}
	}
	counts := map[string]int{}
	for team := range demoGateTeamRepos(world) {
		counts[team] = len(issues[team])
	}
	return counts
}

// At least one team tracks no issues (no issue <-> pull request link into its repositories), so "no
// investment unit" is a correct answer for it and a defect for every team that does track issues; the
// other teams track theirs.
func TestDemoGateOneTeamIsUntracked(t *testing.T) {
	world, _ := demoGateWorld(t)
	counts := demoGateTrackedIssues(world)
	var untracked []string
	for team, count := range counts {
		t.Logf("%s: %d linked issues", team, count)
		if count == 0 {
			untracked = append(untracked, team)
		}
	}
	sort.Strings(untracked)
	if len(untracked) != 1 {
		t.Errorf("untracked teams %v, want exactly one (the world's deliberately untracked team)", untracked)
	}
}

// Checkout's regressions carry their cause in real shapes: revert commits in git's own form (`Revert
// "<subject>"` and "This reverts commit <sha>.") that name a commit of the same repository, and failed
// deployments. No stored relation says "regression of": the platform stores none.
func TestDemoGateCheckoutRegressionsCarryRevertsAndFailedDeploys(t *testing.T) {
	world, _ := demoGateWorld(t)
	checkout := map[string]bool{}
	for _, repoID := range demoGateTeamRepos(world)["Checkout"] {
		checkout[repoID] = true
	}
	shas := map[string]bool{}
	for _, row := range demoGateRows(world, "git_commits") {
		shas[gateText(row["repo_id"])+"/"+gateText(row["hash"])] = true
	}
	reverts := 0
	for _, row := range demoGateRows(world, "git_commits") {
		repoID, message := gateText(row["repo_id"]), gateText(row["message"])
		if !checkout[repoID] || !strings.HasPrefix(message, `Revert "`) {
			continue
		}
		_, rest, ok := strings.Cut(message, "This reverts commit ")
		sha := strings.TrimSuffix(strings.TrimSpace(rest), ".")
		if !ok || !shas[repoID+"/"+sha] {
			t.Errorf("revert commit %s names %q, not a commit of its repository", gateText(row["hash"]), sha)
			continue
		}
		reverts++
	}
	failed := 0
	for _, row := range demoGateRows(world, "deployments") {
		status := gateText(row["status"])
		if checkout[gateText(row["repo_id"])] && (status == "failure" || status == "failed") {
			failed++
		}
	}
	t.Logf("Checkout: %d revert commits naming a commit of their repository, %d failed deployments", reverts, failed)
	if reverts < 3 {
		t.Errorf("Checkout holds %d revert commits, want at least 3", reverts)
	}
	if failed < 2 {
		t.Errorf("Checkout holds %d failed deployments, want at least 2", failed)
	}
}

// Every open ownership row names a synced repository: by repo_id, or by its full name when the producer
// leaves repo_id empty (the GitHub team catalog's provider_access rows always do,
// github_team_catalog.go normalizeGitHubTeamRepoOwnership; readers resolve them by name,
// queryapi/teamscope). Identity owns its two repositories that way.
func TestDemoGateOwnershipResolvesToSyncedRepositories(t *testing.T) {
	world, _ := demoGateWorld(t)
	ids, names := map[string]bool{}, map[string]bool{}
	for _, row := range demoGateRows(world, "repos") {
		ids[gateText(row["id"])] = true
		names[strings.ToLower(gateText(row["repo"]))] = true
	}
	byName := 0
	for _, row := range demoGateRows(world, "team_repo_ownership") {
		if row["valid_to"] != nil {
			continue
		}
		repoID, name := gateText(row["repo_id"]), strings.ToLower(gateText(row["repo_full_name"]))
		switch {
		case repoID != "" && ids[repoID]:
		case repoID == "" && names[name]:
			byName++
		default:
			t.Errorf("team %s owns %q (repo_id %q), which is not a synced repository", gateText(row["team_id"]), name, repoID)
		}
	}
	t.Logf("%d open ownership rows resolve by repository name", byName)
}

// Checkout's failed deployments carry the pull requests the story blames: each failed deployment's
// pull_request_number (set by the deployment routes from the deployed sha) names a feature pull request
// that a later revert commit in the same repository reverts.
func TestDemoGateCheckoutFailedDeploysCarryTheRevertedFeatures(t *testing.T) {
	world, _ := demoGateWorld(t)
	checkout := map[string]bool{}
	for _, repoID := range demoGateTeamRepos(world)["Checkout"] {
		checkout[repoID] = true
	}
	reverted := map[string]bool{}
	for _, row := range demoGateRows(world, "git_commits") {
		repoID, message := gateText(row["repo_id"]), gateText(row["message"])
		if subject, ok := strings.CutPrefix(message, `Revert "`); ok && checkout[repoID] {
			if cut := strings.Index(subject, "\"\n"); cut > 0 {
				reverted[repoID+"/"+subject[:cut]] = true
			}
		}
	}
	titles := map[string]string{}
	for _, row := range demoGateRows(world, "git_pull_requests") {
		number, _ := gateNumber(row["number"])
		titles[fmt.Sprintf("%s#%d", gateText(row["repo_id"]), int(number))] = gateText(row["title"])
	}
	failed := 0
	for _, row := range demoGateRows(world, "deployments") {
		status := gateText(row["status"])
		repoID := gateText(row["repo_id"])
		if !checkout[repoID] || (status != "failure" && status != "failed" && status != "error") {
			continue
		}
		failed++
		number, ok := gateNumber(row["pull_request_number"])
		title := titles[fmt.Sprintf("%s#%d", repoID, int(number))]
		if !ok || !reverted[repoID+"/"+title] {
			t.Errorf("failed deployment %s carries pull request %v (%q), not a feature a revert commit reverts", gateText(row["deployment_id"]), row["pull_request_number"], title)
		}
	}
	if failed < 2 {
		t.Errorf("Checkout holds %d failed deployments, want at least 2", failed)
	}
}

// G10: every blocker link of the world has the provider's own time of the link, dated before the world
// instant, so the oldest blocked age of a team is its real wait and not the time of the load: the Jira
// links read it from the issue changelog, the GitLab and Linear links from their own link times.
func TestDemoGateG10BlockerLinksCarryTheirStartTime(t *testing.T) {
	world, frozenAt := demoGateWorld(t)
	blockers := 0
	for _, row := range demoGateRows(world, "work_item_dependencies") {
		if gateText(row["relationship_type"]) != "blocks" {
			continue
		}
		blockers++
		pair := gateText(row["source_work_item_id"]) + " -> " + gateText(row["target_work_item_id"])
		started := gateText(row["relation_started_at"])
		if started == "" || row["relation_started_at"] == nil {
			t.Errorf("blocker %s has no start time of its link: its age would read the time of the load", pair)
			continue
		}
		at, err := time.Parse("2006-01-02 15:04:05.000", started)
		if err != nil || !at.Before(frozenAt.Add(-24*time.Hour)) {
			t.Errorf("blocker %s starts %q: not a time more than a day before the world instant", pair, started)
		}
	}
	if blockers == 0 {
		t.Fatal("the world has no blocker link")
	}
}

// G11: the daily state-duration job skips an item with no status transition, so an item without one has no
// state time and no blocked hours. Every item a tracker keeps (Jira, Linear, and the GitHub and GitLab
// issues) has a status history that starts at its creation and ends at its current status, written by the
// trackers' own history (changelog, issue history, label and state events), and every item of a blocker
// link has one.
func TestDemoGateG11TrackerItemsHaveAStatusHistory(t *testing.T) {
	world, _ := demoGateWorld(t)
	transitions := map[string][]map[string]any{}
	for _, row := range demoGateRows(world, "work_item_transitions") {
		id := gateText(row["work_item_id"])
		transitions[id] = append(transitions[id], row)
	}
	inBlocker := map[string]bool{}
	for _, row := range demoGateRows(world, "work_item_dependencies") {
		if gateText(row["relationship_type"]) == "blocks" {
			inBlocker[gateText(row["source_work_item_id"])] = true
			inBlocker[gateText(row["target_work_item_id"])] = true
		}
	}
	checked, without := 0, 0
	for _, item := range demoGateRows(world, "work_items") {
		id := gateText(item["work_item_id"])
		// A pull request or merge request is a work item too; no tracker event maps an open one to a status.
		tracker := strings.HasPrefix(id, "jira:") || strings.HasPrefix(id, "linear:") ||
			(strings.HasPrefix(id, "gh:") || (strings.HasPrefix(id, "gitlab:") && strings.Contains(id, "#")))
		if !tracker && !inBlocker[id] {
			continue
		}
		checked++
		rows := transitions[id]
		if len(rows) == 0 {
			without++
			t.Errorf("%s (status %s) has no status transition: the daily job skips it", id, gateText(item["status"]))
			continue
		}
		last := ""
		var lastAt time.Time
		for _, row := range rows {
			at, ok := gateTime(row["occurred_at"])
			if ok && !at.Before(lastAt) {
				lastAt, last = at, gateText(row["to_status"])
			}
		}
		if status := gateText(item["status"]); last != status {
			t.Errorf("%s: its status history ends at %q, its current status is %q", id, last, status)
		}
	}
	if checked == 0 {
		t.Fatal("the world has no tracker item")
	}
	t.Logf("%d tracker or blocker items checked, %d without a history", checked, without)
}

// G12: the measured complexity of a demo repository is where real code is. The complexity job measured
// 411 to 520 cyclomatic per thousand lines on files that held nothing but branches; real code is about 10 to 60.
// Each repository's measured day (the as-of day's repo_complexity_daily row, which the job wrote from the
// repository's files) reads between 20 and 80.
func TestDemoGateG12MeasuredComplexityIsRealistic(t *testing.T) {
	world, _ := demoGateWorld(t)
	rows := demoGateRows(world, "repo_complexity_daily")
	newest := ""
	for _, row := range rows {
		if day := gateText(row["day"]); day > newest {
			newest = day
		}
	}
	checked := 0
	for _, row := range rows {
		if gateText(row["day"]) != newest {
			continue
		}
		checked++
		perKLOC, _ := gateNumber(row["cyclomatic_per_kloc"])
		if perKLOC < 20 || perKLOC > 80 {
			t.Errorf("repository %v measures %.1f cyclomatic per thousand lines on %s, want 20..80", row["repo_id"], perKLOC, newest)
		}
	}
	if checked < 18 {
		t.Fatalf("%d repositories measured on %s, want all 18", checked, newest)
	}
}
