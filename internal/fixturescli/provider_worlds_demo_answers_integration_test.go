//go:build integration

package fixturescli

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The demo world's provider answers, built from the teams' stories.

// demoTeam is one team's tracker and code host.
type demoTeam struct {
	// Name is the team's name in the world's stories (a cross-team blocker names its team by it).
	Name string
	// Tracker: jira, linear, github or gitlab; "none" is a team that tracks no issues (its merge requests
	// name none).
	Tracker string
	// Key is the Jira project key or the Linear team key.
	Key       string
	Project   jiraProject // jira
	LinearPrj string      // linear project id
	// Home is where the team's GitHub or GitLab issues live.
	Home     string
	GitHub   []githubUser
	GitLab   []gitlabUser
	Jira     []jiraUser
	Linear   []linearUser
	Reviewer githubUser // when set, every review is by this person
	Stories  []demoStory
	// EstimateShare is the percentage of the team's items that carry an estimate (story points, a
	// Linear estimate or a GitLab weight); a GitHub tracker has no estimate field.
	EstimateShare int
	// QuickReview: a change gets its first review within hours of opening, not on the day it merges.
	// OwnFiles: a change touches only its own files (no shared handler), and a follow-up commit its own
	// test file, so the team's churn does not fall on files touched again and again. Together they make
	// a team's risk score low (Mobile); QuickReview alone makes it middling (Identity, Data).
	QuickReview, OwnFiles bool
	// AfterHoursShare is the percentage of the team's weekday commits made outside 09:00-17:00 UTC
	// (early, 06 to 08, or late, 18 to 21); the others fall in the working day (demoCommitHour).
	AfterHoursShare int
}

var demoTeams = []demoTeam{
	{Name: "payments", Tracker: "jira", Key: "PAY", Project: demoJiraPAY, GitHub: []githubUser{dmPia, dmQuin, dmRaf}, Jira: []jiraUser{dmJPia, dmJQuin, dmJRaf}, Stories: demoPaymentsStories, EstimateShare: 60, AfterHoursShare: 20},
	{Name: "checkout", Tracker: "linear", Key: "CHK", LinearPrj: demoLinearProjects[0].ID, GitHub: []githubUser{dmSol, dmTeo, dmUma}, Linear: []linearUser{dmLSol, dmLTeo, dmLUma}, Stories: demoCheckoutStories, EstimateShare: 75, AfterHoursShare: 12},
	{Name: "support", Tracker: "jira", Key: "SUP", Project: demoJiraSUP, GitHub: []githubUser{dmVic, dmWen}, Jira: []jiraUser{dmJVic, dmJWen}, Stories: demoSupportStories, EstimateShare: 40, AfterHoursShare: 8},
	{Name: "identity", Tracker: "github", Home: "identity-service", GitHub: []githubUser{dmXan, dmYara, dmZed}, Stories: demoIdentityStories, QuickReview: true, AfterHoursShare: 35},
	{Name: "mobile", Tracker: "linear", Key: "MOB", LinearPrj: demoLinearProjects[1].ID, GitHub: []githubUser{dmAbe, dmBea}, Linear: []linearUser{dmLAbe, dmLBea}, Reviewer: dmMoLed, Stories: demoMobileStories, EstimateShare: 70, QuickReview: true, OwnFiles: true, AfterHoursShare: 4},
	{Name: "delivery", Tracker: "gitlab", Home: "chaos-gl/deploy-pipeline", GitLab: []gitlabUser{dmDax, dmEva, dmFin}, Stories: demoDeliveryStories, EstimateShare: 50, AfterHoursShare: 28},
	{Name: "data", Tracker: "gitlab", Home: "chaos-gl/data/warehouse-etl", GitLab: []gitlabUser{dmGia, dmHal, dmIda}, Stories: demoDataStories, EstimateShare: 80, QuickReview: true, AfterHoursShare: 16},
	{Name: "web", Tracker: "none", Home: "chaos-gl/web/marketing-site", GitLab: []gitlabUser{dmJon, dmKit}, Stories: demoWebStories, EstimateShare: 30, AfterHoursShare: 10},
}

// demoTeamNamed is the team of that name.
func demoTeamNamed(name string) demoTeam {
	for _, team := range demoTeams {
		if team.Name == name {
			return team
		}
	}
	panic("no demo team " + name)
}

// demoPoints is a story's estimate: its own, else one for EstimateShare percent of the team's items,
// chosen and sized by the story's title (1, 2, 3, 5 or 8 points); 0 is none.
func demoPoints(team demoTeam, story demoStory) float64 {
	if story.Points > 0 {
		return story.Points
	}
	sum := sha256.Sum256([]byte("dev-health-demo-estimate:" + team.Name + ":" + story.Title))
	if int(sum[0])*100/256 >= team.EstimateShare {
		return 0
	}
	return []float64{1, 2, 3, 5, 8}[int(sum[1])%5]
}

// demoGitHubRepo is everything one GitHub repository answers.
type demoGitHubRepo struct {
	Items   []githubIssue
	Commits []demoCommit
	Runs    []demoRun
	Deploys []demoDeploy
	next    int
}

// demoGitLabProject is everything one GitLab project answers.
type demoGitLabProject struct {
	Items     []gitlabItem
	Commits   []demoCommit
	Pipelines []demoRun
	Deploys   []demoDeploy
	Incidents []demoIncident
	nextIssue int
	nextMR    int
}

type demoCommit struct {
	SHA, Message, Author, At string
	Files                    []string
}

// demoRun is one CI run (GitHub Actions workflow run or GitLab pipeline).
type demoRun struct {
	ID              int
	SHA, Branch, At string
	Failed          bool
	Deploy          bool
}

type demoDeploy struct {
	ID, MR          int
	SHA, At, Merged string
	Failed          bool
}

// demoBuild is the whole demo world's provider data.
type demoBuild struct {
	GitHub map[string]*demoGitHubRepo
	GitLab map[string]*demoGitLabProject
	Jira   []jiraIssue
	Linear []linearIssue
	nextID int
	// refs are each team's stories' tracker references (demoStoryRefs), by team name.
	refs map[string][]string
}

func (build *demoBuild) id() int {
	build.nextID++
	return build.nextID
}

func demoSlug(text string) string {
	var out strings.Builder
	for _, r := range strings.ToLower(text) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			out.WriteRune(r)
		case out.Len() > 0 && !strings.HasSuffix(out.String(), "-"):
			out.WriteRune('-')
		}
	}
	slug := strings.TrimSuffix(out.String(), "-")
	if len(slug) > 40 {
		slug = strings.TrimSuffix(slug[:40], "-")
	}
	return slug
}

func isGitLab(repo string) bool { return strings.Contains(repo, "/") }

// demoEnd is the day a change's activity ends: its merge, else the day after it opened.
func demoEnd(c demoChange) int {
	if c.MergedAgo >= 0 {
		return c.MergedAgo
	}
	return max(c.CreatedAgo-1, 0)
}

// demoSeed is a seeded hash of a story's change: the times and the commit count of each change vary
// with it, so no two changes share a cycle time by construction.
func demoSeed(parts ...any) [32]byte {
	return sha256.Sum256([]byte(fmt.Sprint(append([]any{"dev-health-demo-times"}, parts...)...)))
}

// demoAfter is at moved by minutes.
func demoAfter(at string, minutes int) string {
	parsed, err := time.Parse(time.RFC3339, at)
	if err != nil {
		panic(err)
	}
	return parsed.Add(time.Duration(minutes) * time.Minute).UTC().Format(time.RFC3339)
}

// demoChangeTimes are when a change opened, was approved, merged, ran on main and deployed: opened in
// the morning and merged in the afternoon of their days, at seeded minutes; the approval comes 20 to 140
// minutes before the merge (never before the opening), the main run 8 minutes after it and the
// deployment 25 to 85 minutes after it.
type demoChangeTimes struct {
	Created, Approved, ChangesRequested, Merged, MainRun, Deploy string
}

func demoTimesOf(team demoTeam, story demoStory, c demoChange) demoChangeTimes {
	seed := demoSeed(c.Repo, story.Title, c.CreatedAgo, c.MergedAgo)
	times := demoChangeTimes{Created: demoAt(c.CreatedAgo, 8+int(seed[0])%4, int(seed[1])%60)}
	if c.MergedAgo < 0 {
		times.ChangesRequested = demoAt(demoEnd(c), 11, int(seed[2])%60)
		return times
	}
	times.Merged = demoAt(c.MergedAgo, 13+int(seed[3])%5, int(seed[4])%60)
	if c.MergedAgo == 0 {
		// Merged on the as-of day itself: after the commits (which are before 11 on that day), 11:00 to
		// 11:09, so it and the deployment (25 to 44 minutes later) are before the world's noon instant.
		times.Merged = demoAt(0, 11, int(seed[4])%10)
	}
	times.Approved = demoAfter(times.Merged, -(20 + int(seed[5])%121))
	if team.QuickReview {
		// Reviewed one to three hours after opening, whenever it merges.
		times.Approved = min(times.Approved, demoAfter(times.Created, 60+int(seed[5])%121))
	}
	if times.Approved <= times.Created {
		times.Approved = demoAfter(times.Created, 30)
	}
	created, _ := time.Parse(time.RFC3339, times.Created)
	approved, _ := time.Parse(time.RFC3339, times.Approved)
	times.ChangesRequested = created.Add(approved.Sub(created) / 3).UTC().Format(time.RFC3339)
	times.MainRun = demoAfter(times.Merged, 8)
	times.Deploy = demoAfter(times.Merged, 25+int(seed[6])%61)
	if c.MergedAgo == 0 {
		times.Deploy = demoAfter(times.Merged, 25+int(seed[6])%20)
	}
	return times
}

// demoChangeCommits are a change's commits: one, plus one per failed CI run (at most two), plus zero to
// two review follow-ups (seeded), on the days the change is open, at the hours demoCommitHour picks. A team with OwnFiles
// touches only the change's own file, and each follow-up its own test file.
func demoChangeCommits(team demoTeam, c demoChange, key, title, author string) []demoCommit {
	failed := min(c.CIFailed, 2)
	seed := demoSeed(c.Repo, title, c.CreatedAgo, c.MergedAgo, "commits")
	count := 1 + failed + int(seed[0])%3
	end := demoEnd(c)
	module := c.Repo[strings.LastIndex(c.Repo, "/")+1:]
	files := c.Files
	switch {
	case len(files) > 0:
	case team.OwnFiles:
		files = []string{"src/" + module + "/" + demoSlug(title) + ".go"}
	default:
		files = []string{"src/" + module + "/handler.go", "src/" + module + "/" + demoSlug(title) + ".go"}
	}
	merged := demoTimesOf(team, demoStory{Title: title}, c).Merged
	var commits []demoCommit
	for index := 0; index < count; index++ {
		day := c.CreatedAgo - (c.CreatedAgo-end)*index/count
		message := title
		switch {
		case index > 0 && index <= failed:
			message = "Fix CI: " + strings.ToLower(title[:1]) + title[1:]
		case index > failed:
			message = "Review follow-up: " + strings.ToLower(title[:1]) + title[1:]
		}
		if c.Text {
			message = key + " " + message
		}
		touched := files
		if team.OwnFiles && index > failed {
			touched = []string{fmt.Sprintf("src/%s/%s_%d_test.go", module, strings.ReplaceAll(demoSlug(title), "-", "_"), index)}
		}
		commits = append(commits, demoCommit{
			SHA: demoSHA(c.Repo, key, title, c.CreatedAgo, c.MergedAgo, index), Message: message, Author: author,
			At: demoAt(day, demoCommitHour(team, c, title, index, day, merged), int(seed[1+index])%60), Files: touched,
		})
	}
	return demoCommitsInOrder(commits)
}

// demoCommitHour is the UTC hour of a change's commit: outside 09:00-17:00 for AfterHoursShare percent of
// the commits (seeded: early, 06 to 08, or late, 18 to 21), else in the working day, 09 to 16. A commit
// on the day its change merges stays before the merge hour, and one on the as-of day before 11 (the
// world's instant is noon, and a change merged that day merges at 11); a late after-hours commit there
// moves to the early slot.
func demoCommitHour(team demoTeam, c demoChange, title string, index, day int, merged string) int {
	seed := demoSeed(c.Repo, title, c.CreatedAgo, c.MergedAgo, "commit-hours", index)
	before := 24
	if day == 0 {
		before = 11
	}
	if merged != "" && day == c.MergedAgo {
		at, err := time.Parse(time.RFC3339, merged)
		if err != nil {
			panic(err)
		}
		before = min(before, at.Hour())
	}
	if int(seed[0])%100 < team.AfterHoursShare {
		if late := 18 + int(seed[1])%4; seed[2]%2 == 0 && late < before {
			return late
		}
		return 6 + int(seed[1])%3
	}
	return 9 + int(seed[3])%max(min(8, before-9), 1)
}

// demoCommitsInOrder gives a change's commit times back in commit order: the times sorted ascending are
// assigned to the commits in turn, so a later commit is never earlier than the one before it.
func demoCommitsInOrder(commits []demoCommit) []demoCommit {
	times := make([]string, len(commits))
	for index, commit := range commits {
		times[index] = commit.At
	}
	sort.Strings(times)
	for index := range commits {
		commits[index].At = times[index]
	}
	return commits
}

// withRevert makes a reverting change's first commit the revert git writes: `Revert "<subject>"` and
// "This reverts commit <sha>." of the first commit of the team's story it reverts, in the same repository.
func (build *demoBuild) withRevert(team demoTeam, c demoChange, commits []demoCommit, author string) []demoCommit {
	if !c.Revert {
		return commits
	}
	target := team.Stories[c.RevertOf]
	for _, tc := range target.Changes {
		if tc.Repo != c.Repo {
			continue
		}
		key := ""
		if team.Tracker == "jira" || team.Tracker == "linear" {
			key = build.refs[team.Name][c.RevertOf]
		}
		reverted := demoChangeCommits(team, tc, key, target.Title, author)[0]
		subject, _, _ := strings.Cut(reverted.Message, "\n")
		commits[0].Message = "Revert \"" + subject + "\"\n\nThis reverts commit " + reverted.SHA + "."
		return commits
	}
	panic(fmt.Sprintf("a change in %s reverts %q, which has no change there", c.Repo, target.Title))
}

// demoRuns are a change's CI runs, one per commit: the first ones fail (CIFailed of them, at most two),
// the rest pass, and an open change with failing CI fails its last run too; a merged change also runs on
// main after the merge.
func (build *demoBuild) demoRuns(c demoChange, times demoChangeTimes, commits []demoCommit, branch string, deployRepo bool) []demoRun {
	var runs []demoRun
	failed := min(c.CIFailed, 2)
	for index, commit := range commits {
		last := index == len(commits)-1
		run := demoRun{ID: 1700000 + build.id(), SHA: commit.SHA, Branch: branch, At: commit.At, Failed: index < failed || (last && c.MergedAgo < 0 && c.CIFailed > 0)}
		runs = append(runs, run)
	}
	if c.MergedAgo >= 0 {
		runs = append(runs, demoRun{ID: 1700000 + build.id(), SHA: commits[len(commits)-1].SHA, Branch: "main",
			At: times.MainRun, Deploy: deployRepo})
	}
	return runs
}

func newDemoBuild() *demoBuild {
	build := &demoBuild{GitHub: map[string]*demoGitHubRepo{}, GitLab: map[string]*demoGitLabProject{}, refs: demoStoryRefs()}
	for _, name := range demoGitHubRepoNames {
		build.GitHub[name] = &demoGitHubRepo{}
	}
	for _, path := range demoGitLabProjectPaths {
		build.GitLab[path] = &demoGitLabProject{}
	}
	for _, team := range demoTeams {
		build.addTeam(team)
	}
	build.GitLab["chaos-gl/deploy-pipeline"].Incidents = demoDeliveryIncidents
	return build
}

// demoIssueHome is the repository a GitHub or GitLab team's story's issue lives in: the repository of
// its first change for a GitHub team, the project of its first change when the team owns it for a
// GitLab team, else the team's home.
func demoIssueHome(team demoTeam, story demoStory) string {
	home := team.Home
	if len(story.Changes) > 0 {
		first := story.Changes[0].Repo
		switch {
		case team.Tracker == "github":
			home = first
		case team.Tracker == "gitlab" && isGitLab(first) && demoOwnsProject(team, first):
			home = first
		}
	}
	return home
}

// demoStoryRefs names every story's tracker item the way the tracker's own links name it: the Jira or
// Linear key, the GitLab "project#iid". GitHub issues are not named (no GitHub story blocks another).
// The GitLab numbers are the ones addTeam gives, in the same order; addTeam checks they agree.
func demoStoryRefs() map[string][]string {
	refs := map[string][]string{}
	issues := map[string]int{}
	for _, team := range demoTeams {
		out := make([]string, len(team.Stories))
		for index, story := range team.Stories {
			switch team.Tracker {
			case "jira", "linear":
				out[index] = fmt.Sprintf("%s-%d", team.Key, index+1)
			case "gitlab":
				home := demoIssueHome(team, story)
				issues[home]++
				out[index] = fmt.Sprintf("%s#%d", home, issues[home])
			}
		}
		refs[team.Name] = out
	}
	return refs
}

// blockedRefs are the references of the items a story blocks: its own team's (Blocks) and another
// team's (BlocksIn). A tracker's link names only its own tracker's items, so a cross-team blocker is
// between two teams of the same tracker.
func (build *demoBuild) blockedRefs(team demoTeam, story demoStory) []string {
	var out []string
	for _, index := range story.Blocks {
		out = append(out, build.refs[team.Name][index])
	}
	for _, target := range story.BlocksIn {
		if other := demoTeamNamed(target.Team); other.Tracker != team.Tracker {
			panic(fmt.Sprintf("%q blocks a %s item of %s from %s: a tracker links only its own items", story.Title, other.Tracker, target.Team, team.Tracker))
		}
		out = append(out, build.refs[target.Team][target.Story])
	}
	for _, ref := range out {
		if ref == "" {
			panic(fmt.Sprintf("%q blocks an item no tracker link can name", story.Title))
		}
	}
	return out
}

func demoJiraType(kind string) (string, []string) {
	switch kind {
	case "feature":
		return "Story", []string{"feature"}
	case "bug":
		return "Bug", []string{"bug"}
	case "support":
		return "Task", []string{"support", "unplanned"}
	case "security":
		return "Task", []string{"security"}
	default:
		return "Task", []string{"maintenance"}
	}
}

func demoLabel(kind string) string {
	switch kind {
	case "feature":
		return "Feature"
	case "bug":
		return "Bug"
	case "support":
		return "Support"
	case "security":
		return "Security"
	default:
		return "Chore"
	}
}

func (build *demoBuild) addTeam(team demoTeam) {
	for index, story := range team.Stories {
		key := ""
		if team.Tracker == "jira" || team.Tracker == "linear" {
			key = build.refs[team.Name][index]
		}
		seed := demoSeed(team.Name, story.Title)
		created := demoAt(story.CreatedAgo, 8+int(seed[0])%3, int(seed[1])%60)
		done := ""
		if story.DoneAgo >= 0 {
			done = demoAt(story.DoneAgo, 18+int(seed[2])%3, int(seed[3])%60)
			if story.DoneAgo == 0 {
				// Done on the as-of day: after its changes merged and deployed (before 11:54), before noon.
				done = demoAt(0, 11, 54+int(seed[3])%5)
			}
		}
		// The tracker item of a GitHub or GitLab team: an issue in the repository of its first change (its
		// home repository when the change is in another team's repository).
		home := demoIssueHome(team, story)
		points := demoPoints(team, story)
		blocked := build.blockedRefs(team, story)
		linkedAt := demoAfter(created, 45)
		// The status history the trackers keep: the move to In Progress (or the To Do label of an item no change
		// has started) shortly after the item was created, before it was done.
		startedAt := demoAfter(created, 120)
		if done != "" && startedAt >= done {
			startedAt = demoAfter(created, 10)
		}
		statusLabel := "in progress"
		if len(story.Changes) == 0 {
			statusLabel = "to do"
		}
		issueNumber := 0
		switch team.Tracker {
		case "github":
			repo := build.GitHub[home]
			repo.next++
			issueNumber = repo.next
		case "gitlab":
			project := build.GitLab[home]
			project.nextIssue++
			issueNumber = project.nextIssue
			if ref := fmt.Sprintf("%s#%d", home, issueNumber); ref != build.refs[team.Name][index] {
				panic(fmt.Sprintf("%q is %s, but the stories' references name it %s", story.Title, ref, build.refs[team.Name][index]))
			}
		}
		lastActivity := created
		var nativePRs, attachments []string
		var closedBy []string
		for changeIndex, c := range story.Changes {
			title := story.Title
			if c.Text && key != "" {
				title = key + " " + title
			}
			if isGitLab(c.Repo) {
				mr := build.addMergeRequest(team, index+changeIndex, story, c, home, issueNumber)
				if !c.Text && !c.Heuristic {
					closedBy = append(closedBy, c.Repo+"!"+strconv.Itoa(mr))
				}
				lastActivity = demoAt(demoEnd(c), 15, 30)
				continue
			}
			number := build.addPullRequest(team, team.GitHub[(index+changeIndex)%len(team.GitHub)], story, c, key, title, home, issueNumber)
			url := fmt.Sprintf("https://github.com/%s/%s/pull/%d", githubOrg, c.Repo, number)
			if !c.Text && !c.Heuristic {
				nativePRs = append(nativePRs, "GitHub "+url)
				attachments = append(attachments, url)
			}
			lastActivity = demoAt(demoEnd(c), 15, 30)
		}
		updated := lastActivity
		if done != "" {
			updated = done
		}
		switch team.Tracker {
		case "jira":
			kind, labels := demoJiraType(story.Kind)
			assignee := team.Jira[index%len(team.Jira)]
			issue := jiraIssue{
				ID: strconv.Itoa(30000 + build.id()), Key: key, Project: team.Project, Summary: story.Title, Type: kind,
				Assignee: assignee, Reporter: team.Jira[(index+1)%len(team.Jira)], Created: created, Updated: updated,
				Resolved: done, Labels: labels, PullRequests: nativePRs, Description: demoIssueText(story),
				Blocks: blocked, LinkedAt: linkedAt, StartedAt: startedAt, StoryPoints: points,
			}
			build.Jira = append(build.Jira, issue)
		case "linear":
			assignee := team.Linear[index%len(team.Linear)]
			started := demoAt(max(story.CreatedAgo-1, 0), 9, 0)
			var relations []string
			for _, ref := range blocked {
				relations = append(relations, "blocks "+ref+"@"+linkedAt)
			}
			build.Linear = append(build.Linear, linearIssue{
				ID: fmt.Sprintf("a2000000-0000-4000-8000-%012d", build.id()), Identifier: key, Title: story.Title,
				Team: team.Key, Project: team.LinearPrj, Assignee: assignee, Creator: team.Linear[(index+1)%len(team.Linear)],
				CreatedAt: created, UpdatedAt: updated, StartedAt: started, HistoryFrom: started, CompletedAt: done,
				Attachments: attachments, Labels: []string{demoLabel(story.Kind)}, Description: demoIssueText(story),
				Relations: relations, Estimate: points,
			})
		case "github":
			author := team.GitHub[index%len(team.GitHub)]
			repo := build.GitHub[home]
			repo.Items = append(repo.Items, githubIssue{
				Number: issueNumber, Title: story.Title, Body: demoIssueText(story), Author: author, Assignees: []githubUser{author},
				Labels: []string{strings.ToLower(demoLabel(story.Kind))}, CreatedAt: created, UpdatedAt: updated, ClosedAt: done,
				StatusLabel: statusLabel, StatusAt: startedAt,
			})
		case "gitlab":
			author := team.GitLab[index%len(team.GitLab)]
			project := build.GitLab[home]
			var links []string
			for _, ref := range blocked {
				links = append(links, "blocks "+ref+"@"+linkedAt)
			}
			project.Items = append(project.Items, gitlabItem{
				IID: issueNumber, Title: story.Title, Description: demoIssueText(story), Author: author, Assignees: []gitlabUser{author},
				Labels: []string{strings.ToLower(demoLabel(story.Kind))}, CreatedAt: created, UpdatedAt: updated, ClosedAt: done,
				ClosedBy: closedBy, Links: links, Weight: int(points), StatusLabel: statusLabel, StatusAt: startedAt,
			})
		}
	}
}

func demoOwnsProject(team demoTeam, path string) bool {
	for _, group := range demoGitLabGroups {
		for _, member := range group.Members {
			if len(team.GitLab) > 0 && member.ID == team.GitLab[0].ID {
				for _, project := range group.Projects {
					if project == path {
						return true
					}
				}
			}
		}
	}
	return false
}

// addPullRequest adds a GitHub pull request, its commits, CI runs and deployment; it returns its number.
func (build *demoBuild) addPullRequest(team demoTeam, author githubUser, story demoStory, c demoChange, key, title, home string, issueNumber int) int {
	repo := build.GitHub[c.Repo]
	repo.next++
	number := repo.next
	commits := build.withRevert(team, c, demoChangeCommits(team, c, key, story.Title, author.Login), author.Login)
	body := demoChangeText(story)
	var closes []string
	if team.Tracker == "github" {
		switch {
		case c.Heuristic:
		case c.Text:
			body = fmt.Sprintf("Implements the work asked for in #%d.\n\n", issueNumber) + body
		default:
			body = fmt.Sprintf("Fixes #%d\n\n", issueNumber) + body
			closes = []string{fmt.Sprintf("%s/%s#%d", githubOrg, home, issueNumber)}
		}
	}
	reviewer := team.Reviewer
	if reviewer.ID == 0 {
		for _, member := range team.GitHub {
			if member.ID != author.ID {
				reviewer = member
				break
			}
		}
	}
	times := demoTimesOf(team, story, c)
	item := githubIssue{
		Number: number, Title: title, Body: body, Author: author, PR: true, Head: "feature/" + demoSlug(story.Title),
		CreatedAt: times.Created, UpdatedAt: demoAt(demoEnd(c), 15, 30), Closes: closes,
	}
	if c.MergedAgo >= 0 {
		item.ClosedAt, item.MergedAt, item.UpdatedAt = times.Merged, times.Merged, times.Merged
		if c.Changes {
			item.Reviews = append(item.Reviews, githubReview{build.id() + 910000, "CHANGES_REQUESTED", times.ChangesRequested, reviewer})
		}
		item.Reviews = append(item.Reviews, githubReview{build.id() + 910000, "APPROVED", times.Approved, reviewer})
	} else if c.Changes {
		item.Reviews = append(item.Reviews, githubReview{build.id() + 910000, "CHANGES_REQUESTED", times.ChangesRequested, reviewer})
	}
	repo.Items = append(repo.Items, item)
	repo.Commits = append(repo.Commits, commits...)
	repo.Runs = append(repo.Runs, build.demoRuns(c, times, commits, item.Head, c.Deploy)...)
	if c.Deploy && c.MergedAgo >= 0 {
		repo.Deploys = append(repo.Deploys, demoDeploy{ID: 2200000 + build.id(), MR: number, SHA: commits[len(commits)-1].SHA,
			At: times.Deploy, Merged: item.MergedAt, Failed: c.DeployFailed})
	}
	return number
}

// addMergeRequest adds a GitLab merge request, its commits, pipelines and deployment; it returns its iid.
func (build *demoBuild) addMergeRequest(team demoTeam, index int, story demoStory, c demoChange, home string, issueNumber int) int {
	project := build.GitLab[c.Repo]
	project.nextMR++
	iid := project.nextMR
	author := team.GitLab[index%len(team.GitLab)]
	reviewer := team.GitLab[(index+1)%len(team.GitLab)]
	ref := fmt.Sprintf("#%d", issueNumber)
	if home != c.Repo {
		ref = fmt.Sprintf("%s#%d", home, issueNumber)
	}
	description := "Closes " + ref + "\n\n" + demoChangeText(story)
	switch {
	case team.Tracker == "none", c.Heuristic:
		description = demoChangeText(story)
	case c.Text:
		description = "Implements " + ref + ".\n\n" + demoChangeText(story)
	}
	commits := build.withRevert(team, c, demoChangeCommits(team, c, "", story.Title, author.Name), author.Name)
	branch := "feature/" + demoSlug(story.Title)
	times := demoTimesOf(team, story, c)
	item := gitlabItem{
		IID: iid, Title: story.Title, Description: description, Author: author, MR: true, Branch: branch,
		CreatedAt: times.Created, UpdatedAt: demoAt(demoEnd(c), 15, 30), Reviewer: reviewer,
	}
	for _, commit := range commits {
		item.Commits = append(item.Commits, commit.SHA)
	}
	if c.MergedAgo >= 0 {
		item.ClosedAt, item.MergedAt, item.UpdatedAt = times.Merged, times.Merged, times.Merged
		item.ApprovedAt = times.Approved
	}
	project.Items = append(project.Items, item)
	project.Commits = append(project.Commits, commits...)
	project.Pipelines = append(project.Pipelines, build.demoRuns(c, times, commits, branch, c.Deploy)...)
	if c.Deploy && c.MergedAgo >= 0 {
		project.Deploys = append(project.Deploys, demoDeploy{ID: 5100 + build.id(), MR: iid, SHA: commits[len(commits)-1].SHA,
			At: times.Deploy, Merged: item.MergedAt, Failed: c.DeployFailed})
	}
	return iid
}

// demoChangeCount is the world's pull and merge requests.
func demoChangeCount() int {
	count := 0
	for _, team := range demoTeams {
		for _, story := range team.Stories {
			count += len(story.Changes)
		}
	}
	return count
}

// ---- answers ----

func demoTimeQuery(request replayRequest, keys ...string) (time.Time, bool) {
	for _, key := range keys {
		if value := request.Query.Get(key); value != "" {
			parsed, err := time.Parse(time.RFC3339Nano, value)
			if err == nil {
				return parsed, true
			}
		}
	}
	return time.Time{}, false
}

func demoInWindow(at string, request replayRequest, sinceKeys, untilKeys []string) bool {
	instant, err := time.Parse(time.RFC3339, at)
	if err != nil {
		panic(err)
	}
	if since, ok := demoTimeQuery(request, sinceKeys...); ok && instant.Before(since) {
		return false
	}
	if until, ok := demoTimeQuery(request, untilKeys...); ok && instant.After(until) {
		return false
	}
	return true
}

// demoCreatedMatches answers GitHub's `created` search qualifier the way the API does: "A..B" (either
// side "*") or ">=YYYY-MM-DD".
func demoCreatedMatches(at, created string) bool {
	if created == "" {
		return true
	}
	instant, err := time.Parse(time.RFC3339, at)
	if err != nil {
		panic(err)
	}
	if day, ok := strings.CutPrefix(created, ">="); ok {
		from, err := time.Parse(time.DateOnly, day)
		if err != nil {
			panic(fmt.Sprintf("created qualifier %q", created))
		}
		return !instant.Before(from)
	}
	low, high, ok := strings.Cut(created, "..")
	if !ok {
		panic(fmt.Sprintf("created qualifier %q", created))
	}
	for _, bound := range []struct {
		text  string
		after bool
	}{{low, false}, {high, true}} {
		if bound.text == "*" {
			continue
		}
		limit, err := time.Parse(time.RFC3339, bound.text)
		if err != nil {
			panic(fmt.Sprintf("created qualifier %q", created))
		}
		if (!bound.after && instant.Before(limit)) || (bound.after && instant.After(limit)) {
			return false
		}
	}
	return true
}

func newestFirst(commits []demoCommit) []demoCommit {
	out := append([]demoCommit{}, commits...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].At > out[j].At })
	return out
}

func demoLines(sha string) (int, int) {
	return 5 + int(sha[0])%40, int(sha[1]) % 12
}

func demoGitHubAnswers(build *demoBuild) []providerAnswer {
	var answers []providerAnswer
	answers = append(answers, githubTeamCatalogAnswersFor(demoGitHubTeams, "")...)
	for _, name := range demoGitHubRepoNames {
		repo := build.GitHub[name]
		root := "/repos/" + githubOrg + "/" + name
		answers = append(answers, githubRepoAnswers(name, repo.Items)...)
		answers = append(answers, githubPRSocialAnswer(name, repo.Items))
		commits := newestFirst(repo.Commits)
		answers = append(answers, providerAnswer{Method: "GET", Host: githubHost, Path: root + "/commits", Query: "*", Status: 200,
			Respond: func(request replayRequest) (any, bool) {
				out := []any{}
				for _, commit := range commits {
					if !demoInWindow(commit.At, request, []string{"since"}, []string{"until"}) {
						continue
					}
					person := map[string]any{"name": commit.Author, "email": commit.Author + "@demo.example", "date": commit.At}
					out = append(out, map[string]any{"sha": commit.SHA, "commit": map[string]any{"message": commit.Message, "author": person, "committer": person},
						"author": map[string]any{"login": commit.Author}, "committer": map[string]any{"login": commit.Author}, "parents": []any{map[string]any{"sha": "0000"}}})
				}
				return out, true
			}})
		for _, commit := range commits {
			files := []any{}
			for _, file := range commit.Files {
				additions, deletions := demoLines(demoSHA(commit.SHA, file))
				files = append(files, map[string]any{"filename": file, "additions": additions, "deletions": deletions})
			}
			answers = append(answers, get(githubHost, root+"/commits/"+commit.SHA, "", map[string]any{"sha": commit.SHA, "files": files}))
		}
		runs := repo.Runs
		answers = append(answers, providerAnswer{Method: "GET", Host: githubHost, Path: root + "/actions/runs", Query: "*", Status: 200,
			Respond: func(request replayRequest) (any, bool) {
				out := []any{}
				branch := request.Query.Get("branch")
				for _, run := range runs {
					if branch != "" && run.Branch != branch {
						continue
					}
					if !demoCreatedMatches(run.At, request.Query.Get("created")) {
						continue
					}
					conclusion, event := "success", "push"
					if run.Failed {
						conclusion = "failure"
					}
					if run.Branch != "main" {
						event = "pull_request"
					}
					started, _ := time.Parse(time.RFC3339, run.At)
					out = append(out, map[string]any{"id": run.ID, "name": "CI", "status": "completed", "conclusion": conclusion, "event": event,
						"created_at": run.At, "run_started_at": started.Add(time.Minute).Format(time.RFC3339),
						"updated_at": started.Add(9 * time.Minute).Format(time.RFC3339), "run_attempt": 1, "head_sha": run.SHA, "head_branch": run.Branch,
						"html_url": fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d", githubOrg, name, run.ID), "pull_requests": []any{}})
				}
				return map[string]any{"total_count": len(out), "workflow_runs": out}, true
			}})
		for _, run := range runs {
			started, _ := time.Parse(time.RFC3339, run.At)
			jobs := []any{}
			names := []string{"build", "test"}
			if run.Deploy {
				names = append(names, "deploy")
			}
			for index, job := range names {
				conclusion := "success"
				if run.Failed && job == "test" {
					conclusion = "failure"
				}
				jobs = append(jobs, map[string]any{"id": run.ID*10 + index, "name": job, "status": "completed", "conclusion": conclusion,
					"started_at":   started.Add(time.Duration(1+3*index) * time.Minute).Format(time.RFC3339),
					"completed_at": started.Add(time.Duration(4+3*index) * time.Minute).Format(time.RFC3339), "labels": []string{"ubuntu-latest"}})
			}
			answers = append(answers, get(githubHost, fmt.Sprintf("%s/actions/runs/%d/jobs", root, run.ID), "*", map[string]any{"total_count": len(jobs), "jobs": jobs}))
			if run.Branch == "main" {
				answers = append(answers, get(githubHost, fmt.Sprintf("%s/actions/runs/%d/artifacts", root, run.ID), "*", map[string]any{"total_count": 0, "artifacts": []any{}}))
			}
		}
		deployments := []any{}
		for index := len(repo.Deploys) - 1; index >= 0; index-- {
			deploy := repo.Deploys[index]
			deployments = append(deployments, map[string]any{"id": deploy.ID, "sha": deploy.SHA, "ref": "main", "environment": "production",
				"created_at": deploy.At, "payload": map[string]any{}})
		}
		sort.SliceStable(deployments, func(i, j int) bool {
			return deployments[i].(map[string]any)["created_at"].(string) > deployments[j].(map[string]any)["created_at"].(string)
		})
		answers = append(answers,
			get(githubHost, root+"/releases", "*", []any{}),
			get(githubHost, root+"/deployments", "*", deployments))
		for _, deploy := range repo.Deploys {
			at, _ := time.Parse(time.RFC3339, deploy.At)
			final := "success"
			if deploy.Failed {
				final = "failure"
			}
			answers = append(answers,
				get(githubHost, root+"/commits/"+deploy.SHA+"/pulls", "*", []any{map[string]any{"number": deploy.MR, "merged_at": deploy.Merged, "merge_commit_sha": deploy.SHA}}),
				get(githubHost, fmt.Sprintf("%s/deployments/%d/statuses", root, deploy.ID), "*", []any{
					map[string]any{"state": "in_progress", "created_at": at.Add(30 * time.Second).Format(time.RFC3339)},
					map[string]any{"state": final, "created_at": at.Add(6 * time.Minute).Format(time.RFC3339)},
				}))
		}
	}
	answers = append(answers, demoGitHubSecurityAnswers()...)
	return answers
}

// demoGitHubSecurityAnswers: the identity repositories' Dependabot, code scanning and advisory alerts.
func demoGitHubSecurityAnswers() []providerAnswer {
	dependabot := func(repo string, number int, severity, cve, summary, pkg string, daysAgo int) map[string]any {
		return map[string]any{"number": number, "state": "open", "created_at": demoAt(daysAgo, 7, 0), "fixed_at": nil, "dismissed_at": nil,
			"html_url":          fmt.Sprintf("https://github.com/%s/%s/security/dependabot/%d", githubOrg, repo, number),
			"security_advisory": map[string]any{"severity": severity, "cve_id": cve, "summary": summary, "description": summary + "."},
			"dependency":        map[string]any{"package": map[string]any{"name": pkg}}}
	}
	service := "/repos/" + githubOrg + "/identity-service"
	sdk := "/repos/" + githubOrg + "/identity-sdk"
	return []providerAnswer{
		get(githubHost, service+"/dependabot/alerts", "per_page=100&state=open", []any{
			dependabot("identity-service", 11, "critical", "CVE-2026-31011", "XML external entity expansion in the XML parser", "xmlparse", 9),
			dependabot("identity-service", 12, "high", "CVE-2026-31012", "JWT algorithm confusion", "jwt-core", 46),
		}),
		get(githubHost, service+"/code-scanning/alerts", "per_page=100&state=open", []any{
			map[string]any{"number": 4, "state": "open", "created_at": demoAt(27, 7, 0),
				"html_url":             fmt.Sprintf("https://github.com/%s/identity-service/security/code-scanning/4", githubOrg),
				"rule":                 map[string]any{"severity": "error", "description": "Missing rate limit on password reset"},
				"most_recent_instance": map[string]any{"message": map[string]any{"text": "The reset handler has no rate limit."}}},
		}),
		get(githubHost, service+"/security-advisories", "per_page=100", []any{
			map[string]any{"ghsa_id": "GHSA-dm01-idnt-0001", "state": "published", "severity": "high", "cve_id": nil, "created_at": demoAt(64, 7, 0),
				"html_url": fmt.Sprintf("https://github.com/%s/identity-service/security/advisories/GHSA-dm01-idnt-0001", githubOrg),
				"summary":  "Refresh tokens survive a password reset", "description": "Fixed by revoking tokens on reset."},
		}),
		get(githubHost, sdk+"/dependabot/alerts", "per_page=100&state=open", []any{
			dependabot("identity-sdk", 3, "high", "CVE-2026-31013", "Token written to debug logs", "logkit", 15),
		}),
		get(githubHost, sdk+"/code-scanning/alerts", "per_page=100&state=open", []any{}),
		get(githubHost, sdk+"/security-advisories", "per_page=100", []any{}),
	}
}

func demoGitLabCommit(commit demoCommit) map[string]any {
	return map[string]any{"id": commit.SHA, "short_id": commit.SHA[:8], "message": commit.Message + "\n", "title": commit.Message,
		"author_name": commit.Author, "authored_date": commit.At, "committer_name": commit.Author, "committed_date": commit.At,
		"parent_ids": []string{demoSHA("parent", commit.SHA)}}
}

func demoGitLabAnswers(build *demoBuild) []providerAnswer {
	var answers []providerAnswer
	answers = append(answers, gitlabTeamCatalogAnswers(demoGitLabGroups, "")...)
	for _, path := range demoGitLabProjectPaths {
		project := build.GitLab[path]
		meta := gitlabProjects[path]
		root := "/api/v4/projects/" + strconv.Itoa(meta.ID)
		for _, answer := range gitlabProjectAnswers(path, project.Items) {
			if answer.Path == root+"/issues" && answer.Query == "*" {
				answer = demoGitLabIssuesAnswer(answer, project, meta)
			}
			answers = append(answers, answer)
		}
		commits := newestFirst(project.Commits)
		answers = append(answers, providerAnswer{Method: "GET", Host: gitlabHost, Path: root + "/repository/commits", Query: "*", Status: 200,
			Headers: http.Header{"X-Next-Page": {""}},
			Respond: func(request replayRequest) (any, bool) {
				out := []any{}
				for _, commit := range commits {
					if demoInWindow(commit.At, request, []string{"since"}, []string{"until"}) {
						out = append(out, demoGitLabCommit(commit))
					}
				}
				return out, true
			}})
		for _, commit := range commits {
			additions, deletions := 0, 0
			for _, file := range commit.Files {
				a, d := demoLines(demoSHA(commit.SHA, file))
				additions, deletions = additions+a, deletions+d
			}
			body := demoGitLabCommit(commit)
			body["stats"] = map[string]any{"additions": additions, "deletions": deletions, "total": additions + deletions}
			answers = append(answers, get(gitlabHost, root+"/repository/commits/"+commit.SHA, "", body))
		}
		pipelines := []any{}
		for index := len(project.Pipelines) - 1; index >= 0; index-- {
			run := project.Pipelines[index]
			started, _ := time.Parse(time.RFC3339, run.At)
			status := "success"
			if run.Failed {
				status = "failed"
			}
			pipelines = append(pipelines, map[string]any{"id": run.ID, "status": status, "ref": run.Branch, "sha": run.SHA, "source": "push",
				"created_at": run.At, "started_at": started.Add(time.Minute).Format(time.RFC3339),
				"finished_at": started.Add(11 * time.Minute).Format(time.RFC3339), "updated_at": started.Add(11 * time.Minute).Format(time.RFC3339),
				"web_url": fmt.Sprintf("https://gitlab.com/%s/-/pipelines/%d", path, run.ID)})
			jobs := []any{}
			names := []string{"build", "test"}
			if run.Deploy {
				names = append(names, "deploy")
			}
			for jobIndex, job := range names {
				jobStatus := "success"
				if run.Failed && job == "test" {
					jobStatus = "failed"
				}
				jobs = append(jobs, map[string]any{"id": run.ID*10 + jobIndex, "name": job, "stage": job, "status": jobStatus,
					"started_at":  started.Add(time.Duration(1+3*jobIndex) * time.Minute).Format(time.RFC3339),
					"finished_at": started.Add(time.Duration(4+3*jobIndex) * time.Minute).Format(time.RFC3339), "tag_list": []string{"docker"}, "retried": false})
			}
			answers = append(answers, get(gitlabHost, fmt.Sprintf("%s/pipelines/%d/jobs", root, run.ID), "*", jobs))
			if run.Branch == "main" {
				notFound := get(gitlabHost, fmt.Sprintf("%s/pipelines/%d/test_report", root, run.ID), "", map[string]any{"message": "404 Not found"})
				notFound.Status = 404
				answers = append(answers, notFound)
			}
		}
		sort.SliceStable(pipelines, func(i, j int) bool {
			return pipelines[i].(map[string]any)["updated_at"].(string) > pipelines[j].(map[string]any)["updated_at"].(string)
		})
		answers = append(answers, get(gitlabHost, root+"/pipelines", "*", pipelines))
		deployments := []any{}
		for _, deploy := range project.Deploys {
			at, _ := time.Parse(time.RFC3339, deploy.At)
			status := "success"
			if deploy.Failed {
				status = "failed"
			}
			deployments = append(deployments, map[string]any{"id": deploy.ID, "iid": deploy.ID - 5000, "ref": "main", "sha": deploy.SHA, "status": status,
				"created_at": deploy.At, "updated_at": at.Add(8 * time.Minute).Format(time.RFC3339),
				"environment": map[string]any{"id": 3, "name": "production"},
				"deployable": map[string]any{"id": deploy.ID * 10, "status": status, "started_at": at.Add(time.Minute).Format(time.RFC3339),
					"finished_at": at.Add(8 * time.Minute).Format(time.RFC3339)}})
			answers = append(answers, get(gitlabHost, root+"/repository/commits/"+deploy.SHA+"/merge_requests", "*",
				[]any{map[string]any{"iid": deploy.MR, "state": "merged", "merged_at": deploy.Merged, "sha": deploy.SHA}}))
		}
		sort.SliceStable(deployments, func(i, j int) bool {
			return deployments[i].(map[string]any)["created_at"].(string) > deployments[j].(map[string]any)["created_at"].(string)
		})
		answers = append(answers,
			get(gitlabHost, root+"/releases", "*", []any{}),
			get(gitlabHost, root+"/deployments", "*", deployments))
	}
	return answers
}

// demoGitLabIssuesAnswer answers a project's issue list: its incidents for the incidents route
// (issue_type=incident), its other issues for the work-items route.
func demoGitLabIssuesAnswer(answer providerAnswer, project *demoGitLabProject, meta gitlabProject) providerAnswer {
	issues := answer.Body
	incidents := []any{}
	for index, incident := range project.Incidents {
		started := demoAt(incident.StartedAgo, incident.Hour, 0)
		at, _ := time.Parse(time.RFC3339, started)
		state, closed, updated := "opened", any(nil), at.Add(30*time.Minute).Format(time.RFC3339)
		if incident.ResolvedAfter >= 0 {
			state = "closed"
			closed = at.Add(time.Duration(incident.ResolvedAfter) * time.Hour).Format(time.RFC3339)
			updated = closed.(string)
		}
		incidents = append(incidents, map[string]any{"id": 880000 + meta.ID%1000*100 + index, "iid": 500 + index, "issue_type": "incident",
			"state": state, "title": incident.Title, "description": incident.Title + ". See the deploy pipeline runbook.",
			"severity": incident.Severity, "created_at": started, "updated_at": updated, "closed_at": closed,
			"web_url": fmt.Sprintf("https://gitlab.com/%s/-/issues/%d", meta.Path, 500+index)})
	}
	sort.SliceStable(incidents, func(i, j int) bool {
		return incidents[i].(map[string]any)["updated_at"].(string) > incidents[j].(map[string]any)["updated_at"].(string)
	})
	answer.Respond = func(request replayRequest) (any, bool) {
		if request.Query.Get("issue_type") == "incident" {
			if request.Query.Get("page") != "1" {
				return []any{}, true
			}
			return incidents, true
		}
		return issues, true
	}
	return answer
}

func demoJiraAnswers(build *demoBuild) []providerAnswer {
	projects := []any{demoJiraPAY.payload(), demoJiraSUP.payload()}
	search := func(values []any) map[string]any {
		return map[string]any{"self": "https://" + jiraHost + "/rest/api/3/project/search", "maxResults": 100, "startAt": 0,
			"total": len(values), "isLast": true, "values": values}
	}
	answers := []providerAnswer{
		get(jiraHost, "/rest/api/3/project/search", "maxResults=100", search(projects)),
		get(jiraHost, "/rest/api/3/project/search", "maxResults=100&status=archived", search([]any{})),
		atlassianGatewayAnswer(demoAtlassianTeams),
	}
	for index, project := range []jiraProject{demoJiraPAY, demoJiraSUP} {
		boardID := 400 + index
		answers = append(answers, get(jiraHost, "/rest/api/3/project/"+project.Key, "", project.payload()),
			get(jiraHost, "/rest/agile/1.0/board", "maxResults=100&projectKeyOrId="+project.Key+"&startAt=0", map[string]any{
				"maxResults": 100, "startAt": 0, "total": 1, "isLast": true, "values": []any{map[string]any{
					"id": boardID, "name": project.Key + " board", "type": "kanban",
					"self":     fmt.Sprintf("https://%s/rest/agile/1.0/board/%d", jiraHost, boardID),
					"location": map[string]any{"projectId": project.ID, "projectKey": project.Key, "projectName": project.Name},
				}}}),
			get(jiraHost, fmt.Sprintf("/rest/agile/1.0/board/%d/sprint", boardID), "*", map[string]any{
				"maxResults": 50, "startAt": 0, "isLast": true, "values": []any{}}),
		)
	}
	return append(answers, jiraIssueAnswers(build.Jira)...)
}

func demoAnswers() []providerAnswer {
	// The catalogs' dates follow the as-of day, which is known once the flags are parsed.
	registerDemoRepositories()
	build := newDemoBuild()
	var answers []providerAnswer
	answers = append(answers, demoGitHubAnswers(build)...)
	answers = append(answers, demoGitLabAnswers(build)...)
	answers = append(answers, demoJiraAnswers(build)...)
	answers = append(answers, linearGraphQLAnswer(demoLinearTeams, demoLinearProjectsDated(), build.Linear, ""))
	answers = append(answers, demoFileAnswers(build)...)
	return answers
}

// demoCommitStatsSteps sync the heavy commit-stats dataset in the seven-day windows successive incremental
// syncs read, oldest first, so every commit of the world's depth gets its stats; the last window ends at
// the instant.
func demoCommitStatsSteps(provider string, credential map[string]string, sources [][2]string) []worldStep {
	var steps []worldStep
	for days := demoDepthDays; days > 0; days -= 7 {
		steps = append(steps, worldStep{Kind: "sync", Provider: provider, Credential: credential, DepthDays: days, Sources: sources, Datasets: []string{"commit-stats"}})
	}
	return steps
}

func demoSources() (github, identity, gitlab [][2]string) {
	for _, name := range demoGitHubRepoNames {
		github = append(github, [2]string{githubOrg + "/" + name, name})
		if strings.HasPrefix(name, "identity-") {
			identity = append(identity, [2]string{githubOrg + "/" + name, name})
		}
	}
	for index, path := range demoGitLabProjectPaths {
		gitlab = append(gitlab, [2]string{strconv.Itoa(demoGitLabProjectID(index)), path})
	}
	return github, identity, gitlab
}

// demoCatalogBefore: the team catalogs were first synced when the world's history starts, so the teams
// own their repositories and projects over the whole history.
const demoCatalogBefore = demoDepthDays * 24 * time.Hour

func demoSteps() []worldStep {
	github, identity, gitlab := demoSources()
	jiraSources := [][2]string{{demoJiraPAY.Key, demoJiraPAY.Name}, {demoJiraSUP.Key, demoJiraSUP.Name}}
	steps := []worldStep{
		{Kind: "team_catalog", Provider: "jira", Credential: jiraCredential, Config: jiraConfig, Sources: jiraSources, Before: demoCatalogBefore},
		{Kind: "team_catalog", Provider: "github", Credential: githubToken, Config: map[string]string{"org": githubOrg}, Before: demoCatalogBefore},
		{Kind: "team_catalog", Provider: "gitlab", Credential: gitlabToken, Sources: gitlab, SyncOptions: map[string]any{"group_path": "chaos-gl"}, Before: demoCatalogBefore},
		{Kind: "team_catalog", Provider: "linear", Credential: linearToken, Before: demoCatalogBefore},
		{Kind: "sync", Provider: "github", Credential: githubToken, DepthDays: demoDepthDays, Sources: github,
			Datasets: append([]string{"repo-metadata", "prs", "commits", "cicd", "deployments"}, workItemDatasets...)},
		{Kind: "sync", Provider: "github", Credential: githubToken, DepthDays: demoDepthDays, Sources: identity, Datasets: []string{"security"}},
		{Kind: "sync", Provider: "gitlab", Credential: gitlabToken, DepthDays: demoDepthDays, Sources: gitlab,
			Datasets: append([]string{"repo-metadata", "prs", "commits", "cicd", "deployments", "incidents"}, workItemDatasets...)},
		{Kind: "sync", Provider: "jira", Credential: jiraCredential, Config: jiraConfig, DepthDays: demoDepthDays, Sources: jiraSources,
			Datasets: workItemDatasets, DatasetOptions: map[string]any{"fetch_dev_status": true, "story_points_field": jiraStoryPointsField}},
		{Kind: "sync", Provider: "linear", Credential: linearToken, DepthDays: demoDepthDays,
			Sources: [][2]string{{"CHK", "Checkout"}, {"MOB", "Mobile"}}, Datasets: workItemDatasets},
	}
	steps = append(steps, demoCommitStatsSteps("github", githubToken, github)...)
	steps = append(steps, demoCommitStatsSteps("gitlab", gitlabToken, gitlab)...)
	// The files at the instant: the tree and the text of the default branch, which the complexity job
	// scans.
	steps = append(steps,
		worldStep{Kind: "sync", Provider: "github", Credential: githubToken, DepthDays: 7, Sources: github, Datasets: []string{"files"}},
		worldStep{Kind: "sync", Provider: "gitlab", Credential: gitlabToken, DepthDays: 7, Sources: gitlab, Datasets: []string{"files"}})
	return append(steps, worldStep{Kind: "work_graph"}, worldStep{Kind: "ownership_derivation"}, worldStep{Kind: "repo_complexity"})
}

// demoWorld is the demo world as the committed file holds it: frozen at demoFrozenAsOf. The parameters do
// not name the as-of day; the file's frozen_at does, and a load moves the world to the load day.
var demoWorld = providerWorld{
	Params: GenerateParams{
		Provider: "mixed", RepoName: "chaos-fixtures/shared-api", RepoCount: len(demoGitHubRepoNames) + len(demoGitLabProjectPaths), Days: demoDepthDays - 1,
		CommitsPerDay: 0, PRCount: demoChangeCount(), TeamCount: 8, Seed: 9440,
	},
	OrgID:    "f1c7e000-0000-4000-8000-00000000a0d0",
	FrozenAt: demoInstantOf(demoFrozenAsOf),
	Answers:  demoAnswers,
	Steps:    demoSteps(),
}

// demoWorldAsOf is the demo world of this run's as-of day (-demo-as-of): the one a freeze writes.
func demoWorldAsOf() providerWorld {
	world := demoWorld
	world.FrozenAt = demoInstant()
	return world
}
