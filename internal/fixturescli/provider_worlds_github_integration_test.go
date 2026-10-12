//go:build integration

package fixturescli

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// githubIssue is one GitHub issue or pull request of a fixture repository.
type githubIssue struct {
	Number    int
	Title     string
	Body      string
	Author    githubUser
	Assignees []githubUser
	Labels    []string
	CreatedAt string
	UpdatedAt string
	ClosedAt  string // "" while open
	// Pull request fields.
	PR       bool
	MergedAt string
	Head     string
	// Closes are the issues GitHub lists as the PR's closingIssuesReferences ("owner/repo#n").
	Closes []string
}

func (issue githubIssue) state() string {
	if issue.ClosedAt == "" {
		return "open"
	}
	return "closed"
}

func nullable(text string) any {
	if text == "" {
		return nil
	}
	return text
}

func githubLabels(names []string) []map[string]any {
	out := []map[string]any{}
	for index, name := range names {
		out = append(out, map[string]any{
			"id": 5000 + index, "node_id": fmt.Sprintf("LA_fx%d", 5000+index), "name": name,
			"color": "ededed", "default": false, "description": nil,
		})
	}
	return out
}

func githubUsers(users []githubUser) []map[string]any {
	out := []map[string]any{}
	for _, user := range users {
		out = append(out, user.simple())
	}
	return out
}

func (issue githubIssue) restIssue(repo string) map[string]any {
	full := githubOrg + "/" + repo
	var assignee any
	if len(issue.Assignees) > 0 {
		assignee = issue.Assignees[0].simple()
	}
	out := map[string]any{
		"id": 400000 + issue.Number, "node_id": fmt.Sprintf("I_fx%s%d", repo, issue.Number),
		"url":      fmt.Sprintf("https://api.github.com/repos/%s/issues/%d", full, issue.Number),
		"html_url": fmt.Sprintf("https://github.com/%s/issues/%d", full, issue.Number),
		"number":   issue.Number, "title": issue.Title, "body": nullable(issue.Body), "state": issue.state(),
		"state_reason": nil, "locked": false, "user": issue.Author.simple(), "labels": githubLabels(issue.Labels),
		"assignee": assignee, "assignees": githubUsers(issue.Assignees), "milestone": nil, "comments": 0,
		"created_at": issue.CreatedAt, "updated_at": issue.UpdatedAt, "closed_at": nullable(issue.ClosedAt),
		"author_association": "MEMBER", "draft": false,
	}
	if issue.ClosedAt != "" {
		out["state_reason"] = "completed"
	}
	if issue.PR {
		out["html_url"] = fmt.Sprintf("https://github.com/%s/pull/%d", full, issue.Number)
		out["pull_request"] = map[string]any{
			"url":       fmt.Sprintf("https://api.github.com/repos/%s/pulls/%d", full, issue.Number),
			"html_url":  fmt.Sprintf("https://github.com/%s/pull/%d", full, issue.Number),
			"merged_at": nullable(issue.MergedAt),
		}
	}
	return out
}

// restEvents is the issue's event list: the assignment when it was opened, and its close.
func (issue githubIssue) restEvents(repo string) []map[string]any {
	events := []map[string]any{}
	event := func(id int, kind, at string, actor githubUser) map[string]any {
		return map[string]any{
			"id": id, "node_id": fmt.Sprintf("E_fx%d", id), "event": kind, "created_at": at, "actor": actor.simple(),
			"url":       fmt.Sprintf("https://api.github.com/repos/%s/%s/issues/events/%d", githubOrg, repo, id),
			"commit_id": nil, "commit_url": nil, "performed_via_github_app": nil,
		}
	}
	for index, assignee := range issue.Assignees {
		assigned := event(800000+issue.Number*10+index, "assigned", issue.CreatedAt, issue.Author)
		assigned["assignee"], assigned["assigner"] = assignee.simple(), issue.Author.simple()
		events = append(events, assigned)
	}
	if issue.ClosedAt != "" {
		events = append(events, event(800000+issue.Number*10+9, "closed", issue.ClosedAt, issue.Author))
	}
	return events
}

func (issue githubIssue) restPull(repo string, detail bool) map[string]any {
	full := githubOrg + "/" + repo
	ref := func(name string) map[string]any {
		return map[string]any{"label": githubOrg + ":" + name, "ref": name, "sha": fmt.Sprintf("%040x", issue.Number*7919+len(name)),
			"repo": githubRepos[repo].payload(), "user": map[string]any{"login": githubOrg, "id": 7000, "type": "Organization"}}
	}
	out := map[string]any{
		"id": 600000 + issue.Number, "node_id": fmt.Sprintf("PR_fx%s%d", repo, issue.Number),
		"url":      fmt.Sprintf("https://api.github.com/repos/%s/pulls/%d", full, issue.Number),
		"html_url": fmt.Sprintf("https://github.com/%s/pull/%d", full, issue.Number),
		"number":   issue.Number, "state": issue.state(), "locked": false, "title": issue.Title,
		"user": issue.Author.simple(), "body": nullable(issue.Body), "labels": githubLabels(issue.Labels),
		"assignee": nil, "assignees": githubUsers(issue.Assignees), "requested_reviewers": []any{},
		"created_at": issue.CreatedAt, "updated_at": issue.UpdatedAt, "closed_at": nullable(issue.ClosedAt),
		"merged_at": nullable(issue.MergedAt), "draft": false, "head": ref(issue.Head), "base": ref("main"),
		"author_association": "MEMBER",
	}
	if detail {
		out["merged"] = issue.MergedAt != ""
		out["comments"], out["review_comments"], out["commits"] = 0, 0, 1
		out["additions"], out["deletions"], out["changed_files"] = 10*issue.Number, 3*issue.Number, 1+issue.Number%3
		out["mergeable"], out["mergeable_state"] = nil, "unknown"
	}
	return out
}

// githubRepoAnswers are the answers to every read the repo-metadata, prs and work-items routes make
// of one repository.
func githubRepoAnswers(repo string, items []githubIssue) []providerAnswer {
	root := "/repos/" + githubOrg + "/" + repo
	issues := []map[string]any{}
	pulls := []map[string]any{}
	answers := []providerAnswer{get(githubHost, root, "", githubRepos[repo].payload())}
	for _, item := range items {
		issues = append(issues, item.restIssue(repo))
		if item.PR {
			pulls = append(pulls, item.restPull(repo, false))
			answers = append(answers, get(githubHost, root+"/pulls/"+strconv.Itoa(item.Number), "", item.restPull(repo, true)))
		}
	}
	answers = append(answers,
		get(githubHost, root+"/issues", "*", issues),
		get(githubHost, root+"/pulls", "*", pulls),
		get(githubHost, root+"/milestones", "*", []any{}),
	)
	for _, item := range items {
		if item.PR {
			continue
		}
		answers = append(answers,
			get(githubHost, root+"/issues/"+strconv.Itoa(item.Number)+"/events", "*", item.restEvents(repo)),
			get(githubHost, root+"/issues/"+strconv.Itoa(item.Number)+"/comments", "*", []any{}),
		)
	}
	return answers
}

var graphQLAlias = regexp.MustCompile(`pr(\d+): pullRequest\(number: (\d+)\)`)

// githubPRSocialAnswer answers the PR social batch query (comments, merge/close events and
// closingIssuesReferences of each aliased pull request) of one repository.
func githubPRSocialAnswer(repo string, items []githubIssue) providerAnswer {
	byNumber := map[int]githubIssue{}
	for _, item := range items {
		byNumber[item.Number] = item
	}
	return providerAnswer{
		Method: "POST", Host: githubHost, Path: "/graphql", Status: 200,
		Respond: func(query string, variables json.RawMessage) (any, bool) {
			var vars struct{ Owner, Repo string }
			if json.Unmarshal(variables, &vars) != nil || vars.Owner != githubOrg || vars.Repo != repo || !strings.Contains(query, "pullRequest(number:") {
				return nil, false
			}
			repository := map[string]any{}
			for _, match := range graphQLAlias.FindAllStringSubmatch(query, -1) {
				number, _ := strconv.Atoi(match[2])
				item, ok := byNumber[number]
				if !ok {
					return nil, false
				}
				pull := map[string]any{"number": number}
				if strings.Contains(query, "comments(first:") {
					pull["comments"] = map[string]any{"nodes": []any{}, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}}
				}
				if strings.Contains(query, "timelineItems(") {
					events := []any{}
					if item.MergedAt != "" {
						events = append(events, map[string]any{"__typename": "MergedEvent", "createdAt": item.MergedAt, "actor": map[string]any{"login": item.Author.Login}})
					}
					pull["timelineItems"] = map[string]any{"nodes": events, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}}
				}
				if strings.Contains(query, "closingIssuesReferences(") {
					nodes := []any{}
					for _, ref := range item.Closes {
						name, number, _ := strings.Cut(ref, "#")
						n, _ := strconv.Atoi(number)
						nodes = append(nodes, map[string]any{"number": n, "repository": map[string]any{"nameWithOwner": name}})
					}
					pull["closingIssuesReferences"] = map[string]any{"nodes": nodes, "pageInfo": map[string]any{"hasNextPage": false}}
				}
				repository["pr"+match[1]] = pull
			}
			return map[string]any{"data": map[string]any{"repository": repository}}, true
		},
	}
}

// The payments-api repository: issues and pull requests over five months, most of them finished
// before the organization's first sync (the frozen instant).
var githubPaymentsItems = []githubIssue{
	{Number: 1, Title: "Card refunds fail on retry", Body: "A retried refund is charged twice.", Author: ghAlice, Assignees: []githubUser{ghAlice},
		Labels: []string{"bug"}, CreatedAt: "2026-06-10T09:00:00Z", UpdatedAt: "2026-06-20T16:00:00Z", ClosedAt: "2026-06-20T16:00:00Z"},
	{Number: 2, Title: "Add payout export", Body: "Finance needs a CSV export of payouts.", Author: ghCarol, Assignees: []githubUser{ghCarol},
		Labels: []string{"enhancement"}, CreatedAt: "2026-08-01T10:00:00Z", UpdatedAt: "2026-09-15T11:00:00Z", ClosedAt: "2026-09-15T11:00:00Z"},
	{Number: 3, Title: "Rotate the webhook signing secret", Body: "Rotate it every 90 days.", Author: ghAlice, Assignees: []githubUser{ghAlice},
		Labels: []string{"security"}, CreatedAt: "2026-10-01T08:00:00Z", UpdatedAt: "2026-10-08T08:00:00Z"},
	// Native link: GitHub lists issue 1 in the PR's closingIssuesReferences.
	{Number: 4, Title: "Fix refund retry double charge", Body: "Fixes #1", Author: ghAlice, PR: true, Head: "fix/refund-retry",
		CreatedAt: "2026-06-12T09:00:00Z", UpdatedAt: "2026-06-19T15:00:00Z", ClosedAt: "2026-06-19T15:00:00Z", MergedAt: "2026-06-19T15:00:00Z",
		Closes: []string{githubOrg + "/payments-api#1"}},
	// Explicit text link only: the body names the issue, GitHub records no closing reference.
	{Number: 5, Title: "Payout CSV export", Body: "Implements the export asked for in #2.", Author: ghCarol, PR: true, Head: "feature/payout-export",
		CreatedAt: "2026-09-08T09:00:00Z", UpdatedAt: "2026-09-14T15:00:00Z", ClosedAt: "2026-09-14T15:00:00Z", MergedAt: "2026-09-14T15:00:00Z"},
	// Heuristic link only: no reference, same author as the open issue 3, inside the heuristic window.
	{Number: 6, Title: "Webhook signing secret rotation", Body: "Rotates the secret on a schedule.", Author: ghAlice, PR: true, Head: "feature/webhook-rotation",
		CreatedAt: "2026-10-05T09:00:00Z", UpdatedAt: "2026-10-09T09:00:00Z"},
}

// The platform-core repository: two teams hold it (payments and platform), so its work has two owners.
var githubPlatformItems = []githubIssue{
	{Number: 1, Title: "Connection pool exhausts under load", Body: "The pool caps at 10.", Author: ghBob, Assignees: []githubUser{ghBob},
		Labels: []string{"bug"}, CreatedAt: "2026-05-20T09:00:00Z", UpdatedAt: "2026-06-02T10:00:00Z", ClosedAt: "2026-06-02T10:00:00Z"},
	{Number: 2, Title: "Upgrade the HTTP router", Body: "Move to the maintained fork.", Author: ghCarol, Assignees: []githubUser{ghCarol},
		Labels: []string{"maintenance"}, CreatedAt: "2026-09-20T09:00:00Z", UpdatedAt: "2026-10-03T09:00:00Z", ClosedAt: "2026-10-03T09:00:00Z"},
	{Number: 3, Title: "Raise the connection pool cap", Body: "Fixes #1", Author: ghBob, PR: true, Head: "fix/pool-cap",
		CreatedAt: "2026-05-28T09:00:00Z", UpdatedAt: "2026-06-01T15:00:00Z", ClosedAt: "2026-06-01T15:00:00Z", MergedAt: "2026-06-01T15:00:00Z",
		Closes: []string{githubOrg + "/platform-core#1"}},
	// A pull request of one repository that closes an issue of another repository.
	{Number: 4, Title: "Router upgrade and refund retry guard", Body: "Fixes #2 and closes chaos-fixtures/payments-api#3", Author: ghCarol, PR: true, Head: "chore/router-upgrade",
		CreatedAt: "2026-09-25T09:00:00Z", UpdatedAt: "2026-10-02T15:00:00Z", ClosedAt: "2026-10-02T15:00:00Z", MergedAt: "2026-10-02T15:00:00Z",
		Closes: []string{githubOrg + "/platform-core#2", githubOrg + "/payments-api#3"}},
}
