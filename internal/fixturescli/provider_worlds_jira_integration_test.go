//go:build integration

package fixturescli

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ---- Jira: answers in the shape of the Jira Cloud REST v3 / Agile 1.0 / dev-status answers and of the
// Atlassian GraphQL gateway (the shapes the CHAOS-7902 and CHAOS-9124 probes measured) ----

const (
	jiraHost    = "chaos-fx.atlassian.net"
	jiraCloudID = "5b0e3c1a-7f00-4c8e-9a51-0fa1c0de0001"
	jiraOrgID   = "0b1c2d3e-4f50-4a6b-8c7d-0e1f2a3b4c5d"
)

var jiraCredential = map[string]string{"email": "fixtures@fixtures.example", "api_token": "fixture-world-placeholder"}

var jiraConfig = map[string]string{
	"base_url": "https://" + jiraHost, "atlassian_cloud_id": jiraCloudID, "atlassian_organization_id": jiraOrgID,
}

type jiraUser struct {
	AccountID string
	Name      string
	Email     string
}

var (
	jiraHana = jiraUser{"712020:fx-hana", "Hana Example", "hana@fixtures.example"}
	jiraIvan = jiraUser{"712020:fx-ivan", "Ivan Example", "ivan@fixtures.example"}
	jiraJude = jiraUser{"712020:fx-jude", "Jude Example", "jude@fixtures.example"}
	jiraKai  = jiraUser{"712020:fx-kai", "Kai Example", "kai@fixtures.example"}
)

func (user jiraUser) payload() map[string]any {
	return map[string]any{
		"self":      "https://" + jiraHost + "/rest/api/3/user?accountId=" + user.AccountID,
		"accountId": user.AccountID, "displayName": user.Name, "emailAddress": user.Email,
		"active": true, "accountType": "atlassian", "timeZone": "UTC",
	}
}

type jiraProject struct {
	ID, Key, Name string
}

var (
	jiraPAY = jiraProject{"10001", "PAY", "Payments"}
	jiraOPS = jiraProject{"10002", "OPS", "Operations"}
)

func (project jiraProject) payload() map[string]any {
	return map[string]any{
		"self": "https://" + jiraHost + "/rest/api/3/project/" + project.ID,
		"id":   project.ID, "key": project.Key, "name": project.Name, "projectTypeKey": "software",
		"simplified": true, "style": "next-gen", "isPrivate": false,
		"avatarUrls": map[string]any{"48x48": "https://" + jiraHost + "/rest/api/3/universal_avatar/view/type/project/avatar/10400"},
	}
}

// atlassianTeam is one Atlassian Team: its members and the Jira projects it is connected to.
type atlassianTeam struct {
	ID       string // the team's uuid; the ARI is ari:cloud:identity::team/<ID>
	Name     string
	State    string
	Members  []jiraUser
	Projects []jiraProject
}

var atlassianTeams = []atlassianTeam{
	{"9a1f0c3e-0000-4000-8000-0000000a0001", "Payments Squad", "ACTIVE", []jiraUser{jiraHana, jiraIvan}, []jiraProject{jiraPAY}},
	// Payments has a second active owning team.
	{"9a1f0c3e-0000-4000-8000-0000000a0002", "Platform Squad", "ACTIVE", []jiraUser{jiraIvan, jiraJude}, []jiraProject{jiraPAY, jiraOPS}},
	// An archived team that is still connected to a project: it owns nothing any more.
	{"9a1f0c3e-0000-4000-8000-0000000a0003", "Legacy Ops", "ARCHIVED", []jiraUser{jiraKai}, []jiraProject{jiraOPS}},
	// An active team with no connected project: it owns nothing.
	{"9a1f0c3e-0000-4000-8000-0000000a0004", "Design Guild", "ACTIVE", []jiraUser{jiraJude}, nil},
}

func ariTeam(id string) string { return "ari:cloud:identity::team/" + id }

func atlassianAriNode(id, typename string, data map[string]any) map[string]any {
	data["__typename"] = typename
	return map[string]any{"__typename": "GraphStoreCypherQueryV2AriNode", "id": id, "data": data}
}

// atlassianGatewayAnswer answers the three gateway reads of the Atlassian Teams collection of these teams: the team
// search, a team's users and a team's connected containers.
func atlassianGatewayAnswer(atlassianTeams []atlassianTeam) providerAnswer {
	byARI := map[string]atlassianTeam{}
	for _, team := range atlassianTeams {
		byARI[ariTeam(team.ID)] = team
	}
	return providerAnswer{
		Method: "POST", Host: jiraHost, Path: "/gateway/api/graphql", Status: 200,
		Respond: func(request replayRequest) (any, bool) {
			query, variables := request.Document, request.Variables
			var vars map[string]any
			if json.Unmarshal(variables, &vars) != nil {
				return nil, false
			}
			switch {
			case strings.Contains(query, "teamSearchV2("):
				if vars["after"] != nil {
					return nil, false
				}
				nodes := []any{}
				for _, team := range atlassianTeams {
					nodes = append(nodes, map[string]any{"team": map[string]any{
						"id": ariTeam(team.ID), "displayName": team.Name, "state": team.State,
						"smallAvatarImageUrl": "https://avatar-management.services.atlassian.com/default/16",
					}})
				}
				return map[string]any{"data": map[string]any{"team": map[string]any{"teamSearchV2": map[string]any{
					"pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}, "nodes": nodes,
				}}}}, true
			case strings.Contains(query, "teamworkGraph_teamUsers("):
				team, ok := byARI[fmt.Sprint(vars["teamId"])]
				if !ok || vars["after"] != nil {
					return nil, false
				}
				edges := []any{}
				for _, member := range team.Members {
					edges = append(edges, map[string]any{"node": map[string]any{"columns": []any{
						map[string]any{"key": "user", "value": atlassianAriNode("ari:cloud:identity::user/"+member.AccountID, "AtlassianAccountUser",
							map[string]any{"id": "ari:cloud:identity::user/" + member.AccountID, "accountId": member.AccountID, "name": member.Name})},
					}}})
				}
				return map[string]any{"data": map[string]any{"teamworkGraph_teamUsers": map[string]any{
					"version": "1", "pageInfo": map[string]any{"hasNextPage": false, "hasPreviousPage": false}, "edges": edges,
				}}}, true
			case strings.Contains(query, "graphStore_teamConnectedToContainer("):
				team, ok := byARI[fmt.Sprint(vars["id"])]
				if !ok || vars["after"] != nil {
					return nil, false
				}
				edges := []any{}
				for _, project := range team.Projects {
					edges = append(edges, map[string]any{"id": "edge-" + team.ID + "-" + project.ID, "node": map[string]any{
						"__typename": "JiraProject", "id": "ari:cloud:jira:" + jiraCloudID + ":project/" + project.ID,
						"key": project.Key, "projectId": project.ID,
					}})
				}
				return map[string]any{"data": map[string]any{"graphStore_teamConnectedToContainer": map[string]any{
					"pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}, "edges": edges,
				}}}, true
			}
			return nil, false
		},
	}
}

// jiraIssue is one Jira issue of a fixture project.
type jiraIssue struct {
	ID       string
	Key      string
	Project  jiraProject
	Summary  string
	Type     string
	Assignee jiraUser
	Reporter jiraUser
	Created  string
	Updated  string
	Resolved string // "" while open
	Labels   []string
	// PullRequests are the pull requests the GitHub-for-Jira and GitLab-for-Jira apps attach to the
	// issue (the dev-status panel): "GitHub https://github.com/o/r/pull/n" or "GitLab https://...".
	PullRequests []string
	// Blocks are the keys of the issues this issue blocks (Jira's "Blocks" link, outward).
	Blocks []string
}

func (issue jiraIssue) issueLinks() []any {
	links := []any{}
	for index, key := range issue.Blocks {
		links = append(links, map[string]any{
			"id": issue.ID + strconv.Itoa(90+index), "type": map[string]any{"id": "10000", "name": "Blocks", "inward": "is blocked by", "outward": "blocks"},
			"outwardIssue": map[string]any{"key": key},
		})
	}
	return links
}

func (issue jiraIssue) status() (string, string) {
	if issue.Resolved != "" {
		return "Done", "done"
	}
	return "In Progress", "indeterminate"
}

func jiraTime(text string) string {
	parsed, err := time.Parse(time.RFC3339, text)
	if err != nil {
		panic(err)
	}
	return parsed.UTC().Format("2006-01-02T15:04:05.000-0700")
}

func (issue jiraIssue) payload() map[string]any {
	name, category := issue.status()
	var resolved any
	if issue.Resolved != "" {
		resolved = jiraTime(issue.Resolved)
	}
	return map[string]any{
		"id": issue.ID, "key": issue.Key, "self": "https://" + jiraHost + "/rest/api/3/issue/" + issue.ID,
		"fields": map[string]any{
			"summary": issue.Summary, "issuetype": map[string]any{"name": issue.Type, "subtask": false},
			"project":  map[string]any{"id": issue.Project.ID, "key": issue.Project.Key, "name": issue.Project.Name},
			"status":   map[string]any{"name": name, "statusCategory": map[string]any{"key": category}},
			"assignee": issue.Assignee.payload(), "reporter": issue.Reporter.payload(), "creator": issue.Reporter.payload(),
			"created": jiraTime(issue.Created), "updated": jiraTime(issue.Updated), "resolutiondate": resolved,
			"labels": append([]string{}, issue.Labels...), "priority": map[string]any{"name": "Medium"},
			"issuelinks": issue.issueLinks(), "subtasks": []any{}, "components": []any{}, "fixVersions": []any{},
		},
	}
}

func (issue jiraIssue) changelog() map[string]any {
	histories := []any{}
	if issue.Resolved != "" {
		histories = append(histories, map[string]any{
			"id": issue.ID + "01", "created": jiraTime(issue.Resolved), "author": issue.Assignee.payload(),
			"items": []any{map[string]any{"field": "status", "fieldtype": "jira", "from": "3", "fromString": "In Progress", "to": "10001", "toString": "Done"}},
		})
	}
	return map[string]any{"startAt": 0, "maxResults": 100, "total": len(histories), "isLast": true, "values": histories}
}

func jiraIssueAnswers(issues []jiraIssue) []providerAnswer {
	var answers []providerAnswer
	byProject := map[string][]any{}
	for _, issue := range issues {
		byProject[issue.Project.Key] = append(byProject[issue.Project.Key], issue.payload())
		answers = append(answers,
			get(jiraHost, "/rest/api/3/issue/"+issue.Key+"/changelog", "*", issue.changelog()),
			get(jiraHost, "/rest/api/3/issue/"+issue.Key+"/comment", "*", map[string]any{"startAt": 0, "maxResults": 100, "total": 0, "comments": []any{}}),
		)
		for _, application := range []string{"GitHub", "GitLab"} {
			pulls := []any{}
			for _, pull := range issue.PullRequests {
				kind, link, _ := strings.Cut(pull, " ")
				if kind != application {
					continue
				}
				pulls = append(pulls, map[string]any{
					"id": "#" + link[strings.LastIndex(link, "/")+1:], "name": issue.Key + " change", "url": link, "status": "MERGED",
					"lastUpdate": jiraTime(issue.Updated), "repositoryName": "repository", "author": map[string]any{"name": issue.Assignee.Name},
				})
			}
			answers = append(answers, get(jiraHost, "/rest/dev-status/1.0/issue/detail",
				"applicationType="+application+"&dataType=pullrequest&issueId="+issue.ID,
				map[string]any{"errors": []any{}, "detail": []any{map[string]any{"pullRequests": pulls, "branches": []any{}, "repositories": []any{},
					"_instance": map[string]any{"name": application, "type": application}}}}))
		}
	}
	// The search answers each project's own JQL with that project's issues, in one page.
	answers = append(answers, providerAnswer{
		Method: "GET", Host: jiraHost, Path: "/rest/api/3/search/jql", Query: "*", Status: 200,
		Respond: func(request replayRequest) (any, bool) {
			jql := request.Query.Get("jql")
			for key, values := range byProject {
				if strings.HasPrefix(jql, "project = '"+key+"'") && request.Query.Get("nextPageToken") == "" {
					return map[string]any{"issues": values, "isLast": true}, true
				}
			}
			return nil, false
		},
	})
	return answers
}

var jiraIssues = []jiraIssue{
	{ID: "20001", Key: "PAY-1", Project: jiraPAY, Summary: "Ledger totals drift after refunds", Type: "Bug", Assignee: jiraHana, Reporter: jiraIvan,
		Created: "2026-05-06T09:00:00Z", Updated: "2026-05-20T15:00:00Z", Resolved: "2026-05-20T15:00:00Z", Labels: []string{"ledger"},
		PullRequests: []string{"GitHub https://github.com/chaos-fixtures/ledger/pull/1"}},
	{ID: "20002", Key: "PAY-2", Project: jiraPAY, Summary: "Monthly ledger export", Type: "Story", Assignee: jiraIvan, Reporter: jiraHana,
		Created: "2026-08-03T09:00:00Z", Updated: "2026-09-10T15:00:00Z", Resolved: "2026-09-10T15:00:00Z"},
	{ID: "20003", Key: "OPS-1", Project: jiraOPS, Summary: "Billing job retries forever", Type: "Bug", Assignee: jiraJude, Reporter: jiraKai,
		Created: "2026-04-14T09:00:00Z", Updated: "2026-04-30T15:00:00Z", Resolved: "2026-04-30T15:00:00Z",
		PullRequests: []string{"GitLab https://gitlab.com/chaos-gl/billing-service/-/merge_requests/3"}},
	{ID: "20004", Key: "OPS-2", Project: jiraOPS, Summary: "Page the on-call on ledger drift", Type: "Task", Assignee: jiraJude, Reporter: jiraJude,
		Created: "2026-10-02T09:00:00Z", Updated: "2026-10-09T15:00:00Z"},
}

// The ledger repository's pull requests. PR 1 is linked to PAY-1 by the Jira dev-status panel (native,
// cross-provider); PR 2 names PAY-2 in its title only (explicit text, cross-provider).
var githubLedgerItems = []githubIssue{
	{Number: 1, Title: "Fix ledger drift after refunds", Body: "Recompute the ledger on refund.", Author: ghAlice, PR: true, Head: "fix/ledger-drift",
		CreatedAt: "2026-05-12T09:00:00Z", UpdatedAt: "2026-05-19T15:00:00Z", ClosedAt: "2026-05-19T15:00:00Z", MergedAt: "2026-05-19T15:00:00Z"},
	{Number: 2, Title: "PAY-2 monthly ledger export", Body: "Adds the export.", Author: ghBob, PR: true, Head: "feature/PAY-2-export",
		CreatedAt: "2026-09-01T09:00:00Z", UpdatedAt: "2026-09-09T15:00:00Z", ClosedAt: "2026-09-09T15:00:00Z", MergedAt: "2026-09-09T15:00:00Z"},
}

// The GitHub team that holds the ledger repository in the jira and linear worlds.
var ledgerGitHubTeams = []githubTeam{
	{104, "ledger-keepers", "Ledger Keepers", "Keeps the ledger", []string{"ledger"}, []githubUser{ghAlice, ghBob}},
}

func jiraWorldAnswers() []providerAnswer {
	var answers []providerAnswer
	projects := []any{jiraPAY.payload(), jiraOPS.payload()}
	search := func(values []any) map[string]any {
		return map[string]any{"self": "https://" + jiraHost + "/rest/api/3/project/search", "maxResults": 100, "startAt": 0,
			"total": len(values), "isLast": true, "values": values}
	}
	answers = append(answers,
		get(jiraHost, "/rest/api/3/project/search", "maxResults=100", search(projects)),
		// No archived project.
		get(jiraHost, "/rest/api/3/project/search", "maxResults=100&status=archived", search([]any{})),
		atlassianGatewayAnswer(atlassianTeams),
	)
	for index, project := range []jiraProject{jiraPAY, jiraOPS} {
		boardID := 300 + index
		answers = append(answers, get(jiraHost, "/rest/api/3/project/"+project.Key, "", project.payload()),
			get(jiraHost, "/rest/agile/1.0/board", "maxResults=100&projectKeyOrId="+project.Key+"&startAt=0", map[string]any{
				"maxResults": 100, "startAt": 0, "total": 1, "isLast": true, "values": []any{map[string]any{
					"id": boardID, "name": project.Key + " board", "type": "scrum",
					"self":     fmt.Sprintf("https://%s/rest/agile/1.0/board/%d", jiraHost, boardID),
					"location": map[string]any{"projectId": project.ID, "projectKey": project.Key, "projectName": project.Name},
				}}}),
			get(jiraHost, fmt.Sprintf("/rest/agile/1.0/board/%d/sprint", boardID), "*", map[string]any{
				"maxResults": 50, "startAt": 0, "isLast": true, "values": []any{}}),
		)
	}
	answers = append(answers, jiraIssueAnswers(jiraIssues)...)
	answers = append(answers, githubTeamCatalogAnswersFor(ledgerGitHubTeams, "")...)
	answers = append(answers, githubRepoAnswers("ledger", githubLedgerItems)...)
	answers = append(answers, githubPRSocialAnswer("ledger", githubLedgerItems))
	answers = append(answers, gitlabProjectAnswers("chaos-gl/billing-service", gitlabBillingItems)...)
	return answers
}

var workItemDatasets = []string{"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"}

var jiraWorld = providerWorld{
	Params: GenerateParams{
		Provider: "jira", RepoName: "chaos-fx/PAY", RepoCount: 2, Days: 180,
		CommitsPerDay: 0, PRCount: 4, TeamCount: 4, Seed: 1,
	},
	OrgID:    "f1c7e000-0000-4000-8000-00000000a003",
	FrozenAt: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC),
	Answers:  jiraWorldAnswers,
	Steps: []worldStep{
		{Kind: "team_catalog", Provider: "jira", Credential: jiraCredential, Config: jiraConfig,
			Sources: [][2]string{{jiraPAY.Key, jiraPAY.Name}, {jiraOPS.Key, jiraOPS.Name}}},
		{Kind: "team_catalog", Provider: "github", Credential: githubToken, Config: map[string]string{"org": githubOrg}},
		{Kind: "sync", Provider: "github", Credential: githubToken, DepthDays: 200,
			Sources:  [][2]string{{githubOrg + "/ledger", "ledger"}},
			Datasets: append([]string{"repo-metadata", "prs"}, workItemDatasets...)},
		{Kind: "sync", Provider: "gitlab", Credential: gitlabToken, DepthDays: 200,
			Sources:  [][2]string{{"41001", "chaos-gl/billing-service"}},
			Datasets: append([]string{"repo-metadata", "prs"}, workItemDatasets...)},
		{Kind: "sync", Provider: "jira", Credential: jiraCredential, Config: jiraConfig, DepthDays: 200,
			Sources:        [][2]string{{jiraPAY.Key, jiraPAY.Name}, {jiraOPS.Key, jiraOPS.Name}},
			Datasets:       workItemDatasets,
			DatasetOptions: map[string]any{"fetch_dev_status": true}},
		{Kind: "work_graph"},
	},
}
