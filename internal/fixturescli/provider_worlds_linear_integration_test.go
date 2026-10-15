//go:build integration

package fixturescli

import (
	"encoding/json"
	"regexp"
	"strings"
	"time"
)

// ---- Linear: answers in the shape of the Linear GraphQL API answers (the shapes the CHAOS-9124
// recorder measured for teams and projects) ----

const linearHost = "api.linear.app"

var linearToken = map[string]string{"api_key": "fixture-world-placeholder"}

type linearUser struct {
	ID, Name, Email string
}

var (
	linLea = linearUser{"5e1d0000-0000-4000-8000-00000000b001", "Lea Example", "lea@fixtures.example"}
	linMo  = linearUser{"5e1d0000-0000-4000-8000-00000000b002", "Mo Example", "mo@fixtures.example"}
	linNia = linearUser{"5e1d0000-0000-4000-8000-00000000b003", "Nia Example", "nia@fixtures.example"}
	linOli = linearUser{"5e1d0000-0000-4000-8000-00000000b004", "Oli Example", "oli@fixtures.example"}
)

func (user linearUser) payload() map[string]any {
	return map[string]any{"id": user.ID, "name": user.Name, "email": user.Email, "active": true}
}

type linearTeam struct {
	ID, Key, Name string
	Members       []linearUser
}

var linearTeams = []linearTeam{
	{"7e40c0de-0000-4000-8000-00000000c001", "ENG", "Engineering", []linearUser{linLea, linMo}},
	{"7e40c0de-0000-4000-8000-00000000c002", "MOB", "Mobile", []linearUser{linMo, linNia}},
	// A team in no project: it owns nothing.
	{"7e40c0de-0000-4000-8000-00000000c003", "QA", "Quality", []linearUser{linOli}},
}

type linearProject struct {
	ID, Name  string
	Teams     []string // team keys
	CreatedAt string
	// StartDate and TargetDate are the project's planned start and committed end (YYYY-MM-DD, "" = none);
	// UpdatedAt is its last update ("" = the provider worlds' fixed stamp).
	StartDate, TargetDate, UpdatedAt string
}

var linearProjects = []linearProject{
	// A project of two teams.
	{"9f00d0e1-0000-4000-8000-00000000d001", "Checkout Revamp", []string{"ENG", "MOB"}, "2026-03-02T09:00:00Z", "", "", ""},
	{"9f00d0e1-0000-4000-8000-00000000d002", "API Hardening", []string{"ENG"}, "2026-04-06T09:00:00Z", "", "", ""},
}

// linearOldTeam was a team of the API Hardening project at the organization's first sync and is gone
// by the second: the second sync closes its project ownership and its memberships.
var linearOldTeam = linearTeam{"7e40c0de-0000-4000-8000-00000000c004", "OLD", "Old Platform", []linearUser{linOli}}

// linearFirstProjects are the projects at the first sync: API Hardening also had the old team.
var linearFirstProjects = []linearProject{
	linearProjects[0],
	{"9f00d0e1-0000-4000-8000-00000000d002", "API Hardening", []string{"ENG", "OLD"}, "2026-04-06T09:00:00Z", "", "", ""},
}

func linearTeamByKey(key string) linearTeam {
	for _, team := range append(append(append([]linearTeam{}, linearTeams...), linearOldTeam), demoLinearTeams...) {
		if team.Key == key {
			return team
		}
	}
	panic("no linear team " + key)
}

func linearPage(items []any) map[string]any {
	return map[string]any{"nodes": items, "pageInfo": map[string]any{"hasNextPage": false, "endCursor": nil}}
}

// linearIssue is one Linear issue.
type linearIssue struct {
	ID, Identifier, Title string
	Team                  string
	Project               string // project id
	Assignee, Creator     linearUser
	CreatedAt, UpdatedAt  string
	StartedAt             string
	CompletedAt           string
	// HistoryFrom is when the issue moved from Todo to In Progress: the issue history carries that entry ("" = none).
	HistoryFrom string
	// Attachments are the URLs of the pull requests the GitHub and GitLab integrations attach.
	Attachments []string
	// Relations are "related <identifier>@<createdAt>".
	Relations []string
	// Labels are the issue's label names.
	Labels []string
	// Description is the issue's description ("" = none).
	Description string
	// Estimate is the issue's estimate in points (0 = none).
	Estimate float64
}

// linearEstimate is an issue's estimate as Linear sends it: a number, or null when none is set.
func linearEstimate(points float64) any {
	if points == 0 {
		return nil
	}
	return points
}

func (issue linearIssue) payload() map[string]any {
	team := linearTeamByKey(issue.Team)
	state := map[string]any{"name": "In Progress", "type": "started"}
	if issue.CompletedAt != "" {
		state = map[string]any{"name": "Done", "type": "completed"}
	}
	var project any
	for _, candidate := range append(append([]linearProject{}, linearProjects...), demoLinearProjects...) {
		if candidate.ID == issue.Project {
			project = map[string]any{"id": candidate.ID, "name": candidate.Name}
		}
	}
	attachments := []any{}
	for index, link := range issue.Attachments {
		source := "github"
		if strings.Contains(link, "gitlab.com") {
			source = "gitlab"
		}
		attachments = append(attachments, map[string]any{"id": issue.ID + "-a" + string(rune('0'+index)), "url": link, "sourceType": source})
	}
	relations := []any{}
	for index, relation := range issue.Relations {
		kind, rest, _ := strings.Cut(relation, " ")
		target, at, _ := strings.Cut(rest, "@")
		relations = append(relations, map[string]any{"id": issue.ID + "-r" + string(rune('0'+index)), "type": kind, "createdAt": at,
			"issue": map[string]any{"identifier": issue.Identifier}, "relatedIssue": map[string]any{"identifier": target}})
	}
	labels := []any{}
	for _, name := range issue.Labels {
		labels = append(labels, map[string]any{"id": issue.ID + "-l-" + strings.ToLower(name), "name": name})
	}
	history := []any{}
	if issue.HistoryFrom != "" {
		history = append(history, map[string]any{"id": issue.ID + "-h0", "createdAt": issue.HistoryFrom,
			"fromState": map[string]any{"name": "Todo", "type": "unstarted"}, "toState": map[string]any{"name": "In Progress", "type": "started"},
			"fromProjectId": nil, "toProjectId": nil, "actor": map[string]any{"name": issue.Assignee.Name, "email": issue.Assignee.Email}})
	}
	if issue.CompletedAt != "" {
		history = append(history, map[string]any{"id": issue.ID + "-h1", "createdAt": issue.CompletedAt,
			"fromState": map[string]any{"name": "In Progress", "type": "started"}, "toState": map[string]any{"name": "Done", "type": "completed"},
			"fromProjectId": nil, "toProjectId": nil, "actor": map[string]any{"name": issue.Assignee.Name, "email": issue.Assignee.Email}})
	}
	return map[string]any{
		"id": issue.ID, "identifier": issue.Identifier, "title": issue.Title, "description": nullable(issue.Description), "priority": 2, "estimate": linearEstimate(issue.Estimate),
		"createdAt": issue.CreatedAt, "updatedAt": issue.UpdatedAt, "startedAt": nullable(issue.StartedAt), "completedAt": nullable(issue.CompletedAt),
		"canceledAt": nil, "dueDate": nil, "url": "https://linear.app/chaos-fx/issue/" + strings.ToLower(issue.Identifier), "archivedAt": nil,
		"state":    state,
		"assignee": map[string]any{"name": issue.Assignee.Name, "email": issue.Assignee.Email},
		"creator":  map[string]any{"name": issue.Creator.Name, "email": issue.Creator.Email},
		"labels":   linearPage(labels), "parent": nil, "project": project, "cycle": nil,
		"team":    map[string]any{"id": team.ID, "key": team.Key, "name": team.Name},
		"history": linearPage(history), "comments": linearPage([]any{}), "attachments": linearPage(attachments),
		"relations": linearPage(relations), "inverseRelations": linearPage([]any{}),
	}
}

var linearIssues = []linearIssue{
	{ID: "a1000000-0000-4000-8000-00000000e001", Identifier: "ENG-1", Title: "Checkout API returns stale totals", Team: "ENG",
		Project: "9f00d0e1-0000-4000-8000-00000000d001", Assignee: linLea, Creator: linMo,
		CreatedAt: "2026-05-11T09:00:00Z", UpdatedAt: "2026-06-03T15:00:00Z", StartedAt: "2026-05-12T09:00:00Z", CompletedAt: "2026-06-03T15:00:00Z",
		Attachments: []string{"https://github.com/chaos-fixtures/ledger/pull/3"}},
	{ID: "a1000000-0000-4000-8000-00000000e002", Identifier: "ENG-2", Title: "Rate limit the public API", Team: "ENG",
		Project: "9f00d0e1-0000-4000-8000-00000000d002", Assignee: linMo, Creator: linLea,
		CreatedAt: "2026-08-17T09:00:00Z", UpdatedAt: "2026-09-25T15:00:00Z", StartedAt: "2026-08-18T09:00:00Z", CompletedAt: "2026-09-25T15:00:00Z",
		Attachments: []string{"https://gitlab.com/chaos-gl/billing-service/-/merge_requests/4"}},
	{ID: "a1000000-0000-4000-8000-00000000e003", Identifier: "MOB-1", Title: "Checkout screen redesign", Team: "MOB",
		Project: "9f00d0e1-0000-4000-8000-00000000d001", Assignee: linNia, Creator: linNia,
		CreatedAt: "2026-09-29T09:00:00Z", UpdatedAt: "2026-10-10T15:00:00Z", StartedAt: "2026-09-30T09:00:00Z",
		Relations: []string{"related ENG-1@2026-09-29T10:00:00Z"}},
}

// githubLinearItems are the ledger repository's pull requests of the linear world: PR 3 is attached to
// ENG-1 by the GitHub integration (native, cross-provider); PR 4 names MOB-1 in its title only.
var githubLinearItems = []githubIssue{
	{Number: 3, Title: "Fresh checkout totals", Body: "Recompute totals on read.", Author: ghAlice, PR: true, Head: "fix/checkout-totals",
		CreatedAt: "2026-05-20T09:00:00Z", UpdatedAt: "2026-06-02T15:00:00Z", ClosedAt: "2026-06-02T15:00:00Z", MergedAt: "2026-06-02T15:00:00Z"},
	{Number: 4, Title: "MOB-1 checkout layout endpoints", Body: "Endpoints for the new layout.", Author: ghBob, PR: true, Head: "feature/mob-1",
		CreatedAt: "2026-10-03T09:00:00Z", UpdatedAt: "2026-10-08T15:00:00Z"},
}

// linearGraphQLAnswer answers every Linear GraphQL read of the reference catalog and the work-items
// route.
func linearGraphQLAnswer(linearTeams []linearTeam, linearProjects []linearProject, linearIssues []linearIssue, phase string) providerAnswer {
	return providerAnswer{
		Method: "POST", Host: linearHost, Path: "/graphql", Status: 200, Phase: phase,
		Respond: func(request replayRequest) (any, bool) {
			document := request.Document
			var vars map[string]any
			if len(request.Variables) > 0 && json.Unmarshal(request.Variables, &vars) != nil {
				return nil, false
			}
			if vars["after"] != nil {
				return nil, false
			}
			// The absence reads of a later sync (linear_absence.go), answered as the recorded answers
			// (testdata/provider_answers/linear) answer them.
			if match := linearTeamsByKey.FindStringSubmatch(document); match != nil {
				nodes := []any{}
				for _, team := range linearTeams {
					if team.Key == match[1] {
						nodes = append(nodes, map[string]any{"id": team.ID, "key": team.Key})
					}
				}
				return map[string]any{"data": map[string]any{"teams": map[string]any{"nodes": nodes}}}, true
			}
			if match := linearProjectByID.FindStringSubmatch(document); match != nil {
				for _, project := range linearProjects {
					if project.ID == match[1] {
						teams := []any{}
						for _, key := range project.Teams {
							teams = append(teams, map[string]any{"key": key})
						}
						return map[string]any{"data": map[string]any{"project": map[string]any{"id": project.ID, "name": project.Name, "teams": map[string]any{"nodes": teams}}}}, true
					}
				}
				return linearNotFound("Project"), true
			}
			if match := linearTeamByID.FindStringSubmatch(document); match != nil {
				for _, team := range linearTeams {
					if team.Key == match[1] || team.ID == match[1] {
						return map[string]any{"data": map[string]any{"team": map[string]any{"id": team.ID, "key": team.Key, "name": team.Name}}}, true
					}
				}
				return linearNotFound("Team"), true
			}
			switch {
			case strings.Contains(document, "query LinearReferenceCatalogTeams("):
				teams := []any{}
				for _, team := range linearTeams {
					members := []any{}
					for _, member := range team.Members {
						members = append(members, member.payload())
					}
					teams = append(teams, map[string]any{"id": team.ID, "key": team.Key, "name": team.Name, "description": team.Name + " team",
						"members": linearPage(members)})
				}
				return map[string]any{"data": map[string]any{"teams": linearPage(teams)}}, true
			case strings.Contains(document, "query LinearReferenceCatalogProjects("):
				projects := []any{}
				for _, project := range linearProjects {
					teams := []any{}
					for _, key := range project.Teams {
						team := linearTeamByKey(key)
						teams = append(teams, map[string]any{"id": team.ID, "key": team.Key})
					}
					projects = append(projects, map[string]any{
						"id": project.ID, "name": project.Name, "description": project.Name,
						"status":  map[string]any{"id": project.ID + "-s", "name": "In Progress", "type": "started"},
						"trashed": false, "progress": 0.5, "startDate": nullable(project.StartDate), "targetDate": nullable(project.TargetDate),
						"createdAt": project.CreatedAt, "updatedAt": orDefault(project.UpdatedAt, "2026-10-01T09:00:00Z"), "archivedAt": nil,
						"url":   "https://linear.app/chaos-fx/project/" + strings.ToLower(strings.ReplaceAll(project.Name, " ", "-")),
						"lead":  map[string]any{"id": linLea.ID, "name": linLea.Name, "email": linLea.Email},
						"teams": linearPage(teams),
					})
				}
				return map[string]any{"data": map[string]any{"projects": linearPage(projects)}}, true
			case strings.Contains(document, "query LinearWorkItemsTeam("):
				teams := []any{}
				for _, team := range linearTeams {
					teams = append(teams, map[string]any{"id": team.ID, "key": team.Key, "name": team.Name})
				}
				return map[string]any{"data": map[string]any{"teams": linearPage(teams)}}, true
			case strings.Contains(document, "query LinearWorkItemsCycles("):
				return map[string]any{"data": map[string]any{"cycles": linearPage([]any{})}}, true
			case strings.Contains(document, "query LinearWorkItems("):
				filter, _ := vars["filter"].(map[string]any)
				team, _ := filter["team"].(map[string]any)
				key, _ := team["key"].(map[string]any)
				in, _ := key["in"].([]any)
				if len(in) != 1 {
					return nil, false
				}
				issues := []any{}
				for _, issue := range linearIssues {
					if issue.Team == in[0] {
						issues = append(issues, issue.payload())
					}
				}
				return map[string]any{"data": map[string]any{"issues": linearPage(issues)}}, true
			}
			return nil, false
		},
	}
}

var (
	linearTeamsByKey  = regexp.MustCompile(`^query \{ teams\(filter: \{key: \{eq: "([^"]+)"\}\}\)`)
	linearProjectByID = regexp.MustCompile(`^query \{ project\(id: "([^"]+)"\)`)
	linearTeamByID    = regexp.MustCompile(`^query \{ team\(id: "([^"]+)"\)`)
)

// linearNotFound is Linear's answer to a read of an entity it does not hold (the recorded
// team_by_key_not_found and project_by_id_not_found answers).
func linearNotFound(entity string) map[string]any {
	path := strings.ToLower(entity)
	return map[string]any{"data": nil, "errors": []any{map[string]any{
		"message": "Entity not found: " + entity, "path": []any{path},
		"locations": []any{map[string]any{"line": 1, "column": 9}},
		"extensions": map[string]any{"type": "invalid input", "code": "INPUT_ERROR", "statusCode": 400, "userError": true,
			"userPresentableMessage": "Could not find referenced " + entity + "."},
	}}}
}

func linearWorldAnswers() []providerAnswer {
	var answers []providerAnswer
	answers = append(answers, linearGraphQLAnswer(append(append([]linearTeam{}, linearTeams...), linearOldTeam), linearFirstProjects, linearIssues, "first"))
	answers = append(answers, linearGraphQLAnswer(linearTeams, linearProjects, linearIssues, "second"))
	answers = append(answers, githubTeamCatalogAnswersFor(ledgerGitHubTeams, "")...)
	answers = append(answers, githubRepoAnswers("ledger", githubLinearItems)...)
	answers = append(answers, githubPRSocialAnswer("ledger", githubLinearItems))
	answers = append(answers, gitlabProjectAnswers("chaos-gl/billing-service", gitlabBillingItems)...)
	return answers
}

var linearWorld = providerWorld{
	Params: GenerateParams{
		Provider: "linear", RepoName: "chaos-fx/ENG", RepoCount: 2, Days: 160,
		CommitsPerDay: 0, PRCount: 4, TeamCount: 4, Seed: 1,
	},
	OrgID:    "f1c7e000-0000-4000-8000-00000000a004",
	FrozenAt: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC),
	Answers:  linearWorldAnswers,
	Steps: []worldStep{
		// The first sync of the catalog a month before the world's instant, the second two days before it.
		{Kind: "team_catalog", Provider: "linear", Credential: linearToken, Phase: "first", Before: 30 * 24 * time.Hour},
		{Kind: "team_catalog", Provider: "linear", Credential: linearToken, Phase: "second", Before: 2 * 24 * time.Hour},
		{Kind: "team_catalog", Provider: "github", Credential: githubToken, Config: map[string]string{"org": githubOrg}},
		{Kind: "sync", Provider: "github", Credential: githubToken, DepthDays: 200,
			Sources:  [][2]string{{githubOrg + "/ledger", "ledger"}},
			Datasets: append([]string{"repo-metadata", "prs"}, workItemDatasets...)},
		{Kind: "sync", Provider: "gitlab", Credential: gitlabToken, DepthDays: 200,
			Sources:  [][2]string{{"41001", "chaos-gl/billing-service"}},
			Datasets: append([]string{"repo-metadata", "prs"}, workItemDatasets...)},
		{Kind: "sync", Provider: "linear", Credential: linearToken, DepthDays: 200, Phase: "second",
			Sources: [][2]string{{"ENG", "Engineering"}, {"MOB", "Mobile"}}, Datasets: workItemDatasets},
		{Kind: "work_graph"},
	},
}
