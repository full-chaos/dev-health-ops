//go:build integration

package fixturescli

import (
	"encoding/json"
	"fmt"
	"time"
)

// The frozen provider worlds and the run of production verbs that writes each.
var providerWorlds = []providerWorld{githubWorld, gitlabWorld}

// answerJSON marshals a provider answer body.
func answerJSON(value any) json.RawMessage {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func get(host, path, query string, body any) providerAnswer {
	return providerAnswer{Method: "GET", Host: host, Path: path, Query: query, Status: 200, Body: answerJSON(body)}
}

// ---- GitHub: answers in the shape of the GitHub REST v3 and GraphQL v4 answers ----

const (
	githubHost = "api.github.com"
	githubOrg  = "chaos-fixtures"
)

var githubToken = map[string]string{"token": "fixture-world-placeholder"}

type githubUser struct {
	Login string
	ID    int
	Name  string
	Email string
}

var (
	ghAlice = githubUser{"alice-fx", 7001, "Alice Example", "alice@fixtures.example"}
	ghBob   = githubUser{"bob-fx", 7002, "Bob Example", "bob@fixtures.example"}
	ghCarol = githubUser{"carol-fx", 7003, "Carol Example", "carol@fixtures.example"}
	ghErin  = githubUser{"erin-fx", 7005, "Erin Example", "erin@fixtures.example"}
)

func (user githubUser) simple() map[string]any {
	return map[string]any{
		"login": user.Login, "id": user.ID, "node_id": fmt.Sprintf("U_fx%d", user.ID),
		"avatar_url":  fmt.Sprintf("https://avatars.githubusercontent.com/u/%d?v=4", user.ID),
		"gravatar_id": "", "url": "https://api.github.com/users/" + user.Login,
		"html_url": "https://github.com/" + user.Login, "type": "User", "site_admin": false,
	}
}

func (user githubUser) full() map[string]any {
	out := user.simple()
	out["name"], out["email"], out["company"], out["blog"], out["location"] = user.Name, user.Email, nil, "", nil
	out["bio"], out["public_repos"], out["followers"], out["following"] = nil, 0, 0, 0
	out["created_at"], out["updated_at"] = "2024-01-02T03:04:05Z", "2026-09-01T00:00:00Z"
	return out
}

type githubTeam struct {
	ID          int
	Slug, Name  string
	Description string
	Repos       []string
	Members     []githubUser
}

var githubTeams = []githubTeam{
	{101, "payments", "Payments", "Owns the payments service", []string{"payments-api", "platform-core"}, []githubUser{ghAlice, ghCarol}},
	{102, "platform", "Platform", "Owns the shared platform", []string{"platform-core"}, []githubUser{ghBob, ghCarol}},
	// A team that holds no repository: it owns nothing, so no work is ever its own.
	{103, "docs", "Docs", "Writes the docs", nil, []githubUser{ghErin}},
}

type githubRepo struct {
	ID          int
	Name        string
	Description string
	CreatedAt   string
}

var githubRepos = map[string]githubRepo{
	"payments-api":  {9101, "payments-api", "Payments API", "2025-01-06T09:00:00Z"},
	"platform-core": {9102, "platform-core", "Shared platform core", "2025-01-06T09:30:00Z"},
}

func (repo githubRepo) payload() map[string]any {
	full := githubOrg + "/" + repo.Name
	return map[string]any{
		"id": repo.ID, "node_id": fmt.Sprintf("R_fx%d", repo.ID), "name": repo.Name, "full_name": full,
		"private": true, "owner": map[string]any{"login": githubOrg, "id": 7000, "type": "Organization"},
		"html_url": "https://github.com/" + full, "description": repo.Description, "fork": false,
		"url": "https://api.github.com/repos/" + full, "created_at": repo.CreatedAt,
		"updated_at": "2026-10-11T08:00:00Z", "pushed_at": "2026-10-11T08:00:00Z",
		"default_branch": "main", "language": "Go", "archived": false, "disabled": false,
		"visibility": "private", "topics": []string{},
		"permissions": map[string]any{"admin": false, "maintain": true, "push": true, "triage": true, "pull": true},
		"role_name":   "maintain",
	}
}

func githubTeamCatalogAnswers() []providerAnswer {
	var teams []map[string]any
	for _, team := range githubTeams {
		teams = append(teams, map[string]any{
			"id": team.ID, "node_id": fmt.Sprintf("T_fx%d", team.ID), "name": team.Name, "slug": team.Slug,
			"description": team.Description, "privacy": "closed", "notification_setting": "notifications_enabled",
			"permission": "pull", "url": fmt.Sprintf("https://api.github.com/organizations/7000/team/%d", team.ID),
			"html_url":         "https://github.com/orgs/" + githubOrg + "/teams/" + team.Slug,
			"members_url":      fmt.Sprintf("https://api.github.com/organizations/7000/team/%d/members{/member}", team.ID),
			"repositories_url": fmt.Sprintf("https://api.github.com/organizations/7000/team/%d/repos", team.ID),
			"parent":           nil,
		})
	}
	answers := []providerAnswer{get(githubHost, "/orgs/"+githubOrg+"/teams", "per_page=100", teams)}
	users := map[string]bool{}
	for _, team := range githubTeams {
		repos := []map[string]any{}
		for _, name := range team.Repos {
			repos = append(repos, githubRepos[name].payload())
		}
		members := []map[string]any{}
		for _, member := range team.Members {
			members = append(members, member.simple())
			users[member.Login] = true
		}
		answers = append(answers,
			get(githubHost, "/orgs/"+githubOrg+"/teams/"+team.Slug+"/repos", "per_page=100", repos),
			get(githubHost, "/orgs/"+githubOrg+"/teams/"+team.Slug+"/members", "per_page=100", members),
		)
	}
	for _, user := range []githubUser{ghAlice, ghBob, ghCarol, ghErin} {
		if users[user.Login] {
			answers = append(answers, get(githubHost, "/users/"+user.Login, "", user.full()))
		}
	}
	return answers
}

func githubAnswers() []providerAnswer {
	var answers []providerAnswer
	answers = append(answers, githubTeamCatalogAnswers()...)
	answers = append(answers, githubRepoAnswers("payments-api", githubPaymentsItems)...)
	answers = append(answers, githubPRSocialAnswer("payments-api", githubPaymentsItems))
	answers = append(answers, githubRepoAnswers("platform-core", githubPlatformItems)...)
	answers = append(answers, githubPRSocialAnswer("platform-core", githubPlatformItems))
	return answers
}

var githubWorld = providerWorld{
	Params: GenerateParams{
		Provider: "github", RepoName: "chaos-fixtures/payments-api", RepoCount: 2, Days: 120,
		CommitsPerDay: 0, PRCount: 6, TeamCount: 3, Seed: 1,
	},
	OrgID:    "f1c7e000-0000-4000-8000-00000000a001",
	FrozenAt: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC),
	Answers:  githubAnswers,
	Steps: []worldStep{
		{Kind: "team_catalog", Provider: "github", Credential: githubToken, Config: map[string]string{"org": githubOrg}},
		{Kind: "sync", Provider: "github", Credential: githubToken, DepthDays: 150,
			Sources:  [][2]string{{githubOrg + "/payments-api", "payments-api"}, {githubOrg + "/platform-core", "platform-core"}},
			Datasets: []string{"repo-metadata", "prs", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"}},
		{Kind: "work_graph"},
	},
}
