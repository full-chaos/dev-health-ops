//go:build integration

package fixturescli

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// ---- GitLab: answers in the shape of the GitLab REST v4 answers ----

const (
	gitlabHost = "gitlab.com"
	gitlabRoot = "chaos-gl"
)

var gitlabToken = map[string]string{"token": "fixture-world-placeholder"}

type gitlabUser struct {
	ID       int
	Username string
	Name     string
	Email    string
}

var (
	glDana = gitlabUser{8101, "dana-fx", "Dana Example", "dana@fixtures.example"}
	glEli  = gitlabUser{8102, "eli-fx", "Eli Example", "eli@fixtures.example"}
	glFay  = gitlabUser{8103, "fay-fx", "Fay Example", "fay@fixtures.example"}
	glGus  = gitlabUser{8104, "gus-fx", "Gus Example", "gus@fixtures.example"}
)

func (user gitlabUser) payload() map[string]any {
	return map[string]any{
		"id": user.ID, "username": user.Username, "name": user.Name, "state": "active", "locked": false,
		"avatar_url": fmt.Sprintf("https://gitlab.com/uploads/-/system/user/avatar/%d/avatar.png", user.ID),
		"web_url":    "https://gitlab.com/" + user.Username,
	}
}

type gitlabGroup struct {
	ID       int
	FullPath string
	Name     string
	Projects []string // path_with_namespace of the projects the group's /projects lists (its own and shared)
	Members  []gitlabUser
}

type gitlabProject struct {
	ID        int
	Path      string // path_with_namespace
	Name      string
	CreatedAt string
}

var gitlabProjects = map[string]gitlabProject{
	"chaos-gl/billing-service":       {41001, "chaos-gl/billing-service", "billing-service", "2025-02-03T09:00:00Z"},
	"chaos-gl/checkout/checkout-web": {41002, "chaos-gl/checkout/checkout-web", "checkout-web", "2025-02-04T09:00:00Z"},
}

var gitlabGroups = []gitlabGroup{
	{3000, "chaos-gl", "Chaos GL", []string{"chaos-gl/billing-service"}, []gitlabUser{glDana, glFay}},
	// The checkout group owns its own project and has billing-service shared with it: billing-service
	// has two owning groups.
	{3001, "chaos-gl/checkout", "Checkout", []string{"chaos-gl/checkout/checkout-web", "chaos-gl/billing-service"}, []gitlabUser{glEli, glFay}},
	// A group that holds no project.
	{3002, "chaos-gl/search", "Search", nil, []gitlabUser{glGus}},
}

func (project gitlabProject) payload() map[string]any {
	namespace := project.Path[:strings.LastIndex(project.Path, "/")]
	return map[string]any{
		"id": project.ID, "name": project.Name, "path": project.Name, "path_with_namespace": project.Path,
		"name_with_namespace": strings.ReplaceAll(project.Path, "/", " / "), "description": project.Name + " service",
		"created_at": project.CreatedAt, "default_branch": "main", "visibility": "private", "archived": false,
		"web_url":          "https://gitlab.com/" + project.Path,
		"http_url_to_repo": "https://gitlab.com/" + project.Path + ".git",
		"last_activity_at": "2026-10-11T08:00:00Z", "topics": []string{},
		"namespace": map[string]any{"id": 3000, "name": namespace, "path": namespace, "kind": "group", "full_path": namespace},
	}
}

func (group gitlabGroup) payload() map[string]any {
	var parent any
	if index := strings.LastIndex(group.FullPath, "/"); index > 0 {
		parent = 3000
	}
	path := group.FullPath[strings.LastIndex(group.FullPath, "/")+1:]
	return map[string]any{
		"id": group.ID, "name": group.Name, "path": path, "full_name": strings.ReplaceAll(group.FullPath, "/", " / "),
		"full_path": group.FullPath, "description": group.Name + " group", "visibility": "private",
		"web_url": "https://gitlab.com/groups/" + group.FullPath, "parent_id": parent, "created_at": "2025-01-10T09:00:00Z",
	}
}

// glList is one GitLab list answer of a single page, with GitLab's pagination headers: an empty
// X-Next-Page is GitLab's own end of the list.
func glList[T any](path, query string, items []T) providerAnswer {
	answer := get(gitlabHost, path, query, items)
	answer.Headers = http.Header{
		"X-Next-Page": {""}, "X-Page": {"1"}, "X-Per-Page": {"100"}, "X-Prev-Page": {""},
		"X-Total": {strconv.Itoa(len(items))}, "X-Total-Pages": {"1"},
	}
	return answer
}

func gitlabGroupPath(fullPath string) string {
	return "/api/v4/groups/" + fullPath
}

func gitlabTeamCatalogAnswers() []providerAnswer {
	var answers []providerAnswer
	root := gitlabGroups[0]
	answers = append(answers, get(gitlabHost, gitlabGroupPath(root.FullPath), "", root.payload()))
	subgroups := []map[string]any{}
	for _, group := range gitlabGroups[1:] {
		subgroups = append(subgroups, group.payload())
	}
	answers = append(answers, glList(gitlabGroupPath(root.FullPath)+"/subgroups", "*", subgroups))
	for _, group := range gitlabGroups {
		projects := []map[string]any{}
		for _, path := range group.Projects {
			projects = append(projects, gitlabProjects[path].payload())
		}
		members := []map[string]any{}
		for _, member := range group.Members {
			payload := member.payload()
			payload["access_level"] = 30
			members = append(members, payload)
		}
		answers = append(answers,
			glList(gitlabGroupPath(group.FullPath)+"/projects", "*", projects),
			glList(gitlabGroupPath(group.FullPath)+"/members", "*", members),
		)
	}
	all := []map[string]any{}
	for _, path := range []string{"chaos-gl/billing-service", "chaos-gl/checkout/checkout-web"} {
		all = append(all, gitlabProjects[path].payload())
	}
	answers = append(answers, glList(gitlabGroupPath(root.FullPath)+"/projects", "include_subgroups=true&page=1&per_page=100", all))
	return answers
}

// gitlabItem is one GitLab issue or merge request of a fixture project.
type gitlabItem struct {
	IID         int
	Title       string
	Description string
	Author      gitlabUser
	Assignees   []gitlabUser
	Labels      []string
	CreatedAt   string
	UpdatedAt   string
	ClosedAt    string
	MR          bool
	MergedAt    string
	Branch      string
	// ClosedBy are the merge requests GitLab lists as closing this issue ("group/project!iid").
	ClosedBy []string
	// Links are the issue's links: "relates_to group/project#iid@link_created_at".
	Links []string
}

func (item gitlabItem) state() string {
	switch {
	case item.MergedAt != "":
		return "merged"
	case item.ClosedAt != "":
		return "closed"
	default:
		return "opened"
	}
}

func gitlabUsers(users []gitlabUser) []map[string]any {
	out := []map[string]any{}
	for _, user := range users {
		out = append(out, user.payload())
	}
	return out
}

func (item gitlabItem) payload(project gitlabProject) map[string]any {
	kind, sigil := "issues", "#"
	if item.MR {
		kind, sigil = "merge_requests", "!"
	}
	web := fmt.Sprintf("https://gitlab.com/%s/-/%s/%d", project.Path, kind, item.IID)
	out := map[string]any{
		"id": project.ID*100 + item.IID, "iid": item.IID, "project_id": project.ID, "title": item.Title,
		"description": nullable(item.Description), "state": item.state(), "created_at": item.CreatedAt, "updated_at": item.UpdatedAt,
		"closed_at": nullable(item.ClosedAt), "labels": append([]string{}, item.Labels...), "milestone": nil,
		"assignees": gitlabUsers(item.Assignees), "author": item.Author.payload(), "web_url": web,
		"references": map[string]any{"short": sigil + strconv.Itoa(item.IID), "full": project.Path + sigil + strconv.Itoa(item.IID)},
		"user_notes_count": 0, "upvotes": 0, "downvotes": 0,
	}
	if item.MR {
		out["merged_at"], out["source_branch"], out["target_branch"] = nullable(item.MergedAt), item.Branch, "main"
		out["draft"], out["merge_status"] = false, "can_be_merged"
	} else {
		out["weight"], out["issue_type"], out["type"] = nil, "issue", "ISSUE"
	}
	return out
}

func (item gitlabItem) stateEvents() []map[string]any {
	events := []map[string]any{}
	if item.MergedAt != "" {
		events = append(events, map[string]any{"id": item.IID*10 + 1, "state": "merged", "created_at": item.MergedAt, "user": item.Author.payload(), "resource_type": "MergeRequest"})
	} else if item.ClosedAt != "" {
		events = append(events, map[string]any{"id": item.IID*10 + 1, "state": "closed", "created_at": item.ClosedAt, "user": item.Author.payload(), "resource_type": "Issue"})
	}
	return events
}

// approvals is the merge request's approval state: a merged one was approved by a reviewer who is not
// its author.
func (item gitlabItem) approvals(project gitlabProject) map[string]any {
	approvedBy := []map[string]any{}
	if item.MergedAt != "" {
		reviewer := glFay
		if item.Author.ID == glFay.ID {
			reviewer = glDana
		}
		approvedBy = append(approvedBy, map[string]any{"user": reviewer.payload()})
	}
	return map[string]any{
		"id": project.ID*100 + item.IID, "iid": item.IID, "project_id": project.ID, "title": item.Title,
		"state": item.state(), "created_at": item.CreatedAt, "updated_at": item.UpdatedAt, "merge_status": "can_be_merged",
		"approved": len(approvedBy) > 0, "approvals_required": 0, "approvals_left": 0, "approved_by": approvedBy,
	}
}

func gitlabProjectAnswers(path string, items []gitlabItem) []providerAnswer {
	project := gitlabProjects[path]
	root := "/api/v4/projects/" + strconv.Itoa(project.ID)
	answers := []providerAnswer{get(gitlabHost, root, "*", project.payload())}
	issues, mrs := []map[string]any{}, []map[string]any{}
	for _, item := range items {
		if item.MR {
			mrs = append(mrs, item.payload(project))
			mrRoot := root + "/merge_requests/" + strconv.Itoa(item.IID)
			answers = append(answers,
				glList(mrRoot+"/resource_state_events", "*", item.stateEvents()),
				glList(mrRoot+"/notes", "*", []any{}),
				get(gitlabHost, mrRoot+"/approvals", "*", item.approvals(project)),
			)
			continue
		}
		issues = append(issues, item.payload(project))
		issueRoot := root + "/issues/" + strconv.Itoa(item.IID)
		closedBy := []map[string]any{}
		for _, ref := range item.ClosedBy {
			name, iid, _ := strings.Cut(ref, "!")
			number, _ := strconv.Atoi(iid)
			closedBy = append(closedBy, map[string]any{"id": gitlabProjects[name].ID*100 + number, "iid": number, "project_id": gitlabProjects[name].ID,
				"state": "merged", "references": map[string]any{"short": "!" + iid, "full": ref}})
		}
		links := []map[string]any{}
		for _, link := range item.Links {
			kind, rest, _ := strings.Cut(link, " ")
			ref, at, _ := strings.Cut(rest, "@")
			name, iid, _ := strings.Cut(ref, "#")
			number, _ := strconv.Atoi(iid)
			links = append(links, map[string]any{"id": gitlabProjects[name].ID*100 + number, "iid": number, "project_id": gitlabProjects[name].ID,
				"title": "linked issue", "state": "opened", "link_type": kind, "link_created_at": at, "link_updated_at": at,
				"references": map[string]any{"short": "#" + iid, "full": ref}})
		}
		answers = append(answers,
			glList(issueRoot+"/resource_label_events", "*", []any{}),
			glList(issueRoot+"/resource_state_events", "*", item.stateEvents()),
			glList(issueRoot+"/links", "*", links),
			glList(issueRoot+"/closed_by", "*", closedBy),
			glList(issueRoot+"/notes", "*", []any{}),
		)
	}
	answers = append(answers,
		glList(root+"/milestones", "*", []any{}),
		glList(root+"/issues", "*", issues),
		glList(root+"/merge_requests", "*", mrs),
	)
	return answers
}

var gitlabBillingItems = []gitlabItem{
	{IID: 1, Title: "Invoice totals round the wrong way", Description: "Totals round half down.", Author: glDana, Assignees: []gitlabUser{glDana},
		Labels: []string{"bug"}, CreatedAt: "2026-05-04T09:00:00Z", UpdatedAt: "2026-05-15T12:00:00Z", ClosedAt: "2026-05-15T12:00:00Z",
		ClosedBy: []string{"chaos-gl/billing-service!3"}},
	{IID: 2, Title: "Dunning emails for failed payments", Description: "Send three reminders.", Author: glFay, Assignees: []gitlabUser{glFay},
		Labels: []string{"feature"}, CreatedAt: "2026-08-10T09:00:00Z", UpdatedAt: "2026-09-20T12:00:00Z", ClosedAt: "2026-09-20T12:00:00Z"},
	{IID: 3, Title: "Round invoice totals half up", Description: "Closes #1", Author: glDana, MR: true, Branch: "fix/invoice-rounding",
		CreatedAt: "2026-05-10T09:00:00Z", UpdatedAt: "2026-05-15T11:00:00Z", ClosedAt: "2026-05-15T11:00:00Z", MergedAt: "2026-05-15T11:00:00Z"},
	{IID: 4, Title: "Dunning reminder schedule", Description: "Implements #2.", Author: glFay, MR: true, Branch: "feature/dunning",
		CreatedAt: "2026-09-12T09:00:00Z", UpdatedAt: "2026-09-19T15:00:00Z", ClosedAt: "2026-09-19T15:00:00Z", MergedAt: "2026-09-19T15:00:00Z"},
}

var gitlabCheckoutItems = []gitlabItem{
	{IID: 1, Title: "Checkout shows the wrong invoice total", Description: "Follows the billing rounding.", Author: glEli, Assignees: []gitlabUser{glEli},
		Labels: []string{"bug"}, CreatedAt: "2026-07-01T09:00:00Z", UpdatedAt: "2026-07-20T12:00:00Z", ClosedAt: "2026-07-20T12:00:00Z",
		Links: []string{"relates_to chaos-gl/billing-service#2@2026-08-12T09:00:00Z"}},
	{IID: 2, Title: "Show the rounded total at checkout", Description: "Relates to chaos-gl/billing-service#2 and fixes #1", Author: glEli, MR: true, Branch: "fix/checkout-total",
		CreatedAt: "2026-07-15T09:00:00Z", UpdatedAt: "2026-07-20T11:00:00Z", ClosedAt: "2026-07-20T11:00:00Z", MergedAt: "2026-07-20T11:00:00Z"},
}

func gitlabAnswers() []providerAnswer {
	var answers []providerAnswer
	answers = append(answers, gitlabTeamCatalogAnswers()...)
	answers = append(answers, gitlabProjectAnswers("chaos-gl/billing-service", gitlabBillingItems)...)
	answers = append(answers, gitlabProjectAnswers("chaos-gl/checkout/checkout-web", gitlabCheckoutItems)...)
	return answers
}

var gitlabSources = [][2]string{{"41001", "chaos-gl/billing-service"}, {"41002", "chaos-gl/checkout/checkout-web"}}

var gitlabWorld = providerWorld{
	Params: GenerateParams{
		Provider: "gitlab", RepoName: "chaos-gl/billing-service", RepoCount: 2, Days: 160,
		CommitsPerDay: 0, PRCount: 3, TeamCount: 3, Seed: 1,
	},
	OrgID:    "f1c7e000-0000-4000-8000-00000000a002",
	FrozenAt: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC),
	Answers:  gitlabAnswers,
	Steps: []worldStep{
		{Kind: "team_catalog", Provider: "gitlab", Credential: gitlabToken, Sources: gitlabSources,
			SyncOptions: map[string]any{"group_path": gitlabRoot}},
		{Kind: "sync", Provider: "gitlab", Credential: gitlabToken, DepthDays: 180, Sources: gitlabSources,
			Datasets: []string{"repo-metadata", "prs", "work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"}},
		{Kind: "work_graph"},
	},
}

