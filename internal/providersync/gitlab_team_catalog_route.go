package providersync

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	gitlabTeamCatalogListPerPage         = 100
	gitlabTeamCatalogSubgroupsMaxPages   = 5  // 500 subgroups (MAX_GITLAB_DISCOVERY_SUBGROUPS)
	gitlabTeamCatalogProjectsMaxPages    = 5  // 500 projects per group (MAX_GITLAB_DISCOVERY_PROJECTS)
	gitlabTeamCatalogAllProjectsMaxPages = 50 // 5000 projects (MAX_GITLAB_DISCOVERY_ALL_PROJECTS)
	gitlabTeamCatalogMembersMaxPages     = 50 // 5000 members per group; unbounded in Python, capped here defensively
)

// GitLabTeamCatalogRouteHandler owns the provider-only team/project-ownership/
// membership/native-project catalog walk that ports
// src/dev_health_ops/workers/team_autoimport_gitlab.py. Like
// LinearReferenceCatalogRouteHandler it is deliberately NOT a
// CompleteRouteHandler -- production wiring (retiring the Python bridge call)
// is CHAOS-4198's delete-child ticket, not this one.
type GitLabTeamCatalogRouteHandler struct{}

type GitLabTeamCatalogEvidence struct {
	Provider  string `json:"provider"`
	Requests  int    `json:"requests"`
	Pages     int    `json:"pages"`
	Groups    int    `json:"groups"`
	Truncated bool   `json:"truncated"`
}

type GitLabTeamCatalogResult struct {
	TeamsImported                int  `json:"teams_imported"`
	ProjectsImported             int  `json:"projects_imported"`
	NativeProjectsImported       int  `json:"native_projects_imported"`
	MembersImported              int  `json:"members_imported"`
	TeamMembershipsImported      int  `json:"team_memberships_imported"`
	TeamProjectOwnershipImported int  `json:"team_project_ownership_imported"`
	Complete                     bool `json:"complete"`
}

type GitLabTeamCatalogBatch struct {
	Rows     GitLabTeamCatalogRows     `json:"rows"`
	Effects  GitLabTeamCatalogEffects  `json:"-"`
	Result   GitLabTeamCatalogResult   `json:"result"`
	Evidence GitLabTeamCatalogEvidence `json:"evidence"`
}

// GitLabTeamCatalogSelection is the CHAOS-4323 three-way selection
// (auto_import_teams/auto_import_projects/auto_import_members), read off the
// claim the same way native_post_sync.go reads sync_options for the dispatch
// gate. All three default false (fail closed, matching populate()'s
// `if not (want_teams or want_projects or want_members): return zero`).
type GitLabTeamCatalogSelection struct {
	Teams    bool
	Projects bool
	Members  bool
}

func gitlabTeamCatalogSelectionFromClaim(claim Claim) GitLabTeamCatalogSelection {
	options := claim.DatasetOptions
	return GitLabTeamCatalogSelection{
		Teams:    gitlabTeamCatalogBoolOption(options, "auto_import_teams"),
		Projects: gitlabTeamCatalogBoolOption(options, "auto_import_projects"),
		Members:  gitlabTeamCatalogBoolOption(options, "auto_import_members"),
	}
}

func gitlabTeamCatalogBoolOption(options map[string]any, key string) bool {
	if options == nil {
		return false
	}
	value, ok := options[key].(bool)
	return ok && value
}

// gitlabTeamCatalogGroupPath mirrors team_autoimport_gitlab._gitlab_group:
// credential config first (group_path/group/owner), then dataset options.
func gitlabTeamCatalogGroupPath(credential providerfoundation.Credential, claim Claim) string {
	for _, key := range []string{"group_path", "group", "owner"} {
		if value := strings.TrimSpace(credential.Config[key]); value != "" {
			return value
		}
	}
	for _, key := range []string{"group_path", "group", "owner"} {
		if value, ok := claim.DatasetOptions[key].(string); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (handler GitLabTeamCatalogRouteHandler) CollectTeamCatalog(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (GitLabTeamCatalogBatch, error) {
	// claim.Validate() requires a registered (provider, dataset) capability
	// (Capability(unit.Provider, unit.Dataset) must resolve). There is no
	// dedicated "teams" dataset in the shared Go/Python provider-matrix
	// contract -- team-autoimport has never been a sync_run_units dataset in
	// Python, it is dispatched through the separate sync.team_autoimport job
	// kind -- so, exactly like LinearReferenceCatalogRouteHandler rides the
	// already-registered "work-items" claim instead of minting a new one,
	// this collector is invoked under the gitlab/work-items claim.
	if ctx == nil || claim.Validate() != nil || claim.Provider != gitlabTeamCatalogProvider ||
		claim.Dataset != "work-items" || credential.Provider != gitlabTeamCatalogProvider ||
		client == nil || client.Provider != gitlabTeamCatalogProvider || client.BaseURL == nil ||
		client.Doer == nil || client.Lease == nil || normalizedAt.IsZero() {
		return GitLabTeamCatalogBatch{}, ErrInvalidConfiguration
	}
	selection := gitlabTeamCatalogSelectionFromClaim(claim)
	if !selection.Teams && !selection.Projects && !selection.Members {
		return GitLabTeamCatalogBatch{Result: GitLabTeamCatalogResult{Complete: true}}, nil
	}
	groupPath := gitlabTeamCatalogGroupPath(credential, claim)
	if groupPath == "" {
		return GitLabTeamCatalogBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	evidence := GitLabTeamCatalogEvidence{Provider: gitlabTeamCatalogProvider}

	rootPath := providerRelativePath(client, "api", "v4", "groups", groupPath)
	var root gitlabTeamCatalogGroupPayload
	if err := fetchObject(ctx, client, rootPath, &root); err != nil {
		return GitLabTeamCatalogBatch{}, err
	}
	evidence.Requests++
	if strings.TrimSpace(root.FullPath) == "" {
		return GitLabTeamCatalogBatch{}, providerfoundation.ErrNormalizationInvalid
	}

	subgroupPages, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
		Path: rootPath + "/subgroups", PerPage: gitlabTeamCatalogListPerPage, MaxPages: gitlabTeamCatalogSubgroupsMaxPages,
	})
	if err != nil {
		return GitLabTeamCatalogBatch{}, err
	}
	evidence.Requests += subgroupPages.Pages
	evidence.Pages += subgroupPages.Pages
	if subgroupPages.PageBudgetExhausted {
		evidence.Truncated = true
	}

	groups := []gitlabTeamCatalogGroupPayload{root}
	for _, raw := range subgroupPages.Items {
		var subgroup gitlabTeamCatalogGroupPayload
		if err := json.Unmarshal(raw, &subgroup); err != nil {
			return GitLabTeamCatalogBatch{}, providerfoundation.ErrNormalizationInvalid
		}
		groups = append(groups, subgroup)
	}
	evidence.Groups = len(groups)

	fullPaths := make([]string, 0, len(groups))
	for _, group := range groups {
		fullPaths = append(fullPaths, group.FullPath)
	}
	parentByTeam := gitlabTeamCatalogParentByTeam(fullPaths)

	rows := GitLabTeamCatalogRows{}
	seenOwnership := map[string]bool{}
	seenMembership := map[string]bool{}

	for _, group := range groups {
		groupPathValue := providerRelativePath(client, "api", "v4", "groups", strings.TrimSpace(group.FullPath))
		projectPages, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: groupPathValue + "/projects", PerPage: gitlabTeamCatalogListPerPage, MaxPages: gitlabTeamCatalogProjectsMaxPages,
		})
		if err != nil {
			return GitLabTeamCatalogBatch{}, err
		}
		evidence.Requests += projectPages.Pages
		evidence.Pages += projectPages.Pages
		if projectPages.PageBudgetExhausted {
			evidence.Truncated = true
		}
		projectKeys := make([]string, 0, len(projectPages.Items))
		for _, raw := range projectPages.Items {
			var project gitlabTeamCatalogProjectPayload
			if err := json.Unmarshal(raw, &project); err != nil {
				return GitLabTeamCatalogBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			path := strings.TrimSpace(project.PathWithNamespace)
			if path == "" {
				continue
			}
			projectKeys = append(projectKeys, path)
		}

		if selection.Teams {
			rows.Teams = append(rows.Teams, normalizeGitLabTeamRow(claim.OrgID, group, projectKeys, normalizedAt))
		}

		if selection.Projects {
			teamID := gitlabTeamID(group.FullPath)
			specificity := uint16(gitlabTeamCatalogBaseSpecificity + gitlabTeamDepth(teamID, parentByTeam)*gitlabTeamCatalogChildSpecificityStep)
			for _, path := range projectKeys {
				key := teamID + "\x00" + path
				if seenOwnership[key] {
					continue
				}
				seenOwnership[key] = true
				rows.Ownership = append(rows.Ownership, normalizeGitLabOwnershipRow(claim.OrgID, teamID, path, specificity, normalizedAt))
			}
		}

		if selection.Members {
			teamID := gitlabTeamID(group.FullPath)
			memberPages, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
				Path: groupPathValue + "/members", PerPage: gitlabTeamCatalogListPerPage, MaxPages: gitlabTeamCatalogMembersMaxPages,
			})
			if err != nil {
				return GitLabTeamCatalogBatch{}, err
			}
			evidence.Requests += memberPages.Pages
			evidence.Pages += memberPages.Pages
			if memberPages.PageBudgetExhausted {
				evidence.Truncated = true
			}
			for _, raw := range memberPages.Items {
				var member gitlabTeamCatalogMemberPayload
				if err := json.Unmarshal(raw, &member); err != nil {
					return GitLabTeamCatalogBatch{}, providerfoundation.ErrNormalizationInvalid
				}
				row, memberID, ok := normalizeGitLabMembershipRow(claim.OrgID, teamID, member, normalizedAt)
				if !ok {
					continue
				}
				key := teamID + "\x00" + memberID
				if seenMembership[key] {
					continue
				}
				seenMembership[key] = true
				rows.Memberships = append(rows.Memberships, row)
			}
		}
	}

	if selection.Members {
		roster := gitlabRosterFromMemberships(rows.Memberships)
		for i := range rows.Teams {
			rows.Teams[i].Members = roster[rows.Teams[i].ID]
			rows.Teams[i].MembersAuthoritative = true
		}
	}

	if selection.Projects {
		allProjectPages, err := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
			Path: rootPath + "/projects", PerPage: gitlabTeamCatalogListPerPage, MaxPages: gitlabTeamCatalogAllProjectsMaxPages,
			Query: url.Values{"include_subgroups": {"true"}},
		})
		if err != nil {
			return GitLabTeamCatalogBatch{}, err
		}
		evidence.Requests += allProjectPages.Pages
		evidence.Pages += allProjectPages.Pages
		if allProjectPages.PageBudgetExhausted {
			evidence.Truncated = true
		}
		for _, raw := range allProjectPages.Items {
			var project gitlabTeamCatalogProjectPayload
			if err := json.Unmarshal(raw, &project); err != nil {
				return GitLabTeamCatalogBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			row, ok := normalizeGitLabProjectCatalogRow(claim.OrgID, project, normalizedAt)
			if !ok {
				continue
			}
			rows.Projects = append(rows.Projects, row)
		}
		rows.Projects = dedupeGitLabProjectCatalogRows(rows.Projects)
	}

	for _, row := range rows.Teams {
		if err := validateGitLabTeamRow(claim, row); err != nil {
			return GitLabTeamCatalogBatch{}, err
		}
	}
	for _, row := range rows.Ownership {
		if err := validateGitLabOwnershipRow(claim, row); err != nil {
			return GitLabTeamCatalogBatch{}, err
		}
	}
	for _, row := range rows.Memberships {
		if err := validateGitLabMembershipRow(claim, row); err != nil {
			return GitLabTeamCatalogBatch{}, err
		}
	}
	for _, row := range rows.Projects {
		if err := row.validate(claim); err != nil {
			return GitLabTeamCatalogBatch{}, err
		}
	}

	effects, err := BuildGitLabTeamCatalogEffects(rows, selection.Teams, selection.Projects, selection.Members)
	if err != nil {
		return GitLabTeamCatalogBatch{}, err
	}

	result := GitLabTeamCatalogResult{
		TeamsImported: len(rows.Teams), TeamProjectOwnershipImported: len(rows.Ownership),
		TeamMembershipsImported: len(rows.Memberships), NativeProjectsImported: len(rows.Projects),
		ProjectsImported: len(distinctGitLabOwnershipProjects(rows.Ownership)),
		MembersImported:  len(distinctGitLabMembershipMembers(rows.Memberships)),
		Complete:         !evidence.Truncated,
	}
	return GitLabTeamCatalogBatch{Rows: rows, Effects: effects, Result: result, Evidence: evidence}, nil
}

func distinctGitLabOwnershipProjects(rows []gitlabTeamCatalogOwnershipRow) map[string]struct{} {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.ProjectID] = struct{}{}
	}
	return seen
}

func distinctGitLabMembershipMembers(rows []gitlabTeamCatalogMembershipRow) map[string]struct{} {
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		seen[row.MemberID] = struct{}{}
	}
	return seen
}
