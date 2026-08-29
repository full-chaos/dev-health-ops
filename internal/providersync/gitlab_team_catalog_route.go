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
// src/dev_health_ops/workers/team_autoimport_gitlab.py. It is claim-free
// (CHAOS-4431 ruling, team-lead 2026-08-28, option (c)): team/member/project
// reference discovery runs once per sync run per provider, never as a
// claimed provider-unit, so this takes a TeamCatalogReference instead of a
// Claim and carries no lease. GitLabTeamCatalogCollector below adapts this
// walk (plus GitLabTeamCatalogClickHouseEffects, the writer) to the shared
// TeamCatalogCollector seam every native provider implements.
//
// No injectable resolver fields (team-lead ruling, 2026-08-28: "no
// per-provider injection seams on the shared wiring") -- group_path comes
// straight off ref.SyncOptions, the run's own canonical
// sync_configurations.sync_options, the same source Python's
// _gitlab_group falls back to.
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

// gitlabTeamCatalogGroupPathKeys is the exact key precedence
// team_autoimport_gitlab._gitlab_group uses (_first_string(mapping,
// "group_path", "group", "owner") -- the first present, non-empty string
// wins): group_path outranks group outranks owner. Verified against that
// function's current source, not assumed.
var gitlabTeamCatalogGroupPathKeys = []string{"group_path", "group", "owner"}

func gitlabTeamCatalogFirstString(mapping map[string]any, keys []string) string {
	for _, key := range keys {
		if value, ok := mapping[key].(string); ok {
			if trimmed := strings.TrimSpace(value); trimmed != "" {
				return trimmed
			}
		}
	}
	return ""
}

// gitlabTeamCatalogGroupPath mirrors team_autoimport_gitlab._gitlab_group
// exactly: credential.Config (credentials, in Python) first, then
// ref.SyncOptions (scope.sync_options, in Python) -- same key precedence on
// both sides. credential.Config carries only auth material for GitLab in
// practice today (empty), so this resolves from ref.SyncOptions in
// production; the credential-first check stays for parity with Python's
// contract and for a future credential shape that DOES carry it.
func gitlabTeamCatalogGroupPath(credential providerfoundation.Credential, ref TeamCatalogReference) string {
	for _, key := range gitlabTeamCatalogGroupPathKeys {
		if value := strings.TrimSpace(credential.Config[key]); value != "" {
			return value
		}
	}
	return gitlabTeamCatalogFirstString(ref.SyncOptions, gitlabTeamCatalogGroupPathKeys)
}

func (handler GitLabTeamCatalogRouteHandler) resolveGroupPath(
	ref TeamCatalogReference, credential providerfoundation.Credential,
) (string, error) {
	groupPath := gitlabTeamCatalogGroupPath(credential, ref)
	if groupPath == "" {
		return "", ErrInvalidConfiguration
	}
	return groupPath, nil
}

func (handler GitLabTeamCatalogRouteHandler) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (GitLabTeamCatalogBatch, error) {
	if ctx == nil || ref.validate() != nil || credential.Provider != gitlabTeamCatalogProvider ||
		client == nil || client.Provider != gitlabTeamCatalogProvider || client.BaseURL == nil ||
		client.Doer == nil || client.Lease == nil || normalizedAt.IsZero() {
		return GitLabTeamCatalogBatch{}, ErrInvalidConfiguration
	}
	if !selections.Any() {
		return GitLabTeamCatalogBatch{Result: GitLabTeamCatalogResult{Complete: true}}, nil
	}
	groupPath, err := handler.resolveGroupPath(ref, credential)
	if err != nil {
		return GitLabTeamCatalogBatch{}, err
	}
	// orgID is used for every normalized row below; a synthetic, lease-free
	// Claim carries it through the existing validate*Row helpers (which only
	// ever read claim.Provider/claim.OrgID, never claim.Validate()) rather
	// than duplicating those checks against a bare string -- the same
	// adaptation LinearTeamCatalogCollector's sibling route uses.
	claim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: gitlabTeamCatalogProvider}}
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

		if selections.Teams {
			rows.Teams = append(rows.Teams, normalizeGitLabTeamRow(ref.OrgID, group, projectKeys, normalizedAt))
		}

		if selections.Projects {
			teamID := gitlabTeamID(group.FullPath)
			specificity := uint16(gitlabTeamCatalogBaseSpecificity + gitlabTeamDepth(teamID, parentByTeam)*gitlabTeamCatalogChildSpecificityStep)
			for _, path := range projectKeys {
				key := teamID + "\x00" + path
				if seenOwnership[key] {
					continue
				}
				seenOwnership[key] = true
				rows.Ownership = append(rows.Ownership, normalizeGitLabOwnershipRow(ref.OrgID, teamID, path, specificity, normalizedAt))
			}
		}

		if selections.Members {
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
				row, memberID, ok := normalizeGitLabMembershipRow(ref.OrgID, teamID, member, normalizedAt)
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

	if selections.Members {
		roster := gitlabRosterFromMemberships(rows.Memberships)
		for i := range rows.Teams {
			rows.Teams[i].Members = roster[rows.Teams[i].ID]
			rows.Teams[i].MembersAuthoritative = true
		}
	}

	if selections.Projects {
		// NOT filtered by selected IntegrationSource ids (codex review
		// finding, CHAOS-4432): team_autoimport_gitlab._gitlab_project_catalog_rows's
		// source_external_ids filter is populated by
		// workers/reference_discovery.py's _load_discovery_context via a
		// LIVE DB join (sync_run_units -> integration_sources.external_id
		// for THIS run's own claimed units) -- a separate computation from
		// integration.config/sync_options (verified against that function's
		// current source: the scope dict carries "source_external_ids" and
		// "sync_options" as two independent keys). ref.SyncOptions carries
		// only the latter, so it cannot derive the former; per team-lead's
		// "no per-provider injection seams" ruling, this collector does not
		// add its own DB dependency to get it either. Every discovered
		// project is cataloged unscoped -- identical to Python's own
		// default for the common post-sync path, but a real, open gap for
		// the strict/reference-discovery path specifically until
		// TeamCatalogReference grows a field for it.
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
			row, ok := normalizeGitLabProjectCatalogRow(ref.OrgID, project, normalizedAt)
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

	effects, err := BuildGitLabTeamCatalogEffects(rows, selections.Teams, selections.Projects, selections.Members)
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

// GitLabTeamCatalogCollector adapts GitLabTeamCatalogRouteHandler (the
// collection walk) and GitLabTeamCatalogClickHouseEffects (the write) to
// the shared, claim-free TeamCatalogCollector seam (CHAOS-4431, team-lead
// ruling 2026-08-28, option (c)) -- the same shape
// LinearTeamCatalogCollector uses. Unlike Linear's GraphQL walk (one round
// trip covers teams+members+projects together), GitLab's REST walk pays a
// real per-surface request cost, so Handler.CollectTeamCatalog gates
// COLLECTION itself by selections (skips the /members call entirely when
// Members is off), not just the write below.
type GitLabTeamCatalogCollector struct {
	Handler GitLabTeamCatalogRouteHandler
	Sink    GitLabTeamCatalogClickHouseEffects
}

func (collector GitLabTeamCatalogCollector) CollectTeamCatalog(
	ctx context.Context,
	ref TeamCatalogReference,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	selections TeamCatalogSelections,
	normalizedAt time.Time,
) (TeamCatalogResult, error) {
	if ctx == nil || !selections.Any() {
		return TeamCatalogResult{}, nil
	}
	if collector.Sink.Conn == nil || collector.Sink.Lease == nil {
		return TeamCatalogResult{}, ErrInvalidConfiguration
	}
	batch, err := collector.Handler.CollectTeamCatalog(ctx, ref, credential, client, selections, normalizedAt)
	if err != nil {
		return TeamCatalogResult{}, err
	}
	writeClaim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: gitlabTeamCatalogProvider}}
	result := TeamCatalogResult{}
	if selections.Teams && batch.Effects.Teams != nil {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, *batch.Effects.Teams); err != nil {
			return result, err
		}
		result.TeamsWritten = batch.Result.TeamsImported
		result.TeamKeys = make([]string, 0, len(batch.Rows.Teams))
		for _, team := range batch.Rows.Teams {
			if team.NativeTeamKey != nil && *team.NativeTeamKey != "" {
				result.TeamKeys = append(result.TeamKeys, *team.NativeTeamKey)
			}
		}
	}
	if selections.Members && batch.Effects.Memberships != nil {
		if err := collector.Sink.WriteEffect(ctx, writeClaim, *batch.Effects.Memberships); err != nil {
			return result, err
		}
		result.MembershipsWritten = batch.Result.TeamMembershipsImported
		result.MembersWritten = batch.Result.MembersImported
	}
	if selections.Projects {
		if batch.Effects.Ownership != nil {
			if err := collector.Sink.WriteEffect(ctx, writeClaim, *batch.Effects.Ownership); err != nil {
				return result, err
			}
			result.OwnershipWritten = batch.Result.TeamProjectOwnershipImported
		}
		if batch.Effects.Projects != nil {
			if err := collector.Sink.WriteEffect(ctx, writeClaim, *batch.Effects.Projects); err != nil {
				return result, err
			}
			result.ProjectsWritten = batch.Result.NativeProjectsImported
		}
	}
	return result, nil
}

var _ TeamCatalogCollector = GitLabTeamCatalogCollector{}
