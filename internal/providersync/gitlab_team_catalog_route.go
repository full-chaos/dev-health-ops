package providersync

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/url"
	"sort"
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
	Provider string `json:"provider"`
	Requests int    `json:"requests"`
	Pages    int    `json:"pages"`
	Groups   int    `json:"groups"`
	// Truncated marks a bounded listing (subgroups/projects/members) that
	// hit its page cap -- the collector adapter fails the whole run closed
	// on this (ErrPaginationCapExceeded), it is never partially written.
	Truncated bool `json:"truncated"`
	// SkippedTeamMemberships (CHAOS-4461, extended to GitLab) counts groups
	// whose /members fetch failed OUTRIGHT (a real request error, not a
	// page-cap truncation) under non-strict -- that group's memberships are
	// skipped and its roster is carried forward instead of aborting the
	// whole run, matching GitHubTeamCatalogEvidence's identical field.
	SkippedTeamMemberships int `json:"skipped_team_memberships"`
	// MissingSelectedSources (codex round 5, P2) lists the ref.SourceExternalIDs
	// entries that were selected but never observed among the discovered
	// projects -- mirrors team_autoimport_gitlab._gitlab_project_catalog_rows's
	// missing_selected_source_ids exactly (a selected source's project was
	// deleted, access was revoked, or discovery's own pagination bound was
	// hit before reaching it). A non-empty set here folds into
	// GitLabTeamCatalogResult.Complete = false, the same fail-closed signal
	// pagination truncation produces -- a partial catalog (missing a
	// selected id) must never be recorded as a complete one, exactly like
	// Python's native_projects_complete = not truncated and not missing.
	MissingSelectedSources []string `json:"missing_selected_sources,omitempty"`
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
		// codex review finding (round 4, P1): this per-group /projects
		// fetch exists only to populate project_keys, which only the Teams
		// row (repo_patterns/project_keys) and the Projects/ownership rows
		// actually consume -- gate it on those two selections so a
		// Members-only run never issues (or can be truncation-poisoned by)
		// a fetch it has no use for. Previously unconditional: a
		// Members-only run's project page-cap truncation on THIS
		// unrelated surface would still trip the whole-run fail-closed
		// guard added for finding #4 above and discard otherwise valid
		// member rows.
		var projectKeys []string
		if selections.Teams || selections.Projects {
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
			projectKeys = make([]string, 0, len(projectPages.Items))
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
			memberPages, memberErr := providerfoundation.CollectGitLabPageParamPages(ctx, client, providerfoundation.GitLabPageOptions{
				Path: groupPathValue + "/members", PerPage: gitlabTeamCatalogListPerPage, MaxPages: gitlabTeamCatalogMembersMaxPages,
			})
			if memberErr != nil {
				// CHAOS-4461 ruling (team-lead, extended from GitHub to
				// GitLab, 2026-08-28): a single group's member-fetch
				// failure under non-strict (post-sync, default) must not
				// abort the whole catalog walk -- skip only this group's
				// memberships and mark it for roster carry-forward via
				// FailedMemberFetchTeamIDs (see that field's doc comment).
				// Under strict (reference discovery), re-raise, matching
				// the pre-existing behavior exactly.
				if ref.Strict {
					return GitLabTeamCatalogBatch{}, memberErr
				}
				rows.FailedMemberFetchTeamIDs = append(rows.FailedMemberFetchTeamIDs, teamID)
				evidence.SkippedTeamMemberships++
				// codex review finding (round 4, P2): TeamCatalogResult (the
				// shared interface) has no field for a partial/degraded
				// outcome, so a caller reading only that struct cannot tell
				// this run's roster for teamID was carried forward rather
				// than freshly observed. A structured log line is the
				// interim signal -- same discipline GitHub's identical
				// CHAOS-4461 fix uses ("pending a shared telemetry field"),
				// not a new interface field of its own.
				slog.Default().WarnContext(ctx, "gitlab_team_catalog_member_fetch_failed",
					"org_id", ref.OrgID, "team_id", teamID, "error", memberErr)
			} else {
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
	}

	if selections.Members {
		failedMemberFetch := make(map[string]bool, len(rows.FailedMemberFetchTeamIDs))
		for _, id := range rows.FailedMemberFetchTeamIDs {
			failedMemberFetch[id] = true
		}
		roster := gitlabRosterFromMemberships(rows.Memberships)
		for i := range rows.Teams {
			if failedMemberFetch[rows.Teams[i].ID] {
				// CHAOS-4461: this team's /members fetch failed under
				// non-strict -- leave MembersAuthoritative false (its
				// zero value from normalizeGitLabTeamRow) rather than
				// stamping the empty roster gitlabRosterFromMemberships
				// necessarily produces for it (no memberships were ever
				// added). GitLabTeamCatalogClickHouseEffects.writeTeams's
				// existing roster-preservation path then confirms and
				// carries forward the currently-persisted roster instead.
				continue
			}
			rows.Teams[i].Members = roster[rows.Teams[i].ID]
			rows.Teams[i].MembersAuthoritative = true
		}
	}

	if selections.Projects {
		// Filtered by ref.SourceExternalIDs (CHAOS-4431's base push,
		// team-lead ruling 2026-08-28): mirrors team_autoimport_gitlab.
		// _gitlab_project_catalog_rows's source_external_ids filter --
		// only a discovered project whose raw numeric id (the SAME id
		// gitlabProjectCatalogID mints this row's id from) is in the
		// run's enabled-source set is cataloged. The shared resolver
		// (teamCatalogSourceResolver, cmd/dev-health-worker/team_catalog_
		// clients.go) populates ref.SourceExternalIDs from the identical
		// sync_run_units-JOIN-integration_sources join Python's
		// _load_discovery_context uses -- this collector stays DB-free,
		// per team-lead's "no per-provider injection seams" ruling.
		//
		// Deliberate simplification vs Python (team-lead ruling,
		// 2026-08-28: "empty = unscoped"): Python's _source_external_ids
		// distinguishes None (key absent -- unscoped, the common post-sync
		// trigger's behavior today) from an explicit empty set (reference
		// discovery enumerated zero enabled sources -- filter everything
		// OUT). Go's ref.SourceExternalIDs collapses both into "no
		// filtering" when empty, since the shared resolver populates it
		// uniformly at both call sites (bea10bee9) and a Go nil/empty-slice
		// split at this boundary would be a fragile, easily-inverted signal
		// to carry across the interface. A zero-enabled-source run is a
		// pre-existing edge case (this org's IntegrationSource rows are all
		// disabled) that both dispatchers already guard elsewhere; not
		// re-litigated here.
		sourceExternalIDs := make(map[string]bool, len(ref.SourceExternalIDs))
		for _, id := range ref.SourceExternalIDs {
			sourceExternalIDs[id] = true
		}
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
		discoveredIDs := make(map[string]bool, len(allProjectPages.Items))
		for _, raw := range allProjectPages.Items {
			var project gitlabTeamCatalogProjectPayload
			if err := json.Unmarshal(raw, &project); err != nil {
				return GitLabTeamCatalogBatch{}, providerfoundation.ErrNormalizationInvalid
			}
			nativeID := strings.TrimSpace(project.ID.String())
			discoveredIDs[nativeID] = true
			if len(sourceExternalIDs) > 0 && !sourceExternalIDs[nativeID] {
				continue
			}
			row, ok := normalizeGitLabProjectCatalogRow(ref.OrgID, project, normalizedAt)
			if !ok {
				continue
			}
			rows.Projects = append(rows.Projects, row)
		}
		rows.Projects = dedupeGitLabProjectCatalogRows(rows.Projects)
		// codex round 5, P2: a selected source id discovery never returned
		// at all (deleted project, revoked access, or a pagination bound
		// hit before reaching it) is a DIFFERENT gap from the filter above
		// (which only narrows discovered -> selected) -- computed here so
		// the batch is honest about it even though the (possibly partial)
		// rows.Projects collected above still gets returned; the caller
		// decides whether to write a partial view via Result.Complete.
		if len(sourceExternalIDs) > 0 {
			missing := make([]string, 0)
			for id := range sourceExternalIDs {
				if !discoveredIDs[id] {
					missing = append(missing, id)
				}
			}
			sort.Strings(missing)
			evidence.MissingSelectedSources = missing
		}
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
		Complete:         !evidence.Truncated && len(evidence.MissingSelectedSources) == 0,
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
	// codex review finding: a bounded GitLab listing hitting its page cap
	// (evidence.Truncated / !batch.Result.Complete) must never be written as
	// though it were a full, authoritative catalog -- TeamCatalogResult (the
	// shared interface, lane-4431's file) carries no completeness field for
	// the caller to inspect, so this fails closed here instead of writing a
	// partial view and reporting success. A caller sees a pagination-cap
	// error and can retry/investigate rather than silently losing teams,
	// memberships, ownership, or native project subjects for a large group.
	if !batch.Result.Complete {
		return TeamCatalogResult{}, ErrPaginationCapExceeded
	}
	writeClaim := Claim{Unit: Unit{OrgID: ref.OrgID, Provider: gitlabTeamCatalogProvider}}
	result := TeamCatalogResult{}

	// CHAOS-4431 codex review finding #6, ROUND 2 correction (P1, mirrored
	// from LinearTeamCatalogCollector): the membership-conflict guard must
	// run BEFORE the team roster is rebuilt, not after -- a membership the
	// guard rejects must never still show up in `teams.members` even though
	// it was correctly kept out of `team_memberships`. Computed once, up
	// front, so both blocks below read from its result.
	var keptMemberships []gitlabTeamCatalogMembershipRow
	var membershipsSkippedManualConflict int
	if selections.Members && len(batch.Rows.Memberships) > 0 {
		var guardErr error
		keptMemberships, membershipsSkippedManualConflict, guardErr = applyGitLabTeamMembershipConflictGuard(
			ctx, collector.Sink.Conn, ref.OrgID, gitlabTeamCatalogProvider, batch.Rows.Memberships,
		)
		if guardErr != nil {
			return result, guardErr
		}
	}

	if selections.Teams && batch.Effects.Teams != nil {
		teamRows := batch.Rows.Teams
		if selections.Members {
			// Rebuild each team's roster from the CONFLICT-FILTERED
			// memberships, not the raw walk-observed roster the Handler
			// baked into the row -- see the doc comment above. A team whose
			// own member fetch failed (FailedMemberFetchTeamIDs, non-strict
			// soft-fail, CHAOS-4461) keeps MembersAuthoritative=false from
			// the walk untouched here -- the sink's existing roster-
			// preservation path (CHAOS-4323 round 2) already handles it,
			// independent of this guard.
			roster := gitlabRosterFromMemberships(keptMemberships)
			teamRows = append([]gitlabTeamCatalogTeamRow(nil), teamRows...)
			for index := range teamRows {
				if !teamRows[index].MembersAuthoritative {
					continue
				}
				teamRows[index].Members = roster[teamRows[index].ID]
			}
		}
		// CHAOS-4431 codex review findings #3/#6, team-lead ruling
		// 2026-08-28 (extended to GitLab): a team whose sync_policy is not
		// the auto-apply default (0) is left completely untouched by this
		// write, not overwritten with this call's observed values.
		keptTeams, skippedTeamIDs, guardErr := applyGitLabTeamSyncPolicyGuard(ctx, collector.Sink.Conn, ref.OrgID, teamRows)
		if guardErr != nil {
			return result, guardErr
		}
		result.TeamsSkippedPolicy = len(skippedTeamIDs)
		teamsEffect, effectErr := effectBatchFromValues(gitlabTeamCatalogTeamsDestination, EffectReadbackRequired, keptTeams)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, teamsEffect); err != nil {
			return result, err
		}
		result.TeamsWritten = len(keptTeams)
		result.TeamKeys = make([]string, 0, len(keptTeams))
		for _, team := range keptTeams {
			if team.NativeTeamKey != nil && *team.NativeTeamKey != "" {
				result.TeamKeys = append(result.TeamKeys, *team.NativeTeamKey)
			}
		}
	}
	if selections.Members && batch.Effects.Memberships != nil {
		// CHAOS-4431 codex review finding #6, team-lead ruling 2026-08-28
		// (extended to GitLab): write the CONFLICT-FILTERED memberships,
		// computed above before the Teams block, so the roster and the
		// memberships table never disagree about which assignments are
		// safe. Independent of the #3 sync_policy guard above: this gate
		// applies even to policy-0 teams (team-attribution.md:793-797).
		result.MembershipsSkippedManualConflict = membershipsSkippedManualConflict
		membershipsEffect, effectErr := effectBatchFromValues(gitlabTeamCatalogMembershipsDestination, EffectReadbackRequired, keptMemberships)
		if effectErr != nil {
			return result, effectErr
		}
		if err := collector.Sink.WriteEffect(ctx, writeClaim, membershipsEffect); err != nil {
			return result, err
		}
		result.MembershipsWritten = len(keptMemberships)
		result.MembersWritten = len(distinctGitLabMembershipMembers(keptMemberships))
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
