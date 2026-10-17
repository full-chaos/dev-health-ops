package teamattribution

import (
	"context"
	"strconv"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// GitLabCatalogProjectIDString is the catalog id `{org}:gitlab:{N}` of a
// GitLab project. providersync.GitLabCatalogProjectID builds the same value;
// a providersync test pins that they agree.
func GitLabCatalogProjectIDString(orgID, nativeID string) string {
	return orgID + ":gitlab:" + nativeID
}

// IsGitLabCatalogProjectID reports whether id is a catalog id. A GitLab path
// holds no ':'.
func IsGitLabCatalogProjectID(id string) bool {
	return strings.Contains(id, ":gitlab:")
}

// GitLabProjectNames are the names a subject answers to in a name join (a repo
// ownership row keyed by full name, a manual repo or project rule): for a
// GitLab item every path of its repos row (the current path and the minted
// path) plus the path it was written under; for any other provider its
// project_id. Never crosses providers.
func GitLabProjectNames(subject GithubWorkItemDerivationSubject) []string {
	if subject.Provider != "gitlab" {
		return []string{GithubWorkItemDerivationStringValue(subject.ProjectID)}
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(subject.GitLabPaths)+2)
	add := func(name string) {
		if name == "" {
			return
		}
		if _, dup := seen[name]; !dup {
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	for _, path := range subject.GitLabPaths {
		add(path)
	}
	if len(out) > 0 {
		return out
	}
	// No repos row was read. An item anchored to a catalog id has no path that is
	// provably its own; a legacy item (the path in project_id) keeps the path it
	// was written under, as before the anchoring.
	if subject.ProjectID != nil && !IsGitLabCatalogProjectID(*subject.ProjectID) {
		add(*subject.ProjectID)
	}
	return out
}

// gitLabIssueProjectKeys are the team project keys a GitLab item is looked up
// by: the paths its repos row answers to (never a path another row holds as its
// current one), its work scope first when it is one of them.
func gitLabIssueProjectKeys(subject GithubWorkItemDerivationSubject) []string {
	names := GitLabProjectNames(subject)
	scope := WorkItemDerivationScope(subject)
	keys := make([]string, 0, len(names))
	for _, name := range names {
		if name == scope {
			keys = append(keys, name)
		}
	}
	for _, name := range names {
		if name != scope {
			keys = append(keys, name)
		}
	}
	return keys
}

func (derived GithubWorkItemDerivationContext) repoByNames(subject GithubWorkItemDerivationSubject) []GithubWorkItemDerivationCandidate {
	var out []GithubWorkItemDerivationCandidate
	for _, name := range GitLabProjectNames(subject) {
		out = append(out, derived.repoByName[AttributionMapKey(subject.Provider, name)]...)
	}
	return out
}

// gitLabSubjectNamedByKey is true for a GitLab subject whose project_id is a
// catalog id and whose project_key holds the path.
func gitLabSubjectNamedByKey(subject GithubWorkItemDerivationSubject) bool {
	return subject.Provider == "gitlab" && subject.ProjectID != nil && IsGitLabCatalogProjectID(*subject.ProjectID) &&
		subject.ProjectKey != nil && strings.TrimSpace(*subject.ProjectKey) != ""
}

// GitLabRowPaths are the names a repos row answers to: its current path, and its
// minted path unless another row holds that path as its current one (a reused
// path belongs to the project that holds it now, never to the project the id
// was first minted for).
func GitLabRowPaths(row GitLabRepoRow, repoID string, currentPathHolders map[string][]string) []string {
	var paths []string
	if row.Repo != "" {
		paths = append(paths, row.Repo)
	}
	if row.IdentityPath == "" || strings.EqualFold(row.IdentityPath, row.Repo) {
		return paths
	}
	for _, holder := range currentPathHolders[strings.ToLower(row.IdentityPath)] {
		if holder != repoID {
			return paths
		}
	}
	return append(paths, row.IdentityPath)
}

// GitLabRepoRow is the part of a GitLab repos row a work item joins through.
type GitLabRepoRow struct {
	NativeProjectID string
	Repo            string
	IdentityPath    string
}

// LoadGitLabRepoRows maps each repo id (of repoIDs) to its GitLab repos row:
// the numeric project id (provider_repo_id, or settings.project_id for a row
// written before migration 134), the current path and the minted path. A repo
// with no GitLab row is absent.
func LoadGitLabRepoRows(
	ctx context.Context, conn driver.Conn, orgID string, repoIDs []string,
) (map[string]GitLabRepoRow, error) {
	out := make(map[string]GitLabRepoRow, len(repoIDs))
	for start := 0; start < len(repoIDs); start += gitLabNativeProjectChunk {
		end := min(start+gitLabNativeProjectChunk, len(repoIDs))
		rows, err := conn.Query(ctx, `
SELECT toString(id), provider_repo_id, JSONExtractInt(ifNull(settings, ''), 'project_id'), repo, identity_path
FROM repos FINAL
WHERE org_id = ? AND provider = 'gitlab' AND id IN (SELECT toUUID(arrayJoin(?)))`,
			orgID, repoIDs[start:end])
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, providerRepoID, repo, identityPath string
			var settingsProjectID int64
			if err := rows.Scan(&id, &providerRepoID, &settingsProjectID, &repo, &identityPath); err != nil {
				_ = rows.Close()
				return nil, err
			}
			row := GitLabRepoRow{Repo: repo, IdentityPath: identityPath, NativeProjectID: providerRepoID}
			if row.NativeProjectID == "" && settingsProjectID > 0 {
				row.NativeProjectID = strconv.FormatInt(settingsProjectID, 10)
			}
			out[id] = row
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	return out, nil
}

// LoadGitLabCurrentPathHolders maps each lower-cased path of paths to the ids of
// the GitLab repos rows whose current path (repos.repo) it is.
func LoadGitLabCurrentPathHolders(
	ctx context.Context, conn driver.Conn, orgID string, paths []string,
) (map[string][]string, error) {
	out := make(map[string][]string, len(paths))
	for start := 0; start < len(paths); start += gitLabNativeProjectChunk {
		end := min(start+gitLabNativeProjectChunk, len(paths))
		rows, err := conn.Query(ctx, `
SELECT lower(repo), toString(id)
FROM repos FINAL
WHERE org_id = ? AND provider = 'gitlab' AND lower(repo) IN ?`,
			orgID, paths[start:end])
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var path, id string
			if err := rows.Scan(&path, &id); err != nil {
				_ = rows.Close()
				return nil, err
			}
			out[path] = append(out[path], id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, err
		}
		_ = rows.Close()
	}
	return out, nil
}

const gitLabNativeProjectChunk = 5000

// GitLabAnchorStats counts what AnchorGitLabSubjectProjects did, for the caller's log.
type GitLabAnchorStats struct{ Anchored, Unanchored, IdentityPathWithheld int }

// AnchorGitLabSubjectProjects reads each GitLab subject's repos row and sets
// the paths it answers to (GitLabPaths). A subject written before the
// numeric-id anchoring (project_id holds the path) also gets the project it
// belongs to: project_id becomes the catalog id of the project its repo_id
// names, and the old path moves to project_key so the work scope is unchanged.
// A subject whose repo names no project keeps its path and so joins no project
// ownership (fail-safe). A subject that already holds a catalog id keeps it.
func AnchorGitLabSubjectProjects(
	ctx context.Context, conn driver.Conn, subjects map[string]GithubWorkItemDerivationSubject,
) (GitLabAnchorStats, error) {
	repoIDsByOrg := map[string]map[string]struct{}{}
	for _, subject := range subjects {
		if subject.Provider != "gitlab" || subject.RepoID == nil || subject.OrgID == "" {
			continue
		}
		if repoIDsByOrg[subject.OrgID] == nil {
			repoIDsByOrg[subject.OrgID] = map[string]struct{}{}
		}
		repoIDsByOrg[subject.OrgID][*subject.RepoID] = struct{}{}
	}
	rowsByOrg := map[string]map[string]GitLabRepoRow{}
	holdersByOrg := map[string]map[string][]string{}
	for orgID, set := range repoIDsByOrg {
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		rows, err := LoadGitLabRepoRows(ctx, conn, orgID, ids)
		if err != nil {
			return GitLabAnchorStats{}, err
		}
		rowsByOrg[orgID] = rows
		identityPaths := make([]string, 0, len(rows))
		for _, row := range rows {
			if row.IdentityPath != "" && !strings.EqualFold(row.IdentityPath, row.Repo) {
				identityPaths = append(identityPaths, strings.ToLower(row.IdentityPath))
			}
		}
		holders, err := LoadGitLabCurrentPathHolders(ctx, conn, orgID, identityPaths)
		if err != nil {
			return GitLabAnchorStats{}, err
		}
		holdersByOrg[orgID] = holders
	}
	anchored, unanchored, withheld := 0, 0, 0
	for id, subject := range subjects {
		if subject.Provider != "gitlab" || subject.RepoID == nil {
			continue
		}
		row, ok := rowsByOrg[subject.OrgID][*subject.RepoID]
		if !ok {
			unanchored++
			continue
		}
		subject.GitLabPaths = GitLabRowPaths(row, *subject.RepoID, holdersByOrg[subject.OrgID])
		if row.IdentityPath != "" && !strings.EqualFold(row.IdentityPath, row.Repo) && len(subject.GitLabPaths) < 2 {
			withheld++
		}
		if (subject.ProjectID == nil || !IsGitLabCatalogProjectID(*subject.ProjectID)) && row.NativeProjectID != "" {
			if subject.ProjectKey == nil || strings.TrimSpace(*subject.ProjectKey) == "" {
				subject.ProjectKey = subject.ProjectID
			}
			catalog := GitLabCatalogProjectIDString(subject.OrgID, row.NativeProjectID)
			subject.ProjectID = &catalog
			anchored++
		} else if subject.ProjectID == nil || !IsGitLabCatalogProjectID(*subject.ProjectID) {
			unanchored++
		}
		subjects[id] = subject
	}
	return GitLabAnchorStats{Anchored: anchored, Unanchored: unanchored, IdentityPathWithheld: withheld}, nil
}

// LoadGitLabProjectNativesByPath maps each lower-cased path of paths to the one
// numeric GitLab project id the org's repos rows name at it (as the current path
// or the minted one). A path two projects have used is ambiguous and absent from
// natives; a path no row names is absent from both maps.
func LoadGitLabProjectNativesByPath(
	ctx context.Context, conn driver.Conn, orgID string, paths []string,
) (natives map[string]string, ambiguous map[string]struct{}, err error) {
	seen := make(map[string]map[string]struct{}, len(paths))
	wanted := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		wanted[path] = struct{}{}
	}
	for start := 0; start < len(paths); start += gitLabNativeProjectChunk {
		end := min(start+gitLabNativeProjectChunk, len(paths))
		rows, queryErr := conn.Query(ctx, `
SELECT lower(repo), lower(identity_path), provider_repo_id, JSONExtractInt(ifNull(settings, ''), 'project_id')
FROM repos FINAL
WHERE org_id = ? AND provider = 'gitlab' AND (lower(repo) IN ? OR lower(identity_path) IN ?)`,
			orgID, paths[start:end], paths[start:end])
		if queryErr != nil {
			return nil, nil, queryErr
		}
		for rows.Next() {
			var repo, identityPath, providerRepoID string
			var settingsProjectID int64
			if scanErr := rows.Scan(&repo, &identityPath, &providerRepoID, &settingsProjectID); scanErr != nil {
				_ = rows.Close()
				return nil, nil, scanErr
			}
			native := providerRepoID
			if native == "" && settingsProjectID > 0 {
				native = strconv.FormatInt(settingsProjectID, 10)
			}
			if native == "" {
				continue
			}
			for _, path := range []string{repo, identityPath} {
				if _, ok := wanted[path]; !ok {
					continue
				}
				if seen[path] == nil {
					seen[path] = map[string]struct{}{}
				}
				seen[path][native] = struct{}{}
			}
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return nil, nil, rowsErr
		}
		_ = rows.Close()
	}
	natives, ambiguous = map[string]string{}, map[string]struct{}{}
	for path, set := range seen {
		if len(set) != 1 {
			ambiguous[path] = struct{}{}
			continue
		}
		for native := range set {
			natives[path] = native
		}
	}
	return natives, ambiguous, nil
}

// remapLegacyGitLabProjectFacts moves a GitLab ownership row written before
// CHAOS-8883 (project_id holds the path) to the catalog id of the one project
// its path names, so it lands on the same key as an item anchored to that id. A
// row whose path two projects have used is dropped (a reused path is not a
// grant); a row whose path no repos row names stays as it is.
func (source ClickHouseFactSource) remapLegacyGitLabProjectFacts(
	ctx context.Context, orgID string, facts []GithubWorkItemDerivationProjectFact,
) (out []GithubWorkItemDerivationProjectFact, remapped, dropped int, err error) {
	var paths []string
	seen := map[string]struct{}{}
	for _, fact := range facts {
		if !isLegacyGitLabOwnershipFact(fact) {
			continue
		}
		path := strings.ToLower(strings.TrimSpace(fact.ProjectID))
		if _, dup := seen[path]; !dup {
			seen[path] = struct{}{}
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return facts, 0, 0, nil
	}
	natives, ambiguous, err := LoadGitLabProjectNativesByPath(ctx, source.Conn, orgID, paths)
	if err != nil {
		return nil, 0, 0, err
	}
	out, remapped, dropped = RemapLegacyGitLabProjectFacts(orgID, facts, natives, ambiguous)
	return out, remapped, dropped, nil
}

func isLegacyGitLabOwnershipFact(fact GithubWorkItemDerivationProjectFact) bool {
	return fact.Provider == "gitlab" && strings.TrimSpace(fact.ProjectID) != "" && !IsGitLabCatalogProjectID(fact.ProjectID)
}

// RemapLegacyGitLabProjectFacts is the pure part of remapLegacyGitLabProjectFacts.
func RemapLegacyGitLabProjectFacts(
	orgID string, facts []GithubWorkItemDerivationProjectFact, natives map[string]string, ambiguous map[string]struct{},
) (out []GithubWorkItemDerivationProjectFact, remapped, dropped int) {
	out = make([]GithubWorkItemDerivationProjectFact, 0, len(facts))
	for _, fact := range facts {
		if !isLegacyGitLabOwnershipFact(fact) {
			out = append(out, fact)
			continue
		}
		path := strings.ToLower(strings.TrimSpace(fact.ProjectID))
		if _, bad := ambiguous[path]; bad {
			dropped++
			continue
		}
		if native, ok := natives[path]; ok {
			if fact.ProjectKey == nil || strings.TrimSpace(*fact.ProjectKey) == "" {
				key := fact.ProjectID
				fact.ProjectKey = &key
			}
			fact.ProjectID = GitLabCatalogProjectIDString(orgID, native)
			remapped++
		}
		out = append(out, fact)
	}
	return out, remapped, dropped
}
