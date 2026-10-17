package teamattribution

import (
	"context"
	"log/slog"
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

// GitLabProjectName is the name a GitLab subject answers to in a name join
// (a repo ownership row keyed by full name, a manual rule naming a path): the
// project's current path. A GitLab item written before the numeric-id
// anchoring holds the path in project_id and no project_key; every other
// provider answers by project_id.
func GitLabProjectName(subject GithubWorkItemDerivationSubject) string {
	if gitLabSubjectNamedByKey(subject) {
		return *subject.ProjectKey
	}
	return GithubWorkItemDerivationStringValue(subject.ProjectID)
}

// gitLabSubjectNamedByKey is true for a GitLab subject whose project_id is a
// catalog id and whose project_key holds the path.
func gitLabSubjectNamedByKey(subject GithubWorkItemDerivationSubject) bool {
	return subject.Provider == "gitlab" && subject.ProjectID != nil && IsGitLabCatalogProjectID(*subject.ProjectID) &&
		subject.ProjectKey != nil && strings.TrimSpace(*subject.ProjectKey) != ""
}

// LoadGitLabNativeProjectIDsByRepo maps each repo id (of repoIDs) to the
// numeric id of the GitLab project its repos row names:
// provider_repo_id, or settings.project_id for a row written before
// migration 134. A repo with no such row is absent.
func LoadGitLabNativeProjectIDsByRepo(
	ctx context.Context, conn driver.Conn, orgID string, repoIDs []string,
) (map[string]string, error) {
	out := make(map[string]string, len(repoIDs))
	for start := 0; start < len(repoIDs); start += gitLabNativeProjectChunk {
		end := min(start+gitLabNativeProjectChunk, len(repoIDs))
		rows, err := conn.Query(ctx, `
SELECT toString(id), provider_repo_id, JSONExtractInt(ifNull(settings, ''), 'project_id')
FROM repos FINAL
WHERE org_id = ? AND provider = 'gitlab' AND id IN (SELECT toUUID(arrayJoin(?)))`,
			orgID, repoIDs[start:end])
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, providerRepoID string
			var settingsProjectID int64
			if err := rows.Scan(&id, &providerRepoID, &settingsProjectID); err != nil {
				_ = rows.Close()
				return nil, err
			}
			switch {
			case providerRepoID != "":
				out[id] = providerRepoID
			case settingsProjectID > 0:
				out[id] = strconv.FormatInt(settingsProjectID, 10)
			}
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

// AnchorGitLabSubjectProjects gives each GitLab subject written before the
// numeric-id anchoring (project_id holds the path) the project it belongs to:
// project_id becomes the catalog id of the project its repo_id names, and the
// old path moves to project_key so a name join still sees it. A subject whose
// repo names no project keeps its path and so joins no project ownership
// (fail-safe). A subject that already holds a catalog id is untouched.
func AnchorGitLabSubjectProjects(
	ctx context.Context, conn driver.Conn, subjects map[string]GithubWorkItemDerivationSubject,
) error {
	repoIDsByOrg := map[string]map[string]struct{}{}
	for _, subject := range subjects {
		if subject.Provider != "gitlab" || subject.RepoID == nil || subject.OrgID == "" ||
			(subject.ProjectID != nil && IsGitLabCatalogProjectID(*subject.ProjectID)) {
			continue
		}
		if repoIDsByOrg[subject.OrgID] == nil {
			repoIDsByOrg[subject.OrgID] = map[string]struct{}{}
		}
		repoIDsByOrg[subject.OrgID][*subject.RepoID] = struct{}{}
	}
	nativeByOrgRepo := map[string]map[string]string{}
	for orgID, set := range repoIDsByOrg {
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		native, err := LoadGitLabNativeProjectIDsByRepo(ctx, conn, orgID, ids)
		if err != nil {
			return err
		}
		nativeByOrgRepo[orgID] = native
	}
	anchored, unanchored := 0, 0
	for id, subject := range subjects {
		if subject.Provider != "gitlab" || subject.RepoID == nil ||
			(subject.ProjectID != nil && IsGitLabCatalogProjectID(*subject.ProjectID)) {
			continue
		}
		native, ok := nativeByOrgRepo[subject.OrgID][*subject.RepoID]
		if !ok {
			unanchored++
			continue
		}
		if subject.ProjectKey == nil || strings.TrimSpace(*subject.ProjectKey) == "" {
			subject.ProjectKey = subject.ProjectID
		}
		catalog := GitLabCatalogProjectIDString(subject.OrgID, native)
		subject.ProjectID = &catalog
		subjects[id] = subject
		anchored++
	}
	if anchored+unanchored > 0 {
		slog.Default().DebugContext(ctx, "gitlab_subject_projects_anchored", "anchored", anchored, "unanchored", unanchored)
	}
	return nil
}
