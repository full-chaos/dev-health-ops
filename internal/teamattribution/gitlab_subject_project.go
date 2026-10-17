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
	if gitLabSubjectNamedByKey(subject) {
		add(*subject.ProjectKey)
	} else if subject.ProjectID != nil && !IsGitLabCatalogProjectID(*subject.ProjectID) {
		add(*subject.ProjectID)
	}
	return out
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

const gitLabNativeProjectChunk = 5000

// AnchorGitLabSubjectProjects reads each GitLab subject's repos row and sets
// the paths it answers to (GitLabPaths). A subject written before the
// numeric-id anchoring (project_id holds the path) also gets the project it
// belongs to: project_id becomes the catalog id of the project its repo_id
// names, and the old path moves to project_key so the work scope is unchanged.
// A subject whose repo names no project keeps its path and so joins no project
// ownership (fail-safe). A subject that already holds a catalog id keeps it.
func AnchorGitLabSubjectProjects(
	ctx context.Context, conn driver.Conn, subjects map[string]GithubWorkItemDerivationSubject,
) error {
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
	for orgID, set := range repoIDsByOrg {
		ids := make([]string, 0, len(set))
		for id := range set {
			ids = append(ids, id)
		}
		rows, err := LoadGitLabRepoRows(ctx, conn, orgID, ids)
		if err != nil {
			return err
		}
		rowsByOrg[orgID] = rows
	}
	anchored, unanchored := 0, 0
	for id, subject := range subjects {
		if subject.Provider != "gitlab" || subject.RepoID == nil {
			continue
		}
		row, ok := rowsByOrg[subject.OrgID][*subject.RepoID]
		if !ok {
			unanchored++
			continue
		}
		subject.GitLabPaths = nil
		for _, path := range []string{row.Repo, row.IdentityPath} {
			if path != "" {
				subject.GitLabPaths = append(subject.GitLabPaths, path)
			}
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
	if anchored+unanchored > 0 {
		slog.Default().DebugContext(ctx, "gitlab_subject_projects_anchored", "anchored", anchored, "unanchored", unanchored)
	}
	return nil
}
