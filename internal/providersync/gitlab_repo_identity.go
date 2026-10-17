package providersync

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLabRepoIdentity is the repository identity of one GitLab project
// (CHAOS-3383, option A): RepoID is repos.id, and Path is the path it was
// minted from (repos.identity_path). Work-item and sprint ids carry Path, so a
// rename or transfer keeps every id of the project.
type GitLabRepoIdentity struct {
	RepoID string
	Path   string
}

// GitLabRepoIdentityReader reads the stored repository identities of an org's
// GitLab projects. A route without one mints every identity from the current
// path (a store that holds no project yet); every production route has one
// (the census in gitlab_repo_identity_census_test.go).
type GitLabRepoIdentityReader interface {
	// StoredRepoIdentities returns the GitLab repos rows that may decide the
	// identity of project projectID at currentPath: the rows that name the
	// project's numeric id, and the row whose id currentPath mints.
	StoredRepoIdentities(ctx context.Context, claim Claim, projectID int64, currentPath string) ([]GitLabStoredRepoIdentity, error)
	// StoredRepoIdentitiesAtPaths returns the GitLab repos rows whose current
	// path (repos.repo, case-insensitive) is one of paths.
	StoredRepoIdentitiesAtPaths(ctx context.Context, claim Claim, paths []string) ([]GitLabStoredRepoIdentity, error)
}

// GitLabProjectPath is a GitLab project named by its numeric id and its
// current path.
type GitLabProjectPath struct {
	ProjectID int64
	Path      string
}

// GitLabRepoIdentityBatchReader reads, in one query, the stored rows that may
// decide the identity of each of several projects: for every project the rows
// StoredRepoIdentities returns for it.
type GitLabRepoIdentityBatchReader interface {
	StoredRepoIdentitiesForProjects(ctx context.Context, orgID string, projects []GitLabProjectPath) ([]GitLabStoredRepoIdentity, error)
}

// resolveGitLabRepoIdentityPaths applies the identity rules of
// resolveGitLabRepoIdentity to each project over one batched read, and maps
// each project's numeric id to its minted path. A project it cannot decide is
// left out, and the caller keeps its current path.
func resolveGitLabRepoIdentityPaths(
	ctx context.Context, reader GitLabRepoIdentityBatchReader, orgID string, projects []GitLabProjectPath,
) (map[int64]string, error) {
	out := make(map[int64]string, len(projects))
	if reader == nil || len(projects) == 0 {
		return out, nil
	}
	stored, err := reader.StoredRepoIdentitiesForProjects(ctx, orgID, projects)
	if err != nil {
		return nil, err
	}
	for _, project := range projects {
		minted, mintErr := repositoryIdentity(project.Path)
		if mintErr != nil {
			continue
		}
		native := strconv.FormatInt(project.ProjectID, 10)
		relevant := make([]GitLabStoredRepoIdentity, 0, 2)
		for _, row := range stored {
			if row.heldBy() == native || row.ID == minted {
				relevant = append(relevant, row)
			}
		}
		identity, kind, decideErr := decideGitLabRepoIdentity(project.ProjectID, project.Path, relevant)
		if decideErr != nil {
			errorClass, errorType := logging.ErrorClass(decideErr), logging.ErrorType(decideErr) // never the error text (CHAOS-7933)
			slog.Default().WarnContext(ctx, "gitlab_repo_identity_batch_unresolved",
				"org_id", orgID, "gitlab_project_id", project.ProjectID, "stored_rows", len(relevant),
				"error_class", errorClass, "error_type", errorType)
			continue
		}
		slog.Default().DebugContext(ctx, "gitlab_repo_identity_batch",
			"org_id", orgID, "gitlab_project_id", project.ProjectID, "decision", kind)
		out[project.ProjectID] = identity.Path
	}
	return out, nil
}

// GitLabStoredRepoIdentity is the part of one repos FINAL row the identity
// rules read.
type GitLabStoredRepoIdentity struct {
	ID             string
	Repo           string
	IdentityPath   string
	ProviderRepoID string
	// SettingsProjectID is repos.settings.project_id: the only numeric id of a
	// row written before provider_repo_id (migration 134).
	SettingsProjectID int64
	LastSynced        time.Time
}

// heldBy is the numeric project id the row names, or "" when it names none.
func (row GitLabStoredRepoIdentity) heldBy() string {
	if row.ProviderRepoID != "" {
		return row.ProviderRepoID
	}
	if row.SettingsProjectID > 0 {
		return strconv.FormatInt(row.SettingsProjectID, 10)
	}
	return ""
}

// identity is the row's repository identity. A row written before
// identity_path (migration 138) was minted from its repo, which the id proves.
func (row GitLabStoredRepoIdentity) identity() (GitLabRepoIdentity, error) {
	if row.IdentityPath != "" {
		return GitLabRepoIdentity{RepoID: row.ID, Path: row.IdentityPath}, nil
	}
	minted, err := repositoryIdentity(row.Repo)
	if err != nil || minted != row.ID {
		return GitLabRepoIdentity{}, ErrRepositoryIdentityAmbiguous
	}
	return GitLabRepoIdentity{RepoID: row.ID, Path: row.Repo}, nil
}

// newestGitLabStoredRepoIdentity is the row last written; equal times take the
// smallest id, so the choice never depends on read order.
func newestGitLabStoredRepoIdentity(rows []GitLabStoredRepoIdentity) GitLabStoredRepoIdentity {
	sorted := append([]GitLabStoredRepoIdentity(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		if !sorted[i].LastSynced.Equal(sorted[j].LastSynced) {
			return sorted[i].LastSynced.After(sorted[j].LastSynced)
		}
		return sorted[i].ID < sorted[j].ID
	})
	return sorted[0]
}

const (
	gitLabRepoIdentityHeld     = "held"
	gitLabRepoIdentityMinted   = "minted"
	gitLabRepoIdentityTakeover = "takeover"
)

// decideGitLabRepoIdentity applies chris's option A (CHAOS-3383, 2026-10-15):
//   - a project the store holds keeps the identity of its newest row, whatever
//     its current path;
//   - a project the store does not hold mints from its current path;
//   - unless that id is held by another project (the path was reused), when it
//     mints from "{path}@{N}". GitLab paths cannot hold '@'.
func decideGitLabRepoIdentity(projectID int64, currentPath string, rows []GitLabStoredRepoIdentity) (GitLabRepoIdentity, string, error) {
	if projectID < 1 {
		return GitLabRepoIdentity{}, "", providerfoundation.ErrNormalizationInvalid
	}
	native := strconv.FormatInt(projectID, 10)
	held := make([]GitLabStoredRepoIdentity, 0, len(rows))
	for _, row := range rows {
		if row.heldBy() == native {
			held = append(held, row)
		}
	}
	if len(held) > 0 {
		newest := newestGitLabStoredRepoIdentity(held)
		identity, err := newest.identity()
		kind := gitLabRepoIdentityHeld + "_by_provider_repo_id"
		if newest.ProviderRepoID == "" {
			kind = gitLabRepoIdentityHeld + "_by_settings_project_id"
		}
		if newest.IdentityPath == "" {
			kind += "_path_from_repo"
		}
		return identity, kind, err
	}
	minted, err := repositoryIdentity(currentPath)
	if err != nil {
		return GitLabRepoIdentity{}, "", err
	}
	for _, row := range rows {
		if row.ID == minted && row.heldBy() != "" {
			path := strings.TrimSpace(currentPath) + "@" + native
			id, err := repositoryIdentity(path)
			if err != nil {
				return GitLabRepoIdentity{}, "", err
			}
			return GitLabRepoIdentity{RepoID: id, Path: path}, gitLabRepoIdentityTakeover, nil
		}
	}
	return GitLabRepoIdentity{RepoID: minted, Path: strings.TrimSpace(currentPath)}, gitLabRepoIdentityMinted, nil
}

// resolveGitLabRepoIdentity is the one way a GitLab route gets its repository
// identity.
func resolveGitLabRepoIdentity(
	ctx context.Context, reader GitLabRepoIdentityReader, claim Claim, projectID int64, currentPath string,
) (GitLabRepoIdentity, error) {
	var rows []GitLabStoredRepoIdentity
	if reader != nil {
		stored, err := reader.StoredRepoIdentities(ctx, claim, projectID, currentPath)
		if err != nil {
			return GitLabRepoIdentity{}, err
		}
		rows = stored
	}
	identity, kind, err := decideGitLabRepoIdentity(projectID, currentPath, rows)
	if err != nil {
		errorClass, errorType := logging.ErrorClass(err), logging.ErrorType(err) // never the error text (CHAOS-7933)
		slog.Default().WarnContext(ctx, "gitlab_repo_identity_unresolved",
			"org_id", claim.OrgID, "unit_id", claim.ID, "dataset", claim.Dataset,
			"gitlab_project_id", projectID, "stored_rows", len(rows),
			"error_class", errorClass, "error_type", errorType)
		return GitLabRepoIdentity{}, err
	}
	slog.Default().DebugContext(ctx, "gitlab_repo_identity",
		"org_id", claim.OrgID, "unit_id", claim.ID, "dataset", claim.Dataset,
		"gitlab_project_id", projectID, "decision", kind, "repo_id", identity.RepoID,
		"renamed", !strings.EqualFold(identity.Path, strings.TrimSpace(currentPath)),
		"stored_rows", len(rows), "reader", reader != nil)
	return identity, nil
}

// resolveGitLabRepoID is resolveGitLabRepoIdentity for a route that keys rows
// by repo_id only.
func resolveGitLabRepoID(
	ctx context.Context, reader GitLabRepoIdentityReader, claim Claim, projectID int64, currentPath string,
) (string, error) {
	identity, err := resolveGitLabRepoIdentity(ctx, reader, claim, projectID, currentPath)
	return identity.RepoID, err
}

// resolveGitLabIdentityPaths maps project paths a GitLab payload names (a
// reference to another project, in its current path) to the identity path of
// the project the store holds at that path; a path the store does not hold
// maps to itself. Keys are lower-case. own is the syncing project's identity
// at currentPath and is never looked up.
func resolveGitLabIdentityPaths(
	ctx context.Context, reader GitLabRepoIdentityReader, claim Claim,
	currentPath string, own GitLabRepoIdentity, paths []string,
) (map[string]string, error) {
	out := map[string]string{strings.ToLower(strings.TrimSpace(currentPath)): own.Path}
	lookup := make([]string, 0, len(paths))
	for _, path := range paths {
		key := strings.ToLower(strings.TrimSpace(path))
		if key == "" {
			continue
		}
		if _, known := out[key]; known {
			continue
		}
		out[key] = strings.TrimSpace(path)
		lookup = append(lookup, key)
	}
	if reader == nil || len(lookup) == 0 {
		return out, nil
	}
	rows, err := reader.StoredRepoIdentitiesAtPaths(ctx, claim, lookup)
	if err != nil {
		return nil, err
	}
	byPath := map[string][]GitLabStoredRepoIdentity{}
	for _, row := range rows {
		key := strings.ToLower(row.Repo)
		byPath[key] = append(byPath[key], row)
	}
	resolved, unresolvable := 0, 0
	for _, key := range lookup {
		held := byPath[key]
		if len(held) == 0 {
			continue
		}
		identity, err := newestGitLabStoredRepoIdentity(held).identity()
		if err != nil {
			unresolvable++
			continue
		}
		out[key] = identity.Path
		resolved++
	}
	slog.Default().DebugContext(ctx, "gitlab_reference_identity_paths",
		"org_id", claim.OrgID, "unit_id", claim.ID, "paths", len(lookup),
		"held", resolved, "unresolvable", unresolvable)
	return out, nil
}

// GitLabRepoIdentityClickHouseReader reads repos FINAL.
type GitLabRepoIdentityClickHouseReader struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

const gitLabStoredRepoIdentityColumns = `toString(id), repo, identity_path, provider_repo_id,
  JSONExtractInt(ifNull(settings, ''), 'project_id'), last_synced`

func (reader GitLabRepoIdentityClickHouseReader) StoredRepoIdentities(
	ctx context.Context, claim Claim, projectID int64, currentPath string,
) ([]GitLabStoredRepoIdentity, error) {
	if ctx == nil || reader.Conn == nil || reader.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "gitlab" || projectID < 1 {
		return nil, ErrInvalidConfiguration
	}
	minted, err := repositoryIdentity(currentPath)
	if err != nil {
		return nil, err
	}
	native := strconv.FormatInt(projectID, 10)
	return reader.query(ctx, `
SELECT `+gitLabStoredRepoIdentityColumns+`
FROM repos FINAL
WHERE org_id = ? AND provider = 'gitlab'
  AND (provider_repo_id = ? OR (provider_repo_id = '' AND JSONExtractInt(ifNull(settings, ''), 'project_id') = ?) OR id = toUUID(?))`,
		claim.OrgID, native, projectID, minted)
}

func (reader GitLabRepoIdentityClickHouseReader) StoredRepoIdentitiesForProjects(
	ctx context.Context, orgID string, projects []GitLabProjectPath,
) ([]GitLabStoredRepoIdentity, error) {
	if ctx == nil || reader.Conn == nil || reader.Lease == nil || orgID == "" {
		return nil, ErrInvalidConfiguration
	}
	natives := make([]string, 0, len(projects))
	ints := make([]int64, 0, len(projects))
	minted := make([]string, 0, len(projects))
	for _, project := range projects {
		id, err := repositoryIdentity(project.Path)
		if err != nil || project.ProjectID < 1 {
			continue
		}
		natives = append(natives, strconv.FormatInt(project.ProjectID, 10))
		ints = append(ints, project.ProjectID)
		minted = append(minted, id)
	}
	if len(natives) == 0 {
		return nil, nil
	}
	return reader.query(ctx, `
SELECT `+gitLabStoredRepoIdentityColumns+`
FROM repos FINAL
WHERE org_id = ? AND provider = 'gitlab'
  AND (provider_repo_id IN ? OR (provider_repo_id = '' AND JSONExtractInt(ifNull(settings, ''), 'project_id') IN ?) OR id IN (SELECT toUUID(arrayJoin(?))))`,
		orgID, natives, ints, minted)
}

func (reader GitLabRepoIdentityClickHouseReader) StoredRepoIdentitiesAtPaths(
	ctx context.Context, claim Claim, paths []string,
) ([]GitLabStoredRepoIdentity, error) {
	if ctx == nil || reader.Conn == nil || reader.Lease == nil || claim.Validate() != nil ||
		claim.Provider != "gitlab" {
		return nil, ErrInvalidConfiguration
	}
	if err := reader.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	return readGitLabStoredRepoIdentitiesAtPaths(ctx, reader.Conn, claim.OrgID, paths)
}

func (reader GitLabRepoIdentityClickHouseReader) query(ctx context.Context, query string, args ...any) ([]GitLabStoredRepoIdentity, error) {
	if err := reader.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	return queryGitLabStoredRepoIdentities(ctx, reader.Conn, query, args...)
}

// readGitLabStoredRepoIdentitiesAtPaths reads the org's GitLab repos rows
// whose current path (repo) or minted path (identity_path) is one of paths,
// case-insensitive. It is the one read of "which projects has the store seen
// at a path": the reference rewrite and the ownership carry both use it, and
// both name a row's project by heldBy.
func readGitLabStoredRepoIdentitiesAtPaths(
	ctx context.Context, conn driver.Conn, orgID string, paths []string,
) ([]GitLabStoredRepoIdentity, error) {
	lower := make([]string, 0, len(paths))
	for _, path := range paths {
		if key := strings.ToLower(strings.TrimSpace(path)); key != "" {
			lower = append(lower, key)
		}
	}
	if len(lower) == 0 {
		return nil, nil
	}
	return queryGitLabStoredRepoIdentities(ctx, conn, `
SELECT `+gitLabStoredRepoIdentityColumns+`
FROM repos FINAL
WHERE org_id = ? AND provider = 'gitlab' AND (lower(repo) IN ? OR lower(identity_path) IN ?)`,
		orgID, lower, lower)
}

func queryGitLabStoredRepoIdentities(ctx context.Context, conn driver.Conn, query string, args ...any) ([]GitLabStoredRepoIdentity, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]GitLabStoredRepoIdentity, 0)
	for rows.Next() {
		var row GitLabStoredRepoIdentity
		if err := rows.Scan(&row.ID, &row.Repo, &row.IdentityPath, &row.ProviderRepoID, &row.SettingsProjectID, &row.LastSynced); err != nil {
			return nil, err
		}
		if row.ID == "" {
			return nil, errors.Join(providerfoundation.ErrNormalizationInvalid, errors.New("repos row without an id"))
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

var _ GitLabRepoIdentityReader = GitLabRepoIdentityClickHouseReader{}
var _ GitLabRepoIdentityBatchReader = GitLabRepoIdentityClickHouseReader{}
