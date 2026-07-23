package providersync

import (
	"context"
	"encoding/json"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const maximumLaunchDarklyReferenceRows = 100_000

type LaunchDarklyClickHouseReferences struct {
	Conn  driver.Conn
	Lease providerfoundation.LeaseGuard
}

type launchDarklyCodeReference struct {
	FlagKey        string
	ProjectKey     string
	RepoName       string
	RepoSourceLink string
	BranchName     string
	FilePath       string
	StartingLine   int
}

func (resolver LaunchDarklyClickHouseReferences) ResolveLaunchDarklyCodeReferences(
	ctx context.Context,
	claim Claim,
	_ string,
	payload json.RawMessage,
	normalizedAt time.Time,
) ([]launchDarklyLinkRow, []launchDarklyEdgeRow, error) {
	if ctx == nil || resolver.Conn == nil || resolver.Lease == nil ||
		claim.Validate() != nil || normalizedAt.IsZero() {
		return nil, nil, ErrInvalidConfiguration
	}
	if err := resolver.Lease.Assert(ctx); err != nil {
		return nil, nil, err
	}
	references, err := parseLaunchDarklyCodeReferences(payload)
	if err != nil {
		return nil, nil, err
	}
	repoIndex, err := resolver.loadRepoIndex(ctx, claim.OrgID)
	if err != nil {
		return nil, nil, err
	}
	repoPaths := map[string]map[string]bool{}
	resolvedRepo := make(map[int]string, len(references))
	for index, reference := range references {
		repoID := resolveLaunchDarklyRepo(reference, repoIndex)
		if repoID == "" {
			continue
		}
		resolvedRepo[index] = repoID
		if repoPaths[repoID] == nil {
			repoPaths[repoID] = map[string]bool{}
		}
		repoPaths[repoID][reference.FilePath] = true
	}
	prIDs, err := resolver.loadPRIDs(ctx, claim.OrgID, repoPaths)
	if err != nil {
		return nil, nil, err
	}
	links := []launchDarklyLinkRow{}
	edges := []launchDarklyEdgeRow{}
	seenLinks := map[string]bool{}
	seenEdges := map[string]bool{}
	for index, reference := range references {
		flagID := launchDarklyFeatureFlagID(
			claim.OrgID, "launchdarkly",
			reference.ProjectKey, reference.FlagKey,
		)
		repoID := resolvedRepo[index]
		fileTarget := reference.RepoName + ":" + reference.FilePath
		if repoID != "" {
			fileTarget = repoID + ":" + reference.FilePath
		}
		linkKey := strings.Join(
			[]string{reference.FlagKey, "file", fileTarget}, "\x00",
		)
		if !seenLinks[linkKey] {
			seenLinks[linkKey] = true
			links = append(links, launchDarklyReferenceLink(
				claim.OrgID, reference.FlagKey, "file", fileTarget, normalizedAt,
			))
		}
		evidence := "ld_code_ref:" + reference.RepoName + ":" +
			reference.BranchName + ":" + reference.FilePath + ":L" +
			strconv.Itoa(reference.StartingLine)
		if repoID != "" {
			edge := newLaunchDarklyEdge(
				claim.OrgID, flagID, "feature_flag", fileTarget, "file",
				"guards", repoID, "launchdarkly",
				launchDarklyCodeReferenceConfidence, evidence,
				normalizedAt, normalizedAt,
			)
			if !seenEdges[edge.EdgeID] {
				seenEdges[edge.EdgeID] = true
				edges = append(edges, edge)
			}
		}
		for _, prID := range prIDs[repoID+"\x00"+reference.FilePath] {
			linkKey = strings.Join(
				[]string{reference.FlagKey, "pr", prID}, "\x00",
			)
			if !seenLinks[linkKey] {
				seenLinks[linkKey] = true
				links = append(links, launchDarklyReferenceLink(
					claim.OrgID, reference.FlagKey, "pr", prID, normalizedAt,
				))
			}
			edge := newLaunchDarklyEdge(
				claim.OrgID, flagID, "feature_flag", prID, "pr",
				"guards", repoID, "launchdarkly",
				launchDarklyCodeReferenceConfidence, evidence,
				normalizedAt, normalizedAt,
			)
			if !seenEdges[edge.EdgeID] {
				seenEdges[edge.EdgeID] = true
				edges = append(edges, edge)
			}
		}
	}
	return links, edges, nil
}

func (resolver LaunchDarklyClickHouseReferences) loadRepoIndex(
	ctx context.Context,
	orgID string,
) (map[string]string, error) {
	rows, err := resolver.Conn.Query(ctx, `
SELECT toString(id), repo
FROM repos
WHERE org_id = ?
LIMIT ?`, orgID, maximumLaunchDarklyReferenceRows+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	index := map[string]string{}
	count := 0
	for rows.Next() {
		var repoID, name string
		if err := rows.Scan(&repoID, &name); err != nil {
			return nil, err
		}
		count++
		if count > maximumLaunchDarklyReferenceRows {
			return nil, ErrEffectRecoveryUnsafe
		}
		if _, err := uuid.Parse(repoID); err != nil {
			return nil, providerfoundation.ErrNormalizationInvalid
		}
		name = strings.Trim(strings.TrimSpace(name), "/")
		for _, key := range []string{name, lastPathPart(name)} {
			key = strings.ToLower(strings.Trim(strings.TrimSpace(key), "/"))
			if key != "" {
				if _, exists := index[key]; !exists {
					index[key] = repoID
				}
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return index, nil
}

func (resolver LaunchDarklyClickHouseReferences) loadPRIDs(
	ctx context.Context,
	orgID string,
	repoPaths map[string]map[string]bool,
) (map[string][]string, error) {
	if len(repoPaths) == 0 {
		return map[string][]string{}, nil
	}
	repoIDs := make([]string, 0, len(repoPaths))
	pathSet := map[string]bool{}
	for repoID, paths := range repoPaths {
		repoIDs = append(repoIDs, repoID)
		for path := range paths {
			pathSet[path] = true
		}
	}
	paths := make([]string, 0, len(pathSet))
	for path := range pathSet {
		paths = append(paths, path)
	}
	sort.Strings(repoIDs)
	sort.Strings(paths)
	if err := resolver.Lease.Assert(ctx); err != nil {
		return nil, err
	}
	rows, err := resolver.Conn.Query(ctx, `
SELECT DISTINCT
    toString(p.repo_id),
    p.pr_number,
    s.file_path
FROM work_graph_pr_commit AS p
INNER JOIN git_commit_stats AS s ON (
    toString(p.repo_id) = toString(s.repo_id)
    AND p.commit_hash = s.commit_hash
    AND toString(p.org_id) = toString(s.org_id)
)
WHERE p.org_id = ?
  AND has(?, toString(p.repo_id))
  AND has(?, s.file_path)
LIMIT ?`, orgID, repoIDs, paths, maximumLaunchDarklyReferenceRows+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	matches := map[string][]string{}
	count := 0
	for rows.Next() {
		var repoID, filePath string
		var prNumber int64
		if err := rows.Scan(&repoID, &prNumber, &filePath); err != nil {
			return nil, err
		}
		count++
		if count > maximumLaunchDarklyReferenceRows {
			return nil, ErrEffectRecoveryUnsafe
		}
		key := repoID + "\x00" + filePath
		matches[key] = append(
			matches[key], repoID+"#pr"+strconv.FormatInt(prNumber, 10),
		)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for key := range matches {
		sort.Strings(matches[key])
	}
	return matches, nil
}

func parseLaunchDarklyCodeReferences(
	payload json.RawMessage,
) ([]launchDarklyCodeReference, error) {
	var document struct {
		Items []map[string]any `json:"items"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.UseNumber()
	if decoder.Decode(&document) != nil {
		return nil, providerfoundation.ErrNormalizationInvalid
	}
	references := []launchDarklyCodeReference{}
	for _, repo := range document.Items {
		repoName := strings.TrimSpace(stringValue(repo["name"]))
		if repoName == "" {
			continue
		}
		sourceLink := stringValue(repo["sourceLink"])
		defaultBranch := stringValue(repo["defaultBranch"])
		branches, _ := repo["branches"].([]any)
		for _, rawBranch := range branches {
			branch, ok := rawBranch.(map[string]any)
			if !ok {
				continue
			}
			branchName := valueOr(stringValue(branch["name"]), defaultBranch)
			rawReferences, _ := branch["references"].([]any)
			for _, rawReference := range rawReferences {
				reference, ok := rawReference.(map[string]any)
				if !ok {
					continue
				}
				path := normalizeLaunchDarklyPath(
					stringValue(reference["path"]), branchName,
				)
				if path == "" {
					continue
				}
				hunks, _ := reference["hunks"].([]any)
				for _, rawHunk := range hunks {
					hunk, ok := rawHunk.(map[string]any)
					if !ok {
						continue
					}
					flagKey := strings.TrimSpace(stringValue(hunk["flagKey"]))
					projectKey := strings.TrimSpace(stringValue(hunk["projKey"]))
					if flagKey == "" || projectKey == "" {
						continue
					}
					startingLine, _ := strconv.Atoi(stringValue(
						hunk["startingLineNumber"],
					))
					references = append(references, launchDarklyCodeReference{
						FlagKey: flagKey, ProjectKey: projectKey,
						RepoName: repoName, RepoSourceLink: sourceLink,
						BranchName: branchName, FilePath: path,
						StartingLine: startingLine,
					})
				}
			}
		}
	}
	return references, nil
}

func normalizeLaunchDarklyPath(path, branch string) string {
	path = strings.TrimPrefix(strings.TrimSpace(path), "/")
	prefix := strings.Trim(strings.TrimSpace(branch), "/")
	if prefix != "" && strings.HasPrefix(path, prefix+"/") {
		path = strings.TrimPrefix(path, prefix+"/")
	}
	return path
}

func resolveLaunchDarklyRepo(
	reference launchDarklyCodeReference,
	index map[string]string,
) string {
	var keys []string
	if reference.RepoSourceLink != "" {
		if parsed, err := url.Parse(reference.RepoSourceLink); err == nil {
			path := strings.Trim(strings.TrimSpace(parsed.Path), "/")
			path = strings.TrimSuffix(path, ".git")
			keys = append(keys, path, lastPathPart(path))
		}
	}
	keys = append(keys, reference.RepoName)
	seen := map[string]bool{}
	for _, key := range keys {
		key = strings.ToLower(strings.Trim(strings.TrimSpace(key), "/"))
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		if repoID := index[key]; repoID != "" {
			return repoID
		}
	}
	return ""
}

func lastPathPart(value string) string {
	if marker := strings.LastIndex(value, "/"); marker >= 0 {
		return value[marker+1:]
	}
	return value
}

func launchDarklyReferenceLink(
	orgID, flagKey, targetType, targetID string,
	normalizedAt time.Time,
) launchDarklyLinkRow {
	return launchDarklyLinkRow{
		OrgID: orgID, FlagKey: flagKey, TargetType: targetType,
		TargetID: targetID, Provider: "launchdarkly", LinkSource: "native",
		LinkType: "code_reference", EvidenceType: "ld_code_ref",
		Confidence: launchDarklyCodeReferenceConfidence,
		ValidFrom:  normalizedAt.UTC(), LastSynced: normalizedAt.UTC(),
	}
}

var _ LaunchDarklyCodeReferenceResolver = LaunchDarklyClickHouseReferences{}
