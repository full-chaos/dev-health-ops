package teamattribution

import (
	"context"
	"slices"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// resolveGitHubRepoNameFacts gives each name-keyed GitHub grant (repo_id NULL)
// the id of the repository its name names in the same organization's repos
// rows (ResolveGitHubRepoNameFacts).
func (source ClickHouseFactSource) resolveGitHubRepoNameFacts(
	ctx context.Context, orgID string, facts []GithubWorkItemDerivationRepoFact,
) ([]GithubWorkItemDerivationRepoFact, error) {
	var names []string
	seen := map[string]struct{}{}
	for _, fact := range facts {
		if !isNameKeyedGitHubRepoFact(fact) {
			continue
		}
		name := strings.ToLower(strings.TrimSpace(fact.RepoFullName))
		if _, dup := seen[name]; !dup {
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return facts, nil
	}
	holders, err := LoadGitHubRepoIDsByName(ctx, source.Conn, orgID, names)
	if err != nil {
		return nil, err
	}
	return ResolveGitHubRepoNameFacts(facts, holders), nil
}

// ResolveGitHubRepoNameFacts is the pure part of resolveGitHubRepoNameFacts.
// holders maps a lower-cased repository name to the ids of the organization's
// GitHub repos rows of that name. The whole name must be equal (case aside, as
// the ownership window joins repos); a name two repos rows hold resolves to
// neither, and a name no row holds resolves to nothing.
func ResolveGitHubRepoNameFacts(
	facts []GithubWorkItemDerivationRepoFact, holders map[string][]string,
) []GithubWorkItemDerivationRepoFact {
	out := make([]GithubWorkItemDerivationRepoFact, 0, len(facts))
	for _, fact := range facts {
		if isNameKeyedGitHubRepoFact(fact) {
			if ids := holders[strings.ToLower(strings.TrimSpace(fact.RepoFullName))]; len(ids) == 1 {
				fact.LinkedRepoID = ids[0]
			}
		}
		out = append(out, fact)
	}
	return out
}

func isNameKeyedGitHubRepoFact(fact GithubWorkItemDerivationRepoFact) bool {
	return strings.TrimSpace(fact.Provider) == "github" &&
		(fact.RepoID == nil || strings.TrimSpace(*fact.RepoID) == "") &&
		strings.TrimSpace(fact.RepoFullName) != ""
}

// LoadGitHubRepoIDsByName maps each lower-cased name of names to the ids of the
// organization's GitHub repos rows whose current name it is.
func LoadGitHubRepoIDsByName(
	ctx context.Context, conn driver.Conn, orgID string, names []string,
) (map[string][]string, error) {
	out := make(map[string][]string, len(names))
	for start := 0; start < len(names); start += gitLabNativeProjectChunk {
		end := min(start+gitLabNativeProjectChunk, len(names))
		rows, err := conn.Query(ctx, `
SELECT lower(repo), toString(id)
FROM repos FINAL
WHERE org_id = ? AND provider = 'github' AND lower(repo) IN ?`,
			orgID, names[start:end])
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var name, id string
			if err := rows.Scan(&name, &id); err != nil {
				_ = rows.Close()
				return nil, err
			}
			if !slices.Contains(out[name], id) {
				out[name] = append(out[name], id)
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
