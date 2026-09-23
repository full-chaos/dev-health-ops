package teamsidentity

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLab discovery pagination bounds, transcribed verbatim from
// team_discovery.py:30-39 (MAX_GITLAB_DISCOVERY_SUBGROUPS/_PROJECTS/
// _ALL_PROJECTS).
const (
	maxGitLabDiscoverySubgroups   = 500
	maxGitLabDiscoveryProjects    = 500
	maxGitLabDiscoveryAllProjects = 5000
)

type gitlabGroupEntry struct {
	ID          int64  `json:"id"`
	FullPath    string `json:"full_path"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type gitlabProjectEntry struct {
	ID                int64  `json:"id"`
	PathWithNamespace string `json:"path_with_namespace"`
}

// discoverGitLab mirrors TeamDiscoveryService.discover_gitlab
// (team_discovery.py:192-297): the root group plus its DIRECT subgroups
// only (each carrying its own project listing as repo_patterns) -- a
// SEPARATE, flatter include_subgroups=true walk against the root group
// ALSO runs (team_discovery.py:268-288) but its own project list is never
// read here (TeamDiscoverResponse has no `projects` field; only
// GitLabDiscoveryResult, a different caller's return type, carries it).
// It is still executed and still counts toward truncated/warnings: both
// walks append to the SAME shared warnings list team_discovery.py builds
// (`_bounded_list(..., warnings=warnings)` for every one of the three
// bounded reads), and `truncated=bool(warnings)` is computed from that
// combined list -- so a truncation of the flat walk IS observable on this
// route's response even though its own item list is discarded, and
// running it is required for parity, not merely for a caller this route
// does not have.
func discoverGitLab(ctx context.Context, credential providerfoundation.Credential, groupPath string) (teams []discoveredTeam, truncated bool, warnings []string, err error) {
	client, err := providerfoundation.NewGitLabClient(credential, discoveryHTTPClient, providerfoundation.DefaultRetryPolicy(), alwaysValidLease{})
	if err != nil {
		return nil, false, nil, err
	}
	rootGroup, err := gitlabGetGroup(ctx, client, groupPath)
	if err != nil {
		return nil, false, nil, err
	}
	subgroups, subTruncated, err := gitlabListGroupPages(ctx, client, "/api/v4/groups/"+url.PathEscape(groupPath)+"/subgroups", maxGitLabDiscoverySubgroups)
	if err != nil {
		return nil, false, nil, err
	}
	if subTruncated {
		warnings = append(warnings, gitlabTruncationWarning("subgroups", groupPath, maxGitLabDiscoverySubgroups))
	}

	groups := append([]gitlabGroupEntry{rootGroup}, subgroups...)
	for _, group := range groups {
		projects, groupTruncated, err := gitlabListProjectPages(ctx, client, "/api/v4/groups/"+strconv.FormatInt(group.ID, 10)+"/projects", maxGitLabDiscoveryProjects, false)
		if err != nil {
			return nil, false, nil, err
		}
		if groupTruncated {
			warnings = append(warnings, gitlabTruncationWarning("projects", group.FullPath, maxGitLabDiscoveryProjects))
		}
		repoPatterns := make([]string, len(projects))
		for index, project := range projects {
			repoPatterns[index] = project.PathWithNamespace
		}
		var description *string
		if group.Description != "" {
			description = &group.Description
		}
		associations := pyjson.NewObject()
		associations.Set("repo_patterns", repoPatterns)
		associations.Set("provider_org", rootGroup.FullPath)
		teams = append(teams, discoveredTeam{
			ProviderType:   "gitlab",
			ProviderTeamID: group.FullPath,
			Name:           group.Name,
			Description:    description,
			Associations:   associations,
		})
	}

	// The flat, include_subgroups=true walk against the root group: its
	// items are never surfaced by this route (see the doc comment above),
	// only its truncation status.
	_, allTruncated, err := gitlabListProjectPages(ctx, client, "/api/v4/groups/"+strconv.FormatInt(rootGroup.ID, 10)+"/projects", maxGitLabDiscoveryAllProjects, true)
	if err != nil {
		return nil, false, nil, err
	}
	if allTruncated {
		warnings = append(warnings, gitlabTruncationWarning("projects (including all subgroups)", groupPath, maxGitLabDiscoveryAllProjects))
	}

	return teams, len(warnings) > 0, warnings, nil
}

func gitlabTruncationWarning(what, scope string, limit int) string {
	return fmt.Sprintf("GitLab team discovery truncated %s for '%s' at %d results; the import may be incomplete.", what, scope, limit)
}

func gitlabGetGroup(ctx context.Context, client *providerfoundation.HTTPClient, groupPath string) (gitlabGroupEntry, error) {
	response, err := client.Do(ctx, "GET", "/api/v4/groups/"+url.PathEscape(groupPath), nil)
	if err != nil {
		return gitlabGroupEntry{}, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return gitlabGroupEntry{}, err
	}
	var group gitlabGroupEntry
	if err := json.Unmarshal(raw, &group); err != nil {
		return gitlabGroupEntry{}, fmt.Errorf("decode gitlab group response: %w", err)
	}
	return group, nil
}

// gitlabListGroupPages walks GitLab's offset pagination (X-Next-Page
// response header) up to limit items, matching python-gitlab's own
// `iterator=True` page walk plus team_discovery.py's own
// `itertools.islice(..., limit + 1)` truncation check (_bounded_list,
// team_discovery.py:42-66): truncated is true when MORE than limit items
// were available, never merely when exactly limit were returned.
func gitlabListGroupPages(ctx context.Context, client *providerfoundation.HTTPClient, path string, limit int) ([]gitlabGroupEntry, bool, error) {
	var out []gitlabGroupEntry
	page := 1
	for {
		response, err := client.Do(ctx, "GET", fmt.Sprintf("%s?per_page=100&page=%d", path, page), nil)
		if err != nil {
			return nil, false, err
		}
		raw, readErr := io.ReadAll(response.Body)
		nextPage := response.Header.Get("X-Next-Page")
		response.Body.Close()
		if readErr != nil {
			return nil, false, readErr
		}
		var entries []gitlabGroupEntry
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, false, fmt.Errorf("decode gitlab group list page for %s: %w", path, err)
		}
		out = append(out, entries...)
		if len(out) > limit {
			return out[:limit], true, nil
		}
		if nextPage == "" {
			break
		}
		next, err := strconv.Atoi(nextPage)
		if err != nil {
			break
		}
		page = next
	}
	return out, false, nil
}

func gitlabListProjectPages(ctx context.Context, client *providerfoundation.HTTPClient, path string, limit int, includeSubgroups bool) ([]gitlabProjectEntry, bool, error) {
	var out []gitlabProjectEntry
	page := 1
	for {
		query := fmt.Sprintf("%s?per_page=100&page=%d", path, page)
		if includeSubgroups {
			query += "&include_subgroups=true"
		}
		response, err := client.Do(ctx, "GET", query, nil)
		if err != nil {
			return nil, false, err
		}
		raw, readErr := io.ReadAll(response.Body)
		nextPage := response.Header.Get("X-Next-Page")
		response.Body.Close()
		if readErr != nil {
			return nil, false, readErr
		}
		var entries []gitlabProjectEntry
		if err := json.Unmarshal(raw, &entries); err != nil {
			return nil, false, fmt.Errorf("decode gitlab project list page for %s: %w", path, err)
		}
		out = append(out, entries...)
		if len(out) > limit {
			return out[:limit], true, nil
		}
		if nextPage == "" {
			break
		}
		next, err := strconv.Atoi(nextPage)
		if err != nil {
			break
		}
		page = next
	}
	return out, false, nil
}
