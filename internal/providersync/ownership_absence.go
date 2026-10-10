package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

// The direct answers that prove an ownership fact is gone (OwnershipAbsenceProver).
// A list read by position (a page number, an offset) cannot prove it: when a
// grant is removed between two page requests the later grants move one place
// up, and one grant that still holds is on no page. Only the provider's own
// answer for that one link says so. Each answer here is ONE response.

// maxOwnershipLookupBody bounds the body of one lookup answer.
const maxOwnershipLookupBody = 1 << 20

// ownershipLookupNotFound says the provider answered 404 for the request.
func ownershipLookupNotFound(err error) bool {
	var providerErr *providerfoundation.ProviderError
	return errors.As(err, &providerErr) && providerErr.Class == providerfoundation.ErrorNotFound &&
		providerErr.StatusCode == http.StatusNotFound
}

// githubRepoGrantAbsence asks GET /orgs/{org}/teams/{slug}/repos/{owner}/{repo}
// ("check team permissions for a repository"): a 2xx answer says the team has
// the repository, and 404 says it does not. Any other answer proves nothing.
type githubRepoGrantAbsence struct {
	client *providerfoundation.HTTPClient
	org    string
}

func (prover githubRepoGrantAbsence) OwnershipAbsence(ctx context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
	slug, okTeam := strings.CutPrefix(row.TeamID, teamid.Prefix(githubTeamCatalogProvider))
	owner, repo, okRepo := strings.Cut(row.ProjectID.String(), "/")
	if !okTeam || !okRepo || prover.client == nil || strings.TrimSpace(prover.org) == "" ||
		strings.TrimSpace(slug) == "" || strings.TrimSpace(owner) == "" || strings.TrimSpace(repo) == "" || strings.Contains(repo, "/") {
		return SnapshotAbsenceNotProven
	}
	response, err := prover.client.Do(ctx, http.MethodGet,
		"/orgs/"+url.PathEscape(prover.org)+"/teams/"+url.PathEscape(slug)+"/repos/"+url.PathEscape(owner)+"/"+url.PathEscape(repo), nil)
	if err != nil {
		if ownershipLookupNotFound(err) {
			return SnapshotAbsenceProven
		}
		return SnapshotAbsenceNotProven
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxOwnershipLookupBody))
	if response.StatusCode == http.StatusNoContent || response.StatusCode == http.StatusOK {
		return SnapshotFactStillHeld
	}
	return SnapshotAbsenceNotProven
}

// gitlabGroupProjectAbsence asks the listing's own endpoint for one project:
// GET /groups/:id/projects?search=<project path>. A project of the answer
// whose path_with_namespace is the fact's path is still a project of the
// group. An answer with no such project that is the whole answer (GitLab's
// own end signal, X-Next-Page sent and empty) says it is not. Any other
// answer proves nothing.
type gitlabGroupProjectAbsence struct {
	client *providerfoundation.HTTPClient
}

func (prover gitlabGroupProjectAbsence) OwnershipAbsence(ctx context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
	groupPath, okTeam := strings.CutPrefix(row.TeamID, teamid.Prefix(gitlabTeamCatalogProvider))
	projectPath := strings.TrimSpace(row.ProjectID.String())
	if !okTeam || prover.client == nil || strings.TrimSpace(groupPath) == "" || projectPath == "" {
		return SnapshotAbsenceNotProven
	}
	name := projectPath
	if cut := strings.LastIndex(projectPath, "/"); cut >= 0 {
		name = projectPath[cut+1:]
	}
	if name == "" {
		return SnapshotAbsenceNotProven
	}
	response, err := prover.client.Do(ctx, http.MethodGet,
		providerRelativePath(prover.client, "api", "v4", "groups", strings.TrimSpace(groupPath))+
			"/projects?per_page=100&search="+url.QueryEscape(name), nil)
	if err != nil {
		return SnapshotAbsenceNotProven
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxOwnershipLookupBody+1))
	if err != nil || len(body) > maxOwnershipLookupBody {
		return SnapshotAbsenceNotProven
	}
	var projects []gitlabTeamCatalogProjectPayload
	if json.Unmarshal(body, &projects) != nil || projects == nil {
		return SnapshotAbsenceNotProven
	}
	for _, project := range projects {
		if strings.TrimSpace(project.PathWithNamespace) == projectPath {
			return SnapshotFactStillHeld
		}
	}
	values, sent := response.Header["X-Next-Page"]
	if sent && len(values) == 1 && strings.TrimSpace(values[0]) == "" {
		return SnapshotAbsenceProven
	}
	return SnapshotAbsenceNotProven
}

// jiraLiveProjectAbsence asks the project search itself for one project:
// GET /rest/api/3/project/search?id=<native id>, the same live-project
// answer the catalog's walk reads. An entry with the id says the project is
// live. An answer with no entry that the provider marks as the whole answer
// says it is not. Any other answer proves nothing.
type jiraLiveProjectAbsence struct {
	client *providerfoundation.HTTPClient
}

func (prover jiraLiveProjectAbsence) OwnershipAbsence(ctx context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
	nativeID := strings.TrimSpace(row.ProjectID.String())
	if prover.client == nil || nativeID == "" {
		return SnapshotAbsenceNotProven
	}
	query := url.Values{"id": {nativeID}, "maxResults": {"50"}}
	var page jiraTeamCatalogProjectSearchPayload
	if err := jiraFetchObject(ctx, prover.client, http.MethodGet, "/rest/api/3/project/search?"+query.Encode(), nil, &page); err != nil {
		return SnapshotAbsenceNotProven
	}
	if len(page.ErrorMessages) > 0 {
		return SnapshotAbsenceNotProven
	}
	for _, entry := range page.Values {
		if strings.TrimSpace(entry.ID) == nativeID {
			return SnapshotFactStillHeld
		}
	}
	whole := (page.IsLast != nil && *page.IsLast) || (page.Total != nil && *page.Total == 0)
	if len(page.Values) == 0 && whole {
		return SnapshotAbsenceProven
	}
	return SnapshotAbsenceNotProven
}
