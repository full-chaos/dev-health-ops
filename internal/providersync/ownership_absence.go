package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
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

// githubLookupNotFound says GitHub itself answered 404 for the request: the
// status, and the body of GitHub's own error answer (a JSON object whose
// message is "Not Found"). A 404 with any other body is not GitHub's (a
// gateway or a proxy in front of the host) and proves nothing.
func githubLookupNotFound(err error) bool {
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorNotFound ||
		providerErr.StatusCode != http.StatusNotFound {
		return false
	}
	var answer struct {
		Message *string `json:"message"`
	}
	return json.Unmarshal([]byte(providerErr.Body), &answer) == nil && answer.Message != nil &&
		strings.TrimSpace(*answer.Message) == "Not Found"
}

// githubRepoGrantAbsence asks GET /orgs/{org}/teams/{slug}/repos/{owner}/{repo}
// ("check team permissions for a repository"): a 2xx answer says the team has
// the repository, and GitHub's own 404 says it does not. Any other answer
// proves nothing.
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
		if githubLookupNotFound(err) {
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

// gitlabAbsenceSearchPerPage is the page size of the one search request.
const gitlabAbsenceSearchPerPage = 100

// gitlabGroupProjectAbsence asks the listing's own endpoint for one project:
// GET /groups/:id/projects?search=<project path>. A project of the answer
// whose path_with_namespace is the fact's path is still a project of the
// group. An answer with no such project that is the whole answer says it is
// not. "The whole answer" is the paginator's own reading of GitLab's end
// signal (X-Next-Page sent once and empty, and no Link to a next page), so the
// two cannot disagree. Any other answer proves nothing.
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
			"/projects?per_page="+strconv.Itoa(gitlabAbsenceSearchPerPage)+"&search="+url.QueryEscape(name), nil)
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
	if providerfoundation.GitLabListEndedInOneResponse(response.Header, len(projects), gitlabAbsenceSearchPerPage) {
		return SnapshotAbsenceProven
	}
	return SnapshotAbsenceNotProven
}

// jiraProjectAbsence asks the project search itself for one project:
// GET /rest/api/3/project/search?id=<native id>&status=live&status=archived.
// The held set of the Jira ownership rows is the live projects AND the
// archived ones (an archived project keeps its open rows), so the question
// names both states: an answer for live projects only would call an archived
// project gone. An entry with the id says the project still exists, live or
// archived. An answer with no entry that the provider marks as the whole
// answer says it is neither. Any other answer proves nothing.
//
// The question uses the identifier the row was BUILT FROM. A row of today holds
// the project's native id and is asked for by `id`. A row of the retired form
// holds an id built from the project KEY ("{org}:jira:{KEY}", see
// jiraKeyBuiltProjectIDPrefix): the provider never gave that value, answers
// 400 for it as an id, and must not be sent the organization id. Such a row is
// asked for by `keys`, with the key alone.
type jiraProjectAbsence struct {
	client *providerfoundation.HTTPClient
	// orgID is the organization of the run: it tells a key-built id from a
	// native one.
	orgID string
}

// jiraProjectAbsenceQuestion is the search parameter and value that name the
// project of a row, and the same value as the provider's entry holds it.
func (prover jiraProjectAbsence) question(row OwnershipSnapshotRow) (parameter, value string, ok bool) {
	projectID := strings.TrimSpace(row.ProjectID.String())
	if projectID == "" {
		return "", "", false
	}
	if strings.TrimSpace(prover.orgID) != "" && jiraProjectIDIsKeyBuilt(prover.orgID, projectID) {
		key := jiraTeamID(strings.TrimPrefix(projectID, jiraKeyBuiltProjectIDPrefix(prover.orgID)))
		return "keys", key, key != ""
	}
	return "id", projectID, true
}

// jiraProjectAbsenceStatuses is every project state the held set admits.
var jiraProjectAbsenceStatuses = []string{"live", jiraTeamCatalogProjectStatusArchived}

func (prover jiraProjectAbsence) OwnershipAbsence(ctx context.Context, row OwnershipSnapshotRow) SnapshotAbsence {
	parameter, value, ok := prover.question(row)
	if prover.client == nil || !ok {
		return SnapshotAbsenceNotProven
	}
	query := url.Values{parameter: {value}, "maxResults": {"50"}, "status": jiraProjectAbsenceStatuses}
	var page jiraTeamCatalogProjectSearchPayload
	if err := jiraFetchObject(ctx, prover.client, http.MethodGet, "/rest/api/3/project/search?"+query.Encode(), nil, &page); err != nil {
		return SnapshotAbsenceNotProven
	}
	if len(page.ErrorMessages) > 0 {
		return SnapshotAbsenceNotProven
	}
	for _, entry := range page.Values {
		held := strings.TrimSpace(entry.ID)
		if parameter == "keys" {
			held = jiraTeamID(entry.Key)
		}
		if held == value {
			return SnapshotFactStillHeld
		}
	}
	whole := (page.IsLast != nil && *page.IsLast) || (page.Total != nil && *page.Total == 0)
	if len(page.Values) == 0 && whole {
		return SnapshotAbsenceProven
	}
	return SnapshotAbsenceNotProven
}
