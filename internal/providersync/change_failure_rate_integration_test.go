//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// changeFailureFixture writes provider rows through the real producers (the
// GitLab, PagerDuty and Jira incident routes and the PagerDuty services route
// with their ClickHouse sinks, the GitHub and GitLab deployment sinks) into a
// ClickHouse migrated by the real chain, then
// runs the real repo_user_commit daily executor, which owns the change-failure
// counts (CHAOS-8981).
type changeFailureFixture struct {
	t     *testing.T
	ctx   context.Context
	conn  driver.Conn
	lease providerfoundation.LeaseGuard
}

func (f changeFailureFixture) claim(org, provider, dataset string) Claim {
	claim := nativeTestClaim(provider, dataset)
	claim.OrgID = org
	return claim
}

// repo stores the repository row the incident loader joins on, under the id the
// GitLab producers derive from its full name.
func (f changeFailureFixture) repo(org, fullName string) uuid.UUID {
	f.t.Helper()
	text, err := repositoryIdentity(fullName)
	if err != nil {
		f.t.Fatal(err)
	}
	id := uuid.MustParse(text)
	if err := f.conn.Exec(f.ctx, fmt.Sprintf(
		"INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('%s', '%s', now64(3), now64(3), '%s', 'gitlab')",
		id, fullName, org)); err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f changeFailureFixture) deploy(org, provider string, repo uuid.UUID, deploymentID string, at time.Time) {
	f.t.Helper()
	claim := f.claim(org, provider, "deployments")
	deployedAt := at
	row := deploymentRow{
		OrgID: org, RepoID: repo.String(), DeploymentID: deploymentID, DeployedAt: &deployedAt,
		ReleaseRef: "v1", ReleaseRefConfidence: 1, LastSynced: at.Add(time.Hour),
	}
	effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, []deploymentRow{row})
	if err != nil {
		f.t.Fatal(err)
	}
	switch provider {
	case "github":
		err = GitHubDeploymentsClickHouseEffects{Conn: f.conn, Lease: f.lease}.WriteEffect(f.ctx, claim, effect)
	case "gitlab":
		err = GitLabDeploymentsClickHouseEffects{Conn: f.conn, Lease: f.lease}.WriteEffect(f.ctx, claim, effect)
	default:
		f.t.Fatalf("no deployment producer for %s", provider)
	}
	if err != nil {
		f.t.Fatalf("write %s deployment %s: %v", provider, deploymentID, err)
	}
}

// gitLabIncident runs the GitLab incidents route for one project and one
// incident and writes its three effects (service, service-to-repository
// mapping, incident): the incident ties to the repository directly.
func (f changeFailureFixture) gitLabIncident(org, fullName string, issueID int, createdAt time.Time) {
	f.t.Helper()
	project := fmt.Sprintf(`{"id":123,"name":"api","path_with_namespace":%q,"web_url":"https://gitlab.example/%s","default_branch":"main","archived":false,"last_activity_at":"2026-07-20T10:00:00Z"}`, fullName, fullName)
	issues := fmt.Sprintf(`[{"id":%d,"iid":%d,"issue_type":"incident","state":"opened","title":"API unavailable","created_at":%q,"updated_at":%q,"severity":"sev1"}]`,
		issueID, issueID, createdAt.UTC().Format(time.RFC3339), createdAt.Add(time.Minute).UTC().Format(time.RFC3339))
	doer := &gitLabCommitsDoer{t: f.t, responses: []gitLabCommitsResponse{{body: project}, {body: issues}}}
	claim := f.claim(org, "gitlab", "incidents")
	batch, err := (GitLabIncidentsRouteHandler{PerPage: 2}).Collect(
		f.ctx, claim, providerfoundation.Credential{},
		gitLabRepositoryClient(f.t, fakehttp.Client(doer), "https://gitlab.example"),
		createdAt.Add(2*time.Hour),
	)
	if err != nil {
		f.t.Fatalf("gitlab incidents route: %v", err)
	}
	sink := GitLabIncidentsClickHouseEffects{Conn: f.conn, Lease: f.lease}
	for _, effect := range batch.Effects {
		if err := sink.WriteEffect(f.ctx, claim, effect); err != nil {
			f.t.Fatalf("write gitlab %s: %v", effect.Destination, err)
		}
	}
}

// pagerDutyServices runs the PagerDuty services route for the given services
// (service id -> full name of the repository its metadata names) and writes
// its two effects. The services sink is a snapshot of the account, so every
// service of the test goes in one call. The sink resolves each mapping to a
// repository in the organization's own repos catalog.
func (f changeFailureFixture) pagerDutyServices(org string, repositories map[string]string, at time.Time) {
	f.t.Helper()
	ids := make([]string, 0, len(repositories))
	for id := range repositories {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	services := make([]string, 0, len(ids))
	for _, id := range ids {
		services = append(services, fmt.Sprintf(
			`{"id":%q,"type":"service","name":"Service %s","updated_at":%q,"metadata":{"repository":"https://gitlab.com/%s"}}`,
			id, id, at.UTC().Format(time.RFC3339), repositories[id]))
	}
	doer := &pagerDutyServicesDoer{t: f.t, responses: []pagerDutyServicesResponse{
		{body: `{"services":[` + strings.Join(services, ",") + `],"more":false}`},
	}}
	claim := f.claim(org, "pagerduty", "services")
	batch, err := (PagerDutyServicesRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		f.ctx, claim, pagerDutyTestCredential,
		pagerDutyServicesTestClient(f.t, fakehttp.Client(doer), providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond}),
		at,
	)
	if err != nil {
		f.t.Fatalf("pagerduty services route: %v", err)
	}
	sink := PagerDutyServicesClickHouseEffects{
		Entitlement: allowIncidentEntitlement, Conn: f.conn, Lease: f.lease,
		ProviderInstanceID: "acme", Now: func() time.Time { return at },
	}
	for _, effect := range batch.Effects {
		if err := sink.WriteEffect(f.ctx, claim, effect); err != nil {
			f.t.Fatalf("write pagerduty %s: %v", effect.Destination, err)
		}
	}
}

// pagerDutyIncident runs the PagerDuty incidents route for one incident of a
// service and writes it. The incident names its service, so it ties to the
// repository of that service's mapping directly.
func (f changeFailureFixture) pagerDutyIncident(org, serviceID, incidentID string, createdAt time.Time) {
	f.t.Helper()
	doer := &pagerDutyIncidentFamilyDoer{t: f.t, responses: []pagerDutyIncidentFamilyResponse{{
		body: fmt.Sprintf(`{"incidents":[{"id":%q,"type":"incident","incident_number":7,"title":"Checkout errors","status":"triggered","urgency":"high","created_at":%q,"updated_at":%q,"service":{"id":%q}}],"more":false}`,
			incidentID, createdAt.UTC().Format(time.RFC3339), createdAt.Add(time.Minute).UTC().Format(time.RFC3339), serviceID),
	}}}
	claim := f.claim(org, "pagerduty", "incidents")
	batch, err := (PagerDutyIncidentFamilyRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		f.ctx, claim, pagerDutyTestCredential, pagerDutyIncidentFamilyTestClient(f.t, fakehttp.Client(doer)), createdAt.Add(2*time.Hour),
	)
	if err != nil {
		f.t.Fatalf("pagerduty incidents route: %v", err)
	}
	sink := PagerDutyIncidentFamilyClickHouseEffects{
		Entitlement: allowIncidentEntitlement, Conn: f.conn, Lease: f.lease, ProviderInstanceID: "acme",
	}
	for _, effect := range batch.Effects {
		if err := sink.WriteEffect(f.ctx, claim, effect); err != nil {
			f.t.Fatalf("write pagerduty %s: %v", effect.Destination, err)
		}
	}
}

var pagerDutyTestCredential = providerfoundation.Credential{Provider: "pagerduty", Config: map[string]string{"subdomain": "acme"}}

// jiraIncident runs the Jira incidents route (one JSM incident, created
// 2026-07-22T10:00Z) and writes it. A Jira incident carries no service, so it
// ties to no repository directly. It returns the incident's id.
func (f changeFailureFixture) jiraIncident(org string) string {
	f.t.Helper()
	claim := f.claim(org, "jira", "incidents")
	claim.SourceExternalID = "JSM"
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", fakehttp.Client(&jiraIncidentDoer{t: f.t}),
		func(request *http.Request) error {
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Content-Type", "application/json")
			return nil
		},
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		f.lease,
	)
	if err != nil {
		f.t.Fatal(err)
	}
	batch, err := (JiraIncidentRouteHandler{Entitlement: allowIncidentEntitlement}).Collect(
		f.ctx, claim, providerfoundation.Credential{}, client, time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err != nil {
		f.t.Fatalf("jira incidents route: %v", err)
	}
	sink := JiraIncidentClickHouseEffects{Writer: f.conn, Lease: f.lease, Entitlement: allowIncidentEntitlement}
	var id string
	for _, effect := range batch.Effects {
		if err := sink.WriteEffect(f.ctx, claim, effect); err != nil {
			f.t.Fatalf("write jira %s: %v", effect.Destination, err)
		}
		var row jiraIncidentRow
		if err := json.Unmarshal(effect.Rows[0], &row); err != nil {
			f.t.Fatal(err)
		}
		id = row.ID
	}
	if id == "" {
		f.t.Fatal("jira route wrote no incident")
	}
	return id
}

// incidentID reads the id of the stored incident that ties directly to repo.
func (f changeFailureFixture) incidentID(org, title string, repo uuid.UUID) string {
	f.t.Helper()
	var id string
	if err := f.conn.QueryRow(f.ctx, `SELECT incident.id FROM operational_incidents AS incident
INNER JOIN operational_service_repository_mappings AS mapping
    ON mapping.org_id = incident.org_id AND mapping.service_id = incident.service_id
WHERE incident.org_id = ? AND incident.title = ? AND mapping.repo_id = ? LIMIT 1`, org, title, repo).Scan(&id); err != nil {
		f.t.Fatalf("read incident id of %s: %v", repo, err)
	}
	return id
}

// linkWithoutRepository stores a deployment-incident link row with no repo_id
// through the edge family's own writer.
func (f changeFailureFixture) linkWithoutRepository(org, deploymentID, incidentID, source string, at time.Time) {
	f.t.Helper()
	orgID := uuid.MustParse(org)
	if _, err := daily.WriteWorkGraphDeploymentIncidentEdges(f.ctx, f.conn, []workgraphedges.DeploymentIncidentEdge{{
		EdgeID: "edge-" + deploymentID + "-" + incidentID, OrgID: orgID, DeploymentID: deploymentID, IncidentID: incidentID,
		Provider: "jira", RepoID: nil, Confidence: 1, Source: source, Evidence: "{}", ObservedAt: at,
	}}, at.Add(time.Hour)); err != nil {
		f.t.Fatal(err)
	}
}

func (f changeFailureFixture) commit(org string, repo uuid.UUID, hash string, at time.Time) {
	f.t.Helper()
	if err := f.conn.Exec(f.ctx, fmt.Sprintf(
		"INSERT INTO git_commits (repo_id, hash, author_name, author_email, author_when, committer_when, parents, last_synced, org_id) VALUES ('%s', '%s', 'Ada', 'ada@example.com', '%s', '%s', 1, now64(3), '%s')",
		repo, hash, at.Format("2006-01-02 15:04:05"), at.Format("2006-01-02 15:04:05"), org)); err != nil {
		f.t.Fatal(err)
	}
}

func (f changeFailureFixture) compute(org string, day time.Time, repos ...uuid.UUID) {
	f.t.Helper()
	executor, err := daily.NewRepoUserCommitExecutor(f.conn)
	if err != nil {
		f.t.Fatal(err)
	}
	ids := make([]daily.RepositoryID, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, daily.RepositoryID(repo.String()))
	}
	if _, err := executor.ComputeFamily(f.ctx,
		daily.Run{ID: "run-" + org, OrganizationID: org, TargetDay: day},
		daily.Partition{ID: "partition-" + org, RepoIDs: ids},
	); err != nil {
		f.t.Fatalf("compute %s %s: %v", org, day.Format("2006-01-02"), err)
	}
}

// counts reads the newest stored counts of one repository and day.
func (f changeFailureFixture) counts(org string, repo uuid.UUID, day time.Time) (changefailure.Counts, bool) {
	f.t.Helper()
	rows, err := f.conn.Query(f.ctx, `SELECT `+changefailureColumns+` FROM `+changefailure.Table+`
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, org, repo, day)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return changefailure.Counts{}, false
	}
	var d, fn, fh, id, iv uint32
	if err := rows.Scan(&d, &fn, &fh, &id, &iv); err != nil {
		f.t.Fatal(err)
	}
	return changefailure.Counts{Deployments: uint64(d), FailedNative: uint64(fn), FailedHeuristic: uint64(fh), IncidentsDirect: uint64(id), IncidentsViaDeployment: uint64(iv)}, true
}

const changefailureColumns = "deployments_count, failed_deployments_native, failed_deployments_heuristic, incidents_direct, incidents_via_deployment"

// windowRate is the reader rule (changefailure.WindowRateSQL over the newest
// rows) for one organization, a set of repositories and a half-open day range.
func (f changeFailureFixture) windowRate(org string, start, end time.Time, repos ...uuid.UUID) *float64 {
	f.t.Helper()
	ids := make([]string, 0, len(repos))
	for _, repo := range repos {
		ids = append(ids, repo.String())
	}
	query := `SELECT ` + changefailure.WindowRateSQL + ` FROM ` +
		changefailure.LatestRowsSQL("start", "end", "AND toString(repo_id) IN {repos:Array(String)}")
	rows, err := f.conn.Query(f.ctx, query, namedArgs(map[string]any{
		"org_id": org, "start": start.Format("2006-01-02"), "end": end.Format("2006-01-02"), "repos": ids,
	})...)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		f.t.Fatal("window rate returned no row")
	}
	var value *float64
	if err := rows.Scan(&value); err != nil {
		f.t.Fatal(err)
	}
	return value
}

// TestChangeFailureCountsFromProviderWrittenRows is the provider matrix of
// CHAOS-8981: incidents from {gitlab, pagerduty, jira, github, linear} x
// deployments from {github, gitlab}. GitLab writes a service-to-repository
// mapping with each incident, and a PagerDuty incident names a service whose
// mapping the PagerDuty services route writes: both are direct ties. Jira
// incidents carry no service (no tie); GitHub and Linear have no incident
// producer. The rule never branches on provider: a cell with deployments and
// no tied incident is unknown, never 0%.
func TestChangeFailureCountsFromProviderWrittenRows(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	ctx, conn := newDeploymentsIntegrationConn(t)
	f := changeFailureFixture{t: t, ctx: ctx, conn: conn,
		lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}

	orgA := "3f8e9a40-0000-4000-8000-0000000000aa"
	orgB := "3f8e9a40-0000-4000-8000-0000000000bb"
	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	nextDay := day.AddDate(0, 0, 1)
	at := func(d time.Time, hour int) time.Time { return d.Add(time.Duration(hour) * time.Hour) }

	type cell struct {
		deploy, incident string
		repo             uuid.UUID
		want             changefailure.Counts
		wantState        changefailure.State
	}
	var cells []cell
	pagerDutyServiceRepositories := map[string]string{}
	jiraID := f.jiraIncident(orgA)
	issue := 9000
	for _, deploy := range []string{"github", "gitlab"} {
		for _, incident := range []string{"gitlab", "jira", "github", "linear", "pagerduty"} {
			name := "Acme/" + deploy + "-" + incident
			repo := f.repo(orgA, name)
			f.deploy(orgA, deploy, repo, deploy+"-"+incident+"-1", at(day, 9))
			f.deploy(orgA, deploy, repo, deploy+"-"+incident+"-2", at(day, 12))
			c := cell{deploy: deploy, incident: incident, repo: repo,
				want: changefailure.Counts{Deployments: 2}, wantState: changefailure.StateUnknown}
			// Today's link producer is heuristic: an incident with a direct tie
			// links to every deployment of its repository on its day.
			measured := changefailure.Counts{Deployments: 2, FailedHeuristic: 2, IncidentsDirect: 1}
			switch incident {
			case "gitlab":
				issue++
				f.gitLabIncident(orgA, name, issue, at(day, 10))
				c.want, c.wantState = measured, changefailure.StateMeasured
			case "pagerduty":
				pagerDutyServiceRepositories["PS-"+deploy] = name
				c.want, c.wantState = measured, changefailure.StateMeasured
			}
			cells = append(cells, c)
		}
	}
	// The mapping of a PagerDuty service resolves against the stored
	// repositories, so the services route runs after the repository rows exist.
	f.pagerDutyServices(orgA, pagerDutyServiceRepositories, at(day, 7))
	for serviceID := range pagerDutyServiceRepositories {
		f.pagerDutyIncident(orgA, serviceID, "PI-"+serviceID, at(day, 10))
	}

	// Incident evidence without a deployment: not applicable.
	incidentOnly := f.repo(orgA, "Acme/incident-only")
	issue++
	f.gitLabIncident(orgA, "Acme/incident-only", issue, at(day, 10))

	// The via-deployment tier: the Jira incident has no direct tie, and a link
	// row without repo_id names a deployment of exactly one repository.
	via := f.repo(orgA, "Acme/via-deployment")
	f.deploy(orgA, "github", via, "via-1", at(day, 8))
	f.deploy(orgA, "github", via, "via-2", at(day, 9))
	f.linkWithoutRepository(orgA, "via-1", jiraID, changefailure.TierNative, at(day, 10))

	// An ambiguous link: its deployment id names two repositories, so it ties
	// nothing.
	dupX, dupY := f.repo(orgA, "Acme/dup-x"), f.repo(orgA, "Acme/dup-y")
	f.deploy(orgA, "gitlab", dupX, "dup-1", at(day, 9))
	f.deploy(orgA, "gitlab", dupY, "dup-1", at(day, 9))
	f.linkWithoutRepository(orgA, "dup-1", jiraID, changefailure.TierHeuristic, at(day, 10))

	// An incident with a direct tie to one repository and a link row without
	// repo_id to a deployment of another: the direct tie outranks the
	// via-deployment tie, also when the two repositories are computed apart.
	directOwner, crossDeploy := f.repo(orgA, "Acme/direct-owner"), f.repo(orgA, "Acme/cross-deploy")
	issue++
	f.gitLabIncident(orgA, "Acme/direct-owner", issue, at(day, 10))
	f.deploy(orgA, "github", crossDeploy, "cross-1", at(day, 9))
	f.linkWithoutRepository(orgA, "cross-1", f.incidentID(orgA, "API unavailable", directOwner), changefailure.TierNative, at(day, 10))

	// A measured zero over a window: the incident is on day one (no
	// deployment), the deployments on day two (no incident).
	window := f.repo(orgA, "Acme/window")
	issue++
	f.gitLabIncident(orgA, "Acme/window", issue, at(day, 10))
	f.deploy(orgA, "gitlab", window, "window-1", at(nextDay, 9))
	f.deploy(orgA, "gitlab", window, "window-2", at(nextDay, 11))

	// Org isolation: the same repository name (so the same repository id) in
	// another organization, with its own deployments and incident.
	isolated := f.repo(orgB, "Acme/github-gitlab")
	for i := 0; i < 5; i++ {
		f.deploy(orgB, "github", isolated, fmt.Sprintf("org-b-%d", i), at(day, 9+i))
	}
	f.gitLabIncident(orgB, "Acme/github-gitlab", 9999, at(day, 10))

	// A commit gives the github-gitlab and github-jira repositories a
	// repo_metrics_daily row for the day.
	f.commit(orgA, cells[0].repo, "c-measured", at(day, 13))
	f.commit(orgA, cells[1].repo, "c-unknown", at(day, 13))

	// A partition computes its own repositories only: computing dup-x alone
	// writes no row for the via-deployment repository, whose link the loader
	// also sees.
	f.compute(orgA, day, dupX)
	if got, found := f.counts(orgA, via, day); found {
		t.Fatalf("computing another partition wrote a row for the via-deployment repository: %+v", got)
	}
	f.compute(orgA, day, crossDeploy)
	if got, _ := f.counts(orgA, crossDeploy, day); got != (changefailure.Counts{Deployments: 1}) {
		t.Errorf("repository of a deployment linked to an incident with a direct tie elsewhere: %+v, want one deployment and no via-deployment tie", got)
	}

	all := []uuid.UUID{incidentOnly, via, dupX, dupY, window, directOwner}
	for _, c := range cells {
		all = append(all, c.repo)
	}
	f.compute(orgA, day, all...)
	f.compute(orgA, nextDay, all...)
	f.compute(orgB, day, isolated)

	for _, c := range cells {
		got, found := f.counts(orgA, c.repo, day)
		if !found || got != c.want {
			t.Errorf("%s deployments x %s incidents: counts %+v (found %v), want %+v", c.deploy, c.incident, got, found, c.want)
		}
		if state := changefailure.Rate(got).State; state != c.wantState {
			t.Errorf("%s deployments x %s incidents: %s, want %s", c.deploy, c.incident, state, c.wantState)
		}
	}
	if got, _ := f.counts(orgA, incidentOnly, day); got != (changefailure.Counts{IncidentsDirect: 1}) {
		t.Errorf("incident-only repository: %+v, want one direct incident and no deployment", got)
	}
	if v := f.windowRate(orgA, day, nextDay, incidentOnly); v != nil {
		t.Errorf("incident-only repository rate %v, want none (not applicable)", *v)
	}
	if got, _ := f.counts(orgA, via, day); got != (changefailure.Counts{Deployments: 2, FailedNative: 1, IncidentsViaDeployment: 1}) {
		t.Errorf("via-deployment repository: %+v, want 2 deployments, 1 native failure, 1 via-deployment incident", got)
	} else if changefailure.LowestTier(got) != changefailure.TierNative {
		t.Errorf("via-deployment repository tier %q, want native", changefailure.LowestTier(got))
	}
	for name, repo := range map[string]uuid.UUID{"dup-x": dupX, "dup-y": dupY} {
		if got, _ := f.counts(orgA, repo, day); got != (changefailure.Counts{Deployments: 1}) {
			t.Errorf("%s: %+v, want one deployment and no tie from the ambiguous link", name, got)
		}
	}
	if got, found := f.counts(orgA, window, day); !found || got != (changefailure.Counts{IncidentsDirect: 1}) {
		t.Errorf("window repository day one: %+v (found %v), want one incident", got, found)
	}
	if got, found := f.counts(orgA, window, nextDay); !found || got != (changefailure.Counts{Deployments: 2}) {
		t.Errorf("window repository day two: %+v (found %v), want two deployments", got, found)
	}
	if v := f.windowRate(orgA, nextDay, nextDay.AddDate(0, 0, 1), window); v != nil {
		t.Errorf("window repository, day two alone: %v, want none (unknown)", *v)
	}
	if v := f.windowRate(orgA, day, nextDay.AddDate(0, 0, 1), window); v == nil || *v != 0 {
		t.Errorf("window repository over both days: %v, want a measured 0", v)
	}
	// A repository and day with nothing to count has no row.
	if _, found := f.counts(orgA, incidentOnly, nextDay); found {
		t.Error("incident-only repository has a row on a day with no deployment and no incident")
	}
	// A team owning the github-gitlab (2 of 2 failed) and github-jira (0 of 2,
	// no evidence) repositories: evidence of one owned repository is the
	// team's, and the rate is 2 / 4.
	if v := f.windowRate(orgA, day, nextDay, cells[0].repo, cells[1].repo); v == nil || *v != 0.5 {
		t.Errorf("team of two repositories: %v, want 0.5", v)
	}
	if got, _ := f.counts(orgA, cells[0].repo, day); got.Deployments != 2 {
		t.Errorf("org A counts %+v include org B's deployments", got)
	}
	if got, _ := f.counts(orgB, isolated, day); got != (changefailure.Counts{Deployments: 5, FailedHeuristic: 5, IncidentsDirect: 1}) {
		t.Errorf("org B counts %+v, want its own 5 deployments", got)
	}

	// repo_metrics_daily carries the day's incident-based value in its own
	// column: measured for github-gitlab, NULL (unknown) for github-jira.
	// revert_rate is NULL without a merged pull request, and the deprecated
	// change_failure_rate column keeps the legacy revert ratio (0 here), which
	// is never the incident-based value.
	for repo, want := range map[uuid.UUID]*float64{cells[0].repo: ptrFloat(1), cells[1].repo: nil} {
		rows, err := conn.Query(ctx, `SELECT change_failure_rate_incident, revert_rate, change_failure_rate FROM repo_metrics_daily
WHERE org_id = ? AND repo_id = ? AND day = ? ORDER BY computed_at DESC LIMIT 1`, orgA, repo, day)
		if err != nil {
			t.Fatal(err)
		}
		if !rows.Next() {
			rows.Close()
			t.Fatalf("repository %s has no repo_metrics_daily row", repo)
		}
		var incident, revert *float64
		var deprecated float64
		if err := rows.Scan(&incident, &revert, &deprecated); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		if (incident == nil) != (want == nil) || (incident != nil && *incident != *want) || revert != nil || deprecated != 0 {
			t.Errorf("repository %s: change_failure_rate_incident %s revert_rate %s deprecated change_failure_rate %v, want %s, NULL, 0",
				repo, showFloat(incident), showFloat(revert), deprecated, showFloat(want))
		}
	}
}

func showFloat(v *float64) string {
	if v == nil {
		return "NULL"
	}
	return fmt.Sprint(*v)
}

func ptrFloat(v float64) *float64 { return &v }

func namedArgs(values map[string]any) []any {
	out := make([]any, 0, len(values))
	for name, value := range values {
		out = append(out, clickhouse.Named(name, value))
	}
	return out
}
