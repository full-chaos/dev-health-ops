//go:build integration

package providersync

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/changefailure"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// statusCell is one cell of the CHAOS-9621 matrix: a repository of one
// provider with one deployment whose own status is status (nil: the provider
// stored none), with or without an incident that ties to the repository.
type statusCell struct {
	provider string
	status   *string
	incident bool
	repo     uuid.UUID
}

func (c statusCell) name() string {
	status := "NULL"
	if c.status != nil {
		status = *c.status
	}
	return fmt.Sprintf("%s status=%q incident=%v", c.provider, status, c.incident)
}

// statusMeaning is the expected meaning of each stored status, written out by
// value and independent of deploymentstatus.Classify: failed, success,
// canceled, or none of them (the deployment is a deployment, it is not an
// outcome).
func statusMeaning(status *string) string {
	if status == nil {
		return "other"
	}
	switch strings.ToLower(strings.TrimSpace(*status)) {
	case "failure", "failed", "error":
		return "failed"
	case "success":
		return "success"
	case "canceled", "cancelled":
		return "canceled"
	default:
		return "other"
	}
}

func strptr(s string) *string { return &s }

// statusRow builds a deployment row the way each provider's own normalizer
// does, so the matrix runs the real spelling of each provider: GitLab keeps
// the payload status, GitHub reads the latest entry of the statuses history
// (the route overwrites the list-level state with it) and keeps no status when
// the history is empty or the lookup did not run.
func statusRow(t *testing.T, org, provider string, repo uuid.UUID, deploymentID string, status *string, at time.Time) deploymentRow {
	t.Helper()
	claim := nativeTestClaim(provider, "deployments")
	claim.OrgID = org
	created := at.UTC().Format(time.RFC3339)
	normalizedAt := at.Add(time.Hour)
	switch provider {
	case "gitlab":
		payload := map[string]any{
			"id": json.Number(deploymentID), "created_at": created,
			"environment": map[string]any{"name": "production"},
		}
		if status != nil {
			payload["status"] = *status
		}
		row, ok, err := normalizeGitLabDeployment(claim, repo.String(), payload, nil, normalizedAt)
		if err != nil || !ok {
			t.Fatalf("normalize gitlab deployment: ok %v err %v", ok, err)
		}
		return row
	case "github":
		row, ok := normalizeGitHubDeployment(claim, repo.String(), gitHubDeploymentPayload{
			ID: json.Number(deploymentID), CreatedAt: &created, Environment: "production",
		}, nil, normalizedAt)
		if !ok {
			t.Fatal("normalize github deployment")
		}
		if status != nil {
			row.Status = deploymentLatestStatus([]gitHubDeploymentStatusPayload{{State: *status, CreatedAt: &created}})
		}
		return row
	}
	t.Fatalf("no deployment producer for %s", provider)
	return deploymentRow{}
}

// TestChangeFailureStatusMatrix is the provider x status matrix of CHAOS-9621:
// deployments of {github, gitlab} x every stored status value (each provider's
// own spellings, the other provider's spellings that a push-ingested row can
// carry, a mixed-case spelling, an unknown string and none) x {no incident,
// an incident tied to the repository}, written through the real
// normalizers and sinks into a ClickHouse migrated by the real chain, counted
// by the real repo_user_commit daily executor and read by the reader rule.
//
// Every number is checked by value:
//   - deployments_count: every deployment but a canceled one;
//   - the failed count: a deployment whose own status is failed, or that an
//     incident names, counted ONCE when both are true;
//   - deployments_status_known: the deployments with a final status;
//   - the served window rate and, below, the state and the evidence basis.
//
// Red on main: without the change a deployment's own status counts for
// nothing, so every "no incident" cell reads unknown whatever its status, a
// canceled deployment counts as a deployment, and the status columns do not
// exist.
func TestChangeFailureStatusMatrix(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	ctx, conn := newDeploymentsIntegrationConn(t)
	f := changeFailureFixture{t: t, ctx: ctx, conn: conn,
		lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}

	const org = "3f8e9a40-0000-4000-8000-0000000000c9"
	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	nextDay := day.AddDate(0, 0, 1)
	deployedAt := day.Add(9 * time.Hour)
	incidentAt := day.Add(10 * time.Hour)

	statuses := []*string{
		strptr("success"), strptr("failure"), strptr("failed"), strptr("error"),
		strptr("canceled"), strptr("cancelled"), strptr("inactive"), strptr("blocked"),
		strptr("created"), strptr("running"), strptr("in_progress"), strptr("queued"),
		strptr("pending"), strptr("Failed"), strptr("weird-status"), nil,
	}
	var cells []statusCell
	issue := 7000
	for _, provider := range []string{"github", "gitlab"} {
		for index, status := range statuses {
			for _, incident := range []bool{false, true} {
				cell := statusCell{provider: provider, status: status, incident: incident}
				name := fmt.Sprintf("Acme/%s-%d-%v", provider, index, incident)
				id, err := repositoryIdentity(name)
				if err != nil {
					t.Fatal(err)
				}
				cell.repo = uuid.MustParse(id)
				if err := conn.Exec(ctx, fmt.Sprintf(
					"INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('%s', '%s', now64(3), now64(3), '%s', '%s')",
					cell.repo, name, org, provider)); err != nil {
					t.Fatal(err)
				}
				row := statusRow(t, org, provider, cell.repo, "5001", status, deployedAt)
				effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, []deploymentRow{row})
				if err != nil {
					t.Fatal(err)
				}
				claim := f.claim(org, provider, "deployments")
				if provider == "github" {
					err = GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: f.lease}.WriteEffect(ctx, claim, effect)
				} else {
					err = GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: f.lease}.WriteEffect(ctx, claim, effect)
				}
				if err != nil {
					t.Fatalf("%s: write deployment: %v", cell.name(), err)
				}
				if incident {
					issue++
					f.gitLabIncident(org, name, issue, incidentAt)
				}
				cells = append(cells, cell)
			}
		}
	}
	repos := make([]uuid.UUID, 0, len(cells))
	for _, cell := range cells {
		repos = append(repos, cell.repo)
	}
	f.compute(org, day, repos...)

	var hasStatusColumns uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.columns
WHERE database = currentDatabase() AND table = 'repo_change_failure_daily'
  AND name IN ('failed_deployments_status', 'deployments_status_known')`).Scan(&hasStatusColumns); err != nil {
		t.Fatal(err)
	}

	for _, cell := range cells {
		meaning := statusMeaning(cell.status)
		// The expected counts, by value.
		var wantDeployments, wantLinked, wantStatusOnly, wantKnown, wantIncidents uint64
		if meaning != "canceled" {
			wantDeployments = 1
		}
		if meaning == "success" || meaning == "failed" {
			wantKnown = 1
		}
		if cell.incident {
			wantIncidents = 1
			if wantDeployments == 1 {
				wantLinked = 1 // the heuristic link producer names the deployment
			}
		} else if meaning == "failed" {
			wantStatusOnly = 1
		}
		wantFailed := wantLinked + wantStatusOnly

		counts, found := f.counts(org, cell.repo, day)
		wantFound := wantDeployments+wantIncidents > 0
		if found != wantFound {
			t.Errorf("%s: stored row %v, want %v", cell.name(), found, wantFound)
			continue
		}
		if counts.Deployments != wantDeployments || counts.FailedNative != 0 || counts.FailedHeuristic != wantLinked || counts.IncidentsDirect != wantIncidents {
			t.Errorf("%s: deployments %d native %d heuristic %d incidents %d; want deployments %d heuristic %d incidents %d",
				cell.name(), counts.Deployments, counts.FailedNative, counts.FailedHeuristic, counts.IncidentsDirect, wantDeployments, wantLinked, wantIncidents)
		}

		// The served rate (the reader rule in SQL) by value.
		var wantRate *float64
		switch {
		case wantDeployments == 0: // not applicable
		case wantIncidents > 0:
			rate := float64(wantFailed) / float64(wantDeployments)
			wantRate = &rate
		case wantKnown > 0:
			rate := float64(wantFailed) / float64(wantKnown)
			wantRate = &rate
		}
		if got := f.windowRate(org, day, nextDay, cell.repo); showFloat(got) != showFloat(wantRate) {
			t.Errorf("%s: served rate %s, want %s", cell.name(), showFloat(got), showFloat(wantRate))
		}

		if hasStatusColumns != 2 {
			continue
		}
		// Status columns, the state and the evidence basis.
		view := readStatusView(t, f, org, cell.repo, day)
		if view.FailedStatus != wantStatusOnly || view.StatusKnown != wantKnown {
			t.Errorf("%s: failed by status %d known %d; want %d and %d", cell.name(), view.FailedStatus, view.StatusKnown, wantStatusOnly, wantKnown)
		}
		outcome := changefailure.Evaluate(view)
		wantState, wantBasis, wantReason := changefailure.StateMeasured, changefailure.BasisNone, changefailure.NotUnknown
		switch {
		case !wantFound:
			wantState, wantReason = changefailure.StateNoStoredCounts, changefailure.ReasonNotCollected
		case wantDeployments == 0:
			wantState, wantReason = changefailure.StateNotApplicable, changefailure.ReasonNoDeploymentsInWindow
		case wantIncidents == 0 && wantKnown == 0:
			wantState, wantReason = changefailure.StateUnknown, changefailure.ReasonDeploymentsWithoutOutcome
		case wantIncidents > 0 && wantKnown > 0:
			wantBasis = changefailure.BasisBoth
		case wantIncidents > 0:
			wantBasis = changefailure.BasisIncident
		default:
			wantBasis = changefailure.BasisStatus
		}
		if outcome.State != wantState || outcome.Basis != wantBasis || outcome.UnknownReason != wantReason {
			t.Errorf("%s: state %q basis %q reason %q, want %q %q %q", cell.name(), outcome.State, outcome.Basis, outcome.UnknownReason, wantState, wantBasis, wantReason)
		}
		if outcome.Deployments != wantDeployments || outcome.StatusKnown != wantKnown {
			t.Errorf("%s: D %d K %d, want %d %d", cell.name(), outcome.Deployments, outcome.StatusKnown, wantDeployments, wantKnown)
		}
	}
}

// TestChangeFailureStatusCoverage is the part of the matrix where a repository
// has several deployments and only some have a final status: a rate that only
// statuses state is over the deployments with a final status (K), the answer
// always carries D, K and the coverage K / D, and incident evidence puts the
// base back to every deployment (D).
func TestChangeFailureStatusCoverage(t *testing.T) {
	t.Setenv("OPERATIONAL_ORDERING_CONTRACT", "2")
	ctx, conn := newDeploymentsIntegrationConn(t)
	f := changeFailureFixture{t: t, ctx: ctx, conn: conn,
		lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil })}

	const org = "3f8e9a40-0000-4000-8000-0000000000ca"
	day := time.Date(2026, 7, 22, 0, 0, 0, 0, time.UTC)
	nextDay := day.AddDate(0, 0, 1)

	cases := []struct {
		name     string
		provider string
		statuses []*string
		incident bool
		d, k     uint64
		value    float64
		basis    changefailure.Basis
		coverage float64
	}{
		{"succeeded and still running", "github", []*string{strptr("success"), strptr("in_progress")}, false, 2, 1, 0, changefailure.BasisStatus, 0.5},
		{"failed, running and canceled", "gitlab", []*string{strptr("failed"), strptr("running"), strptr("canceled")}, false, 2, 1, 1, changefailure.BasisStatus, 0.5},
		{"one failed of three with a final status, one without", "github", []*string{strptr("failure"), strptr("success"), strptr("success"), strptr("pending")}, false, 4, 3, 1.0 / 3.0, changefailure.BasisStatus, 0.75},
		{"a status lookup that was skipped leaves NULL", "github", []*string{nil, nil, strptr("failure")}, false, 3, 1, 1, changefailure.BasisStatus, 1.0 / 3.0},
		{"incident evidence: the base is every deployment", "gitlab", []*string{strptr("failed"), strptr("success"), strptr("running")}, true, 3, 2, 1, changefailure.BasisBoth, 2.0 / 3.0},
	}
	var repos []uuid.UUID
	ids := make([]uuid.UUID, len(cases))
	issue := 7500
	for i, tc := range cases {
		name := fmt.Sprintf("Acme/coverage-%d", i)
		id, err := repositoryIdentity(name)
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = uuid.MustParse(id)
		repos = append(repos, ids[i])
		if err := conn.Exec(ctx, fmt.Sprintf(
			"INSERT INTO repos (id, repo, created_at, last_synced, org_id, provider) VALUES ('%s', '%s', now64(3), now64(3), '%s', '%s')",
			ids[i], name, org, tc.provider)); err != nil {
			t.Fatal(err)
		}
		for j, status := range tc.statuses {
			row := statusRow(t, org, tc.provider, ids[i], fmt.Sprint(6000+j), status, day.Add(time.Duration(8+j)*time.Hour))
			effect, err := effectBatchFromValues("deployments", EffectReadbackRequired, []deploymentRow{row})
			if err != nil {
				t.Fatal(err)
			}
			claim := f.claim(org, tc.provider, "deployments")
			if tc.provider == "github" {
				err = GitHubDeploymentsClickHouseEffects{Conn: conn, Lease: f.lease}.WriteEffect(ctx, claim, effect)
			} else {
				err = GitLabDeploymentsClickHouseEffects{Conn: conn, Lease: f.lease}.WriteEffect(ctx, claim, effect)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if tc.incident {
			issue++
			f.gitLabIncident(org, name, issue, day.Add(20*time.Hour))
		}
	}
	f.compute(org, day, repos...)

	for i, tc := range cases {
		outcome := changefailure.Evaluate(readStatusView(t, f, org, ids[i], day))
		if outcome.Value == nil || !nearly(*outcome.Value, tc.value) || outcome.Basis != tc.basis || outcome.Deployments != tc.d || outcome.StatusKnown != tc.k ||
			outcome.Coverage == nil || !nearly(*outcome.Coverage, tc.coverage) {
			t.Errorf("%s: rate %s basis %s D %d K %d coverage %s; want %v %s %d %d %v", tc.name,
				showFloat(outcome.Value), outcome.Basis, outcome.Deployments, outcome.StatusKnown, showFloat(outcome.Coverage), tc.value, tc.basis, tc.d, tc.k, tc.coverage)
		}
		if sql := f.windowRate(org, day, nextDay, ids[i]); sql == nil || !nearly(*sql, tc.value) {
			t.Errorf("%s: served SQL rate %s, want %v", tc.name, showFloat(sql), tc.value)
		}
	}
}

func nearly(a, b float64) bool {
	d := a - b
	return d < 1e-9 && d > -1e-9
}

// readStatusView reads the newest stored row of one repository and day through
// the reader's own sums (ViewSumsSQL), the shape every reader scans.
func readStatusView(t *testing.T, f changeFailureFixture, org string, repo uuid.UUID, day time.Time) changefailure.View {
	t.Helper()
	query := `SELECT ` + changefailure.ViewSumsSQL + ` FROM ` +
		changefailure.LatestRowsSQL("start", "end", "AND toString(repo_id) IN {repos:Array(String)}")
	rows, err := f.conn.Query(f.ctx, query, namedArgs(map[string]any{
		"org_id": org, "start": day.Format("2006-01-02"), "end": day.AddDate(0, 0, 1).Format("2006-01-02"), "repos": []string{repo.String()},
	})...)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var view changefailure.View
	if !rows.Next() {
		t.Fatal("view sums returned no row")
	}
	if err := rows.Scan(changefailure.ViewScanDest(&view)...); err != nil {
		t.Fatal(err)
	}
	return view
}
