package report

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func TestExecuteStoresOneArtifactAndNotification(t *testing.T) {
	store := &fakeRunStore{claim: true, complete: true, notificationClaim: true}
	dependencies := Dependencies{
		Runs:  store,
		Query: queryFunc(func(context.Context, QueryInput) (QueryResult, error) { return QueryResult{}, nil }),
		Renderer: rendererFunc(func(context.Context, QueryResult) (Artifact, error) {
			return Artifact{Markdown: "# report", Fingerprint: "sha256:abc"}, nil
		}),
		Artifacts:     artifactFunc(func(_ context.Context, _ string, artifact Artifact) (Artifact, error) { return artifact, nil }),
		Notifications: notificationFunc(func(context.Context, string, string) error { return nil }),
	}
	err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", dependencies)
	if err != nil || store.completed != 1 || store.notificationsCompleted != 1 {
		t.Fatalf("execute err=%v completed=%d notifications=%d", err, store.completed, store.notificationsCompleted)
	}
}

func TestExecuteDuplicateOrCancelledClaimDoesNothing(t *testing.T) {
	store := &fakeRunStore{}
	err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", Dependencies{
		Runs: store, Query: queryFunc(nil), Renderer: rendererFunc(nil), Artifacts: artifactFunc(nil), Notifications: notificationFunc(nil),
	})
	if err != nil || store.completed != 0 {
		t.Fatalf("duplicate execution err=%v completed=%d", err, store.completed)
	}
}

func TestExecuteCompletedArtifactRetriesOnlyPendingNotification(t *testing.T) {
	store := &fakeRunStore{notificationClaim: true}
	notified := 0
	err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", Dependencies{
		Runs: store, Query: queryFunc(nil), Renderer: rendererFunc(nil), Artifacts: artifactFunc(nil),
		Notifications: notificationFunc(func(context.Context, string, string) error {
			notified++
			return nil
		}),
	})
	if err != nil || notified != 1 || store.completed != 0 || store.notificationsCompleted != 1 {
		t.Fatalf("retry err=%v notified=%d completed=%d notification_completed=%d",
			err, notified, store.completed, store.notificationsCompleted)
	}
}

func TestExecuteNotificationFailureReleasesClaim(t *testing.T) {
	store := &fakeRunStore{claim: true, complete: true, notificationClaim: true}
	err := execute(context.Background(), reportEnvelope(), "00000000-0000-4000-8000-000000000002", Dependencies{
		Runs:          store,
		Query:         queryFunc(func(context.Context, QueryInput) (QueryResult, error) { return QueryResult{}, nil }),
		Renderer:      rendererFunc(func(context.Context, QueryResult) (Artifact, error) { return Artifact{Fingerprint: "sha256:abc"}, nil }),
		Artifacts:     artifactFunc(func(_ context.Context, _ string, artifact Artifact) (Artifact, error) { return artifact, nil }),
		Notifications: notificationFunc(func(context.Context, string, string) error { return errors.New("offline") }),
	})
	if err == nil || store.notificationsReleased != 1 {
		t.Fatalf("err=%v released=%d", err, store.notificationsReleased)
	}
}

func TestRendererAndArtifactMatchPythonGoldens(t *testing.T) {
	createdAt := time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC)
	input := QueryResult{
		Plan: Plan{
			PlanID: "plan-1", ReportType: "weekly_health", Audience: "team_lead",
			ScopeTeams: []string{"team-a"}, ScopeRepos: []string{"repo-a"},
			TimeRangeStart: "2026-01-01", TimeRangeEnd: "2026-01-07",
			ComparisonPeriod: "prior_week", Sections: []string{"summary", "quality", "testops"},
			RequestedMetrics: []string{"success_rate"}, ConfidenceThreshold: "direct_fact",
			CreatedAt: createdAt, OrganizationID: "org-1",
		},
		Charts: []ChartResult{{
			Spec: ChartSpec{
				ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
				Metric: "success_rate", GroupBy: "day", Title: "Success rate",
				OrganizationID: "org-1",
			},
			DataPoints: []DataPoint{{X: "2026-01-01", Y: 0.95}},
			Title:      "Success rate", SourceTable: "cicd_metrics_daily", Unit: "ratio",
		}},
		Metadata: map[string]string{"renderer_version": "reports.v1"},
	}
	artifact, err := NewDeterministicRenderer().Render(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err = NewSHA256ArtifactAdapter().Store(context.Background(),
		"00000000-0000-4000-8000-000000000001", artifact)
	if err != nil {
		t.Fatal(err)
	}
	wantMarkdown, err := os.ReadFile(filepath.Join("testdata", "weekly_health.golden.md"))
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Markdown != string(wantMarkdown) {
		t.Fatalf("markdown drifted from Python golden:\n%s", artifact.Markdown)
	}
	var wantMetadata struct {
		Fingerprint string             `json:"fingerprint"`
		Provenance  []ProvenanceRecord `json:"provenance"`
	}
	data, err := os.ReadFile(filepath.Join("testdata", "weekly_health.metadata.golden.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &wantMetadata); err != nil {
		t.Fatal(err)
	}
	if artifact.Fingerprint != wantMetadata.Fingerprint {
		t.Fatalf("fingerprint = %s, want %s", artifact.Fingerprint, wantMetadata.Fingerprint)
	}
	if got, want := mustJSON(t, artifact.Provenance), mustJSON(t, wantMetadata.Provenance); got != want {
		t.Fatalf("provenance metadata drifted:\ngot  %s\nwant %s", got, want)
	}
}

func TestBuildChartQueryMatchesPythonContract(t *testing.T) {
	spec := ChartSpec{
		ChartID: "chart-1", PlanID: "plan-1", ChartType: "line",
		Metric: "flake_rate", GroupBy: "day", FilterTeams: []string{"team-a"},
		FilterRepos: []string{"repo-a"}, TimeRangeStart: "2026-01-01",
		TimeRangeEnd: "2026-01-07", OrganizationID: "org-1",
	}
	query, parameters, err := buildChartQuery(spec, supportedMetrics[spec.Metric])
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{
		"toDate(day) AS x", "avg(flake_rate) AS y", "FROM testops_test_metrics_daily",
		"org_id = {org_id:String}", "team_id IN {filter_teams:Array(String)}",
		"repo_id IN {filter_repos:Array(String)}", "ORDER BY x",
	} {
		if !strings.Contains(query, fragment) {
			t.Fatalf("query missing %q:\n%s", fragment, query)
		}
	}
	if len(parameters) != 5 {
		t.Fatalf("parameters = %d, want 5", len(parameters))
	}
}

func TestDecodeReportDefinitionAcceptsChartSpecsWithoutLeakingJobData(t *testing.T) {
	plan := []byte(`{
		"plan_id":"plan-1","report_type":"weekly_health","org_id":"org-1",
		"sections":["summary"],"created_at":"2026-01-08T00:00:00Z",
		"chart_specs":[{
			"chart_id":"chart-1","plan_id":"plan-1","chart_type":"line",
			"metric":"success_rate","group_by":"day","org_id":"org-1"
		}]
	}`)
	definition, err := decodeReportDefinition("report-1", "org-1", plan, nil,
		time.Date(2026, 1, 8, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if definition.Plan.PlanID != "plan-1" || len(definition.Charts) != 1 ||
		definition.Charts[0].Metric != "success_rate" {
		t.Fatalf("definition = %#v", definition)
	}
}

func TestReportRouteCapabilitiesRemainIndependentAndDormant(t *testing.T) {
	t.Chdir(filepath.Join("..", "..", ".."))
	registry, err := jobruntime.Load("contracts/jobs/v1")
	if err != nil {
		t.Fatal(err)
	}
	capabilities, err := RouteCapabilities(registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(capabilities) != 2 || capabilities[0].Kind == capabilities[1].Kind {
		t.Fatalf("capabilities = %#v", capabilities)
	}
	for _, capability := range capabilities {
		if !capability.Compiled || capability.Route != "celery" ||
			capability.RollbackRoute != "celery" || capability.Executable {
			t.Fatalf("route unexpectedly active: %#v", capability)
		}
	}
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func reportEnvelope() jobcontract.Envelope {
	return jobcontract.Envelope{Domain: jobcontract.DomainLink{Type: "report_run", ID: "00000000-0000-4000-8000-000000000001"}}
}

type fakeRunStore struct {
	claim, complete, notificationClaim                       bool
	completed, notificationsCompleted, notificationsReleased int
}

func (store *fakeRunStore) Claim(context.Context, string, string) (bool, error) {
	return store.claim, nil
}
func (store *fakeRunStore) Complete(context.Context, string, Artifact) (bool, error) {
	store.completed++
	return store.complete, nil
}
func (store *fakeRunStore) Fail(context.Context, string, string) error { return nil }
func (store *fakeRunStore) ClaimNotification(context.Context, string) (*NotificationClaim, error) {
	if !store.notificationClaim {
		return nil, nil
	}
	return &NotificationClaim{Key: "report.ready:1", Token: "00000000-0000-4000-8000-000000000099"}, nil
}
func (store *fakeRunStore) CompleteNotification(context.Context, string, NotificationClaim) error {
	store.notificationsCompleted++
	return nil
}
func (store *fakeRunStore) ReleaseNotification(context.Context, string, NotificationClaim) error {
	store.notificationsReleased++
	return nil
}

type queryFunc func(context.Context, QueryInput) (QueryResult, error)

func (fn queryFunc) Query(ctx context.Context, input QueryInput) (QueryResult, error) {
	return fn(ctx, input)
}

type rendererFunc func(context.Context, QueryResult) (Artifact, error)

func (fn rendererFunc) Render(ctx context.Context, input QueryResult) (Artifact, error) {
	return fn(ctx, input)
}

type artifactFunc func(context.Context, string, Artifact) (Artifact, error)

func (fn artifactFunc) Store(ctx context.Context, id string, artifact Artifact) (Artifact, error) {
	return fn(ctx, id, artifact)
}

type notificationFunc func(context.Context, string, string) error

func (fn notificationFunc) Notify(ctx context.Context, reportID, key string) error {
	return fn(ctx, reportID, key)
}
