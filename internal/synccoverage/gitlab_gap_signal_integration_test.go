//go:build integration

package synccoverage

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	syncplanner "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The GitLab test report cap (providersync nativeMaxObjectBytes, 2 MiB); the cell asserts the skip happened.
const gitLabGapReportCap = 2 << 20

type gitLabGapDoer struct {
	t      *testing.T
	report string
}

func (d *gitLabGapDoer) Do(request *http.Request) (*http.Response, error) {
	path := request.URL.Path
	body := ""
	switch {
	case path == "/api/v4/projects/123":
		body = `{"id":123,"path_with_namespace":"acme/api","default_branch":"main","only_allow_merge_if_pipeline_succeeds":true}`
	case path == "/api/v4/projects/123/pipelines":
		body = `[{"id":9000,"name":"CI","ref":"main","status":"success","created_at":"2026-07-22T10:00:00Z","started_at":"2026-07-22T10:01:00Z","finished_at":"2026-07-22T10:05:00Z","source":"push","sha":"abc","web_url":"https://gitlab.example/acme/api/-/pipelines/9000"}]`
	case strings.HasSuffix(path, "/jobs"):
		body = `[]`
	case strings.HasSuffix(path, "/test_report"):
		body = d.report
	default:
		d.t.Fatalf("unexpected request %s", request.URL.String())
	}
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
}

func gitLabGapReport(bytes int) string {
	one := `{"status":"success","name":"TestSomething/with_a_subcase","classname":"pkg/service","execution_time":0.012}`
	cases := bytes/(len(one)+1) + 200
	return `{"test_suites":[{"name":"s","total_time":1,"test_cases":[` + strings.TrimSuffix(strings.Repeat(one+",", cases), ",") + `]}]}`
}

// gitLabGapRealUnit is the unit a GitLab org with only "tests" enabled gets from the real planner (folded onto the
// canonical cicd key with the tests family flag), and the result of the real GitLab tests handler for a given report.
func gitLabGapRealUnit(t *testing.T, report string) (dataset, flags, result string) {
	t.Helper()
	now := time.Date(2026, 8, 12, 8, 0, 0, 0, time.UTC)
	units, err := syncplanner.BuildScheduledPlan(syncplanner.PlannerInput{
		OrgID: "org", IntegrationID: "integration", Mode: syncplanner.SyncModeIncremental, Now: now,
		Sources:  []syncplanner.PlanSource{{ID: "source", ExternalID: "123", Provider: "gitlab", FullName: "acme/api"}},
		Datasets: []syncplanner.PlanDataset{{Key: "tests"}},
	})
	if err != nil || len(units) != 1 {
		t.Fatalf("planner units=%+v err=%v", units, err)
	}
	planned := units[0]
	rawFlags, err := json.Marshal(planned.ProcessorFlags)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := effectiveDatasetKeys(planned.Dataset, rawFlags)
	if err != nil || len(keys) != 1 || keys[0] != "tests" {
		t.Fatalf("effectiveDatasetKeys(%q, %s) = %v, %v; want [tests]", planned.Dataset, rawFlags, keys, err)
	}
	capability, _ := providersync.Capability("gitlab", planned.Dataset)
	since := time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	before := time.Date(2026, 7, 31, 23, 59, 59, 0, time.UTC)
	claim := providersync.Claim{
		Unit: providersync.Unit{
			ID: "11111111-1111-4111-8111-111111111111", SyncRunID: "22222222-2222-4222-8222-222222222222", OrgID: "org-acme",
			IntegrationID: "33333333-3333-4333-8333-333333333333", SourceID: "44444444-4444-4444-8444-444444444444",
			SourceExternalID: "123", SourceName: "acme/api", Provider: "gitlab", Dataset: planned.Dataset, CostClass: capability.CostClass,
			Mode: "incremental", SinceAt: &since, BeforeAt: &before,
			CredentialID: "55555555-5555-4555-8555-555555555555", AuthSource: "integration_credential",
		},
		Owner: "66666666-6666-4666-8666-666666666666", Attempt: 1,
		LeaseExpiresAt: time.Date(2026, 7, 23, 13, 0, 0, 0, time.UTC),
	}
	client, err := providerfoundation.NewHTTPClient("gitlab", "https://gitlab.example", fakehttp.Client(&gitLabGapDoer{t: t, report: report}),
		func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	var final providersync.CompleteRouteBatch
	seen := false
	err = providersync.GitLabTestsRouteHandler{}.CollectChunks(context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC), "", func(e providersync.ChunkRouteEmission) error {
			if e.Final {
				final, seen = e.Batch, true
			}
			return nil
		})
	if err != nil || !seen {
		t.Fatalf("CollectChunks err=%v final=%v", err, seen)
	}
	rawResult, err := json.Marshal(final.Result)
	if err != nil {
		t.Fatal(err)
	}
	return planned.Dataset, string(rawFlags), string(rawResult)
}

// A GitLab unit that succeeded but skipped an over-size report is a coverage gap; a complete one is covered.
func TestGitLabTestsSkippedReportIsACoverageGap(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = instance.Close(context.Background()) }()
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	pgschema.Apply(ctx, t, pool)
	now := time.Date(2026, time.August, 12, 8, 0, 0, 0, time.UTC)
	projector, err := NewProjector(pool, WithClock(func() time.Time { return now }))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		report       string
		gaps, covers int
	}{
		{"skipped report is a gap", gitLabGapReport(gitLabGapReportCap), 1, 0},
		{"complete report is covered", gitLabGapReport(1000), 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dataset, flags, result := gitLabGapRealUnit(t, tc.report)
			resetProjectorTables(t, ctx, pool)
			f := seedProjectorFixture(t, ctx, pool, "org-gitlab-gap", now)
			if _, err := pool.Exec(ctx, `UPDATE sync_configurations SET sync_targets=$2::json WHERE id=$1`, f.ConfigID, `["tests"]`); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `DELETE FROM sync_run_units`); err != nil {
				t.Fatal(err)
			}
			if _, err := pool.Exec(ctx, `INSERT INTO sync_run_units (id,org_id,sync_run_id,integration_id,source_id,provider,dataset_key,processor_flags,since_at,before_at,status,result,created_at,updated_at,cost_class,mode,attempts)
VALUES ($1,$2,$3,$4,$5,'gitlab',$6,$7::json,$8,$9,'success',$10::jsonb,$11,$11,'rest_core','incremental',0)`,
				uuid.New(), f.OrgID, f.RunID, f.IntegrationID, f.SourceID, dataset, flags,
				now.Add(-24*time.Hour), now.Add(-time.Hour), result, now.Add(-time.Hour)); err != nil {
				t.Fatal(err)
			}
			raw, err := projector.Rebuild(ctx, f.OrgID, f.ConfigID)
			if err != nil {
				t.Fatal(err)
			}
			var p map[string]any
			if err := json.Unmarshal(raw, &p); err != nil {
				t.Fatal(err)
			}
			for _, d := range p["datasets"].([]any) {
				if m := d.(map[string]any); m["dataset_key"] == "tests" {
					wantGaps(t, m, tc.gaps)
					wantCovered(t, m, tc.covers)
					return
				}
			}
			t.Fatal("tests dataset missing")
		})
	}
}
