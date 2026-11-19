package providersync

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const gitLabArtifactCapCounter = "gitlab_tests_artifact_jobs_over_cap_total"

func gitLabArtifactCapCount(t *testing.T) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := meterReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != gitLabArtifactCapCounter {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want an int64 sum", m.Name, m.Data)
			}
			for _, point := range sum.DataPoints {
				if outcome, _ := point.Attributes.Value("outcome"); outcome.AsString() != "artifact_jobs_over_cap" {
					t.Fatalf("outcome %q, want artifact_jobs_over_cap", outcome.AsString())
				}
				total += point.Value
			}
		}
	}
	return total
}

func TestGitLabTestsRouteSignalsArtifactJobsOverTheCap(t *testing.T) {
	coverage := githubTestsZip(t, map[string]string{"coverage.info": githubTestsLCOVFixture})
	jobs := func(count int) []gitLabAllRefArtifactJob {
		out := make([]gitLabAllRefArtifactJob, 0, count)
		for id := 100; id < 100+count; id++ {
			out = append(out, gitLabAllRefArtifactJob{ID: strconv.Itoa(id)})
		}
		return out
	}
	for _, cell := range []struct {
		name        string
		counts      []int
		wantWarns   []string
		wantReasons []string
	}{
		{"at_the_cap", []int{gitLabTestsMaxArtifacts}, nil, []string{}},
		{"one_over_the_cap", []int{gitLabTestsMaxArtifacts + 1}, []string{"9001"}, []string{"per_run_cap"}},
		{"two_pipelines_over_the_cap", []int{gitLabTestsMaxArtifacts + 1, gitLabTestsMaxArtifacts + 3}, []string{"9001", "9002"}, []string{"per_run_cap"}},
	} {
		t.Run(cell.name, func(t *testing.T) {
			var buf bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
			t.Cleanup(func() { slog.SetDefault(previous) })
			before := gitLabArtifactCapCount(t)
			pipelines := gitLabAllRefPipelines()[:len(cell.counts)]
			for index, count := range cell.counts {
				pipelines[index].ArtifactJobs = jobs(count)
			}
			walk, _ := gitLabAllRefCollectChunks(t, &gitLabAllRefDoer{t: t, archive: coverage, pipelines: pipelines}, nativeTestClaim("gitlab", "tests"))
			if walk.err != nil {
				t.Fatal(walk.err)
			}
			if delta := gitLabArtifactCapCount(t) - before; delta != int64(len(cell.wantWarns)) {
				t.Fatalf("counter delta %d, want %d", delta, len(cell.wantWarns))
			}
			var warns []string
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				if strings.Contains(line, "level=WARN") && strings.Contains(line, "artifact jobs over the cap") {
					warns = append(warns, line)
				}
			}
			if len(warns) != len(cell.wantWarns) {
				t.Fatalf("got %d cap WARN lines %q, want %d", len(warns), warns, len(cell.wantWarns))
			}
			for index, run := range cell.wantWarns {
				for _, want := range []string{"run=" + run, "jobs=" + strconv.Itoa(cell.counts[index]), "cap=" + strconv.Itoa(gitLabTestsMaxArtifacts)} {
					if !strings.Contains(warns[index], want) {
						t.Fatalf("WARN %q lacks %q", warns[index], want)
					}
				}
			}
			rows := gitLabCompletenessRows(t, walk.batch)
			if len(rows) != len(cell.counts) {
				t.Fatalf("rows %+v, want %d", rows, len(cell.counts))
			}
			for index, row := range rows {
				over := cell.counts[index] > gitLabTestsMaxArtifacts
				if row.Complete == over || !slices.Equal(row.Reasons, map[bool][]string{true: {"per_run_cap"}, false: {}}[over]) {
					t.Fatalf("row %+v, want complete=%v reasons %v", row, !over, cell.wantReasons)
				}
			}
		})
	}
}
