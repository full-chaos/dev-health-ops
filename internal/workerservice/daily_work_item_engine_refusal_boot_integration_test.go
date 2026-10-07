//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestWorkItemEngineFamilyRefusalDoesNotTakeDownTheDailyWorker pins the boot
// policy of the two daily families that need the work-items status mapping
// and investment config on disk (work_item_issue_type, work_item_investment).
//
// A worker that serves the metrics queue without the two artifacts used to
// fail the build of the whole daily family: no daily table was written and
// the worker did not start. The policy is the DORA one: the refusal is scoped
// to the two families. It is a policy, so every half is asserted together on
// the real buildDailyWorker, against real stores:
//
//  1. the two families are NOT constructed (no rows from a missing engine)
//  2. every other daily family IS, and the build returns no error
//  3. the refusal is LOUD: one ERROR line per family with the cause, and the
//     refused counter of each family moves
//
// The control build, same stores with the two artifacts present, constructs
// both families and logs no refusal. Without it, half 1 passes on any build
// that never constructs them.
func TestWorkItemEngineFamilyRefusalDoesNotTakeDownTheDailyWorker(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)

	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouse.Close(context.Background()) })
	chschema.Apply(ctx, t, clickhouse)

	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = postgres.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	prepareMultiReplicaDatabase(t, ctx, admin)

	registry, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	engineFamilies := []string{daily.WorkItemIssueTypeFamilyName, daily.WorkItemInvestmentFamilyName}

	type bootResult struct {
		constructed []string
		refusals    map[string]workItemEngineRefusalLogLine
		exposition  string
	}
	boot := func(t *testing.T, statusMappingPath, investmentConfigPath string) bootResult {
		t.Helper()
		domain, err := pgxpool.New(ctx, postgres.URI)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(domain.Close)
		queue, err := pgxpool.New(ctx, postgres.URI)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(queue.Close)
		database := &postgresWorkerDatabase{
			pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queue},
		}
		// Hand-built, as in the DORA boot test: config.Load is not on this
		// path, so the complexity path has no default here.
		cfg := config.Config{
			Service:                  "dev-health-worker",
			Queues:                   []string{metricsQueue},
			RiverDatabaseSchema:      "river",
			OperationalBridgeTimeout: 20 * time.Second,
			ClickHouseURI:            secrets.NewValue(clickhouse.URI),
			WorkerRemainingComplexityConfigPath: filepath.Join(
				"src", "dev_health_ops", "config", "complexity.yaml",
			),
			WorkerGithubWorkItemsStatusMappingPath:    statusMappingPath,
			WorkerGithubWorkItemsInvestmentConfigPath: investmentConfigPath,
		}
		collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
		if err != nil {
			t.Fatal(err)
		}
		var logOutput bytes.Buffer
		logger := slog.New(slog.NewJSONHandler(&logOutput, nil))

		family, err := buildDailyWorker(
			cfg, database, registry, collector, logger, river.NewWorkers())
		if err != nil {
			t.Fatalf("buildDailyWorker failed; a refusal of %v must not fail the "+
				"whole daily family: %v\nlog:\n%s", engineFamilies, err, logOutput.String())
		}
		t.Cleanup(func() {
			for _, cleanup := range family.cleanups {
				_ = cleanup()
			}
		})
		if !slices.ContainsFunc(family.handlers, func(spec jobruntime.HandlerSpec) bool {
			return spec.Kind == jobcontract.KindDailyMetricsPartition
		}) {
			t.Fatal("the daily partition kind is not registered: the daily worker is down")
		}

		result := bootResult{refusals: map[string]workItemEngineRefusalLogLine{}}
		constructedLines := 0
		for _, raw := range bytes.Split(bytes.TrimSpace(logOutput.Bytes()), []byte("\n")) {
			var line workItemEngineRefusalLogLine
			if err := json.Unmarshal(raw, &line); err != nil {
				t.Fatalf("log line %q: %v", raw, err)
			}
			switch line.Msg {
			case "native daily metrics families constructed":
				constructedLines++
				result.constructed = line.Families
			case dailyFamilyScopedRefusalLogMessage:
				result.refusals[line.Family] = line
			}
		}
		if constructedLines != 1 {
			t.Fatalf("constructed-families log lines = %d, want 1; the family set "+
				"of this worker was not read\nlog:\n%s", constructedLines, logOutput.String())
		}
		result.exposition = collector.PrometheusText()
		return result
	}
	refusedSample := func(family string, count int) string {
		return fmt.Sprintf(
			`worker_daily_metrics_native_family_outcome_total{family=%q,outcome="refused"} %d`,
			family, count,
		)
	}

	t.Run("artifacts missing", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "absent.yaml")
		result := boot(t, missing, missing)

		for _, family := range engineFamilies {
			if slices.Contains(result.constructed, family) {
				t.Errorf("family %q was constructed with no engine artifact", family)
			}
			line, logged := result.refusals[family]
			if !logged {
				t.Errorf("family %q refusal has no ERROR line: a silent skip", family)
			} else {
				if line.Level != slog.LevelError.String() {
					t.Errorf("family %q refusal logged at %q, want ERROR", family, line.Level)
				}
				if !strings.Contains(line.Error, "configuration artifact is unavailable") {
					t.Errorf("family %q refusal logged error %q, want the cause "+
						"(the artifact is unavailable)", family, line.Error)
				}
			}
			// The count is not pinned to one: the worker builds the family
			// registrations for more than one handler, and each build counts.
			if zero := refusedSample(family, 0); strings.Contains(result.exposition, zero) ||
				!strings.Contains(result.exposition, strings.TrimSuffix(zero, "0")) {
				t.Errorf("the refusal of %q has no positive signal (the refused "+
					"counter did not move); its samples:\n%s",
					family, familySamples(result.exposition, family))
			}
		}
		// The siblings: the three other work-item families, and one family of
		// another domain. An empty list would make the blast-radius claim
		// vacuous, so each one is required by name.
		for _, sibling := range []string{"work_item", "work_item_state", "work_item_attribution", "cicd"} {
			if !slices.Contains(result.constructed, sibling) {
				t.Errorf("family %q was not constructed; a refusal of %v must not "+
					"take down a family whose own dependencies are healthy: %v",
					sibling, engineFamilies, result.constructed)
			}
		}
	})

	t.Run("artifacts present", func(t *testing.T) {
		result := boot(t,
			filepath.Join("src", "dev_health_ops", "config", "status_mapping.yaml"),
			filepath.Join("src", "dev_health_ops", "config", "investment_areas.yaml"),
		)
		for _, family := range engineFamilies {
			if !slices.Contains(result.constructed, family) {
				t.Errorf("family %q was not constructed with both artifacts present: %v",
					family, result.constructed)
			}
			if _, logged := result.refusals[family]; logged {
				t.Errorf("family %q logged a refusal with both artifacts present", family)
			}
			if want := refusedSample(family, 0); !strings.Contains(result.exposition, want) {
				t.Errorf("family %q counted a refusal with both artifacts present; its samples:\n%s",
					family, familySamples(result.exposition, family))
			}
		}
	})
}

// familySamples returns the exposition lines that name the family.
func familySamples(exposition, family string) string {
	var lines []string
	for _, line := range strings.Split(exposition, "\n") {
		if strings.Contains(line, fmt.Sprintf("family=%q", family)) {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

// workItemEngineRefusalLogLine is the subset of a JSON log line this test
// reads: the constructed-families line and the scoped refusal line.
type workItemEngineRefusalLogLine struct {
	Level    string   `json:"level"`
	Msg      string   `json:"msg"`
	Family   string   `json:"family"`
	Error    string   `json:"error"`
	Families []string `json:"families"`
}
