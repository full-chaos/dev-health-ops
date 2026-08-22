//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
)

// workerFamilyQueues maps each worker family to the queues it owns, as the
// shipped deployment.json groups them. Rescue-coverage registration used to be
// per family on a shared river.Workers, so any selection spanning two of these
// collided on a duplicate kind and exited at startup (CHAOS-3864).
var workerFamilyQueues = map[string][]string{
	"operational":     {"coverage", "heartbeat", "retention", "webhooks"},
	"daily":           {"metrics"},
	"workgraph":       {"workgraph", "investment"},
	"syncCoordinator": {"sync"},
	"reports":         {"reports"},
	"providerSync":    {"sync_provider"},
}

// TestEveryMultiFamilyQueueSelectionBoots drives the real production builders
// -- not the fake builders the composition unit tests use -- through
// configureWorkerDependenciesWithSources for every two-family combination and
// for each shipped multi-family group in deploy/go-workers/deployment.json.
func TestEveryMultiFamilyQueueSelectionBoots(t *testing.T) {
	t.Chdir(filepath.Join("..", ".."))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

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

	clickhouse, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = clickhouse.Close(context.Background()) })

	// CHAOS-4054: a selection containing sync_provider now really constructs
	// the provider-sync family — it used to short-circuit whenever every route
	// switch was off, which was every run of this test. Booting it for real
	// needs the dependencies it actually opens.
	valkey, err := containers.StartValkey(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = valkey.Close(context.Background()) })

	bridge := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{}`))
	}))
	t.Cleanup(bridge.Close)

	families := make([]string, 0, len(workerFamilyQueues))
	for name := range workerFamilyQueues {
		families = append(families, name)
	}
	sort.Strings(families)

	type bootCase struct {
		name   string
		queues []string
	}
	cases := make([]bootCase, 0, len(families)*len(families))
	for i := 0; i < len(families); i++ {
		for j := i + 1; j < len(families); j++ {
			queues := append(append([]string(nil), workerFamilyQueues[families[i]]...), workerFamilyQueues[families[j]]...)
			cases = append(cases, bootCase{name: families[i] + "+" + families[j], queues: queues})
		}
	}
	// The shipped groups, including "heavy", which spans three families.
	cases = append(cases,
		bootCase{name: "shipped/heavy", queues: []string{"investment", "metrics", "reports", "workgraph"}},
		bootCase{name: "shipped/ops", queues: []string{"coverage", "heartbeat", "retention", "webhooks"}},
		bootCase{name: "shipped/sync", queues: []string{"sync", "sync_provider"}},
	)

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			family, err := bootQueueSelection(
				t, ctx, postgres.URI, clickhouse.URI, valkey.URI, bridge.URL, testCase.queues,
			)
			if err != nil {
				t.Fatalf("queue selection %s did not compose: %v", strings.Join(testCase.queues, ","), err)
			}
			if len(family.handlers) == 0 {
				t.Fatalf("queue selection %s registered no handlers", strings.Join(testCase.queues, ","))
			}
			// No queue may be budgeted twice: two families both claiming one
			// queue is the composition-level shape of the same collision.
			budgeted := make(map[string]int, len(family.queues))
			for _, budget := range family.queues {
				budgeted[budget.Queue]++
			}
			for _, queue := range testCase.queues {
				if budgeted[queue] > 1 {
					t.Fatalf("queue %q budgeted %d times in selection %s",
						queue, budgeted[queue], strings.Join(testCase.queues, ","))
				}
				// sync_provider is owned by the provider-sync family, which
				// stays unconstructed until a WORKER_*_ENABLED route switch is
				// turned on. Every other queue has a default-on owner and must
				// be budgeted exactly once.
				if queue != providerUnitQueue && budgeted[queue] != 1 {
					t.Fatalf("queue %q budgeted %d times in selection %s, want exactly 1",
						queue, budgeted[queue], strings.Join(testCase.queues, ","))
				}
			}
		})
	}
}

func bootQueueSelection(
	t *testing.T,
	ctx context.Context,
	postgresURI string,
	clickhouseURI string,
	valkeyURI string,
	bridgeURL string,
	queues []string,
) (workerFamily, error) {
	t.Helper()
	domain, err := pgxpool.New(ctx, postgresURI)
	if err != nil {
		t.Fatal(err)
	}
	queuePool, err := pgxpool.New(ctx, postgresURI)
	if err != nil {
		domain.Close()
		t.Fatal(err)
	}
	database := &postgresWorkerDatabase{pools: &postgresstore.RuntimePools{Domain: domain, QueueControl: queuePool}}
	t.Cleanup(database.Close)

	concurrency := make(map[string]int, len(queues))
	for _, queue := range queues {
		concurrency[queue] = 1
	}
	// A selection that includes the provider-unit queue must carry the
	// work-item artifacts, because capability is always on and that worker
	// serves every RouteReady route (CHAOS-4054). Setting them for every
	// selection keeps the boot matrix uniform.
	cfg := validGitHubWorkItemsRuntimeConfig(t)
	cfg.Service = "dev-health-worker"
	cfg.Queues = append([]string(nil), queues...)
	cfg.WorkerQueueConcurrency = concurrency
	cfg.WorkerInstanceID = uuid.NewString()
	cfg.RiverDatabaseSchema = "river"
	cfg.ClickHouseURI = secrets.NewValue(clickhouseURI)
	cfg.ValkeyURI = secrets.NewValue(valkeyURI)
	cfg.SettingsEncryptionKey = secrets.NewValue("multi-family-encryption-key")
	cfg.OperationalBridgeURL = bridgeURL
	cfg.OperationalBridgeToken = secrets.NewValue("multi-family-token")
	cfg.OperationalBridgeTimeout = 20 * time.Second
	cfg.OperationalBridgeAllowInsecure = true
	registryTree, err := jobruntime.Load(filepath.Join("contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := buildWorkerMetrics(ctx, cfg, registryTree)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	workers := river.NewWorkers()
	family, err := composeSelectedWorkerFamilies(
		ctx, cfg, database, registryTree, metrics, logger, workers, productionWorkerDependencySources,
	)
	if err == nil {
		t.Cleanup(func() { _ = closeWorkerFamily(family) })
	}
	return family, err
}
