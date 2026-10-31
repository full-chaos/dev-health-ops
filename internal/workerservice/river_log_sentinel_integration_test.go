//go:build integration

package workerservice

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// The default-logger River clients are proven in internal/joboperator (river_log_sentinel_integration_test.go).
//
// River's own logger lines reach the operator through the process log chain. This proof drives real River clients
// against PostgreSQL, makes River log a value it was handed (a job's error text, a job's panic value, a queue name),
// and requires that no sentinel survives the shared redactor. Every run is paired with a control on a raw slog handler:
// the control must show the sentinel, or the harness cannot see a leak and the clean result proves nothing.
//
// The sentinels are synthetic and built at run time. A failing assertion names the sentinel class and the output kind
// and never prints a sentinel or a log line.

type riverSentinel struct {
	class  string
	needle string // the part that must never reach an output
	shaped string // the value handed to River
}

func riverSentinels(t *testing.T) []riverSentinel {
	t.Helper()
	random := func() string {
		raw := make([]byte, 12)
		if _, err := rand.Read(raw); err != nil {
			t.Fatal(err)
		}
		return hex.EncodeToString(raw)
	}
	bearer, provider, userinfo, keyvalue := random(), random(), random(), random()
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		t.Fatal(err)
	}
	return []riverSentinel{
		{"bearer", bearer, "Authorization: Bearer " + bearer},
		{"provider", provider, "token ghp_" + provider + "AB"},
		{"userinfo", userinfo, "postgres://svc:" + userinfo + "@db.internal/app"},
		{"keyvalue", keyvalue, "password=" + keyvalue},
		{"credshape-probe", probes[0], probes[0]},
	}
}

// leaked names the classes whose needle (or its second half) is in output. It returns class names only.
func leaked(output string, sentinels []riverSentinel) []string {
	var classes []string
	for _, sentinel := range sentinels {
		if strings.Contains(output, sentinel.needle) || strings.Contains(output, sentinel.needle[len(sentinel.needle)/2:]) {
			classes = append(classes, sentinel.class)
		}
	}
	return classes
}

type sentinelBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *sentinelBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *sentinelBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type sentinelJobArgs struct {
	Class string `json:"class"`
}

func (sentinelJobArgs) Kind() string { return "river.log.sentinel" }

type sentinelJobWorker struct {
	river.WorkerDefaults[sentinelJobArgs]
	byClass map[string]riverSentinel
	panics  bool
}

func (worker *sentinelJobWorker) Work(_ context.Context, job *river.Job[sentinelJobArgs]) error {
	sentinel := worker.byClass[job.Args.Class]
	if worker.panics {
		panic("worker invariant failed on " + sentinel.shaped)
	}
	return errors.New("upstream said: " + sentinel.shaped)
}

func riverSentinelDatabase(t *testing.T, ctx context.Context) *pgxpool.Pool {
	t.Helper()
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = postgres.Close(closeCtx)
	})
	pool, err := pgxpool.New(ctx, postgres.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, "CREATE SCHEMA river"); err != nil {
		t.Fatal(err)
	}
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), &rivermigrate.Config{
		Logger: slog.New(slog.DiscardHandler), Schema: "river",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		t.Fatal(err)
	}
	return pool
}

// runSentinelWorkerClient starts the shipped worker client configuration with logger, runs one failing (or panicking)
// job per sentinel, and returns everything River logged.
func runSentinelWorkerClient(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	logger *slog.Logger,
	sentinels []riverSentinel,
	panics bool,
) {
	t.Helper()
	byClass := make(map[string]riverSentinel, len(sentinels))
	for _, sentinel := range sentinels {
		byClass[sentinel.class] = sentinel
	}
	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, &sentinelJobWorker{byClass: byClass, panics: panics}); err != nil {
		t.Fatal(err)
	}
	queue := "sentinel_" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
	clientConfig := riverWorkerClientConfig(
		config.Config{WorkerInstanceID: uuid.NewString(), RiverDatabaseSchema: "river"},
		map[string]river.QueueConfig{queue: {MaxWorkers: len(sentinels)}},
		workers,
		logger,
		time.Minute,
	)
	client, err := river.NewClient(riverpgxv5.New(pool), clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	events, unsubscribe := client.Subscribe(river.EventKindJobFailed)
	defer unsubscribe()
	if err := client.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := client.Stop(stopCtx); err != nil {
			t.Errorf("stop River client: %v", err)
		}
	}
	defer stop()

	for _, sentinel := range sentinels {
		if _, err := client.Insert(ctx, sentinelJobArgs{Class: sentinel.class}, &river.InsertOpts{
			MaxAttempts: 1, Queue: queue,
		}); err != nil {
			t.Fatalf("insert job for class %s: %v", sentinel.class, err)
		}
	}
	deadline := time.After(60 * time.Second)
	for failed := 0; failed < len(sentinels); failed++ {
		select {
		case <-events:
		case <-deadline:
			t.Fatalf("only %d of %d jobs failed in time", failed, len(sentinels))
		}
	}
	stop()
}

func TestRiverWorkerClientLogsCarryNoSentinelThroughTheProcessLogChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	pool := riverSentinelDatabase(t, ctx)
	sentinels := riverSentinels(t)

	for _, mode := range []struct {
		name   string
		panics bool
		marker string
	}{
		{"job error text", false, "Job errored"},
		{"job panic value", true, "panic recovery"},
	} {
		t.Run(mode.name, func(t *testing.T) {
			control := &sentinelBuffer{}
			runSentinelWorkerClient(t, ctx, pool,
				slog.New(slog.NewTextHandler(control, &slog.HandlerOptions{Level: slog.LevelDebug})),
				sentinels, mode.panics)
			if !strings.Contains(control.String(), mode.marker) {
				t.Fatalf("control: River did not log the %q line; the proof would be vacuous", mode.marker)
			}
			if missing := missingFrom(control.String(), sentinels); len(missing) != 0 {
				t.Fatalf("control: the raw handler hid sentinel classes %v; the harness cannot see a leak", missing)
			}

			chain := &sentinelBuffer{}
			runSentinelWorkerClient(t, ctx, pool, logging.NewJSON(chain, slog.LevelDebug), sentinels, mode.panics)
			if !strings.Contains(chain.String(), mode.marker) {
				t.Fatalf("chain: River did not log the %q line; the proof would be vacuous", mode.marker)
			}
			if classes := leaked(chain.String(), sentinels); len(classes) != 0 {
				t.Errorf("sentinel classes %v reached the River logger output (%s)", classes, mode.name)
			}
		})
	}
}

func missingFrom(output string, sentinels []riverSentinel) []string {
	var classes []string
	for _, sentinel := range sentinels {
		if !strings.Contains(output, sentinel.needle) {
			classes = append(classes, sentinel.class)
		}
	}
	return classes
}
