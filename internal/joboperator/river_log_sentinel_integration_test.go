//go:build integration

package joboperator

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// NewDirectPostgresBackend builds its River client on slog.Default(). A queue pause makes River log the queue name at
// debug level, so a queue named like a credential is a value River hands to the default logger. The test installs the
// process log chain as the default and requires that no sentinel reaches its output. A control on a raw slog handler
// must show every sentinel, or the harness cannot see a leak and the clean result proves nothing.
//
// The sentinels are synthetic and built at run time. A failing assertion names the sentinel class only and never prints
// a sentinel or a log line.
//
// The other slog.Default River clients (joboutbox NewRiverInserter, NewStrandRepair, NewTerminalDeliveryRepair) reach no
// River path that logs, so they have no output to capture; internal/cigates/emitscan R5 holds their Logger field. The
// worker client is proven in internal/workerservice.

type queueSentinel struct {
	class string
	name  string // the queue name handed to River
	piece string // the part that must never reach an output
}

type queueLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *queueLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *queueLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func queueSentinels(t *testing.T) []queueSentinel {
	t.Helper()
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	provider := hex.EncodeToString(raw)
	scanner, err := credshape.Default()
	if err != nil {
		t.Fatal(err)
	}
	probes, err := scanner.Probes()
	if err != nil {
		// The error text embeds the probe it rejected: report that it failed, never what it held.
		t.Fatal("credshape probes unavailable")
	}
	return []queueSentinel{
		{"provider", "ghp_" + provider + "AB", provider},
		{"credshape-probe", probes[0], probes[0]},
	}
}

func TestDefaultLoggerRiverClientLogsCarryNoSentinelThroughTheProcessLogChain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	t.Cleanup(cancel)
	postgres, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
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
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	sentinels := queueSentinels(t)
	for _, sentinel := range sentinels {
		if _, err := pool.Exec(ctx,
			"INSERT INTO river.river_queue (name, updated_at) VALUES ($1, now())", sentinel.name); err != nil {
			t.Fatalf("seed queue for class %s: %v", sentinel.class, err)
		}
	}

	run := func(logger *slog.Logger, out *queueLogBuffer) string {
		restore := logging.InstallDefault(logger)
		defer restore()
		backend, err := NewDirectPostgresBackend(pool, "river", registry)
		if err != nil {
			t.Fatal(err)
		}
		for _, sentinel := range sentinels {
			if err := backend.PauseQueue(ctx, sentinel.name, Mutation{}); err != nil {
				t.Fatalf("pause queue for class %s: %v", sentinel.class, err)
			}
		}
		return out.String()
	}
	const marker = "Notifying about queue state change"
	hiddenFrom := func(output string) []string {
		var named []string
		for _, sentinel := range sentinels {
			if !strings.Contains(output, sentinel.piece) {
				named = append(named, sentinel.class)
			}
		}
		return named
	}
	leakedInto := func(output string) []string {
		var named []string
		for _, sentinel := range sentinels {
			if strings.Contains(output, sentinel.piece) || strings.Contains(output, sentinel.piece[len(sentinel.piece)/2:]) {
				named = append(named, sentinel.class)
			}
		}
		return named
	}

	controlOut := &queueLogBuffer{}
	control := run(slog.New(slog.NewTextHandler(controlOut, &slog.HandlerOptions{Level: slog.LevelDebug})), controlOut)
	if !strings.Contains(control, marker) {
		t.Fatal("control: River did not log the queue pause line; the proof would be vacuous")
	}
	if hidden := hiddenFrom(control); len(hidden) != 0 {
		t.Fatalf("control: the raw handler hid sentinel classes %v; the harness cannot see a leak", hidden)
	}

	chainOut := &queueLogBuffer{}
	chain := run(logging.NewJSON(chainOut, slog.LevelDebug), chainOut)
	if !strings.Contains(chain, marker) {
		t.Fatal("chain: River did not log the queue pause line; the proof would be vacuous")
	}
	if leaked := leakedInto(chain); len(leaked) != 0 {
		t.Errorf("sentinel classes %v reached the default-logger River client output", leaked)
	}
}
