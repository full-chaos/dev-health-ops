package workersctl

import (
	"bytes"
	"context"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// clickHouseProbe is a loopback listener that stands in for ClickHouse. It
// closes every connection it accepts at once, so a ClickHouse open against it
// fails, and it records each connection. The count of accepted connections is
// the open itself, read from the network and not from any log or error text.
type clickHouseProbe struct {
	uri      string
	listener net.Listener
	mu       sync.Mutex
	accepted []string
}

func newClickHouseProbe(t *testing.T) *clickHouseProbe {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	probe := &clickHouseProbe{listener: listener, uri: "clickhouse://" + listener.Addr().String() + "/default"}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			probe.mu.Lock()
			probe.accepted = append(probe.accepted, conn.RemoteAddr().String())
			probe.mu.Unlock()
			_ = conn.Close()
		}
	}()
	return probe
}

// opens returns how many connections reached the listener. It first dials the
// listener itself and waits for that connection to be accepted: the backlog is
// served in order, so every earlier connection has been recorded by then.
func (probe *clickHouseProbe) opens(t *testing.T) int {
	t.Helper()
	sentinel, err := net.Dial("tcp", probe.listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sentinel.Close() }()
	mine := sentinel.LocalAddr().String()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		probe.mu.Lock()
		for _, remote := range probe.accepted {
			if remote == mine {
				count := len(probe.accepted) - 1
				probe.mu.Unlock()
				return count
			}
		}
		probe.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("the listener never accepted the test's own connection")
	return 0
}

// The finalize-redrive VERB, not only its marker helper, opens ClickHouse only
// for a real reset of a succeeded run. The test reads the open itself in two
// ways, neither of which depends on message wording:
//   - with CLICKHOUSE_URI pointing at a loopback listener, the listener sees a
//     connection exactly when the verb opens ClickHouse;
//   - with no ClickHouse setting in the process, the verb asks the environment
//     for CLICKHOUSE_URI exactly when it opens ClickHouse, and a real pass of a
//     succeeded run then fails.
//
// A `false &&` in front of the guard fails the first case of each; dropping the
// guard fails the others.
func TestFinalizeRedriveDispatcherOpensClickHouseOnlyForARealResetOfASucceededRun(t *testing.T) {
	registry, err := jobruntime.Load(filepath.Join("..", "..", "contracts", "jobs", "v1"))
	if err != nil {
		t.Fatal(err)
	}
	// A lazy pool: the verb builds its store on it, and a query fails at once.
	pool, err := pgxpool.New(context.Background(), "postgres://dho@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	base := []string{"--org", "00000000-0000-4000-8000-000000009475", "--from", "2026-10-12", "--to", "2026-10-12"}
	write := append([]string{"--review-evidence", "CHAOS-9475: dispatcher test", "--reason", "deploy_drain", "--correlation-id", "redrive-9475"}, base...)
	for _, test := range []struct {
		name           string
		args           []string
		wantClickHouse bool
	}{
		{"real pass of a succeeded run", write, true},
		{"real pass without include-succeeded", append([]string{"--include-succeeded=false"}, write...), false},
		{"dry run", append([]string{"--dry-run"}, base...), false},
		{"dry run with include-succeeded", append([]string{"--dry-run", "--include-succeeded=true"}, base...), false},
	} {
		dispatch := func(t *testing.T, lookup func(string) (string, bool)) (int, string) {
			t.Helper()
			runtime := commandRuntime(t, commandAuthorizer{})
			runtime.registry = registry
			runtime.pools = &postgresstore.RuntimePools{Domain: pool}
			runtime.lookup = lookup
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var stdout, stderr bytes.Buffer
			return dispatchMetricsFinalizeRedrive(ctx, runtime, test.args, &stdout, &stderr), stdout.String() + stderr.String()
		}
		t.Run(test.name+"/the listener sees the open", func(t *testing.T) {
			probe := newClickHouseProbe(t)
			code, output := dispatch(t, func(key string) (string, bool) {
				if key == "CLICKHOUSE_URI" {
					return probe.uri, true
				}
				return "", false
			})
			if code == 0 {
				t.Fatalf("exit code 0 with no reachable stores; output %s", output)
			}
			opened := probe.opens(t) > 0
			if opened != test.wantClickHouse {
				t.Fatalf("a connection reached the ClickHouse listener = %v, want %v; output %s", opened, test.wantClickHouse, output)
			}
		})
		t.Run(test.name+"/the environment is asked for CLICKHOUSE_URI", func(t *testing.T) {
			asked := false
			code, output := dispatch(t, func(key string) (string, bool) {
				if key == "CLICKHOUSE_URI" {
					asked = true
				}
				return "", false
			})
			if code == 0 {
				t.Fatalf("exit code 0 with no reachable stores; output %s", output)
			}
			if asked != test.wantClickHouse {
				t.Fatalf("CLICKHOUSE_URI was looked up = %v, want %v; output %s", asked, test.wantClickHouse, output)
			}
		})
	}
}
