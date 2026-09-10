package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	clickhousego "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// TestReadinessCheckFailuresLogTheCheckNameAndUnderlyingError proves
// ClickHouseReady/DomainPostgresReady/ValkeyReady no longer swallow their
// underlying dependency error -- CHAOS-5454, the same swallowed-readiness
// class CHAOS-5435 fixed for cmd/dev-health-worker and
// cmd/dev-health-reconciler. Each case builds a REAL, live-but-failing
// dependency object (no mocked interface for domain_postgres/clickhouse: a
// *pgxpool.Pool / driver.Conn dialing a refused local port, reusing
// internal/storage/postgres's and internal/storage/clickhouse's own
// TestDomainAuthorizationRejectsMissingOrUnavailablePool /
// TestOpenReturnsSanitizedUnavailableError shape; valkey's client dials
// eagerly at construction time -- see mockPingFailureServer below for why
// valkey needs a different construction) so the logged error is genuine,
// not fabricated.
// dialFailureSubstrings is the SHARED accepted set for every domain_postgres/
// clickhouse case below -- both the real-dial cases (racing RST vs timeout
// vs context-deadline, see closedPortAddr) and the deterministic
// already-expired-context cases read from this ONE slice. Sharing it is
// load-bearing, not cosmetic: codex r1 F-1 found that the deterministic
// cases originally carried their OWN copy of just the third entry, so
// reverting the widening on the real-dial cases (removing "context deadline
// exceeded" from their list) left the deterministic cases untouched and the
// suite green -- the widening itself had no regression coverage. Reading
// every case from this one slice means removing an accepted spelling here
// fails every case that can produce it, real-dial and deterministic alike.
var dialFailureSubstrings = []string{"connection refused", "i/o timeout", "context deadline exceeded"}

func TestReadinessCheckFailuresLogTheCheckNameAndUnderlyingError(t *testing.T) {
	tests := []struct {
		name string
		// check is the log record's "check" field. wantSubstrings lists the
		// REAL underlying error shapes this construction can actually
		// produce (verified live, not invented) -- asserting only non-empty
		// (as an earlier draft of this test did) would pass even if the
		// wrong message were logged; codex r1 F-2. domain_postgres/clickhouse
		// dial a closed local listener, which is refused ("connection
		// refused") on most stacks but was measured to time out instead
		// ("i/o timeout", CHAOS-5478) on at least one hosted CI runner's
		// network stack; under host load the 500ms test context can also
		// expire before the kernel's RST is even processed, which pgx/
		// clickhouse-go report as "context deadline exceeded" (CHAOS-5540,
		// hit on #2422 twice and #2423). All three are the SAME
		// dependency-unavailable class (a dial failure that never reaches a
		// live peer), so any of the three is accepted; a bare non-network
		// string (e.g. a driver-internal error unrelated to dialing) is not.
		check          string
		wantSubstrings []string
		run            func(t *testing.T, storage *productionStreamStorage) error
	}{
		{
			name:           "domain_postgres",
			check:          "domain_postgres",
			wantSubstrings: dialFailureSubstrings,
			run: func(t *testing.T, storage *productionStreamStorage) error {
				storage.domainPool = newRefusedDomainPool(t)
				storage.domainRole = "domain_role"
				storage.riverSchema = "river"
				return storage.DomainPostgresReady(contextWithTimeout(t))
			},
		},
		{
			name:           "clickhouse",
			check:          "clickhouse",
			wantSubstrings: dialFailureSubstrings,
			run: func(t *testing.T, storage *productionStreamStorage) error {
				storage.clickHouse = newRefusedClickHouseConn(t)
				return storage.ClickHouseReady(contextWithTimeout(t))
			},
		},
		{
			// CHAOS-5540: the RST-vs-timeout race the two cases above cover
			// (closedPortAddr's doc comment) is itself a race against host
			// scheduling -- under load, the 500ms test context can expire
			// before the kernel's RST is even processed, and pgx reports
			// that as "context deadline exceeded" instead of "connection
			// refused"/"i/o timeout". An already-expired context does NOT
			// replay that race -- codex r1 measured (dial_calls=0) that
			// pgxpool short-circuits on ctx.Err() before ever attempting a
			// dial, so no dial happens at all here. What it DOES prove,
			// deterministically: pgx reports that short-circuit as the
			// exact same "context deadline exceeded" text a real dial-side
			// timeout would produce, so the third accepted spelling is a
			// genuine, currently-producible error text, not a hypothetical
			// one -- without depending on a timing race to reproduce it.
			// wantSubstrings is the SHARED dialFailureSubstrings (not a
			// private one-entry copy) so reverting the widening on the
			// cases above fails this case too -- see that var's comment.
			name:           "domain_postgres_context_deadline",
			check:          "domain_postgres",
			wantSubstrings: dialFailureSubstrings,
			run: func(t *testing.T, storage *productionStreamStorage) error {
				storage.domainPool = newRefusedDomainPool(t)
				storage.domainRole = "domain_role"
				storage.riverSchema = "river"
				return storage.DomainPostgresReady(contextAlreadyExceeded(t))
			},
		},
		{
			// Same rationale as domain_postgres_context_deadline above
			// (clickhouse-go's Ping also short-circuits on ctx.Err() with
			// dial_calls=0, per codex r1's probe) -- clickhouse's Ping path.
			name:           "clickhouse_context_deadline",
			check:          "clickhouse",
			wantSubstrings: dialFailureSubstrings,
			run: func(t *testing.T, storage *productionStreamStorage) error {
				storage.clickHouse = newRefusedClickHouseConn(t)
				return storage.ClickHouseReady(contextAlreadyExceeded(t))
			},
		},
		{
			name:  "valkey",
			check: "valkey",
			// The mock server's own literal PING reply body (serveMockRESP
			// below) -- the wire-level Valkey error text, per F-2's
			// suggested fix. Not a dial failure, so only the one exact shape
			// is accepted.
			wantSubstrings: []string{"simulated ping failure"},
			run: func(t *testing.T, storage *productionStreamStorage) error {
				storage.valkey = newFailingValkeyClient(t)
				return storage.ValkeyReady(contextWithTimeout(t))
			},
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			storage := &productionStreamStorage{logger: slog.New(slog.NewJSONHandler(&logs, nil))}

			if err := testCase.run(t, storage); !errors.Is(err, errStreamDependencyUnavailable) {
				t.Fatalf("%s: error = %v, want errStreamDependencyUnavailable", testCase.name, err)
			}

			var record map[string]any
			if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
				t.Fatalf("%s: log line is not JSON (%v): %s", testCase.name, err, logs.String())
			}
			if record["check"] != testCase.check {
				t.Errorf("%s: log check = %v, want %q", testCase.name, record["check"], testCase.check)
			}
			underlying, _ := record["error"].(string)
			if underlying == "" {
				t.Fatalf("%s: log error is empty, want the underlying dependency error", testCase.name)
			}
			matched := false
			for _, want := range testCase.wantSubstrings {
				if strings.Contains(underlying, want) {
					matched = true
					break
				}
			}
			if !matched {
				t.Fatalf("%s: log error = %q, want it to contain one of %q (the real underlying failure class, not an arbitrary non-empty string)", testCase.name, underlying, testCase.wantSubstrings)
			}
		})
	}
}

// TestReadinessChecksWithNoLoggerNeverPanic proves logDependencyCheckFailure
// is a no-op, not a nil-pointer panic, when a productionStreamStorage was
// built with no logger -- the same shape cmd/dev-health-worker's
// TestReadinessCheckWithNoLoggerNeverPanics exercises for
// *workerDependencies.
func TestReadinessChecksWithNoLoggerNeverPanic(t *testing.T) {
	storage := &productionStreamStorage{
		domainPool:  newRefusedDomainPool(t),
		domainRole:  "domain_role",
		riverSchema: "river",
		clickHouse:  newRefusedClickHouseConn(t),
		valkey:      newFailingValkeyClient(t),
	}
	if err := storage.DomainPostgresReady(contextWithTimeout(t)); !errors.Is(err, errStreamDependencyUnavailable) {
		t.Fatalf("DomainPostgresReady() error = %v", err)
	}
	if err := storage.ClickHouseReady(contextWithTimeout(t)); !errors.Is(err, errStreamDependencyUnavailable) {
		t.Fatalf("ClickHouseReady() error = %v", err)
	}
	if err := storage.ValkeyReady(contextWithTimeout(t)); !errors.Is(err, errStreamDependencyUnavailable) {
		t.Fatalf("ValkeyReady() error = %v", err)
	}
}

func contextWithTimeout(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	t.Cleanup(cancel)
	return ctx
}

// contextAlreadyExceeded returns a context whose deadline is already in the
// past. pgxpool/clickhouse-go both short-circuit on ctx.Err() before
// attempting to dial (measured: dial_calls=0), so this does not replay the
// RST-vs-timeout dial race the two cases above cover -- what it DOES
// deterministically produce is the exact "context deadline exceeded" text,
// CHAOS-5540's third accepted spelling, without depending on host/network
// timing to reproduce it.
func contextAlreadyExceeded(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	t.Cleanup(cancel)
	return ctx
}

// closedPortAddr opens a listener on 127.0.0.1:0, records its address, then
// closes it -- dialing that address afterward is refused deterministically
// everywhere. Dialing a permanently-unbound port like 127.0.0.1:1 instead
// depends on how the OS/runner network stack answers an attempt to a port
// nothing ever bound: it usually answers RST ("connection refused") but on
// some hosted CI runners answers nothing at all, so the dial blocks until
// its own timeout and reports "i/o timeout" -- CHAOS-5478, hit twice (PR
// #2381 2026-09-07, PR #2389 2026-09-09), both pure flake fixed only by a
// CI rerun with no code change. A port that JUST had a live listener closed
// on it, by contrast, is guaranteed to be refused (RST) by the local
// kernel's own connection-table entry -- no dependence on runner/network
// behavior for an address nothing ever owned.
func closedPortAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

// newRefusedDomainPool dials a just-closed local port (refused
// deterministically, no live Postgres needed) with a 1ms connect timeout --
// the same construction internal/storage/postgres/domain_authorization_test.go's
// own TestDomainAuthorizationRejectsMissingOrUnavailablePool uses, updated
// per CHAOS-5478 to dial a closed listener port instead of the unbound
// 127.0.0.1:1.
func newRefusedDomainPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	config := postgres.DefaultConfig("postgres://domain:unused@" + closedPortAddr(t) + "/app")
	config.ConnectTimeout = time.Millisecond
	pool, err := postgres.New(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

// newRefusedClickHouseConn builds a driver.Conn against a just-closed local
// port. clickhouse-go's Open dials lazily (proven live: Open succeeds, the
// dial only happens on the first Ping) -- the same shape
// internal/storage/clickhouse/factory_test.go's
// TestOpenReturnsSanitizedUnavailableError uses one level up (through this
// package's own Open, which Pings once itself); here we go straight to the
// driver so ClickHouseReady's own Ping call is what fails.
func newRefusedClickHouseConn(t *testing.T) driver.Conn {
	t.Helper()
	options, err := clickhousego.ParseDSN("clickhouse://" + closedPortAddr(t) + "/default")
	if err != nil {
		t.Fatal(err)
	}
	options.DialTimeout = time.Millisecond
	conn, err := clickhousego.Open(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// newFailingValkeyClient returns a real valkey-go client connected to a
// local mock server whose PING reply is always a RESP error.
//
// Unlike Postgres/ClickHouse, valkey-go's NewClient dials AND completes its
// handshake synchronously (measured directly: pointing it at a refused port
// fails inside NewClient itself, before any Client object exists to call
// Do/Ping on) -- so the refused-port pattern above cannot reach ValkeyReady's
// own Do() call at all; there is no live-but-then-failing client to build
// without a server on the other end. mockPingFailureServer supplies the
// minimal real server: it answers HELLO with "unknown command" (forcing
// valkey-go's documented RESP2 fallback, so no RESP3 map parsing is needed),
// +OK to every other handshake command, and a RESP error specifically to
// PING -- so the failure ValkeyReady logs is a real one read off the wire,
// not fabricated.
func newFailingValkeyClient(t *testing.T) valkeygo.Client {
	t.Helper()
	addr := mockPingFailureServer(t)
	options, err := valkeygo.ParseURL("redis://" + addr + "/0")
	if err != nil {
		t.Fatal(err)
	}
	options.Dialer.Timeout = 2 * time.Second
	options.DisableCache = true
	client, err := valkeygo.NewClient(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func mockPingFailureServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveMockRESP(conn)
		}
	}()
	return ln.Addr().String()
}

func serveMockRESP(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readRESPCommand(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			if _, err := conn.Write([]byte("-ERR unknown command 'HELLO'\r\n")); err != nil {
				return
			}
		case "PING":
			if _, err := conn.Write([]byte("-ERR simulated ping failure\r\n")); err != nil {
				return
			}
		default:
			if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
				return
			}
		}
	}
}

// readRESPCommand reads one RESP array-of-bulk-strings command -- the wire
// shape every Redis/Valkey client command takes regardless of RESP2/RESP3
// negotiation. Only enough of the protocol to parse a command is
// implemented; this is a request parser, not a general RESP decoder.
func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, nil
	}
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, count)
	for i := 0; i < count; i++ {
		lengthLine, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		lengthLine = strings.TrimRight(lengthLine, "\r\n")
		length, err := strconv.Atoi(lengthLine[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length+2) // +2 for the trailing \r\n
		if _, err := readFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:length]))
	}
	return args, nil
}

func readFull(r *bufio.Reader, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := r.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
