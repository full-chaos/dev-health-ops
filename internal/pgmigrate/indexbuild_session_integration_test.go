//go:build integration

package pgmigrate_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// plantSearchPath puts public ahead of pg_catalog, so the test's public functions shadow the catalog functions of the same
// signature for that session only.
func plantSearchPath(t *testing.T, uri string) string {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", "public,pg_catalog")
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// wirePlant relays sessions to the server and rewrites result columns on the wire, the way a client reads them after a
// transaction-mode pooler moved its session: the server runs pg_catalog's own functions, and the client sees a changed
// pg_backend_pid or a pg_advisory_unlock that answers false. Its URI asks for the simple protocol, so every result
// carries its own RowDescription (column names, text format).
type wirePlant struct {
	uri         string
	pidDelta    atomic.Int32
	unlockFalse atomic.Bool
}

func newWirePlant(t *testing.T, uri string) *wirePlant {
	t.Helper()
	parsed, err := url.Parse(uri)
	if err != nil {
		t.Fatal(err)
	}
	upstream := parsed.Host
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	parsed.Host = listener.Addr().String()
	query := parsed.Query()
	query.Set("sslmode", "disable")
	query.Set("default_query_exec_mode", "simple_protocol")
	parsed.RawQuery = query.Encode()
	plant := &wirePlant{uri: parsed.String()}

	var mu sync.Mutex
	var open []net.Conn
	var relays sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		for _, conn := range open {
			_ = conn.Close()
		}
		mu.Unlock()
		relays.Wait()
	})
	relays.Add(1)
	go func() {
		defer relays.Done()
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", upstream)
			if err != nil {
				_ = client.Close()
				continue
			}
			mu.Lock()
			open = append(open, client, server)
			mu.Unlock()
			relays.Add(2)
			go func() {
				defer relays.Done()
				_, _ = io.Copy(server, client)
				_ = server.Close()
			}()
			go func() {
				defer relays.Done()
				plant.relayBackend(client, server)
				_ = client.Close()
			}()
		}
	}()
	return plant
}

// relayBackend copies the server's messages to the client and rewrites the DataRow messages of a result whose
// RowDescription names a planted column in text format.
func (p *wirePlant) relayBackend(client io.Writer, server io.Reader) {
	reader := bufio.NewReader(server)
	pidColumn, unlockColumn := -1, -1
	for {
		header := make([]byte, 5)
		if _, err := io.ReadFull(reader, header); err != nil {
			return
		}
		length := binary.BigEndian.Uint32(header[1:])
		if length < 4 {
			return
		}
		body := make([]byte, length-4)
		if _, err := io.ReadFull(reader, body); err != nil {
			return
		}
		switch header[0] {
		case 'T':
			pidColumn, unlockColumn = plantedColumns(body)
		case 'D':
			body = p.rewriteDataRow(body, pidColumn, unlockColumn)
			binary.BigEndian.PutUint32(header[1:], uint32(len(body)+4))
		}
		if _, err := client.Write(append(header, body...)); err != nil {
			return
		}
	}
}

// plantedColumns reads a RowDescription body: the text-format column named pg_backend_pid and the one named
// pg_advisory_unlock, -1 when absent.
func plantedColumns(body []byte) (pid, unlock int) {
	pid, unlock = -1, -1
	if len(body) < 2 {
		return
	}
	count := int(binary.BigEndian.Uint16(body))
	rest := body[2:]
	for column := 0; column < count; column++ {
		end := bytes.IndexByte(rest, 0)
		if end < 0 || len(rest) < end+19 {
			return -1, -1
		}
		name := string(rest[:end])
		textFormat := binary.BigEndian.Uint16(rest[end+17:]) == 0
		rest = rest[end+19:]
		switch {
		case name == "pg_backend_pid" && textFormat:
			pid = column
		case name == "pg_advisory_unlock" && textFormat:
			unlock = column
		}
	}
	return pid, unlock
}

func (p *wirePlant) rewriteDataRow(body []byte, pidColumn, unlockColumn int) []byte {
	delta, unlockFalse := int(p.pidDelta.Load()), p.unlockFalse.Load()
	if (pidColumn < 0 || delta == 0) && (unlockColumn < 0 || !unlockFalse) {
		return body
	}
	if len(body) < 2 {
		return body
	}
	count := int(binary.BigEndian.Uint16(body))
	out := append([]byte{}, body[:2]...)
	rest := body[2:]
	for column := 0; column < count; column++ {
		if len(rest) < 4 {
			return body
		}
		size := int32(binary.BigEndian.Uint32(rest))
		rest = rest[4:]
		if size < 0 {
			out = binary.BigEndian.AppendUint32(out, uint32(size))
			continue
		}
		if len(rest) < int(size) {
			return body
		}
		value := rest[:size]
		rest = rest[size:]
		switch {
		case column == pidColumn && delta != 0:
			if pid, err := strconv.Atoi(string(value)); err == nil {
				value = []byte(strconv.Itoa(pid + delta))
			}
		case column == unlockColumn && unlockFalse:
			value = []byte("f")
		}
		out = binary.BigEndian.AppendUint32(out, uint32(len(value)))
		out = append(out, value...)
	}
	return out
}

// holdTableLock holds SHARE UPDATE EXCLUSIVE on sync_run_units, so a run stops at its table-lock probe until commit.
func holdTableLock(t *testing.T, uri string) pgx.Tx {
	t.Helper()
	tx, err := connect(t, uri).Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(context.Background()) })
	if _, err := tx.Exec(context.Background(), "LOCK TABLE public.sync_run_units IN SHARE UPDATE EXCLUSIVE MODE"); err != nil {
		t.Fatal(err)
	}
	return tx
}

func probeWaits(t *testing.T, observer *pgx.Conn) func() bool {
	return func() bool {
		var waiting bool
		if err := observer.QueryRow(context.Background(), `SELECT EXISTS (SELECT 1 FROM pg_locks
WHERE locktype = 'relation' AND relation = 'public.sync_run_units'::regclass AND mode = 'ShareUpdateExclusiveLock' AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		return waiting
	}
}

// The session moves to another backend between the build lock and the first DDL (planted on the wire: pg_backend_pid()
// reads a changed pid while the run waits at its table-lock probe). The run refuses with no DDL, no retry and no cleanup: the INVALID index stays the
// same relation. The verb reports it as build_index_session_changed.
func TestBuildIndexRefusesWhenTheBackendChangesUnderTheBuildLock(t *testing.T) {
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
	plantInvalidIndex(t, uri)
	observer := connect(t, uri)
	before := streakIndexState(t, observer)
	plant := newWirePlant(t, uri)
	planted := plant.uri
	ctx := context.Background()

	gate := holdTableLock(t, uri)
	run, recorder := tracedConn(t, planted)
	logs := &syncBuffer{}
	runDone := make(chan buildOutcome, 1)
	go func() {
		result, err := pgmigrate.BuildFailureStreakIndex(ctx, run,
			pgmigrate.IndexBuildOptions{LockTimeout: 30 * time.Second, Attempts: 3, RetryDelay: time.Second, Deadline: 60 * time.Second},
			slog.New(slog.NewJSONHandler(logs, nil)))
		runDone <- buildOutcome{result, err}
	}()
	waitUntil(t, 20*time.Second, "the run's table-lock probe to wait", probeWaits(t, observer))
	plant.pidDelta.Store(1)
	if err := gate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	outcome := <-runDone
	if !errors.Is(outcome.err, pgmigrate.ErrBackendChanged) {
		t.Fatalf("result %+v err %v, want ErrBackendChanged", outcome.result, outcome.err)
	}
	if outcome.result.Attempts != 1 || outcome.result.Valid {
		t.Fatalf("result %+v, want one attempt and no retry", outcome.result)
	}
	for _, sql := range recorder.ddl() {
		if !strings.HasPrefix(sql, "LOCK TABLE") {
			t.Fatalf("the run sent %q after the backend changed", sql)
		}
	}
	releaseRefused := false
	for _, line := range logLines(t, logs.String()) {
		if msg, _ := line["msg"].(string); strings.Contains(msg, "dropped") {
			t.Fatalf("the run logged %q after the backend changed", msg)
		}
		if line["reason"] == "backend_changed" {
			releaseRefused = true
		}
	}
	if !releaseRefused {
		t.Fatalf("the release did not report the changed backend:\n%s", logs.String())
	}
	if after := streakIndexState(t, observer); after != before {
		t.Fatalf("index %+v after the refusal, want it untouched %+v", after, before)
	}

	// The refused run cannot unlock from a changed backend; its session end releases the lock.
	if err := run.Close(ctx); err != nil {
		t.Fatal(err)
	}
	plant.pidDelta.Store(0)
	gate = holdTableLock(t, uri)
	verbDone := make(chan string, 1)
	go func() {
		code, _, stderr := buildIndexVerb(t, planted, "--lock-timeout=30s", "--attempts=1", "--deadline=60s")
		if code == cli.ExitOK {
			stderr = "exit 0: " + stderr
		}
		verbDone <- stderr
	}()
	waitUntil(t, 20*time.Second, "the verb's table-lock probe to wait", probeWaits(t, observer))
	plant.pidDelta.Store(1)
	if err := gate.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if stderr := <-verbDone; strings.HasPrefix(stderr, "exit 0") || !strings.Contains(stderr, `"code":"build_index_session_changed"`) {
		t.Fatalf("verb: %s, want build_index_session_changed", stderr)
	}
	if after := streakIndexState(t, observer); after != before {
		t.Fatalf("index %+v after the verb's refusal, want it untouched %+v", after, before)
	}
}

// pg_advisory_unlock returns false at the release (planted on the wire: the server unlocks, the client reads false). The
// run reports it with its own reason, as an error and as a Warn line, never silently.
func TestBuildIndexReportsAnUnlockThatReturnsFalse(t *testing.T) {
	length := headPosition(t)
	d := newDownInstance(t)
	uri := d.at(t, length)
	observer := connect(t, uri)
	plant := newWirePlant(t, uri)
	plant.unlockFalse.Store(true)
	run, _ := tracedConn(t, plant.uri)
	logs := &syncBuffer{}
	result, err := pgmigrate.BuildFailureStreakIndex(context.Background(), run,
		pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 30 * time.Second},
		slog.New(slog.NewJSONHandler(logs, nil)))
	if !errors.Is(err, pgmigrate.ErrBuildLockLost) {
		t.Fatalf("result %+v err %v, want ErrBuildLockLost", result, err)
	}
	warned := false
	for _, line := range logLines(t, logs.String()) {
		if line["level"] == "WARN" && line["reason"] == "unlock_returned_false" {
			warned = true
		}
	}
	if !warned {
		t.Fatalf("no Warn line with reason unlock_returned_false in:\n%s", logs.String())
	}
	var free bool
	if err := observer.QueryRow(context.Background(), "SELECT pg_try_advisory_lock($1)", buildLockKey).Scan(&free); err != nil || !free {
		t.Fatalf("the build lock is still held after the run (err %v)", err)
	}
}

// Every catalog function a run calls is pg_catalog-qualified: a function of the same signature that the session
// search_path puts ahead of pg_catalog is never called, and the run builds the index and releases the build lock.
func TestBuildIndexCallsOnlyPgCatalogFunctions(t *testing.T) {
	for _, shadow := range []string{
		"pg_backend_pid() RETURNS integer",
		"pg_try_advisory_lock(key bigint) RETURNS boolean",
		"pg_advisory_unlock(key bigint) RETURNS boolean",
		"to_regclass(name text) RETURNS regclass",
		"set_config(name text, value text, is_local boolean) RETURNS text",
	} {
		name := shadow[:strings.IndexByte(shadow, '(')]
		t.Run(name, func(t *testing.T) {
			d := newDownInstance(t)
			uri := d.withoutIndex(t, "0159")
			observer := connect(t, uri)
			if _, err := observer.Exec(context.Background(), "CREATE FUNCTION public."+shadow+
				" LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''shadow "+name+" called''; END'"); err != nil {
				t.Fatal(err)
			}
			run, _ := tracedConn(t, plantSearchPath(t, uri))
			logs := &syncBuffer{}
			result, err := pgmigrate.BuildFailureStreakIndex(context.Background(), run,
				pgmigrate.IndexBuildOptions{LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 30 * time.Second},
				slog.New(slog.NewJSONHandler(logs, nil)))
			if err != nil || result.Action != "built" || !result.Valid {
				t.Fatalf("result %+v err %v, want the index built", result, err)
			}
			if strings.Contains(logs.String(), "shadow") {
				t.Fatalf("the run called the shadow %s:\n%s", name, logs.String())
			}
			if state := streakIndexState(t, observer); !state.valid {
				t.Fatalf("index %+v after the run, want it valid", state)
			}
			var free bool
			if err := observer.QueryRow(context.Background(), "SELECT pg_catalog.pg_try_advisory_lock($1)", buildLockKey).Scan(&free); err != nil || !free {
				t.Fatalf("the build lock is still held after the run (err %v)", err)
			}
		})
	}
}

// The blocker sampler qualifies its operators: a timestamptz "-" that raises and an int "=" that is always false, both
// ahead of pg_catalog on the session search_path, change neither the sample nor the blocker row it logs.
func TestBuildIndexBlockerSampleUsesPgCatalogOperators(t *testing.T) {
	d := newDownInstance(t)
	uri := d.withoutIndex(t, "0159")
	observer := connect(t, uri)
	for _, statement := range []string{
		"CREATE FUNCTION public.shadow_minus(timestamptz, timestamptz) RETURNS interval LANGUAGE plpgsql AS 'BEGIN RAISE EXCEPTION ''shadow minus called''; END'",
		"CREATE OPERATOR public.- (LEFTARG = timestamptz, RIGHTARG = timestamptz, FUNCTION = public.shadow_minus)",
		"CREATE FUNCTION public.shadow_equals(int, int) RETURNS boolean LANGUAGE sql AS 'SELECT false'",
		"CREATE OPERATOR public.= (LEFTARG = int, RIGHTARG = int, FUNCTION = public.shadow_equals)",
	} {
		if _, err := observer.Exec(context.Background(), statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	release := holdWriter(t, uri)
	defer release()
	logs := &syncBuffer{}
	run := connect(t, plantSearchPath(t, uri))
	_, err := pgmigrate.BuildFailureStreakIndex(context.Background(), run, pgmigrate.IndexBuildOptions{
		LockTimeout: 2 * time.Second, Attempts: 1, Deadline: 6 * time.Second, CleanupBudget: 2 * time.Second,
		SampleEvery: 200 * time.Millisecond, HeartbeatEvery: time.Second,
	}, slog.New(slog.NewJSONHandler(logs, nil)))
	if err == nil {
		t.Fatal("the build succeeded under a holder that never ended")
	}
	blockers := linesWith(logLines(t, logs.String()), "build-index: blocker")
	if len(blockers) == 0 || blockers[0]["state"] != "idle in transaction" {
		t.Fatalf("no blocker row for the holder with the shadow operators ahead of pg_catalog: %s", logs.String())
	}
}
