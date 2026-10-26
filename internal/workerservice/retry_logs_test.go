package workerservice

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

const retryLogDialAddress = "10.0.0.10:5432"

func refusedDialToAddress() error {
	return &net.OpError{
		Op: "dial", Net: "tcp",
		Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.10"), Port: 5432},
		Err:  os.NewSyscallError("connect", syscall.ECONNREFUSED),
	}
}

type flakyQueueDatabase struct {
	*fakeWorkerDatabase
	failures int32
	calls    atomic.Int32
	err      error
}

func (database *flakyQueueDatabase) QueueReady(context.Context) error {
	if database.calls.Add(1) <= database.failures {
		return database.err
	}
	return nil
}

func TestPreclaimRetryLogsNeverCarryTheDialAddress(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	database := &flakyQueueDatabase{fakeWorkerDatabase: &fakeWorkerDatabase{}, failures: 3, err: refusedDialToAddress()}
	dependencies := &workerDependencies{database: database, logger: logger}
	registry := health.NewRegistry(2 * time.Second)
	if err := registry.RegisterRequired("queue_postgres", dependencies.queueReady); err != nil {
		t.Fatal(err)
	}
	clock := &preclaimFakeClock{}
	component := preclaimReadinessComponent{registry: registry, logger: logger, budget: time.Minute, now: clock.now, sleep: clock.sleep}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v, want nil once the endpoint answers", err)
	}
	if database.calls.Load() != 4 {
		t.Fatalf("queue check ran %d times, want 4", database.calls.Load())
	}
	if bytes.Contains(logs.Bytes(), []byte("10.0.0.10")) || bytes.Contains(logs.Bytes(), []byte(retryLogDialAddress)) {
		t.Fatalf("retry path logged the dial address: %s", logs.String())
	}
	if !bytes.Contains(logs.Bytes(), []byte("unreachable")) {
		t.Fatalf("retry path must log the bounded cause: %s", logs.String())
	}
}

func TestPreclaimNonTransientFailureKeepsItsErrorText(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	dependencies := &workerDependencies{
		database: &flakyQueueDatabase{fakeWorkerDatabase: &fakeWorkerDatabase{}, failures: 1, err: errors.New("posture mismatch detail")},
		logger:   slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	if err := dependencies.queueReady(context.Background()); err == nil {
		t.Fatal("want the genuine failure")
	}
	if !bytes.Contains(logs.Bytes(), []byte("posture mismatch detail")) {
		t.Fatalf("a non-transient failure keeps today's error text: %s", logs.String())
	}
}

func TestRiverStartRetryLogsNeverCarryTheDialAddress(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	process := newTestRiverWorkerProcess(t)
	process.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	process.budget = time.Minute
	process.sleep = riverStartSleepAfterCheckTimeout(time.Millisecond)
	var attempts atomic.Int32
	process.startClient = func(context.Context) error {
		if attempts.Add(1) < 3 {
			return refusedDialToAddress()
		}
		return nil
	}
	if err := process.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v", err)
	}
	if bytes.Contains(logs.Bytes(), []byte("10.0.0.10")) {
		t.Fatalf("river retry path logged the dial address: %s", logs.String())
	}
	exhausted := newTestRiverWorkerProcess(t)
	var exhaustedLogs bytes.Buffer
	exhausted.logger = slog.New(slog.NewJSONHandler(&exhaustedLogs, nil))
	clock := newFakeRetryClock()
	exhausted.budget, exhausted.now, exhausted.sleep = 5*time.Second, clock.Now, clock.Sleep
	exhausted.startClient = func(context.Context) error { return refusedDialToAddress() }
	if err := exhausted.Start(context.Background()); err == nil {
		t.Fatal("want budget exhausted")
	}
	if bytes.Contains(exhaustedLogs.Bytes(), []byte("10.0.0.10")) {
		t.Fatalf("river give-up logged the dial address: %s", exhaustedLogs.String())
	}
}

func TestCanceledOrDeadlineInTheChainIsNeverTransient(t *testing.T) {
	t.Parallel()
	for name, err := range map[string]error{
		"join_canceled_first":  errors.Join(context.Canceled, syscall.ECONNREFUSED),
		"join_canceled_last":   errors.Join(syscall.ECONNREFUSED, context.Canceled),
		"join_deadline_first":  errors.Join(context.DeadlineExceeded, syscall.ECONNREFUSED),
		"join_deadline_last":   errors.Join(syscall.ECONNRESET, context.DeadlineExceeded),
		"wrapped_canceled_net": &net.OpError{Op: "dial", Err: errors.Join(syscall.ECONNREFUSED, context.Canceled)},
	} {
		if isTransientDialError(err) {
			t.Errorf("%s: classified transient, want not transient (shutdown wins)", name)
		}
		if got := dependencyCheckFailed(err); health.IsTransient(got) {
			t.Errorf("%s: dependencyCheckFailed marked it transient", name)
		}
	}
}

func TestPresenceRetryLogsOneWarnPerRetryAndOneGiveUpWithoutTheDialAddress(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	presence := &presenceFake{errs: []error{refusedDialToAddress(), refusedDialToAddress()}}
	clock := &preclaimFakeClock{}
	component := workerProcessComponent{
		presence: presence, startBudget: time.Minute, now: clock.now, sleep: clock.sleep,
		logger: slog.New(slog.NewJSONHandler(&logs, nil)),
	}
	if err := component.Start(context.Background()); err != nil {
		t.Fatalf("Start() = %v", err)
	}
	if got := bytes.Count(logs.Bytes(), []byte("worker presence registration unreachable, retrying")); got != 2 {
		t.Fatalf("retry warn lines = %d, want 2: %s", got, logs.String())
	}
	if bytes.Contains(logs.Bytes(), []byte("10.0.0.10")) {
		t.Fatalf("presence retry logged the dial address: %s", logs.String())
	}
	for _, field := range []string{`"attempt"`, `"elapsed"`, `"retry_in"`} {
		if !bytes.Contains(logs.Bytes(), []byte(field)) {
			t.Fatalf("retry line lacks %s: %s", field, logs.String())
		}
	}

	var exhausted bytes.Buffer
	clock = &preclaimFakeClock{}
	failing := &presenceFake{}
	for range 40 {
		failing.errs = append(failing.errs, refusedDialToAddress())
	}
	component = workerProcessComponent{
		presence: failing, startBudget: 10 * time.Second, now: clock.now, sleep: clock.sleep,
		logger: slog.New(slog.NewJSONHandler(&exhausted, nil)),
	}
	if err := component.Start(context.Background()); err == nil {
		t.Fatal("want budget exhausted")
	}
	if got := bytes.Count(exhausted.Bytes(), []byte("worker presence registration refused")); got != 1 {
		t.Fatalf("give-up lines = %d, want 1: %s", got, exhausted.String())
	}
	if !bytes.Contains(exhausted.Bytes(), []byte("retry_budget_exhausted")) || bytes.Contains(exhausted.Bytes(), []byte("10.0.0.10")) {
		t.Fatalf("give-up line must name the reason and no address: %s", exhausted.String())
	}
}

func logRecords(t *testing.T, raw []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func TestTransientRetryAndGiveUpLinesCarryTheBoundedCauseOnRiverAndPresence(t *testing.T) {
	t.Parallel()
	var riverLogs bytes.Buffer
	process := newTestRiverWorkerProcess(t)
	process.logger = slog.New(slog.NewJSONHandler(&riverLogs, nil))
	clock := newFakeRetryClock()
	process.budget, process.now, process.sleep = 5*time.Second, clock.Now, clock.Sleep
	process.startClient = func(context.Context) error { return refusedDialToAddress() }
	if err := process.Start(context.Background()); err == nil {
		t.Fatal("want budget exhausted")
	}

	var presenceLogs bytes.Buffer
	failing := &presenceFake{}
	for range 40 {
		failing.errs = append(failing.errs, refusedDialToAddress())
	}
	pclock := &preclaimFakeClock{}
	component := workerProcessComponent{
		presence: failing, startBudget: 10 * time.Second, now: pclock.now, sleep: pclock.sleep,
		logger: slog.New(slog.NewJSONHandler(&presenceLogs, nil)),
	}
	if err := component.Start(context.Background()); err == nil {
		t.Fatal("want budget exhausted")
	}

	want := map[string]bool{
		"river workers start timed out, retrying": false,
		"river workers start refused":             false,
		"worker presence registration refused":    false,
	}
	for _, record := range append(logRecords(t, riverLogs.Bytes()), logRecords(t, presenceLogs.Bytes())...) {
		msg, _ := record["msg"].(string)
		if _, tracked := want[msg]; !tracked {
			continue
		}
		if record["cause"] != "unreachable" {
			t.Errorf("%q: cause = %v, want unreachable", msg, record["cause"])
		}
		want[msg] = true
	}
	for msg, seen := range want {
		if !seen {
			t.Errorf("no %q line was logged", msg)
		}
	}
	if bytes.Contains(riverLogs.Bytes(), []byte("10.0.0.10")) || bytes.Contains(presenceLogs.Bytes(), []byte("10.0.0.10")) {
		t.Fatalf("a retry or give-up line carries the dial address: %s %s", riverLogs.String(), presenceLogs.String())
	}
}

func TestGenuineGiveUpLinesCarryNoUnreachableCause(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	process := newTestRiverWorkerProcess(t)
	process.logger = slog.New(slog.NewJSONHandler(&logs, nil))
	process.budget = time.Minute
	process.startClient = func(context.Context) error { return errors.New("queue and workers must be configured") }
	if err := process.Start(context.Background()); err == nil {
		t.Fatal("want the genuine error")
	}
	for _, record := range logRecords(t, logs.Bytes()) {
		if record["cause"] != nil {
			t.Fatalf("a genuine failure must not claim a cause: %v", record)
		}
	}
}
