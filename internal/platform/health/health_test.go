package health

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type testMetricsSource struct {
	text string
	err  error
}

func (source testMetricsSource) WritePrometheus(output io.Writer) error {
	if source.err != nil {
		return source.err
	}
	_, err := io.WriteString(output, source.text)
	return err
}

func TestReadinessFailsClosedWithoutRequiredChecks(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	registry.SetReady(true)

	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, []string{"dependencies"}) {
		t.Fatalf("expected missing dependencies to fail closed, got %#v", status)
	}
}

func TestReadinessFailsClosedForGateAndRequiredChecks(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	if status := registry.Readiness(context.Background()); status.Ready || !slices.Equal(status.Failed, []string{"runtime"}) {
		t.Fatalf("expected closed runtime gate, got %#v", status)
	}
	if err := registry.RegisterRequired("queue_postgres", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("domain_postgres", func(context.Context) error {
		return errors.New("dial postgres://user:secret@db/app")
	}); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)

	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, []string{"domain_postgres"}) {
		t.Fatalf("expected sanitized dependency failure, got %#v", status)
	}
	if strings.Contains(strings.Join(status.Failed, " "), "secret") {
		t.Fatalf("readiness leaked dependency error: %#v", status)
	}
}

func TestRequiredChecksCanPassBeforePublicReadinessGateOpens(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(time.Second)
	if err := registry.RegisterRequired("database", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if status := registry.CheckRequired(context.Background()); !status.Ready || len(status.Failed) != 0 {
		t.Fatalf("preclaim check = %#v", status)
	}
	if status := registry.Readiness(context.Background()); status.Ready ||
		!slices.Equal(status.Failed, []string{"runtime"}) {
		t.Fatalf("public readiness opened early: %#v", status)
	}
}

func TestReadinessTimesOutAndContainsPanics(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(20 * time.Millisecond)
	if err := registry.RegisterRequired("timeout", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("panic", func(context.Context) error {
		panic("private panic text")
	}); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)

	started := time.Now()
	status := registry.Readiness(context.Background())
	if status.Ready || !slices.Equal(status.Failed, []string{"panic", "timeout"}) {
		t.Fatalf("expected timeout and panic failures, got %#v", status)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("readiness did not honor timeout: %s", elapsed)
	}
}

// CheckStatus.TimedOut lets a caller like the worker's preclaim-readiness
// retry loop tell a merely-slow dependency (retry it) apart from one that
// answered with a real problem (do not retry it) without CheckRequired ever
// exposing the underlying error. A check that notices its own context expire
// must report TimedOut; one that answers quickly with an unrelated error
// must not, even though both fail readiness identically.
func TestCheckRequiredDistinguishesTimeoutFromGenuineFailure(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(20 * time.Millisecond)
	if err := registry.RegisterRequired("slow", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("broken", func(context.Context) error {
		return errors.New("posture refused")
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("fine", func(context.Context) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	status := registry.CheckRequired(context.Background())
	if status.Ready {
		t.Fatalf("expected readiness to fail with slow and broken both failing: %#v", status)
	}
	byName := make(map[string]CheckStatus, len(status.Checks))
	for _, check := range status.Checks {
		byName[check.Name] = check
	}
	if check := byName["slow"]; !check.Failed || !check.TimedOut {
		t.Errorf("slow check = %#v, want Failed and TimedOut", check)
	}
	if check := byName["broken"]; !check.Failed || check.TimedOut {
		t.Errorf("broken check = %#v, want Failed and NOT TimedOut", check)
	}
	if check := byName["fine"]; check.Failed || check.TimedOut {
		t.Errorf("fine check = %#v, want neither Failed nor TimedOut", check)
	}
}

func TestReadinessSharesOneNonCooperativeExecutionAcrossCallers(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(20 * time.Millisecond)
	var invocations atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	if err := registry.RegisterRequired("stuck", func(context.Context) error {
		if invocations.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)

	const callers = 16
	results := make(chan Readiness, callers)
	go func() { results <- registry.Readiness(context.Background()) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("readiness check did not start")
	}
	for index := 1; index < callers; index++ {
		go func() { results <- registry.Readiness(context.Background()) }()
	}

	for range callers {
		select {
		case status := <-results:
			if status.Ready || !slices.Equal(status.Failed, []string{"stuck"}) {
				t.Fatalf("expected stuck check to time out, got %#v", status)
			}
		case <-time.After(time.Second):
			t.Fatal("readiness caller did not observe its timeout")
		}
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("non-cooperative check ran %d times; want exactly one in-flight execution", got)
	}

	releaseOnce.Do(func() { close(release) })
	deadline := time.Now().Add(time.Second)
	for {
		if registry.Readiness(context.Background()).Ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("released readiness check did not recover")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHTTPHandlersExposeSanitizedHealthReadinessAndMetrics(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	if err := registry.RegisterRequired("database", func(context.Context) error {
		return errors.New("postgres://user:secret@db/app")
	}); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	if err := registry.RegisterMetrics("worker", testMetricsSource{text: "worker_execution_saturation_ratio{profile=\"ops\"} 0.5\n"}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerOptions{
		Address:  "127.0.0.1:0",
		Registry: registry,
		Service:  "dev-health-worker",
		Version:  "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		path       string
		wantStatus int
		wantBody   string
	}{
		{path: "/healthz", wantStatus: http.StatusOK, wantBody: `"status":"ok"`},
		{path: "/readyz", wantStatus: http.StatusServiceUnavailable, wantBody: `"database"`},
		{path: "/metrics", wantStatus: http.StatusOK, wantBody: "worker_execution_saturation_ratio"},
	} {
		request := httptest.NewRequest(http.MethodGet, test.path, nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		if response.Code != test.wantStatus || !strings.Contains(response.Body.String(), test.wantBody) {
			t.Fatalf("%s: status=%d body=%s", test.path, response.Code, response.Body.String())
		}
		if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "postgres://") {
			t.Fatalf("%s leaked check error: %s", test.path, response.Body.String())
		}
	}
}

func TestMetricsSourcesAreStableAndFailWithoutPartialOutput(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	if err := registry.RegisterMetrics("z_source", testMetricsSource{text: "z_metric 1\n"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterMetrics("a_source", testMetricsSource{text: "a_metric 1\n"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterMetrics("a_source", testMetricsSource{text: "duplicate 1\n"}); err == nil {
		t.Fatal("duplicate metrics source unexpectedly registered")
	}
	var output bytes.Buffer
	if err := registry.WriteMetrics(&output); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "a_metric 1\nz_metric 1\n" {
		t.Fatalf("metrics source order = %q", got)
	}

	failing := NewRegistry(100 * time.Millisecond)
	if err := failing.RegisterMetrics("broken", testMetricsSource{err: errors.New("postgres://user:secret@db/app")}); err != nil {
		t.Fatal(err)
	}
	if err := failing.RegisterMetrics("healthy", testMetricsSource{text: "healthy_metric 1\n"}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: failing, Service: "test", Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	// A failing source degrades the scrape, it does not fail the endpoint: the
	// process-level gauges are most useful while a dependency is down.
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200 despite a failing source", response.Code)
	}
	body := response.Body.String()
	for _, want := range []string{
		"dev_health_runtime_live 1\n",
		"healthy_metric 1\n",
		"dev_health_runtime_metrics_source_failed{source=\"broken\"} 1\n",
		"dev_health_runtime_metrics_source_failed{source=\"healthy\"} 0\n",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body missing %q\nbody:\n%s", want, body)
		}
	}
	// The failing source's error text carries a DSN on purpose here. Degrading
	// gracefully must not become a way to leak it.
	for _, forbidden := range []string{"secret", "postgres://", "db/app"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("metrics body leaked source error text %q\nbody:\n%s", forbidden, body)
		}
	}
}

// TestWriteMetricsFailsClosedWithNoPartialBytesEvenWhenAnEarlierSourceSucceeded
// (CHAOS-4175) pins the "all-or-nothing" guarantee WriteMetrics's own doc
// comment claims, which the implementation did not actually provide: it
// wrote each source directly to the caller's output as it iterated in sorted
// name order, so any source sorting BEFORE the one that fails had already
// landed real bytes in output by the time the error was returned. A registry
// with exactly one metrics source that always fails (the shape every
// pre-CHAOS-4175 caller of WriteMetrics happened to have) could never
// observe this: the leak only appears once a second, earlier-sorting,
// always-succeeding source is registered alongside a later-sorting failing
// one -- precisely the shape adding a new always-on counter (like the
// zero-unit-finalization one) to a registry that already has a
// database-backed required source creates.
func TestWriteMetricsFailsClosedWithNoPartialBytesEvenWhenAnEarlierSourceSucceeded(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	// "a_source" sorts before "b_source_broken" -- WriteMetrics iterates
	// registered sources in sorted name order, so this is the exact ordering
	// that let a healthy source's bytes reach output before the failure was
	// discovered.
	if err := registry.RegisterMetrics("a_source", testMetricsSource{text: "a_metric 1\n"}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterMetrics("b_source_broken", testMetricsSource{err: errors.New("boom")}); err != nil {
		t.Fatal(err)
	}

	var output bytes.Buffer
	err := registry.WriteMetrics(&output)
	if err == nil {
		t.Fatal("WriteMetrics() error = nil, want the broken source's error")
	}
	if output.Len() != 0 {
		t.Fatalf("WriteMetrics() leaked %d partial bytes into output despite failing: %q",
			output.Len(), output.String())
	}
}

func TestMetricsExposesPerCheckFailureGauge(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	// Registered out of alphabetical order so a pass requires the handler to
	// sort, not merely echo registration order.
	if err := registry.RegisterRequired("queue_postgres", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("domain_postgres", func(context.Context) error {
		return errors.New("dial postgres://user:secret@db/app")
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("cache_redis", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d", response.Code)
	}
	body := response.Body.String()

	// dev_health_runtime_ready must still collapse to the aggregate bit,
	// unchanged, so existing alerts wired to it keep working.
	if !strings.Contains(body, "dev_health_runtime_ready 0\n") {
		t.Fatalf("aggregate ready gauge changed semantics:\n%s", body)
	}

	// A failing required check must be named, not just reflected in the
	// aggregate boolean.
	if !strings.Contains(body, `dev_health_runtime_check_failed{check="domain_postgres"} 1`) {
		t.Fatalf("failing check was not named in per-check gauge:\n%s", body)
	}
	// Passing checks must emit an explicit 0, not be left absent, so an
	// alert can fire on the absence of a failure value rather than the
	// absence of a series.
	for _, want := range []string{
		`dev_health_runtime_check_failed{check="cache_redis"} 0`,
		`dev_health_runtime_check_failed{check="queue_postgres"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("passing check missing explicit 0 series %q:\n%s", want, body)
		}
	}

	if got := strings.Count(body, "# TYPE dev_health_runtime_check_failed gauge"); got != 1 {
		t.Fatalf("expected exactly one TYPE line for dev_health_runtime_check_failed, got %d", got)
	}

	// Output ordering must be deterministic (sorted by check name), not
	// registration order, so scrapes and diffs are stable.
	first := strings.Index(body, `check="cache_redis"`)
	second := strings.Index(body, `check="domain_postgres"`)
	third := strings.Index(body, `check="queue_postgres"`)
	if first < 0 || second < 0 || third < 0 || !(first < second && second < third) {
		t.Fatalf("per-check gauge lines are not sorted by name:\n%s", body)
	}

	if strings.Contains(body, "secret") || strings.Contains(body, "postgres://") {
		t.Fatalf("per-check gauge leaked dependency error text:\n%s", body)
	}
}

func TestMetricsPerCheckGaugeOrderingIsStableAcrossScrapes(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	for _, name := range []string{"zzz_last", "aaa_first", "mmm_middle"} {
		if err := registry.RegisterRequired(name, func(context.Context) error { return nil }); err != nil {
			t.Fatal(err)
		}
	}
	registry.SetReady(true)
	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}

	extractCheckLines := func() []string {
		request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
		response := httptest.NewRecorder()
		server.Handler().ServeHTTP(response, request)
		var lines []string
		for _, line := range strings.Split(response.Body.String(), "\n") {
			if strings.HasPrefix(line, "dev_health_runtime_check_failed{") {
				lines = append(lines, line)
			}
		}
		return lines
	}

	first := extractCheckLines()
	second := extractCheckLines()
	third := extractCheckLines()
	if len(first) != 3 {
		t.Fatalf("expected 3 per-check gauge lines, got %d: %v", len(first), first)
	}
	if !slices.Equal(first, second) || !slices.Equal(second, third) {
		t.Fatalf("per-check gauge ordering was not deterministic across scrapes:\n%v\n%v\n%v", first, second, third)
	}
	want := []string{
		`dev_health_runtime_check_failed{check="aaa_first"} 0`,
		`dev_health_runtime_check_failed{check="mmm_middle"} 0`,
		`dev_health_runtime_check_failed{check="zzz_last"} 0`,
	}
	if !slices.Equal(first, want) {
		t.Fatalf("per-check gauge lines = %v, want %v", first, want)
	}
}

func TestReadinessCheckNamesRejectValuesThatWouldCorruptExpositionFormat(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	for _, unsafeName := range []string{
		`bad"name`,
		"bad\nname",
		"bad name",
		"",
	} {
		if err := registry.RegisterRequired(unsafeName, func(context.Context) error { return nil }); err == nil {
			t.Fatalf("expected registering unsafe check name %q to fail checkNamePattern", unsafeName)
		}
	}
	if err := registry.RegisterRequired("safe_name", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := registry.RequiredCount(); got != 1 {
		t.Fatalf("expected only the safe name to register, got %d required checks", got)
	}
	registry.SetReady(true)

	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	body := response.Body.String()

	// A rejected registration must never reach the exposition format: a raw
	// quote or newline in a label value would corrupt the Prometheus text
	// format for every metric after it in the scrape.
	if strings.Contains(body, `bad"name`) || strings.Contains(body, "bad\nname") || strings.Contains(body, "bad name") {
		t.Fatalf("unsafe check name reached /metrics output:\n%s", body)
	}
	if !strings.Contains(body, `dev_health_runtime_check_failed{check="safe_name"} 0`) {
		t.Fatalf("safe check name missing from /metrics output:\n%s", body)
	}
}

func TestServerStartsOnEphemeralPortAndShutsDown(t *testing.T) {
	t.Parallel()

	registry := NewRegistry(100 * time.Millisecond)
	if err := registry.RegisterRequired("runtime_dependency", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "test", Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)

	response, err := http.Get("http://" + server.Address() + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected ready status: %d", response.StatusCode)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if registry.Readiness(context.Background()).Ready {
		t.Fatal("shutdown must close readiness")
	}
}

// CHAOS-6883: the registry's refusal log.
func TestRegistryLogsRefusalsOnFirstRepeatRecoveryAndNeverTheErrorText(t *testing.T) {
	t.Parallel()
	var logs bytes.Buffer
	registry := NewRegistry(time.Second)
	registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	var failing atomic.Bool
	failing.Store(true)
	if err := registry.RegisterRequired("dep_a", func(context.Context) error {
		if failing.Load() {
			return errors.New("dial postgres://u:secret@db.internal/x: refused")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterRequired("dep_ok", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)

	for range 5 {
		if status := registry.Readiness(context.Background()); status.Ready {
			t.Fatal("dep_a must refuse")
		}
		registry.flushRefusalLogs()
	}
	if got := strings.Count(logs.String(), `"msg":"readiness check refused"`); got != 1 {
		t.Fatalf("5 consecutive refusals inside one interval logged %d lines, want 1 (first only): %s", got, logs.String())
	}
	if strings.Contains(logs.String(), "dep_ok") {
		t.Fatal("a passing check must not be logged as refused")
	}
	for _, want := range []string{`"check":"dep_a"`, `"cause":"error"`, `"timed_out":false`, `"consecutive_refusals":1`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("first refusal line lacks %s: %s", want, logs.String())
		}
	}
	for _, forbidden := range []string{"secret", "db.internal", "postgres://"} {
		if strings.Contains(logs.String(), forbidden) {
			t.Errorf("the refusal log leaked %q", forbidden)
		}
	}

	// A check that keeps refusing is logged again once the interval has passed.
	registry.refusalMu.Lock()
	registry.refusing["dep_a"].lastLog = time.Now().Add(-2 * refusalLogInterval)
	registry.refusalMu.Unlock()
	registry.Readiness(context.Background())
	registry.flushRefusalLogs()
	if got := strings.Count(logs.String(), `"msg":"readiness check refused"`); got != 2 {
		t.Fatalf("a refusal past the interval logged %d lines total, want 2", got)
	}
	if !strings.Contains(logs.String(), `"consecutive_refusals":6`) {
		t.Errorf("the repeat line must carry the running count: %s", logs.String())
	}

	// Recovery is logged once, and the next refusal starts a fresh run.
	failing.Store(false)
	registry.Readiness(context.Background())
	registry.flushRefusalLogs()
	registry.Readiness(context.Background())
	if got := strings.Count(logs.String(), `"msg":"readiness check recovered"`); got != 1 {
		t.Fatalf("recovery logged %d lines, want 1: %s", got, logs.String())
	}
	var recovered string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, `"msg":"readiness check recovered"`) {
			recovered = line
		}
	}
	for _, want := range []string{`"check":"dep_a"`, `"last_cause":"error"`, `"consecutive_refusals":6`} {
		if !strings.Contains(recovered, want) {
			t.Errorf("the recovery line itself lacks %s: %q", want, recovered)
		}
	}
	failing.Store(true)
	registry.Readiness(context.Background())
	registry.flushRefusalLogs()
	if got := strings.Count(logs.String(), `"msg":"readiness check refused"`); got != 3 {
		t.Fatalf("a new refusal after recovery must log immediately, total %d want 3", got)
	}
}

func TestRegistryRefusalCauseIsBounded(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		check CheckFunc
		cause string
		timed bool
	}{
		{"deadline", func(context.Context) error { return fmt.Errorf("query: %w", context.DeadlineExceeded) }, "timeout", true},
		{"canceled", func(context.Context) error { return context.Canceled }, "canceled", true},
		{"panic", func(context.Context) error { panic("boom") }, "panic", false},
		{"error", func(context.Context) error { return errors.New("x") }, "error", false},
	} {
		var logs bytes.Buffer
		registry := NewRegistry(time.Second)
		registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
		if err := registry.RegisterRequired("dep", test.check); err != nil {
			t.Fatal(err)
		}
		registry.SetReady(true)
		registry.Readiness(context.Background())
		registry.flushRefusalLogs()
		if want := fmt.Sprintf(`"cause":%q`, test.cause); !strings.Contains(logs.String(), want) {
			t.Errorf("%s: want %s in %s", test.name, want, logs.String())
		}
		if want := fmt.Sprintf(`"timed_out":%t`, test.timed); !strings.Contains(logs.String(), want) {
			t.Errorf("%s: want %s in %s", test.name, want, logs.String())
		}
	}
	// The caller's own wait expiring before any answer.
	var logs bytes.Buffer
	registry := NewRegistry(30 * time.Millisecond)
	registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
	release := make(chan struct{})
	defer close(release)
	if err := registry.RegisterRequired("slow", func(context.Context) error { <-release; return nil }); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	registry.Readiness(context.Background())
	registry.flushRefusalLogs()
	if !strings.Contains(logs.String(), `"cause":"wait_expired"`) {
		t.Errorf("a check that never answered must log wait_expired: %s", logs.String())
	}
}

// Without a logger the registry behaves exactly as before.
func TestRegistryWithoutARefusalLoggerStaysSilent(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(time.Second)
	if err := registry.RegisterRequired("dep", func(context.Context) error { return errors.New("x") }); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	if status := registry.Readiness(context.Background()); status.Ready {
		t.Fatal("dep must refuse")
	}
}

// r2 P1: the refusal log must never hold up (or break) /readyz. A sink that
// blocks or panics is written by a bounded background goroutine.
type blockingSink struct {
	release chan struct{}
	mu      sync.Mutex
	buf     bytes.Buffer
}

func (b *blockingSink) Write(p []byte) (int, error) {
	<-b.release
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *blockingSink) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRegistryReadinessDoesNotWaitForABlockedRefusalLogSink(t *testing.T) {
	t.Parallel()
	sink := &blockingSink{release: make(chan struct{})}
	registry := NewRegistry(time.Second)
	registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(sink, nil)))
	if err := registry.RegisterRequired("dep", func(context.Context) error { return errors.New("x") }); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	returned := make(chan Readiness, 1)
	go func() { returned <- registry.Readiness(context.Background()) }()
	select {
	case status := <-returned:
		if status.Ready {
			t.Fatal("dep must refuse")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Readiness waited on a blocked log sink")
	}
	close(sink.release)
	registry.flushRefusalLogs()
	if !strings.Contains(sink.String(), `"msg":"readiness check refused"`) {
		t.Fatalf("the refusal line must still be written once the sink unblocks: %s", sink.String())
	}
}

type panickingHandler struct{ slog.Handler }

func (panickingHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (panickingHandler) Handle(context.Context, slog.Record) error { panic("log sink panic") }
func (h panickingHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h panickingHandler) WithGroup(string) slog.Handler           { return h }

func TestRegistryReadinessSurvivesAPanickingRefusalLogSink(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(time.Second)
	registry.SetRefusalLogger(slog.New(panickingHandler{}))
	if err := registry.RegisterRequired("dep", func(context.Context) error { return errors.New("x") }); err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	if status := registry.Readiness(context.Background()); status.Ready || len(status.Failed) != 1 {
		t.Fatalf("readiness outcome changed under a panicking sink: %+v", status)
	}
	registry.flushRefusalLogs() // must return: the panic is recovered, the slot is released.
	if got := registry.refusalInFlight.Load(); got != 0 {
		t.Fatalf("the in-flight slot leaked after a panic: %d", got)
	}
}

func TestRegistryDropsRefusalLogsInsteadOfStackingGoroutinesBehindAStuckSink(t *testing.T) {
	t.Parallel()
	sink := &blockingSink{release: make(chan struct{})}
	registry := NewRegistry(time.Second)
	registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(sink, nil)))
	for i := range refusalMaxInFlight + 3 {
		name := fmt.Sprintf("dep_%d", i)
		if err := registry.RegisterRequired(name, func(context.Context) error { return errors.New("x") }); err != nil {
			t.Fatal(err)
		}
	}
	registry.SetReady(true)
	// Each Readiness call refuses every dep once; repeated calls with fresh
	// per-check state would log again only past the interval, so drive distinct
	// batches by clearing the limiter between polls.
	for range refusalMaxInFlight + 3 {
		registry.Readiness(context.Background())
		registry.refusalMu.Lock()
		registry.refusing = nil
		registry.refusalMu.Unlock()
	}
	if got := registry.refusalInFlight.Load(); got > refusalMaxInFlight {
		t.Fatalf("%d refusal writers in flight, cap %d", got, refusalMaxInFlight)
	}
	if registry.refusalDropped.Load() == 0 {
		t.Fatal("writes beyond the cap must be dropped and counted")
	}
	close(sink.release)
	registry.flushRefusalLogs()
}

// CHAOS-6955: a startup path that cannot serve the HTTP surface (the worker's preclaim-readiness)
// needs each failed check's CLASS, not only its name, or an exit reads "these four checks failed"
// with no way to tell a slow dependency from a wrong one. Cause is a bounded vocabulary, never text.
func TestCheckRequiredReportsTheBoundedFailureClassOfEachCheck(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(30 * time.Millisecond)
	for name, check := range map[string]CheckFunc{
		"slow":     func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
		"broken":   func(context.Context) error { return errors.New("password=secret refused") },
		"panicky":  func(context.Context) error { panic("boom") },
		"canceled": func(context.Context) error { return context.Canceled },
		"fine":     func(context.Context) error { return nil },
	} {
		if err := registry.RegisterRequired(name, check); err != nil {
			t.Fatal(err)
		}
	}
	status := registry.CheckRequired(context.Background())
	byName := map[string]CheckStatus{}
	for _, check := range status.Checks {
		byName[check.Name] = check
	}
	if got := byName["slow"].Cause; got != "timeout" && got != "wait_expired" {
		t.Errorf("slow Cause = %q, want timeout or wait_expired", got)
	}
	for name, want := range map[string]string{"broken": "error", "panicky": "panic", "canceled": "canceled", "fine": ""} {
		if got := byName[name].Cause; got != want {
			t.Errorf("%s Cause = %q, want %q", name, got, want)
		}
	}
	for _, check := range status.Checks {
		if strings.Contains(check.Cause, "secret") || strings.Contains(check.Cause, "boom") {
			t.Errorf("%s Cause leaked error text: %q", check.Name, check.Cause)
		}
	}
}
