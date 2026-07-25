package health

import (
	"bytes"
	"context"
	"errors"
	"io"
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
	server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: failing, Service: "test", Version: "dev"})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable || response.Body.String() != "{\"status\":\"metrics_unavailable\"}\n" {
		t.Fatalf("metrics failure response: status=%d body=%q", response.Code, response.Body.String())
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
