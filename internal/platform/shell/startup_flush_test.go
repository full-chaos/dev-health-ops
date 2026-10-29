package shell

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"go.opentelemetry.io/otel"
	colmetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"

	"github.com/full-chaos/dev-health-ops/internal/api/apimetrics"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
)

// flushSource is a push whose flush takes delay on the bubble clock, then waits for hold (when set), then returns err.
type flushSource struct {
	delay    time.Duration
	hold     chan struct{}
	err      error
	flushes  atomic.Int32
	returned atomic.Bool
	entry    atomic.Pointer[time.Time]
	deadline atomic.Pointer[time.Time]
}

func (s *flushSource) Shutdown(context.Context) error { return nil }

func (s *flushSource) ForceFlush(ctx context.Context) error {
	s.flushes.Add(1)
	entry := time.Now()
	s.entry.Store(&entry)
	if deadline, ok := ctx.Deadline(); ok {
		s.deadline.Store(&deadline)
	}
	time.Sleep(s.delay)
	if s.hold != nil {
		<-s.hold
	}
	s.returned.Store(true)
	return s.err
}

// shutdownOnly is a source with no push to flush.
type shutdownOnly struct{}

func (shutdownOnly) Shutdown(context.Context) error { return nil }

func newFlush(source metricsShutdowner) (metricsFlush, *bytes.Buffer) {
	var logs bytes.Buffer
	return metricsFlush{source: source, logger: slog.New(slog.NewTextHandler(&logs, nil))}, &logs
}

// TestMetricsFlushStartExportsOnceBeforeItReturns: Start flushes the push exactly once, under a bounded context, and
// returns only after that flush returned, so the components after it (the listeners) start after the seeds left.
func TestMetricsFlushStartExportsOnceBeforeItReturns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &flushSource{delay: 10 * time.Millisecond}
		flush, logs := newFlush(source)
		if err := flush.Start(context.Background()); err != nil {
			t.Fatalf("Start = %v, want nil", err)
		}
		if got := source.flushes.Load(); got != 1 {
			t.Fatalf("Start flushed %d times, want 1", got)
		}
		if !source.returned.Load() {
			t.Fatal("Start returned before its flush returned: a listener could serve before the seeds are exported")
		}
		entry, deadline := source.entry.Load(), source.deadline.Load()
		if deadline == nil {
			t.Fatal("the startup flush context has no deadline: an unreachable collector could hold the start")
		}
		if budget := deadline.Sub(*entry); budget > metricsFlushTimeout {
			t.Errorf("the startup flush budget is %v, want at most %v", budget, metricsFlushTimeout)
		}
		if logs.Len() != 0 {
			t.Errorf("a good flush logged: %s", logs.String())
		}
	})
}

// TestMetricsFlushStartIsBoundedWhenTheFlushIgnoresItsContext: a flush that does not return (the SDK reader may wait
// on its export loop first) costs the start its bounded wait and a warning, never a failed start: Start comes back
// while the flush is still held.
func TestMetricsFlushStartIsBoundedWhenTheFlushIgnoresItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &flushSource{hold: make(chan struct{})}
		defer close(source.hold)
		flush, logs := newFlush(source)
		if err := flush.Start(context.Background()); err != nil {
			t.Fatalf("Start = %v, want nil: an unreachable collector is not a start failure", err)
		}
		if source.returned.Load() {
			t.Error("the stuck flush returned before Start did: Start must stop waiting at its budget, not at the flush")
		}
		if got := strings.Count(logs.String(), "otlp metrics startup flush abandoned"); got != 1 {
			t.Errorf("abandon warnings = %d, want 1; logs:\n%s", got, logs.String())
		}
	})
}

// TestMetricsFlushStartWarnsAndStartsOnAFlushError: a failed flush is one warning and the process starts.
func TestMetricsFlushStartWarnsAndStartsOnAFlushError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &flushSource{err: errors.New("collector refused")}
		flush, logs := newFlush(source)
		if err := flush.Start(context.Background()); err != nil {
			t.Fatalf("Start = %v, want nil: a failed flush is not a start failure", err)
		}
		if got := strings.Count(logs.String(), "otlp metrics startup flush failed"); got != 1 || !strings.Contains(logs.String(), "collector refused") {
			t.Errorf("want one failure warning naming the error; logs:\n%s", logs.String())
		}
	})
}

// TestMetricsFlushStartAddsNoWaitWithoutAPushReader: a process without a push reader (OTEL_METRICS_ENABLED=false, or no
// source when the push could not be built) starts with no flush, no wait on the clock and no log line.
func TestMetricsFlushStartAddsNoWaitWithoutAPushReader(t *testing.T) {
	for name, source := range map[string]metricsShutdowner{
		"nil apimetrics source": (*apimetrics.Source)(nil),
		"source without flush":  shutdownOnly{},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				flush, logs := newFlush(source)
				start := time.Now()
				if err := flush.Start(context.Background()); err != nil {
					t.Fatalf("Start = %v, want nil", err)
				}
				if waited := time.Since(start); waited != 0 {
					t.Errorf("Start waited %v on the bubble clock without a push reader, want 0", waited)
				}
				if logs.Len() != 0 {
					t.Errorf("Start logged without a push reader: %s", logs.String())
				}
			})
		})
	}
}

// heldCollector records every export and holds the first one until released.
type heldCollector struct {
	colmetricpb.UnimplementedMetricsServiceServer
	mu      sync.Mutex
	metrics []*metricpb.Metric
	first   sync.Once
	entered chan struct{}
	release chan struct{}
}

func (c *heldCollector) Export(_ context.Context, req *colmetricpb.ExportMetricsServiceRequest) (*colmetricpb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	for _, resource := range req.GetResourceMetrics() {
		for _, scope := range resource.GetScopeMetrics() {
			c.metrics = append(c.metrics, scope.GetMetrics()...)
		}
	}
	c.mu.Unlock()
	c.first.Do(func() {
		close(c.entered)
		<-c.release
	})
	return &colmetricpb.ExportMetricsServiceResponse{}, nil
}

func (c *heldCollector) seededValues() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var values []int64
	for _, m := range c.metrics {
		if m.GetName() != startupSeedName {
			continue
		}
		if sum, ok := m.GetData().(*metricpb.Metric_Sum); ok {
			for _, point := range sum.Sum.GetDataPoints() {
				values = append(values, point.GetAsInt())
			}
		}
	}
	return values
}

const startupSeedName = "shelltest_startup_seeded"

// TestTheShellExportsTheSeedsBeforeItsListenersServe runs a real shell process (a re-execution of this test binary)
// whose dependency step seeds a counter at 0, against a real collector, with a reader interval of ten minutes so no
// tick falls in the test. The collector holds the first export: while it is held, the operator listener must not
// accept a connection yet; once released, the listener serves and the collector already holds the seed at 0.
func TestTheShellExportsTheSeedsBeforeItsListenersServe(t *testing.T) {
	if os.Getenv("SHELL_OTLP_HELPER") == "startupflush" {
		runShellStartupFlushHelper()
		return
	}
	collector := &heldCollector{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(collector.release) }) }
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	colmetricpb.RegisterMetricsServiceServer(server, collector)
	go func() { _ = server.Serve(lis) }()
	defer server.Stop()
	defer release()
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpAddress := httpListener.Addr().String()
	_ = httpListener.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestTheShellExportsTheSeedsBeforeItsListenersServe$")
	command.Env = append(os.Environ(),
		"SHELL_OTLP_HELPER=startupflush",
		"SHELL_OTLP_HTTP_ADDR="+httpAddress,
		"OTEL_ENABLED=true",
		"OTEL_METRICS_ENABLED=true",
		"OTEL_SAMPLE_RATE=0",
		"OTEL_EXPORTER_OTLP_ENDPOINT="+lis.Addr().String(),
		"OTEL_METRIC_EXPORT_INTERVAL=600000",
	)
	var output syncBuffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = command.Process.Kill(); _ = command.Wait() }()

	client := &http.Client{Timeout: 300 * time.Millisecond}
	healthz := func() bool {
		response, err := client.Get("http://" + httpAddress + "/healthz")
		if err != nil {
			return false
		}
		_ = response.Body.Close()
		return true
	}
	deadline := time.After(20 * time.Second)
waitExport:
	for {
		select {
		case <-collector.entered:
			break waitExport
		case <-deadline:
			t.Fatalf("no export reached the collector within 20 s; process output:\n%s", output.String())
		case <-time.After(20 * time.Millisecond):
			if healthz() {
				t.Fatalf("the operator listener serves and the collector holds no export: the seeds wait for the first tick; process output:\n%s", output.String())
			}
		}
	}
	if conn, err := net.DialTimeout("tcp", httpAddress, 300*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Errorf("the operator listener accepts a connection while the startup flush is still in flight")
	}
	release()
	up := time.Now().Add(20 * time.Second)
	for !healthz() {
		if time.Now().After(up) {
			t.Fatalf("the operator listener never served after the flush; process output:\n%s", output.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := collector.seededValues(); len(got) != 1 || got[0] != 0 {
		t.Errorf("when the listener first served the collector held %s = %v, want exactly [0]; process output:\n%s", startupSeedName, got, output.String())
	}
}

func runShellStartupFlushHelper() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var stdout, stderr bytes.Buffer
	lookup := testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR":        os.Getenv("SHELL_OTLP_HTTP_ADDR"),
		"DEV_HEALTH_SHUTDOWN_TIMEOUT": "1s",
	})
	seed := func(ctx context.Context, _ config.Config, _ *health.Registry) ([]lifecycle.Component, error) {
		counter, err := otel.Meter("shelltest").Int64Counter(startupSeedName)
		if err != nil {
			return nil, err
		}
		counter.Add(ctx, 0)
		return nil, nil
	}
	code := Execute(ctx, Spec{Service: "dev-health-worker", ConfigureDependencies: seed}, nil, lookup, IO{Stdout: &stdout, Stderr: &stderr})
	_, _ = os.Stderr.WriteString("helper exit=" + strconv.Itoa(code) + "\n" + stdout.String() + stderr.String())
	os.Exit(0)
}
