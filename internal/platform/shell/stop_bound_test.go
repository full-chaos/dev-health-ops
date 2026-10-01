package shell

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

// stopBoundBudget is the most a stopping process may take with its telemetry
// collector unreachable: one second under the five a stopping container is given
// by the container smoke (docker stop --time 5), whose kill is the exit 137.
const stopBoundBudget = 4 * time.Second

func runStopBoundHelper() {
	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr bytes.Buffer
	lookup := testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR":        os.Getenv("STOP_BOUND_HTTP_ADDR"),
		"DEV_HEALTH_SHUTDOWN_TIMEOUT": "10s",
	})
	done := make(chan int, 1)
	go func() {
		done <- Execute(ctx, Spec{Service: "dev-health-worker"}, nil, lookup, IO{Stdout: &stdout, Stderr: &stderr})
	}()
	time.Sleep(1500 * time.Millisecond)
	// A sampled span is buffered on the global tracer provider the shell installed.
	_, span := otel.Tracer("stop-bound").Start(context.Background(), "buffered")
	span.End()
	time.Sleep(200 * time.Millisecond)
	stopped := time.Now()
	cancel()
	code := <-done
	_, _ = os.Stderr.WriteString("STOP_MS=" + strconv.FormatInt(time.Since(stopped).Milliseconds(), 10) + " CODE=" + strconv.Itoa(code) + "\n")
	_, _ = os.Stderr.WriteString(stdout.String() + stderr.String())
	os.Exit(0)
}

// NOTE: on main today the stop holds only the trace flush; the OTLP metrics
// push (a separate change) adds its own flush to the same stop, and this number
// is where the two are summed once it lands.
// TestTheWholeStopWithEveryTelemetryEndpointDownStaysUnderTheContainerBudget
// runs the shell (the code `dho api` runs through) with the trace and the
// metrics endpoints pointing at a port nothing listens on, a sampled span
// buffered and a 10 s shutdown budget, stops it, and asserts the total stop is
// under stopBoundBudget. The component bounds (tracing.shutdownFlushTimeout,
// metricsFlushTimeout) are summed here in the one number the container smoke
// cares about: either bound raised to 3 s makes the sum exceed it.
func TestTheWholeStopWithEveryTelemetryEndpointDownStaysUnderTheContainerBudget(t *testing.T) {
	if os.Getenv("STOP_BOUND_HELPER") == "1" {
		runStopBoundHelper()
		return
	}
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpAddress := httpListener.Addr().String()
	_ = httpListener.Close()
	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddress := closed.Addr().String()
	_ = closed.Close()

	command := exec.Command(os.Args[0], "-test.run=^TestTheWholeStopWithEveryTelemetryEndpointDownStaysUnderTheContainerBudget$")
	command.Env = append(os.Environ(),
		"STOP_BOUND_HELPER=1",
		"STOP_BOUND_HTTP_ADDR="+httpAddress,
		"OTEL_ENABLED=true",
		"OTEL_SAMPLE_RATE=1",
		"OTEL_EXPORTER_OTLP_ENDPOINT="+closedAddress,
		"OTEL_METRIC_EXPORT_INTERVAL=200",
	)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		_ = command.Process.Kill()
		t.Fatalf("the shell did not stop within 30 s; output:\n%s", output.String())
	}
	text := output.String()
	index := strings.Index(text, "STOP_MS=")
	if index < 0 {
		t.Fatalf("the helper did not report its stop time:\n%s", text)
	}
	var millis int64
	var code int
	if _, err := fmt.Sscanf(text[index:], "STOP_MS=%d CODE=%d", &millis, &code); err != nil {
		t.Fatalf("unreadable helper report %q: %v", text[index:], err)
	}
	if code != 0 {
		t.Errorf("the stop exited %d with every telemetry endpoint down, want 0", code)
	}
	if stop := time.Duration(millis) * time.Millisecond; stop >= stopBoundBudget {
		t.Errorf("the stop took %v with every telemetry endpoint down, want < %v (the container smoke kills at 5 s)", stop, stopBoundBudget)
	}
	if !strings.Contains(text, "final flush timed out") {
		t.Errorf("the buffered span was not reported as dropped; the sampled span did not reach the flush:\n%s", text)
	}
}
