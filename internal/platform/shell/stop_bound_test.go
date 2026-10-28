package shell

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
)

// stopBoundBudget is the shutdown budget the helper runs under: one second under
// the five a stopping container is given by the container smoke (docker stop
// --time 5), whose kill is the exit 137. The runtime enforces it, so a stop that
// needs more ends in a shutdown error and the exit 1 the test refuses (D6418: no
// wall-time assert in the parent).
const stopBoundBudget = 4 * time.Second

func runStopBoundHelper() {
	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr bytes.Buffer
	lookup := testLookup(map[string]string{
		"DEV_HEALTH_HTTP_ADDR":        os.Getenv("STOP_BOUND_HTTP_ADDR"),
		"DEV_HEALTH_SHUTDOWN_TIMEOUT": stopBoundBudget.String(),
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
	_, _ = os.Stdout.WriteString("READY\n")
	_, _ = os.Stdin.Read(make([]byte, 1))
	cancel()
	code := <-done
	_, _ = os.Stderr.WriteString("STOP_CODE=" + strconv.Itoa(code) + "\n")
	_, _ = os.Stderr.WriteString(stdout.String() + stderr.String())
	os.Exit(0)
}

// NOTE: on main today the stop holds only the trace flush; the OTLP metrics
// push (a separate change) adds its own flush to the same stop, and this number
// is where the two are summed once it lands.
// TestTheWholeStopWithEveryTelemetryEndpointDownStaysUnderTheContainerBudget
// runs the shell (the code `dho api` runs through) with the trace and the
// metrics endpoints pointing at a port nothing listens on, a sampled span
// buffered and stopBoundBudget as the shutdown budget, stops it, and asserts the
// stop ended inside that budget: exit 0 and no shutdown-error or abandoned-flush
// line. The component bounds (tracing.shutdownFlushTimeout, metricsFlushTimeout)
// are summed here against the one number the container smoke cares about.
// Named limits: a flush bound raised by less than the slack left under the budget
// passes here (tracing/flush_test.go owns the tracing bound); and a process that
// stays alive less than the 30 s failure ceiling past the stop request passes (a
// tighter wall bound is about 1.27x of hosted headroom, the D6418 eject class).
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
	text := stopDriven(t, command, 30*time.Second)
	index := strings.Index(text, "STOP_CODE=")
	if index < 0 {
		t.Fatalf("the helper did not report its stop time:\n%s", text)
	}
	var code int
	if _, err := fmt.Sscanf(text[index:], "STOP_CODE=%d", &code); err != nil {
		t.Fatalf("unreadable helper report %q: %v", text[index:], err)
	}
	if code != 0 {
		t.Errorf("the stop exited %d with every telemetry endpoint down, want 0", code)
	}
	for _, forbidden := range []string{"service stopped with error", "shutdown component"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("the stop needed more than its %v budget with every telemetry endpoint down (the container smoke kills at 5 s); output has %q:\n%s", stopBoundBudget, forbidden, text)
		}
	}
	if !strings.Contains(text, `"msg":"service stopped"`) {
		t.Errorf("the stop did not end with the service stopped line:\n%s", text)
	}
	if !strings.Contains(text, "final flush timed out") {
		t.Errorf("the buffered span was not reported as dropped; the sampled span did not reach the flush:\n%s", text)
	}
}

// stopDriven runs command, a helper that prints READY on stdout and then waits for
// one byte on stdin before it stops, and drives the stop by events: wait for READY,
// check the process is still alive before the stop request (it must not exit on its
// own), send the byte, wait for the exit (Wait returned). The ceiling only guards
// the failure path; no elapsed time is compared. It returns the stderr output.
func stopDriven(t *testing.T, command *exec.Cmd, ceiling time.Duration) string {
	t.Helper()
	var output bytes.Buffer
	command.Stderr = &output
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	ready, drained := make(chan struct{}), make(chan struct{})
	go func() {
		reader := bufio.NewReader(stdout)
		if line, _ := reader.ReadString('\n'); strings.TrimSpace(line) == "READY" {
			close(ready)
		}
		_, _ = io.Copy(io.Discard, reader)
		close(drained)
	}()
	done := make(chan error, 1)
	go func() {
		<-drained
		done <- command.Wait()
	}()
	select {
	case <-ready:
	case <-done:
		t.Fatalf("the helper exited before READY; output:\n%s", output.String())
	case <-time.After(ceiling):
		_ = command.Process.Kill()
		<-done
		t.Fatalf("the helper never printed READY within the %v ceiling; output:\n%s", ceiling, output.String())
	}
	select {
	case <-done:
		t.Fatalf("the helper exited before the stop request; output:\n%s", output.String())
	default:
	}
	_, _ = stdin.Write([]byte{'x'})
	_ = stdin.Close()
	select {
	case <-done:
	case <-time.After(ceiling):
		_ = command.Process.Kill()
		<-done
		t.Fatalf("the process did not exit within the %v ceiling after the stop request; output:\n%s", ceiling, output.String())
	}
	return output.String()
}
