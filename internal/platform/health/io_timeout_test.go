package health

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

type ioTimeoutFake struct{}

func (ioTimeoutFake) Error() string   { return "dial tcp 10.0.0.9:5432: i/o timeout secret-host" }
func (ioTimeoutFake) Timeout() bool   { return true }
func (ioTimeoutFake) Temporary() bool { return false }

func ioTimeoutErrors() map[string]error {
	return map[string]error{
		"os_deadline":         os.ErrDeadlineExceeded,
		"wrapped_os_deadline": fmt.Errorf("read: %w", os.ErrDeadlineExceeded),
		"net_op_error":        &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded},
		"net_error_fake":      ioTimeoutFake{},
		"transient_timeout":   fmt.Errorf("w: %w", &flaggedTimeout{}),
	}
}

// An I/O timeout has the SAME form as a context deadline: cause=timeout, timed_out=true, transient=false.
func TestRequiredCheckClassifiesIOTimeoutsAsTimeout(t *testing.T) {
	t.Parallel()
	registry := NewRegistry(time.Second)
	checks := map[string]error{
		"ctx_deadline":     context.DeadlineExceeded,
		"canceled":         context.Canceled,
		"refused":          fmt.Errorf("w: %w", transientFake{true}),
		"refused_op_error": &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		"plain":            errors.New("boom"),
	}
	want := map[string]struct {
		cause     string
		timedOut  bool
		transient bool
	}{
		"ctx_deadline":     {"timeout", true, false},
		"canceled":         {"canceled", true, false},
		"refused":          {CauseUnreachable, false, true},
		"refused_op_error": {"error", false, false},
		"plain":            {"error", false, false},
	}
	for name, err := range ioTimeoutErrors() {
		checks["io_"+name] = err
		want["io_"+name] = struct {
			cause     string
			timedOut  bool
			transient bool
		}{"timeout", true, false}
	}
	for name, err := range checks {
		if regErr := registry.RegisterRequired(name, func(context.Context) error { return err }); regErr != nil {
			t.Fatal(regErr)
		}
	}
	for _, status := range registry.CheckRequired(context.Background()).Checks {
		w := want[status.Name]
		if !status.Failed || status.Cause != w.cause || status.TimedOut != w.timedOut || status.Transient != w.transient {
			t.Errorf("%s = %+v, want %+v", status.Name, status, w)
		}
	}
}

// flaggedTimeout is both a net timeout and a TransientError: the timeout must win, as a context deadline does.
type flaggedTimeout struct{}

func (*flaggedTimeout) Error() string   { return "x" }
func (*flaggedTimeout) Timeout() bool   { return true }
func (*flaggedTimeout) Temporary() bool { return false }
func (*flaggedTimeout) Transient() bool { return true }

func TestRefusalLogNamesAnIOTimeoutAsATimedOutTimeout(t *testing.T) {
	t.Parallel()
	for name, failure := range ioTimeoutErrors() {
		var logs bytes.Buffer
		registry := NewRegistry(time.Second)
		registry.SetRefusalLogger(slog.New(slog.NewJSONHandler(&logs, nil)))
		if err := registry.RegisterRequired("dep", func(context.Context) error { return failure }); err != nil {
			t.Fatal(err)
		}
		registry.SetReady(true)
		registry.Readiness(context.Background())
		registry.flushRefusalLogs()
		for _, want := range []string{`"cause":"timeout"`, `"timed_out":true`} {
			if !strings.Contains(logs.String(), want) {
				t.Errorf("%s: want %s in %s", name, want, logs.String())
			}
		}
		if strings.Contains(logs.String(), "secret-host") || strings.Contains(logs.String(), "10.0.0.9") {
			t.Errorf("%s: refusal log leaked the error text: %s", name, logs.String())
		}
	}
}

// The HTTP surface reads only the failed check NAMES, never the cause: an I/O timeout must fail /readyz and
// set the per-check gauge exactly as a context deadline does, with no cause text.
func TestReadyzAndMetricsTreatAnIOTimeoutLikeAContextDeadline(t *testing.T) {
	t.Parallel()
	body := func(failure error) (readyz, metrics string) {
		registry := NewRegistry(time.Second)
		if err := registry.RegisterRequired("dep", func(context.Context) error { return failure }); err != nil {
			t.Fatal(err)
		}
		registry.SetReady(true)
		server, err := NewServer(ServerOptions{Address: "127.0.0.1:0", Registry: registry, Service: "svc", Version: "dev"})
		if err != nil {
			t.Fatal(err)
		}
		get := func(path string) (int, string) {
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
			return response.Code, response.Body.String()
		}
		code, readyzBody := get("/readyz")
		if code != http.StatusServiceUnavailable {
			t.Fatalf("/readyz = %d, want 503", code)
		}
		_, metricsBody := get("/metrics")
		return readyzBody, metricsBody
	}
	wantReadyz, wantMetrics := body(context.DeadlineExceeded)
	for name, failure := range ioTimeoutErrors() {
		gotReadyz, gotMetrics := body(failure)
		if gotReadyz != wantReadyz {
			t.Errorf("%s: /readyz = %q, want %q", name, gotReadyz, wantReadyz)
		}
		if !strings.Contains(gotMetrics, `dev_health_runtime_check_failed{check="dep"} 1`) || !strings.Contains(wantMetrics, `dev_health_runtime_check_failed{check="dep"} 1`) {
			t.Errorf("%s: per-check gauge missing", name)
		}
		if strings.Contains(gotReadyz, "timeout") || strings.Contains(gotReadyz, "unreachable") {
			t.Errorf("%s: /readyz exposes the cause: %q", name, gotReadyz)
		}
	}
}
