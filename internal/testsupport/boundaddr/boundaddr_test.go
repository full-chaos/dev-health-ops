package boundaddr

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func TestFindReadsOnlyTheAddressAttributeOfTheNamedComponent(t *testing.T) {
	t.Parallel()
	logs := strings.Join([]string{
		`not json at all`,
		`{"msg":"runtime component listening","component":"other","address":"127.0.0.1:1"}`,
		`{"msg":"starting runtime component","component":"operator-http","address":"127.0.0.1:2"}`,
		`{"msg":"runtime component listening","component":"operator-http","address":"127.0.0.1:3","note":"127.0.0.1:9"}`,
		`{"msg":"runtime component listening","component":"operator-http","addr`,
	}, "\n")
	address, ok := Find(logs, "operator-http")
	if !ok || address != "127.0.0.1:3" {
		t.Fatalf("Find = %q, %v, want 127.0.0.1:3", address, ok)
	}
	if _, ok := Find(`{"msg":"runtime component listening","component":"operator-http"}`, "operator-http"); ok {
		t.Fatal("a line with no address attribute was accepted")
	}
}

func TestAwaitFailsLoudlyWithoutAListeningLine(t *testing.T) {
	t.Parallel()
	logs := &Logs{}
	_, _ = logs.Write([]byte(`{"msg":"starting runtime component","component":"operator-http"}` + "\n"))
	if _, err := await(logs, "operator-http", make(chan int), 30*time.Millisecond); err == nil || !strings.Contains(err.Error(), "no listening line for operator-http") {
		t.Fatalf("await error = %v, want the no-listening-line failure", err)
	}
	exited := make(chan int, 1)
	exited <- 1
	if _, err := await(logs, "operator-http", exited, time.Minute); err == nil || !strings.Contains(err.Error(), "exited 1") {
		t.Fatalf("await error = %v, want the early-exit failure", err)
	}
}

func startOperator(t *testing.T, address string) (*Logs, <-chan int, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	logs := &Logs{}
	done := make(chan int, 1)
	lookup := func(key string) (string, bool) {
		values := map[string]string{"DEV_HEALTH_HTTP_ADDR": address, "DEV_HEALTH_SHUTDOWN_TIMEOUT": "1s"}
		value, ok := values[key]
		return value, ok
	}
	go func() {
		done <- shell.Execute(ctx, shell.Spec{
			Service: "dev-health-worker",
			ConfigureDependencies: func(_ context.Context, _ config.Config, registry *health.Registry) ([]lifecycle.Component, error) {
				return nil, registry.RegisterRequired("missing_dependency", func(context.Context) error { return errors.New("down") })
			},
		}, nil, secrets.LookupEnv(lookup), shell.IO{Stdout: logs, Stderr: io.Discard, Environ: []string{}})
	}()
	stop := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("the operator did not stop")
		}
	}
	return logs, done, stop
}

func takeAddress(t *testing.T, address string) net.Listener {
	t.Helper()
	thief, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("the thief could not take %s: %v", address, err)
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })}
	go func() { _ = server.Serve(thief) }()
	t.Cleanup(func() { _ = server.Close() })
	return thief
}

// The old shape: the address is released, a thief takes it, then the service starts on it. The service cannot bind, so
// Await fails; a probe of that address would be answered by the thief.
func TestPortThiefBeatsTheReleaseThenBindShape(t *testing.T) {
	released, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := released.Addr().String()
	if err := released.Close(); err != nil {
		t.Fatal(err)
	}
	takeAddress(t, address)
	logs, done, _ := startOperator(t, address)
	if _, err := await(logs, "operator-http", done, 10*time.Second); err == nil || !strings.Contains(err.Error(), "exited") {
		t.Fatalf("old shape with a planted thief: await error = %v, want an early exit (address in use)", err)
	}
	response, err := http.Get("http://" + address + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("thief answered %d, want 200", response.StatusCode)
	}
}

// The new shape: port 0, address read back. A thief churning ephemeral listeners cannot take the bound address, and every
// probe is answered by the operator (503, a dependency is down), never by a thief (200).
func TestPortThiefCannotTakeAnAddressTheServiceBoundItself(t *testing.T) {
	stopThief := make(chan struct{})
	thiefDone := make(chan struct{})
	go func() {
		defer close(thiefDone)
		for {
			select {
			case <-stopThief:
				return
			default:
			}
			var held []net.Listener
			for range 64 {
				if l, err := net.Listen("tcp", "127.0.0.1:0"); err == nil {
					held = append(held, l)
				}
			}
			for _, l := range held {
				_ = l.Close()
			}
		}
	}()
	t.Cleanup(func() { close(stopThief); <-thiefDone })

	for i := 0; i < 5; i++ {
		logs, done, stop := startOperator(t, "127.0.0.1:0")
		address := Await(t, logs, "operator-http", done)
		if l, err := net.Listen("tcp", address); err == nil {
			_ = l.Close()
			t.Fatalf("a thief took the bound address %s", address)
		} else if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("second listen on %s: %v, want EADDRINUSE", address, err)
		}
		deadline := time.Now().Add(20 * time.Second)
		for {
			response, err := http.Get("http://" + address + "/readyz")
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusServiceUnavailable {
					break
				}
				if response.StatusCode == http.StatusOK {
					t.Fatalf("readyz = 200 from %s, want 503 from the operator", address)
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("no 503 from %s: %v", address, err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		stop()
	}
}
