package queryapiservice

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/shell"
)

func lookupOf(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// freeAddr returns a loopback address nothing listens on right now.
func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type running struct {
	done           <-chan int
	stdout, stderr *bytes.Buffer
	cancel         context.CancelFunc
}

// start runs the real service through the shell, as `dho query-api` does.
func start(t *testing.T, args []string, env map[string]string) running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- shell.Execute(ctx, Spec, args, lookupOf(env), shell.IO{Stdout: &stdout, Stderr: &stderr})
	}()
	return running{done: done, stdout: &stdout, stderr: &stderr, cancel: cancel}
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(body)
}

// waitFor polls url until it answers want, failing when the service exits first.
func waitFor(t *testing.T, r running, url string, want int) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case code := <-r.done:
			t.Fatalf("the service returned %d before %s answered %d\nstdout: %s\nstderr: %s", code, url, want, r.stdout.String(), r.stderr.String())
		default:
		}
		if code, body := get(t, url); code == want {
			return body
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s never answered %d", url, want)
	return ""
}

func stop(t *testing.T, r running) {
	t.Helper()
	r.cancel()
	select {
	case code := <-r.done:
		if code != 0 {
			t.Fatalf("the service returned %d after a clean shutdown, want 0\nstdout: %s", code, r.stdout.String())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the service did not return after its context ended")
	}
}

// The service serves its query routes on --query-addr and the operator surface
// (/healthz, /readyz, /metrics) on --http-addr; for one release (D2627) the query
// listener also answers the operator paths in their old shapes, and a query route never
// meets the operator listener.
func TestServesTheQueryRoutesAndTheOperatorSurfaceOnBothListenersForOneRelease(t *testing.T) {
	query, operator := freeAddr(t), freeAddr(t)
	r := start(t, []string{"--query-addr", query, "--http-addr", operator}, nil)
	waitFor(t, r, "http://"+operator+"/healthz", http.StatusOK)
	body := waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	if strings.TrimSpace(body) != `{"status":"ok"}` {
		t.Fatalf("operator /readyz = %q: no /query configured is one explicit passing check, ready", body)
	}
	waitFor(t, r, "http://"+query+"/registry", http.StatusNotFound) // not configured: unmounted, but the listener answers
	// One release of compatibility (D2627): the query listener still answers the three
	// operator paths in the shapes the chart's probes and the scrape know, until the deploy
	// repo moves them. The release after this one removes them, and this block with them.
	if got := waitFor(t, r, "http://"+query+"/healthz", http.StatusOK); got != "ok" {
		t.Fatalf("compat /healthz = %q, want the old plain ok", got)
	}
	if got := waitFor(t, r, "http://"+query+"/readyz", http.StatusOK); got != "ready: /query not configured" {
		t.Fatalf("compat /readyz = %q, want the old not-configured body", got)
	}
	if got := waitFor(t, r, "http://"+query+"/metrics", http.StatusOK); !strings.Contains(got, `dev_health_runtime_info{service="dev-health-query-api"`) || !strings.Contains(got, `target_info{service_name="dev-health-query-api",`) {
		t.Fatalf("compat /metrics is not the process metrics:\n%s", got)
	}
	for _, path := range []string{"/query", "/registry", "/api/v1/meta"} {
		if code, _ := get(t, "http://"+operator+path); code == http.StatusOK {
			t.Fatalf("the operator listener answered %s with 200", path)
		}
	}
	stop(t, r)
	for _, addr := range []string{query, operator} {
		if connection, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			_ = connection.Close()
			t.Fatalf("%s still accepts connections after shutdown", addr)
		}
	}
}

// The operator /metrics carries what the query code records: the readiness outcome
// counter, with the "no /query configured" mode countable apart from a healthy one.
func TestOperatorMetricsCarryTheReadinessOutcomeCounter(t *testing.T) {
	query, operator := freeAddr(t), freeAddr(t)
	r := start(t, []string{"--query-addr", query, "--http-addr", operator}, nil)
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	body := waitFor(t, r, "http://"+operator+"/metrics", http.StatusOK)
	if !strings.Contains(body, "devhealth_query_api_readyz_total") || !strings.Contains(body, `outcome="not_configured"`) {
		t.Fatalf("/metrics does not carry the readiness outcome counter for the unconfigured mode:\n%s", body)
	}
	stop(t, r)
}

// An unknown flag and a positional argument are usage errors (exit 2) that write nothing
// to the listeners: the binary used to ignore its arguments (CHAOS-6447).
func TestUnknownFlagsAndPositionalArgumentsAreUsageErrors(t *testing.T) {
	for name, args := range map[string][]string{
		"unknown flag":        {"--no-such-flag"},
		"positional argument": {"extra"},
		"misspelled option":   {"--query-adr", ":9"},
	} {
		t.Run(name, func(t *testing.T) {
			r := start(t, args, nil)
			select {
			case code := <-r.done:
				if code != 2 {
					t.Fatalf("exit %d, want 2 (stderr %q)", code, r.stderr.String())
				}
				if !strings.Contains(r.stderr.String(), "argument error") {
					t.Fatalf("stderr %q does not report an argument error", r.stderr.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("did not return")
			}
		})
	}
}

// --help is rendered from the option registry and lists every setting of the service,
// including the ones that used to be environment-only.
func TestHelpListsTheServicesOptions(t *testing.T) {
	r := start(t, []string{"--help"}, nil)
	select {
	case code := <-r.done:
		if code != 0 {
			t.Fatalf("exit %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("did not return")
	}
	help := r.stdout.String()
	for _, want := range []string{"--query-addr", "--internal-addr", "--http-addr", "QUERY_API_ADDR", "GO_API_ENVELOPE_JWKS_PATH", "--drilldown-prs-enabled", "IDENTITY_MAPPING_PATH"} {
		if !strings.Contains(help, want) {
			t.Errorf("--help does not mention %s:\n%s", want, help)
		}
	}
}

// A taken query address fails the start with exit 1, before the service reports ready.
func TestATakenQueryAddressFailsTheStart(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	r := start(t, []string{"--query-addr", occupied.Addr().String(), "--http-addr", freeAddr(t)}, nil)
	select {
	case code := <-r.done:
		if code != 1 {
			t.Fatalf("exit %d when the query address was taken, want 1", code)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("did not return when the listener could not bind")
	}
}

// The environment names the same settings as the flags (the chart's declared-only
// contract): QUERY_API_ADDR alone moves the query listener; a flag beats it.
func TestSettingsComeFromTheFlagsThenTheEnvironment(t *testing.T) {
	envQuery, flagQuery, operator := freeAddr(t), freeAddr(t), freeAddr(t)
	r := start(t, []string{"--http-addr", operator}, map[string]string{"QUERY_API_ADDR": envQuery})
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	waitFor(t, r, "http://"+envQuery+"/registry", http.StatusNotFound)
	stop(t, r)

	r = start(t, []string{"--http-addr", operator, "--query-addr", flagQuery}, map[string]string{"QUERY_API_ADDR": envQuery})
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	waitFor(t, r, "http://"+flagQuery+"/registry", http.StatusNotFound)
	if connection, err := net.DialTimeout("tcp", envQuery, time.Second); err == nil {
		_ = connection.Close()
		t.Fatal("the environment address is served although a flag names another: the flag must win")
	}
	stop(t, r)
}

// CHAOS-6780: --internal-addr (QUERY_API_INTERNAL_ADDR) opens the second listener and
// all three close on shutdown; the three addresses must differ.
func TestOpensTheInternalListenerWhenItsAddressIsSet(t *testing.T) {
	query, internal, operator := freeAddr(t), freeAddr(t), freeAddr(t)
	r := start(t, []string{"--query-addr", query, "--internal-addr", internal, "--http-addr", operator}, nil)
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	waitFor(t, r, "http://"+query+"/registry", http.StatusNotFound)
	waitFor(t, r, "http://"+internal+"/registry", http.StatusNotFound)
	stop(t, r)
	for _, addr := range []string{query, internal, operator} {
		if connection, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			_ = connection.Close()
			t.Fatalf("%s still accepts connections after shutdown", addr)
		}
	}
}

func TestTwoListenersOnOneAddressAreRefusedBeforeServing(t *testing.T) {
	addr := freeAddr(t)
	r := start(t, []string{"--query-addr", addr, "--http-addr", addr}, nil)
	select {
	case code := <-r.done:
		if code != 1 || !strings.Contains(r.stderr.String(), "must differ") {
			t.Fatalf("exit %d stderr %q, want 1 naming that the listeners must differ", code, r.stderr.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("did not return")
	}
}

// The verb is a dho service: root --log-level typed before it is its own flag, and the
// service identity is the chart's.
func TestCommandIsAServiceOfTheDhoTree(t *testing.T) {
	command := Command()
	if command.Name != "query-api" || command.Kind != cli.Service || command.IgnoresArguments {
		t.Fatalf("command = %+v: a service that takes flags (unknown ones exit 2), not one that ignores its arguments", command)
	}
	if Spec.Service != config.QueryAPIServiceName || Spec.TraceServiceName != config.QueryAPIServiceName || Spec.Invocation != "dho query-api" {
		t.Fatalf("spec = %+v", Spec)
	}
}

// The query listener's readiness check fails until the listener is bound: before the
// runtime starts its components the registry is not ready, naming query_listener.
func TestTheListenerCheckFailsUntilTheListenerIsBound(t *testing.T) {
	registry := health.NewRegistry(time.Second)
	cfg, err := config.Load(config.Spec{Service: config.QueryAPIServiceName, LookupEnv: func(name string) (string, bool) {
		if name == "QUERY_API_ADDR" {
			return "127.0.0.1:0", true
		}
		return "", false
	}})
	if err != nil {
		t.Fatal(err)
	}
	components, err := configure(context.Background(), cfg, registry, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	registry.SetReady(true)
	status := registry.Readiness(context.Background())
	if status.Ready || len(status.Failed) != 1 || status.Failed[0] != "query_listener" {
		t.Fatalf("readiness before the listener is bound = %+v, want not ready, failed [query_listener]", status)
	}
	for _, component := range components {
		if err := component.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for i := len(components) - 1; i >= 0; i-- {
			_ = components[i].Shutdown(context.Background())
		}
	}()
	if status := registry.Readiness(context.Background()); !status.Ready {
		t.Fatalf("readiness after the components started = %+v", status)
	}
}
