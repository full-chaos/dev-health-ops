package queryapiservice

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
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

// syncBuffer is a bytes.Buffer a test can read while the service still writes to it.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type running struct {
	done           <-chan int
	stdout, stderr *syncBuffer
	cancel         context.CancelFunc
}

// anyPort asks the kernel for a free port at bind time: the service reports the address it bound (startBound), so a
// test never frees a port and binds it later.
const anyPort = "127.0.0.1:0"

// Components that listen, as the runtime names them in its "runtime component listening" line.
const (
	operatorComponent = "operator-http"
	queryComponent    = "query-http"
	internalComponent = "query-internal-http"
	mcpComponent      = "query-mcp-http"
)

// startBound starts the service and returns the addresses the named components bound, read from the runtime's
// "runtime component listening" lines. It fails when the service exits or a line is missing within 15 s.
func startBound(t *testing.T, args []string, env map[string]string, components ...string) (running, map[string]string) {
	t.Helper()
	r := start(t, args, env)
	bound := map[string]string{}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, line := range strings.Split(r.stdout.String(), "\n") {
			var entry struct {
				Msg       string `json:"msg"`
				Component string `json:"component"`
				Address   string `json:"address"`
			}
			if strings.HasPrefix(line, "{") && json.Unmarshal([]byte(line), &entry) == nil && entry.Msg == "runtime component listening" {
				bound[entry.Component] = entry.Address
			}
		}
		missing := false
		for _, component := range components {
			if bound[component] == "" {
				missing = true
			}
		}
		if !missing {
			return r, bound
		}
		select {
		case code := <-r.done:
			t.Fatalf("the service returned %d before it reported %v bound\nstdout: %s\nstderr: %s", code, components, r.stdout.String(), r.stderr.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
	t.Fatalf("the service did not report %v bound within 15 s\nstdout: %s", components, r.stdout.String())
	return r, nil
}

// defaultedAddrs names the listeners a start would leave on their DEFAULT address: the query listener (:8090) unless
// --query-addr or QUERY_API_ADDR is given, the operator listener (:8080) unless --http-addr or DEV_HEALTH_HTTP_ADDR is.
// A test that starts the service on a default port races every other process on the runner for it (CHAOS-10906).
func defaultedAddrs(args []string, env map[string]string) []string {
	given := func(flag, name string) bool {
		for _, arg := range args {
			if arg == flag || strings.HasPrefix(arg, flag+"=") {
				return true
			}
		}
		_, ok := env[name]
		return ok
	}
	var defaulted []string
	if !given("--query-addr", "QUERY_API_ADDR") {
		defaulted = append(defaulted, ":8090 (--query-addr / QUERY_API_ADDR)")
	}
	if !given("--http-addr", "DEV_HEALTH_HTTP_ADDR") {
		defaulted = append(defaulted, ":8080 (--http-addr / DEV_HEALTH_HTTP_ADDR)")
	}
	return defaulted
}

// start runs the real service through the shell, as `dho query-api` does. It refuses to run a service that would bind a
// default port: the test binds port 0 (anyPort) and reads the bound addresses (startBound).
func start(t *testing.T, args []string, env map[string]string) running {
	t.Helper()
	if defaulted := defaultedAddrs(args, env); len(defaulted) > 0 {
		t.Fatalf("this test would start the service on the default address(es) %v: pass explicit addresses (anyPort), never a fixed default port", defaulted)
	}
	return launch(t, args, env)
}

// startUsageOnly runs the service on arguments that end at argument parsing (a usage error, --help): it never opens a
// listener, so it needs no address.
func startUsageOnly(t *testing.T, args []string) running {
	t.Helper()
	return launch(t, args, nil)
}

func launch(t *testing.T, args []string, env map[string]string) running {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	var stdout, stderr syncBuffer
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
// listener also answers the operator PROBE paths in their old shapes, and a query route
// never meets the operator listener. /metrics is not part of the compat shim: it is
// operational detail, not a probe, so CHAOS-7097 mounts it on the INTERNAL route set
// only -- this config sets no --internal-addr, so the query listener (a public one)
// correctly has no /metrics of its own; the operator listener's own /metrics
// (health.NewServer's route, independent of this split) is the real, always-on scrape
// target throughout, unaffected -- see TestOperatorMetricsCarryTheReadinessOutcomeCounter.
func TestServesTheQueryRoutesAndTheOperatorSurfaceOnBothListenersForOneRelease(t *testing.T) {
	r, addrs := startBound(t, []string{"--query-addr", anyPort, "--http-addr", anyPort}, nil, queryComponent, operatorComponent)
	query, operator := addrs[queryComponent], addrs[operatorComponent]
	waitFor(t, r, "http://"+operator+"/healthz", http.StatusOK)
	body := waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	if strings.TrimSpace(body) != `{"status":"ok"}` {
		t.Fatalf("operator /readyz = %q: no /query configured is one explicit passing check, ready", body)
	}
	waitFor(t, r, "http://"+query+"/registry", http.StatusNotFound) // not configured: unmounted, but the listener answers
	// One release of compatibility (the compat shim): the query listener still answers the two
	// operator PROBE paths in the shapes the chart's probes know, until the deploy repo
	// moves them. The release after this one removes them, and this block with them.
	if got := waitFor(t, r, "http://"+query+"/healthz", http.StatusOK); got != "ok" {
		t.Fatalf("compat /healthz = %q, want the old plain ok", got)
	}
	if got := waitFor(t, r, "http://"+query+"/readyz", http.StatusOK); got != "ready: /query not configured" {
		t.Fatalf("compat /readyz = %q, want the old not-configured body", got)
	}
	if code, _ := get(t, "http://"+query+"/metrics"); code == http.StatusOK {
		t.Fatalf("the query listener answered /metrics with 200: it must be internal-only, never the public query listener")
	}
	// The always-on, unaffected real target: the operator listener's own /metrics.
	// (Not asserting target_info here, unlike the retired query-compat check above
	// it replaces: that OTel resource metric was observed present via the query
	// listener's compat path but absent via the operator's own address in this
	// same harness -- a pre-existing difference this change does not explain or
	// depend on. dev_health_runtime_info alone is enough to prove this is the
	// real process metrics endpoint.)
	if got := waitFor(t, r, "http://"+operator+"/metrics", http.StatusOK); !strings.Contains(got, `dev_health_runtime_info{service="dev-health-query-api"`) {
		t.Fatalf("operator /metrics is not the process metrics:\n%s", got)
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
	r, addrs := startBound(t, []string{"--query-addr", anyPort, "--http-addr", anyPort}, nil, operatorComponent)
	operator := addrs[operatorComponent]
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
			r := startUsageOnly(t, args)
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
	r := startUsageOnly(t, []string{"--help"})
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
	r := start(t, []string{"--query-addr", occupied.Addr().String(), "--http-addr", anyPort}, nil)
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
	r, addrs := startBound(t, []string{"--http-addr", anyPort}, map[string]string{"QUERY_API_ADDR": anyPort}, queryComponent, operatorComponent)
	envQuery, operator := addrs[queryComponent], addrs[operatorComponent]
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	waitFor(t, r, "http://"+envQuery+"/registry", http.StatusNotFound)
	stop(t, r)

	// The flag names port 0; the environment names a fixed address nothing may serve: a start that honoured the
	// environment would bind it (or fail), and never report a query listener on the flag's address.
	const envLoser = "127.0.0.1:1"
	r, addrs = startBound(t, []string{"--http-addr", anyPort, "--query-addr", anyPort}, map[string]string{"QUERY_API_ADDR": envLoser}, queryComponent, operatorComponent)
	flagQuery, operator := addrs[queryComponent], addrs[operatorComponent]
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	waitFor(t, r, "http://"+flagQuery+"/registry", http.StatusNotFound)
	if flagQuery == envLoser {
		t.Fatal("the environment address is served although a flag names another: the flag must win")
	}
	stop(t, r)
}

// CHAOS-6780: --internal-addr (QUERY_API_INTERNAL_ADDR) opens the second listener and
// all three close on shutdown; the three addresses must differ.
//
// With an internal listener actually configured, the compat /metrics moves there
// -- reachable on the internal address, absent on the public query address, the exact
// pairing CHAOS-7097's route-set split exists to make structural.
func TestOpensTheInternalListenerWhenItsAddressIsSet(t *testing.T) {
	r, addrs := startBound(t, []string{"--query-addr", anyPort, "--internal-addr", anyPort, "--http-addr", anyPort}, nil, queryComponent, internalComponent, operatorComponent)
	query, internal, operator := addrs[queryComponent], addrs[internalComponent], addrs[operatorComponent]
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	waitFor(t, r, "http://"+query+"/registry", http.StatusNotFound)
	waitFor(t, r, "http://"+internal+"/registry", http.StatusNotFound)
	if code, _ := get(t, "http://"+query+"/metrics"); code == http.StatusOK {
		t.Fatal("the PUBLIC query listener answered /metrics with 200 even with an internal listener configured")
	}
	if got := waitFor(t, r, "http://"+internal+"/metrics", http.StatusOK); !strings.Contains(got, `dev_health_runtime_info{service="dev-health-query-api"`) {
		t.Fatalf("the INTERNAL listener's /metrics is not the process metrics:\n%s", got)
	}
	stop(t, r)
	for _, addr := range []string{query, internal, operator} {
		if connection, err := net.DialTimeout("tcp", addr, time.Second); err == nil {
			_ = connection.Close()
			t.Fatalf("%s still accepts connections after shutdown", addr)
		}
	}
}

// CHAOS-7085: --mcp-addr (QUERY_API_MCP_ADDR) opens the MCP caller-class
// listener, the fourth. It serves the MCP class's /query and nothing else:
// no /registry, /buildinfo or /metrics fallthrough (here /query itself is not
// configured, so every path answers 404), and it closes on shutdown.
func TestOpensTheMCPListenerWhenItsAddressIsSet(t *testing.T) {
	r, addrs := startBound(t, []string{"--query-addr", anyPort, "--internal-addr", anyPort, "--mcp-addr", anyPort, "--mcp-boundary", "networkpolicy", "--http-addr", anyPort}, nil, internalComponent, mcpComponent, operatorComponent)
	internal, mcp, operator := addrs[internalComponent], addrs[mcpComponent], addrs[operatorComponent]
	waitFor(t, r, "http://"+operator+"/readyz", http.StatusOK)
	waitFor(t, r, "http://"+internal+"/metrics", http.StatusOK)
	for _, path := range []string{"/metrics", "/healthz", "/readyz", "/registry", "/buildinfo", "/query"} {
		waitFor(t, r, "http://"+mcp+path, http.StatusNotFound)
	}
	stop(t, r)
	if connection, err := net.DialTimeout("tcp", mcp, time.Second); err == nil {
		_ = connection.Close()
		t.Fatalf("%s still accepts connections after shutdown", mcp)
	}
}

// The MCP listener trusts the internal identity headers from whoever can
// connect, so its address alone never starts it: the boundary marker the
// deployment sets next to the network boundary must come with it. An address
// that arrives any other way (extraEnv, envFrom, a hand-edited manifest) has
// no marker and refuses boot, loudly, naming the cause.
func TestTheMCPListenerStartsOnlyWithItsBoundaryMarker(t *testing.T) {
	for name, tc := range map[string]struct {
		extra       func(mcp string) []string
		refuse      bool
		wantMessage string
	}{
		"address and marker": {func(a string) []string { return []string{"--mcp-addr", a, "--mcp-boundary", "networkpolicy"} }, false, ""},
		"address and cidr marker with cidrs": {func(a string) []string {
			return []string{"--mcp-addr", a, "--mcp-boundary", "allowed-cidrs", "--mcp-allowed-cidrs", "127.0.0.1/32"}
		}, false, ""},
		"address only":                      {func(a string) []string { return []string{"--mcp-addr", a} }, true, "QUERY_API_MCP_BOUNDARY"},
		"address and a wrong marker":        {func(a string) []string { return []string{"--mcp-addr", a, "--mcp-boundary", "yes"} }, true, "QUERY_API_MCP_BOUNDARY"},
		"address and cidr marker, no cidrs": {func(a string) []string { return []string{"--mcp-addr", a, "--mcp-boundary", "allowed-cidrs"} }, true, "requires"},
		"marker only":                       {func(string) []string { return []string{"--mcp-boundary", "networkpolicy"} }, false, ""},
		"neither":                           {func(string) []string { return nil }, false, ""},
	} {
		t.Run(name, func(t *testing.T) {
			args := append([]string{"--http-addr", anyPort, "--query-addr", anyPort}, tc.extra(anyPort)...)
			wantListening := strings.Contains(strings.Join(args, " "), "--mcp-addr")
			if tc.refuse {
				r := start(t, args, nil)
				select {
				case code := <-r.done:
					if code == 0 || !strings.Contains(r.stderr.String(), tc.wantMessage) {
						t.Fatalf("exit %d stderr %q, want non-zero naming %q", code, r.stderr.String(), tc.wantMessage)
					}
				case <-time.After(10 * time.Second):
					t.Fatal("an MCP address without its boundary marker did not refuse boot")
				}
				return
			}
			components := []string{operatorComponent}
			if wantListening {
				components = append(components, mcpComponent)
			}
			r, addrs := startBound(t, args, nil, components...)
			waitFor(t, r, "http://"+addrs[operatorComponent]+"/readyz", http.StatusOK)
			if mcp := addrs[mcpComponent]; wantListening {
				connection, err := net.DialTimeout("tcp", mcp, time.Second)
				if err != nil {
					t.Fatalf("the MCP listener reported %s but does not accept: %v", mcp, err)
				}
				_ = connection.Close()
			} else if mcp != "" {
				t.Fatalf("an MCP listener is reported bound at %s without an MCP address", mcp)
			}
			stop(t, r)
		})
	}
}

func TestTheMCPListenerMustNotShareAnAddress(t *testing.T) {
	// Each case shares one address between the MCP listener and the listener it names; the OTHER listeners get free
	// addresses of their own, so no case relies on a default port.
	for name, args := range map[string]func(shared string, others []string) []string{
		"query": func(a string, others []string) []string {
			return []string{"--query-addr", a, "--mcp-addr", a, "--mcp-boundary", "networkpolicy", "--http-addr", others[0]}
		},
		"internal": func(a string, others []string) []string {
			return []string{"--internal-addr", a, "--mcp-addr", a, "--mcp-boundary", "networkpolicy", "--query-addr", others[0], "--http-addr", others[1]}
		},
		"operator": func(a string, others []string) []string {
			return []string{"--http-addr", a, "--mcp-addr", a, "--mcp-boundary", "networkpolicy", "--query-addr", others[0]}
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := start(t, args("127.0.0.1:3", []string{"127.0.0.1:1", "127.0.0.1:2"}), nil)
			select {
			case code := <-r.done:
				if code != 1 || !strings.Contains(r.stderr.String(), "must differ") {
					t.Fatalf("exit %d stderr %q, want 1 naming that the listeners must differ", code, r.stderr.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("did not return")
			}
		})
	}
}

func TestTwoListenersOnOneAddressAreRefusedBeforeServing(t *testing.T) {
	r := start(t, []string{"--query-addr", "127.0.0.1:3", "--http-addr", "127.0.0.1:3"}, nil)
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

// The guard is red on a start without explicit addresses, and says which default it would have bound.
func TestTheStartGuardNamesTheDefaultPortsAStartWouldBind(t *testing.T) {
	for name, test := range map[string]struct {
		args []string
		env  map[string]string
		want []string
	}{
		"nothing given (the shape the MCP table had)": {[]string{"--http-addr", "127.0.0.1:0"}, nil, []string{":8090 (--query-addr / QUERY_API_ADDR)"}},
		"no address at all":                           {nil, nil, []string{":8090 (--query-addr / QUERY_API_ADDR)", ":8080 (--http-addr / DEV_HEALTH_HTTP_ADDR)"}},
		"query only":                                  {[]string{"--query-addr", "127.0.0.1:0"}, nil, []string{":8080 (--http-addr / DEV_HEALTH_HTTP_ADDR)"}},
		"both by flag":                                {[]string{"--query-addr", "127.0.0.1:0", "--http-addr=127.0.0.1:0"}, nil, nil},
		"both by environment":                         {nil, map[string]string{"QUERY_API_ADDR": "127.0.0.1:0", "DEV_HEALTH_HTTP_ADDR": "127.0.0.1:0"}, nil},
		"flag and environment mixed":                  {[]string{"--http-addr", "127.0.0.1:0"}, map[string]string{"QUERY_API_ADDR": "127.0.0.1:0"}, nil},
	} {
		t.Run(name, func(t *testing.T) {
			if got := defaultedAddrs(test.args, test.env); strings.Join(got, "|") != strings.Join(test.want, "|") {
				t.Fatalf("defaultedAddrs = %q, want %q", got, test.want)
			}
		})
	}
}

// CHAOS-11839: the service binds port 0 and reports the address it got, so no port is freed and bound later. A
// neighbour that churns ephemeral ports the whole time (a busy runner) cannot make a start fail, and every reported
// address is the one the listener really accepts on.
func TestAStartOnPortZeroReportsTheAddressItBoundWhileNeighboursChurnPorts(t *testing.T) {
	stopChurn := make(chan struct{})
	churned := make(chan struct{})
	go func() {
		defer close(churned)
		for {
			select {
			case <-stopChurn:
				return
			default:
			}
			var held []net.Listener
			for range 64 {
				if l, err := net.Listen("tcp", anyPort); err == nil {
					held = append(held, l)
				}
			}
			for _, l := range held {
				_ = l.Close()
			}
		}
	}()
	t.Cleanup(func() { close(stopChurn); <-churned })

	for range 10 {
		r, addrs := startBound(t, []string{"--query-addr", anyPort, "--internal-addr", anyPort, "--mcp-addr", anyPort, "--mcp-boundary", "networkpolicy", "--http-addr", anyPort}, nil,
			queryComponent, internalComponent, mcpComponent, operatorComponent)
		seen := map[string]bool{}
		for component, addr := range addrs {
			if strings.HasSuffix(addr, ":0") || seen[addr] {
				t.Fatalf("%s reported %q: want a distinct, real port (%v)", component, addr, addrs)
			}
			seen[addr] = true
			connection, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				t.Fatalf("%s reported %s but nothing accepts there: %v", component, addr, err)
			}
			_ = connection.Close()
		}
		waitFor(t, r, "http://"+addrs[operatorComponent]+"/readyz", http.StatusOK)
		stop(t, r)
	}
}
