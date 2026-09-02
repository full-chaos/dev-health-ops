package authruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// reservePort binds an ephemeral port, records it, and releases it. The
// alternative -- letting the service bind :0 -- would leave the test with no
// way to address the listener, since Execute deliberately exposes no handle to
// its components. The reserve-and-release window is the standard trade and is
// re-attempted by the readiness poll below if anything grabs the port first.
func reservePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release reserved port: %v", err)
	}
	return address
}

// brokenEnvironment is the deliberately-broken deployment CHAOS-4881's
// executed proof calls for: a syntactically valid DSN pointing at a port
// nothing listens on, and a signing-key path naming a file that does not
// exist. Both are DEPENDENCY faults, so the process must start and report
// them, not refuse to boot.
func brokenEnvironment(t *testing.T, apiAddress, operatorAddress string) map[string]string {
	t.Helper()
	return map[string]string{
		// Port 1 is reserved and unbound: the dial is refused immediately
		// rather than hanging, which keeps the proof fast and deterministic.
		authconfig.EnvDatabaseURI:     "postgres://auth@127.0.0.1:1/devhealth",
		authconfig.EnvSigningKeyFile:  filepath.Join(t.TempDir(), "absent-signing-key.pem"),
		authconfig.EnvSigningKeyID:    "auth-2026-09",
		authconfig.EnvAPIAddress:      apiAddress,
		authconfig.EnvOperatorAddress: operatorAddress,
		// Keep the readiness checks snappy so a failing probe answers inside
		// the poll budget below.
		authconfig.EnvHealthCheckTimeout: "1s",
		authconfig.EnvShutdownTimeout:    "5s",
		authconfig.EnvLogLevel:           "error",
	}
}

func lookupFrom(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// safeBuffer is a concurrency-safe sink for the process's own log stream: the
// service writes from its own goroutines while the test reads.
type safeBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (s *safeBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.Write(p)
}

func (s *safeBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buffer.String()
}

// runService starts Execute in its own goroutine and returns a stop function
// that cancels it and returns the exit code.
func runService(t *testing.T, env map[string]string) (*safeBuffer, func() int) {
	t.Helper()
	logs := &safeBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	exit := make(chan int, 1)
	go func() {
		exit <- Execute(ctx, nil, lookupFrom(env), IO{Stdout: logs, Stderr: logs})
	}()

	stopped := false
	stop := func() int {
		if stopped {
			t.Fatal("stop called twice")
		}
		stopped = true
		cancel()
		select {
		case code := <-exit:
			return code
		case <-time.After(30 * time.Second):
			t.Fatal("Execute did not return within 30s of cancellation")
			return -1
		}
	}
	t.Cleanup(func() {
		if !stopped {
			cancel()
			<-exit
		}
	})
	return logs, stop
}

// awaitResponse polls url until it answers, so the test never races the
// listener's bind.
func awaitResponse(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		response, err := client.Get(url)
		if err == nil {
			return response
		}
		lastErr = err
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("no response from %s within 20s: %v", url, lastErr)
	return nil
}

// TestReadinessFailsClosedOnBrokenDatabaseAndSigningKey is CHAOS-4881's
// executed negative proof, run as a test so CI re-runs it on every change
// rather than trusting a transcript pasted once.
//
// It starts the real Execute path -- the same code cmd/auth-service/main.go
// calls -- with a deliberately broken DB DSN and a deliberately missing
// signing-key file, and asserts /readyz answers 503 naming BOTH failing
// checks, while /healthz still answers 200 because the process itself is
// alive. Liveness and readiness collapsing into one signal is the defect
// CHAOS-4512 recorded; this is the control against it.
func TestReadinessFailsClosedOnBrokenDatabaseAndSigningKey(t *testing.T) {
	apiAddress := reservePort(t)
	operatorAddress := reservePort(t)
	env := brokenEnvironment(t, apiAddress, operatorAddress)

	logs, stop := runService(t, env)
	client := &http.Client{Timeout: 10 * time.Second}
	defer client.CloseIdleConnections()

	response := awaitResponse(t, client, "http://"+operatorAddress+"/readyz")
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("read /readyz body: %v", err)
	}

	t.Logf("GET http://%s/readyz -> %d %s", operatorAddress, response.StatusCode, strings.TrimSpace(string(body)))

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("/readyz = %d, want 503", response.StatusCode)
	}
	var readiness struct {
		Status       string   `json:"status"`
		FailedChecks []string `json:"failed_checks"`
	}
	if err := json.Unmarshal(body, &readiness); err != nil {
		t.Fatalf("/readyz body %q is not JSON: %v", body, err)
	}
	if readiness.Status != "not_ready" {
		t.Fatalf("status = %q, want not_ready", readiness.Status)
	}
	for _, want := range []string{CheckPostgres, CheckSigningKey} {
		if !slices.Contains(readiness.FailedChecks, want) {
			t.Fatalf("failed_checks = %v, want it to contain %q", readiness.FailedChecks, want)
		}
	}
	// The response names the failing CHECKS and nothing else. The DSN's host
	// and the key file's path are what a pgx dial error and an fs.PathError
	// would carry, and this surface is unauthenticated (CHAOS-4724).
	for _, forbidden := range []string{"127.0.0.1:1", "absent-signing-key", os.TempDir()} {
		if forbidden != "" && bytes.Contains(body, []byte(forbidden)) {
			t.Fatalf("/readyz body leaked %q: %s", forbidden, body)
		}
	}

	// Liveness is a separate signal: the process is alive even though its
	// dependencies are not.
	live := awaitResponse(t, client, "http://"+operatorAddress+"/healthz")
	liveBody, _ := io.ReadAll(live.Body)
	_ = live.Body.Close()
	t.Logf("GET http://%s/healthz -> %d %s", operatorAddress, live.StatusCode, strings.TrimSpace(string(liveBody)))
	if live.StatusCode != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", live.StatusCode)
	}

	// The API listener is up and dormant: every path gets the 404 envelope.
	api := awaitResponse(t, client, "http://"+apiAddress+"/v1/tokens")
	apiBody, _ := io.ReadAll(api.Body)
	_ = api.Body.Close()
	t.Logf("GET http://%s/v1/tokens -> %d %s", apiAddress, api.StatusCode, strings.TrimSpace(string(apiBody)))
	if api.StatusCode != http.StatusNotFound {
		t.Fatalf("dormant API = %d, want 404", api.StatusCode)
	}

	if code := stop(); code != 0 {
		t.Fatalf("Execute exited %d after a clean shutdown, want 0", code)
	}
	// The operator can still diagnose: the reason codes are in the log even
	// though they are not in the response.
	if transcript := logs.String(); !strings.Contains(transcript, "postgres_unreachable") ||
		!strings.Contains(transcript, "key_file_unreadable") {
		t.Fatalf("the log did not carry both bounded reason codes:\n%s", transcript)
	}
}

// TestGracefulShutdownLeavesNoGoroutineBehind is CHAOS-4881's second executed
// proof: "No goroutine or background worker outlives its lifecycle owner
// (verified, not just claimed)". Run under -race in CI.
//
// The baseline is taken after a warm-up service has already started and
// stopped once, so that one-time lazily-created runtime goroutines (the HTTP
// transport's, the DNS resolver's) are already in the count and cannot be
// mistaken for a leak.
func TestGracefulShutdownLeavesNoGoroutineBehind(t *testing.T) {
	client := &http.Client{Timeout: 10 * time.Second}

	start := func() (string, func() int) {
		apiAddress := reservePort(t)
		operatorAddress := reservePort(t)
		_, stop := runService(t, brokenEnvironment(t, apiAddress, operatorAddress))
		response := awaitResponse(t, client, "http://"+operatorAddress+"/readyz")
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return operatorAddress, stop
	}

	// Warm-up cycle: start, exercise, stop.
	_, stopWarmup := start()
	if code := stopWarmup(); code != 0 {
		t.Fatalf("warm-up Execute exited %d", code)
	}
	client.CloseIdleConnections()
	baseline := settledGoroutines(t)
	t.Logf("goroutines after warm-up cycle: %d", baseline)

	// Measured cycle.
	_, stop := start()
	duringRun := runtime.NumGoroutine()
	t.Logf("goroutines while the service is running: %d", duringRun)
	if duringRun <= baseline {
		t.Fatalf(
			"the running service added no goroutines (%d vs baseline %d); "+
				"the measurement cannot detect a leak it never saw created",
			duringRun, baseline,
		)
	}

	if code := stop(); code != 0 {
		t.Fatalf("Execute exited %d after a clean shutdown, want 0", code)
	}
	client.CloseIdleConnections()

	after := settledGoroutines(t)
	t.Logf("goroutines after graceful shutdown: %d (baseline %d)", after, baseline)
	if after > baseline {
		t.Fatalf(
			"%d goroutine(s) outlived the runtime (baseline %d, after %d):\n%s",
			after-baseline, baseline, after, goroutineDump(),
		)
	}
}

// settledGoroutines waits for the count to stop falling before reading it: a
// goroutine that has been asked to stop unwinds asynchronously, so an
// immediate read reports a leak that is not one. It returns the LOWEST count
// observed, so the assertion above cannot be satisfied by a transient dip.
func settledGoroutines(t *testing.T) int {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	lowest := runtime.NumGoroutine()
	stableSince := time.Now()
	for time.Now().Before(deadline) {
		runtime.Gosched()
		current := runtime.NumGoroutine()
		if current < lowest {
			lowest = current
			stableSince = time.Now()
		}
		if time.Since(stableSince) > time.Second {
			return lowest
		}
		time.Sleep(20 * time.Millisecond)
	}
	return lowest
}

func goroutineDump() string {
	buffer := make([]byte, 1<<16)
	return string(buffer[:runtime.Stack(buffer, true)])
}

func TestExecuteRejectsAConfigurationFaultBeforeStarting(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(map[string]string)
		wantSub string
	}{
		{
			name:    "no database configured",
			mutate:  func(env map[string]string) { delete(env, authconfig.EnvDatabaseURI) },
			wantSub: "is required",
		},
		{
			name: "signing material supplied directly",
			mutate: func(env map[string]string) {
				env[authconfig.EnvSigningKeyDirect] = "-----BEGIN PRIVATE KEY-----"
			},
			wantSub: "must not carry signing material directly",
		},
		{
			name: "both secret sources set",
			mutate: func(env map[string]string) {
				env[authconfig.EnvDatabaseURI+"_FILE"] = "/run/secrets/dsn"
			},
			wantSub: "mutually exclusive",
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			env := brokenEnvironment(t, reservePort(t), reservePort(t))
			testCase.mutate(env)
			var stderr bytes.Buffer
			code := Execute(context.Background(), nil, lookupFrom(env), IO{Stderr: &stderr})
			if code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}
			if !strings.Contains(stderr.String(), testCase.wantSub) {
				t.Fatalf("stderr = %q, want it to contain %q", stderr.String(), testCase.wantSub)
			}
		})
	}
}

// TestConfigurationErrorsAreRedacted: Execute's configuration-error path
// writes to a stream that a deployment captures into logs, so a value quoted
// back from the operator's own environment goes through the redactor first.
func TestConfigurationErrorsAreRedacted(t *testing.T) {
	env := brokenEnvironment(t, reservePort(t), reservePort(t))
	env[authconfig.EnvDatabaseURI] = "postgres://auth:hunter2@db.internal:5432/devhealth"
	env[authconfig.EnvDatabaseURI+"_FILE"] = "/run/secrets/dsn"

	var stderr bytes.Buffer
	if code := Execute(context.Background(), nil, lookupFrom(env), IO{Stderr: &stderr}); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if strings.Contains(stderr.String(), "hunter2") {
		t.Fatalf("stderr leaked a credential: %q", stderr.String())
	}
}

func TestArgumentHandling(t *testing.T) {
	cases := []struct {
		name     string
		args     []string
		wantCode int
		wantOut  string
	}{
		{name: "help", args: []string{"--help"}, wantCode: 0, wantOut: "--signing-key-file"},
		{name: "unknown flag", args: []string{"--nope"}, wantCode: 2, wantOut: ""},
		{name: "positional argument", args: []string{"serve"}, wantCode: 2, wantOut: ""},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			env := brokenEnvironment(t, reservePort(t), reservePort(t))
			code := Execute(
				context.Background(), testCase.args, lookupFrom(env),
				IO{Stdout: &stdout, Stderr: &stderr},
			)
			if code != testCase.wantCode {
				t.Fatalf("exit code = %d, want %d (stderr: %s)", code, testCase.wantCode, stderr.String())
			}
			if testCase.wantOut != "" && !strings.Contains(stdout.String(), testCase.wantOut) {
				t.Fatalf("stdout = %q, want it to contain %q", stdout.String(), testCase.wantOut)
			}
		})
	}
}

func TestVersionFlagPrintsBuildMetadata(t *testing.T) {
	var stdout bytes.Buffer
	env := brokenEnvironment(t, reservePort(t), reservePort(t))
	code := Execute(
		context.Background(), []string{"--version"}, lookupFrom(env),
		IO{Stdout: &stdout},
	)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	var metadata map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &metadata); err != nil {
		t.Fatalf("--version output %q is not JSON: %v", stdout.String(), err)
	}
	if metadata["service"] != authconfig.Service {
		t.Fatalf("service = %v, want %q", metadata["service"], authconfig.Service)
	}
}

// TestFlagsOverrideTheEnvironment proves the flag layer is wired to the same
// resolution site as the environment, end to end through Execute: a flag that
// only existed in --help would fail this.
func TestFlagsOverrideTheEnvironment(t *testing.T) {
	apiAddress := reservePort(t)
	operatorAddress := reservePort(t)
	env := brokenEnvironment(t, apiAddress, operatorAddress)
	// Point the environment at an address the flag will override. If the flag
	// were ignored, the operator listener would bind the environment's
	// address and the poll below would never succeed.
	flagOperator := reservePort(t)
	env[authconfig.EnvOperatorAddress] = operatorAddress

	logs := &safeBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exit := make(chan int, 1)
	go func() {
		exit <- Execute(
			ctx, []string{"--operator-http-addr", flagOperator}, lookupFrom(env),
			IO{Stdout: logs, Stderr: logs},
		)
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	defer client.CloseIdleConnections()
	response := awaitResponse(t, client, "http://"+flagOperator+"/healthz")
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()

	// The environment's address must be free: nothing bound it.
	listener, err := net.Listen("tcp", operatorAddress)
	if err != nil {
		t.Fatalf("the environment's operator address is bound, so the flag did not win: %v", err)
	}
	_ = listener.Close()

	cancel()
	select {
	case code := <-exit:
		if code != 0 {
			t.Fatalf("Execute exited %d", code)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Execute did not return")
	}
}

// TestRoutesAreEmpty is the dormancy assertion at the call site. If a later
// wave mounts a route without updating CHAOS-4881's dormancy claim, this test
// is what says so.
func TestRoutesAreEmpty(t *testing.T) {
	if got := len(Routes()); got != 0 {
		t.Fatalf(
			"Routes() returns %d route(s); CHAOS-4881 builds this service dormant, "+
				"so mounting one is a deliberate change that must update the dormancy claim",
			got,
		)
	}
}
