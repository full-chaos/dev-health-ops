package rivermigrate

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// The package's tests that reach an unreachable database wait 1 s, not the
// 60 s production budget.
func TestMain(m *testing.M) {
	firstConnect = postgresstore.FirstConnect{Budget: time.Second, AttemptTimeout: time.Second}
	os.Exit(m.Run())
}

func refusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

// fakeClockFirstConnect is a 10 s budget on a clock only the pauses move:
// pauses of 2, 4 and 4 s, so 4 attempts and 3 retry lines. onPause runs at
// each pause with its number.
func fakeClockFirstConnect(onPause func(int)) postgresstore.FirstConnect {
	clock := time.Unix(0, 0)
	pauses := 0
	return postgresstore.FirstConnect{
		Budget: 10 * time.Second, AttemptTimeout: time.Second,
		Now: func() time.Time { return clock },
		Sleep: func(_ context.Context, d time.Duration) {
			clock = clock.Add(d)
			pauses++
			if onPause != nil {
				onPause(pauses)
			}
		},
	}
}

func useFirstConnect(t *testing.T, wait postgresstore.FirstConnect) {
	t.Helper()
	saved := firstConnect
	firstConnect = wait
	t.Cleanup(func() { firstConnect = saved })
}

func rolesLookup(uri string) func(string) (string, bool) {
	return envLookup(map[string]string{
		"MIGRATION_DATABASE_URI":              uri,
		"RIVER_DOMAIN_DATABASE_ROLE":          "devhealth_domain",
		"RIVER_DOMAIN_DATABASE_PASSWORD":      "pw-domain",
		"RIVER_QUEUE_DATABASE_ROLE":           "devhealth_queue",
		"RIVER_QUEUE_DATABASE_PASSWORD":       "pw-queue",
		"RIVER_COORDINATOR_DATABASE_PASSWORD": "pw-coordinator",
	})
}

// `migrate river` (river-migrate hook) waits for its first connect, writes one
// line per retry, and fails closed at the bound with its unavailable message.
func TestRiverWaitsForTheFirstConnectThenFailsClosed(t *testing.T) {
	useFirstConnect(t, fakeClockFirstConnect(nil))
	uri := "postgresql://migrator:sekret-pw@" + refusedAddress(t) + "/devhealth?sslmode=disable"
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{"--apply-and-check"}, env(map[string]string{
		"MIGRATION_DATABASE_URI":     uri,
		"RIVER_DOMAIN_DATABASE_ROLE": "devhealth_domain",
		"RIVER_QUEUE_DATABASE_ROLE":  "devhealth_queue",
	}), &stdout, &stderr)
	out := stderr.String()
	if code != 1 || !strings.Contains(out, "migration error: PostgreSQL migration endpoint unavailable") {
		t.Fatalf("exit %d, want 1 with the unavailable message:\n%s", code, out)
	}
	if got := strings.Count(out, "migrate river waiting: PostgreSQL migration endpoint not reachable (attempt "); got != 3 ||
		strings.Count(out, ", refused, next in ") != 3 {
		t.Fatalf("%d wait lines, want 3 refused lines (4 attempts in the 10 s budget):\n%s", got, out)
	}
	if strings.Contains(out, "sekret-pw") {
		t.Fatalf("the output names the password:\n%s", out)
	}
}

// `migrate roles` (provision-roles hook, river-hooks.yaml; prod rolls s89-a and
// s90-a saw its first connect refused) waits for its first connect, writes one
// line per retry, and fails closed at the bound with the unavailable message.
func TestProvisionRolesWaitsForTheFirstConnectThenFailsClosed(t *testing.T) {
	useFirstConnect(t, fakeClockFirstConnect(nil))
	uri := "postgresql://migrator:sekret-pw@" + refusedAddress(t) + "/devhealth?sslmode=disable"
	var stdout, stderr bytes.Buffer
	code := ExecuteRoles(context.Background(), "dho", nil, rolesLookup(uri), &stdout, &stderr)
	out := stderr.String()
	if code != 1 || !strings.Contains(out, "migration error: PostgreSQL migration endpoint unavailable") {
		t.Fatalf("exit %d, want 1 with the unavailable message:\n%s", code, out)
	}
	if got := strings.Count(out, "migrate roles waiting: PostgreSQL migration endpoint not reachable (attempt "); got != 3 ||
		strings.Count(out, ", refused, next in ") != 3 {
		t.Fatalf("%d wait lines, want 3 refused lines (4 attempts in the 10 s budget):\n%s", got, out)
	}
	for _, secret := range []string{"sekret-pw", "pw-domain", "pw-queue", "pw-coordinator"} {
		if strings.Contains(out, secret) {
			t.Fatalf("the output names a password:\n%s", out)
		}
	}
}

// proxyTo listens on address and forwards every connection to target.
func proxyTo(t *testing.T, address, target string) {
	t.Helper()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		t.Fatalf("listen again on %s: %v", address, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				server, err := net.Dial("tcp", target)
				if err != nil {
					return
				}
				defer server.Close()
				go func() { _, _ = io.Copy(server, client) }()
				_, _ = io.Copy(client, server)
			}()
		}
	}()
}

// `migrate roles` against an endpoint that refuses twice and then opens: the
// wait connects on the third attempt and the verb goes on to provision. The
// server accepts the login and the ping and fails every statement, so the
// verb then fails at the apply step, not at the connect.
func TestProvisionRolesConnectsOnceTheEndpointOpens(t *testing.T) {
	server := fakepg.StartEchoing(t)
	target := net.JoinHostPort(server.Host, strconv.Itoa(server.Port))
	address := refusedAddress(t)
	useFirstConnect(t, fakeClockFirstConnect(func(pause int) {
		if pause == 2 {
			proxyTo(t, address, target)
		}
	}))
	uri := "postgresql://migrator:sekret-pw@" + address + "/appdb?sslmode=disable"
	before := server.Connections()
	var stdout, stderr bytes.Buffer
	code := ExecuteRoles(context.Background(), "dho", nil, rolesLookup(uri), &stdout, &stderr)
	out := stderr.String()
	if got := strings.Count(out, "migrate roles waiting: PostgreSQL migration endpoint not reachable (attempt "); got != 2 ||
		strings.Count(out, ", refused, next in ") != 2 {
		t.Fatalf("%d wait lines, want 2 refused lines before the endpoint opened:\n%s", got, out)
	}
	if strings.Contains(out, "endpoint unavailable") {
		t.Fatalf("the wait did not connect once the endpoint opened:\n%s", out)
	}
	if server.Connections() == before {
		t.Fatal("no connection reached the server after it opened")
	}
	if code != 1 || !strings.Contains(out, "migration failed: the runtime roles were not provisioned") {
		t.Fatalf("exit %d, want the apply step's failure after the connect:\n%s", code, out)
	}
	if strings.Contains(out, "sekret-pw") || len(server.Leaks(out)) != 0 {
		t.Fatalf("the output names a credential:\n%s", out)
	}
}
