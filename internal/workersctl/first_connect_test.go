package workersctl

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// `workers routes apply` (the route-activate hook's init containers) waits for
// its first connect, writes one JSON line per retry with no DSN, host or error
// text, and fails closed at the bound with database_unavailable.
func TestRoutesApplyWaitsForTheFirstConnectThenFailsClosed(t *testing.T) {
	saved := firstConnect
	clock := time.Unix(0, 0)
	firstConnect = postgresstore.FirstConnect{
		Budget: 10 * time.Second, AttemptTimeout: time.Second,
		Now:   func() time.Time { return clock },
		Sleep: func(_ context.Context, d time.Duration) { clock = clock.Add(d) },
	}
	t.Cleanup(func() { firstConnect = saved })

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	uri := func(role string) string {
		return "postgresql://" + role + ":sekret-pw@" + address + "/devhealth?sslmode=disable"
	}
	var stdout, stderr bytes.Buffer
	code := execute(context.Background(), []string{
		"workers", "routes", "apply", "--reason", "helm_release_start", "--correlation-id", "t-route-activate", "dispatch_sync_run",
	}, dsnTestLookup(map[string]string{
		"POSTGRES_URI":             uri(defaultDomainRole),
		"WORKER_DATABASE_URI":      uri(defaultQueueRole),
		"COORDINATOR_DATABASE_URI": uri(defaultCoordinatorRole),
	}), &stdout, &stderr)
	out := stderr.String()
	if code != 1 || !strings.HasSuffix(out, `{"error":{"code":"database_unavailable"}}`+"\n") {
		t.Fatalf("exit %d, want 1 ending in database_unavailable:\n%s", code, out)
	}
	if !strings.Contains(out, `{"database_wait":{"store":"postgres","attempt":1,"elapsed_ms":`) ||
		!strings.Contains(out, `"category":"refused"`) {
		t.Fatalf("no wait record before the failure:\n%s", out)
	}
	// A 10 s budget on a clock only the pauses (2, 4, 4 s) move: 4 attempts, 3 records.
	if got := strings.Count(out, `{"database_wait":`); got != 3 {
		t.Fatalf("%d wait records, want 3:\n%s", got, out)
	}
	if strings.Contains(out, "sekret-pw") || strings.Contains(out, address) {
		t.Fatalf("the output names the password or the address:\n%s", out)
	}
}
