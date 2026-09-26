package workersignals

import (
	"strings"
	"testing"
)

// resetForTest zeroes the process-wide counters so the tests do not depend on order or -count.
func resetForTest() {
	heartbeatFail.Store(0)
	for index := range lockWait {
		lockWait[index].Store(0)
	}
	for index := range leaseRenewal {
		leaseRenewal[index].Store(0)
	}
}

func scrape(t *testing.T) string {
	t.Helper()
	var output strings.Builder
	if err := MetricsSource().WritePrometheus(&output); err != nil {
		t.Fatal(err)
	}
	return output.String()
}

// Every series exists on the very first scrape, at 0, before any event: a counter that has
// never incremented is otherwise absent, and a clean window cannot be told from "not exposed".
func TestEverySeriesIsWrittenAtZeroBeforeAnyEvent(t *testing.T) {
	resetForTest()
	body := scrape(t)
	for _, want := range []string{
		"worker_presence_heartbeat_failed_total 0\n",
		`dev_health_sync_dispatch_advisory_lock_wait_total{outcome="acquired"} 0` + "\n",
		`dev_health_sync_dispatch_advisory_lock_wait_total{outcome="busy"} 0` + "\n",
		`dev_health_sync_dispatch_advisory_lock_wait_total{outcome="other"} 0` + "\n",
		`dev_health_provider_sync_lease_renewal_failed_total{reason="error"} 0` + "\n",
		`dev_health_provider_sync_lease_renewal_failed_total{reason="lease_lost"} 0` + "\n",
		`dev_health_provider_sync_lease_renewal_failed_total{reason="other"} 0` + "\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("a fresh scrape lacks %q:\n%s", want, body)
		}
	}
	// One HELP/TYPE per family: a duplicate declaration makes most parsers refuse the scrape.
	for _, family := range []string{
		"worker_presence_heartbeat_failed_total", "dev_health_sync_dispatch_advisory_lock_wait_total",
		"dev_health_provider_sync_lease_renewal_failed_total",
	} {
		if got := strings.Count(body, "# TYPE "+family+" counter"); got != 1 {
			t.Fatalf("%s has %d TYPE lines, want exactly 1", family, got)
		}
	}
}

func TestRecordingMovesOnlyTheNamedSeriesAndBoundsUnknownValues(t *testing.T) {
	resetForTest()
	before := scrape(t)
	RecordHeartbeatFailed()
	RecordDispatchLockWait(LockOutcomeBusy)
	RecordDispatchLockWait("a-value-nobody-registered")
	RecordLeaseRenewalFailed(LeaseReasonLeaseLost)
	RecordLeaseRenewalFailed("SELECT secret FROM somewhere")
	after := scrape(t)
	if before == after {
		t.Fatal("recording changed nothing")
	}
	for _, unwanted := range []string{"a-value-nobody-registered", "SELECT secret"} {
		if strings.Contains(after, unwanted) {
			t.Fatalf("an unregistered value became a label: %q", unwanted)
		}
	}
	if strings.Count(after, "\n") != strings.Count(before, "\n") {
		t.Fatal("recording added a series: the vocabulary must be closed")
	}
	for _, want := range []string{
		"worker_presence_heartbeat_failed_total 1\n",
		`dev_health_sync_dispatch_advisory_lock_wait_total{outcome="busy"} 1` + "\n",
		`dev_health_sync_dispatch_advisory_lock_wait_total{outcome="other"} 1` + "\n",
		`dev_health_provider_sync_lease_renewal_failed_total{reason="lease_lost"} 1` + "\n",
		`dev_health_provider_sync_lease_renewal_failed_total{reason="other"} 1` + "\n",
	} {
		if !strings.Contains(after, want) {
			t.Fatalf("after recording, the scrape lacks %q:\n%s", want, after)
		}
	}
}
