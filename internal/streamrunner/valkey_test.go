package streamrunner

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// The Python PagerDuty worker dead-letters to "<stream>:dlq". Routing must
// preserve that contract without changing where the existing families land.
func TestQuarantineStreamRoutesEachStreamFamilyToItsOwnDLQ(t *testing.T) {
	for _, test := range []struct{ stream, want string }{
		{stream: "pagerduty-webhooks:binding-1", want: "pagerduty-webhooks:binding-1:dlq"},
		{stream: "external-ingest:org-1:batches", want: "external-ingest:org-1:dlq"},
		{stream: "ingest:org-1:commits", want: "ingest:dlq:commits"},
		{stream: "product-telemetry:org-1:events", want: "product-telemetry:dlq"},
		{stream: "pagerduty-webhooks:", want: "product-telemetry:dlq"},
		{stream: "unknown", want: "product-telemetry:dlq"},
	} {
		if got := quarantineStream(test.stream); got != test.want {
			t.Fatalf("quarantineStream(%q) = %q, want %q", test.stream, got, test.want)
		}
	}
}

// "pagerduty-webhooks:*" also matches the DLQ key that the same runner writes,
// because a Redis glob cannot exclude the separator.
func TestRunnerNeverConsumesADeadLetterStream(t *testing.T) {
	transport := &fakeTransport{
		discovered: []string{
			"pagerduty-webhooks:binding-1",
			"pagerduty-webhooks:binding-1:dlq",
		},
		stats: StreamStats{Lag: 1},
	}
	config := testConfig()
	config.Streams = nil
	config.Patterns = []string{"pagerduty-webhooks:*"}
	config.DiscoveryLimit = 32
	runner, err := New(
		transport,
		handlerFunc(func(context.Context, Message) error { return nil }),
		config,
		health.NewRegistry(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.window(context.Background()); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if !slices.Equal(transport.ensured, []string{"pagerduty-webhooks:binding-1"}) {
		t.Fatalf("ensured groups = %v", transport.ensured)
	}
	if !slices.Equal(transport.readStreams[0], []string{"pagerduty-webhooks:binding-1"}) {
		t.Fatalf("read lanes = %v", transport.readStreams[0])
	}
}
