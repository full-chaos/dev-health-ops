package pagerduty

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
	"github.com/jackc/pgx/v5/pgxpool"
)

// nonNilPool satisfies Reconcile's nil-pool guard without opening a
// connection. Every case in this file returns before the transaction begins,
// which is precisely the property under test -- a pool that could connect
// would hide a regression that started touching Postgres on these paths.
func nonNilPool() *pgxpool.Pool { return &pgxpool.Pool{} }

func asPermanent(err error, target **streamrunner.PermanentError) bool {
	return errors.As(err, target)
}

// Both codex r1 P2 regressions live here. Neither needs Postgres: they fire
// before the binding lock, which is exactly why they were easy to miss.

// captureLogs swaps the default logger for one writing into a buffer, so a
// test can assert that a branch SAID something rather than merely returned
// the right error.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buffer := &bytes.Buffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buffer, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buffer
}

func testNativeReconciler(t *testing.T) (*NativeReconciler, *providerfoundation.Metrics) {
	t.Helper()
	metrics := providerfoundation.NewMetrics()
	reconciler := &NativeReconciler{
		entitlement: refusingEntitlement{},
		receipts:    nil,
		metrics:     metrics,
		now:         func() time.Time { return time.Date(2026, 7, 17, 12, 0, 5, 0, time.UTC) },
	}
	return reconciler, metrics
}

type refusingEntitlement struct{}

func (refusingEntitlement) Require(context.Context, string) error {
	return providersync.ErrIncidentEntitlementDisabled
}

// A permanently quarantined delivery must leave a line explaining itself.
// Both of these returns ran silently until codex r1 probed them: the
// dead-letter row appeared with nothing in the log naming a reason, which
// defeats the entire point of the bounded reason vocabulary.
func TestReconcileLogsEveryPreLockQuarantine(t *testing.T) {
	for _, test := range []struct {
		name  string
		event Event
		want  string
	}{
		{
			name:  "no binding id",
			event: Event{Payload: json.RawMessage(`{"event":{"id":"e"}}`), ReceiptID: "r1"},
			want:  "no binding id or no payload",
		},
		{
			name:  "no payload",
			event: Event{BindingID: "b", ReceiptID: "r2"},
			want:  "no binding id or no payload",
		},
		{
			name:  "payload is not json",
			event: Event{BindingID: "b", Payload: json.RawMessage(`{`), ReceiptID: "r3"},
			want:  "missing id, event_type, occurred_at, or data",
		},
		{
			name: "envelope has no event type",
			event: Event{
				BindingID: "b", ReceiptID: "r4",
				Payload: json.RawMessage(`{"event":{"id":"e","occurred_at":"2026-07-17T12:00:00Z","data":{"id":"x"}}}`),
			},
			want: "missing id, event_type, occurred_at, or data",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logs := captureLogs(t)
			// A pool is never reached: every case here returns before the
			// transaction begins, which is the property under test.
			reconciler := &NativeReconciler{pool: nonNilPool(), now: time.Now}
			err := reconciler.Reconcile(context.Background(), test.event, ReceiptClaim{})
			var permanent *streamrunner.PermanentError
			if !asPermanent(err, &permanent) || permanent.Reason != reasonSchemaInvalid {
				t.Fatalf("error = %v, want a %s PermanentError", err, reasonSchemaInvalid)
			}
			output := logs.String()
			if !strings.Contains(output, webhookMalformedEvent) {
				t.Fatalf("quarantine emitted no line: %q", output)
			}
			if !strings.Contains(output, test.want) {
				t.Fatalf("line does not say why: %q", output)
			}
			if !strings.Contains(output, "reason="+reasonSchemaInvalid) {
				t.Fatalf("line carries no reason: %q", output)
			}
			// The raw payload must never become a log field.
			if strings.Contains(output, `"data"`) {
				t.Fatalf("line leaked the payload: %q", output)
			}
		})
	}
}

// The entitlement refusal counter is owned by providersync's
// requireIncidentEntitlement, which the sinks call. This handler checks the
// entitlement BEFORE any sink, so it bypasses that counter -- and without an
// explicit increment the metric reads zero for every webhook a disabled org
// refuses, which is the most trustworthy-looking way for a metric to be wrong.
func TestDisabledEntitlementIsCountedNotOnlyLogged(t *testing.T) {
	reconciler, metrics := testNativeReconciler(t)
	before := renderedRefusals(t, metrics)
	if before != 0 {
		t.Fatalf("counter started at %d", before)
	}
	logs := captureLogs(t)
	reconciler.metrics.RecordIncidentEntitlementRefused(
		"pagerduty", pagerDutyWebhookEntitlementDataset,
		providersync.IncidentEntitlementSeamCollect,
	)
	reconciler.logRefusal(
		context.Background(),
		Event{BindingID: "b", ReceiptID: "r"},
		webhookEnvelope{},
		"org-1", reasonFeatureDisabled, providersync.ErrIncidentEntitlementDisabled,
	)
	if after := renderedRefusals(t, metrics); after != before+1 {
		t.Fatalf("refusals = %d, want %d", after, before+1)
	}
	if !strings.Contains(logs.String(), "reason="+reasonFeatureDisabled) {
		t.Fatalf("refusal line carries no reason: %q", logs.String())
	}
}

// renderedRefusals reads the counter the way a scrape does, so the assertion
// covers the label vocabulary too: a series minted under a different provider
// or dataset label would not be found here.
func renderedRefusals(t *testing.T, metrics *providerfoundation.Metrics) int {
	t.Helper()
	output := &bytes.Buffer{}
	if err := metrics.WritePrometheus(output); err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, line := range strings.Split(output.String(), "\n") {
		if !strings.Contains(line, "incident_entitlement_refused") ||
			strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, `provider="pagerduty"`) ||
			!strings.Contains(line, `dataset="`+pagerDutyWebhookEntitlementDataset+`"`) ||
			!strings.Contains(line, `seam="`+providersync.IncidentEntitlementSeamCollect+`"`) {
			continue
		}
		fields := strings.Fields(line)
		count, err := strconv.Atoi(fields[len(fields)-1])
		if err != nil {
			t.Fatalf("counter line is not numeric: %q", line)
		}
		total += count
	}
	return total
}

// NewNativeReconciler must refuse a nil metrics fragment the same way it
// refuses a nil pool: a counter nothing publishes is the defect this whole
// test file exists for.
func TestNewNativeReconcilerRequiresItsMetricsFragment(t *testing.T) {
	config := NativeReconcilerConfig{
		Pool: nonNilPool(), Entitlement: refusingEntitlement{}, Receipts: nilFence{},
		Sinks: func(string, providerfoundation.LeaseGuard) providersync.PagerDutyWebhookSinks {
			return providersync.PagerDutyWebhookSinks{}
		},
		Hydrator: func(LockedGraph, providerfoundation.LeaseGuard) providersync.PagerDutyIncidentHydrator {
			return nil
		},
	}
	if _, err := NewNativeReconciler(config); err == nil {
		t.Fatal("a nil Metrics fragment was accepted")
	}
	config.Metrics = providerfoundation.NewMetrics()
	if _, err := NewNativeReconciler(config); err != nil {
		t.Fatalf("complete config rejected: %v", err)
	}
}

type nilFence struct{}

func (nilFence) Assert(context.Context, ReceiptClaim) error { return nil }
