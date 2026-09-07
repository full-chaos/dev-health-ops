package main

import (
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// TestBuildOperationalHTTPDispatcher_WebhookOnlyConstructsNone_HelperContract
// pins buildOperationalHTTPDispatcher's OWN per-kind contract in isolation:
// webhook_delivery alone needs no HTTP bridge at all (CHAOS-5320/#2346
// deleted the webhook leg), so it must construct no dispatcher and must not
// fail on an empty/unset bridge URL it never uses.
//
// codex r1 F2 (CHAOS-5384): this scenario is NOT production-reachable
// TODAY, and this test does not claim it is. Per
// contracts/jobs/v1/registry.json, billing_notification and webhook_delivery
// both sit on the "webhooks" queue, so any queue selection that enables
// webhook_delivery also enables billing_notification in the same
// buildOperationalWorker call -- a real worker can never reach
// buildOperationalHTTPDispatcher with webhook_delivery as its only spec. See
// TestBuildOperationalHTTPDispatcher_BillingAndWebhookTogetherConstructsOne
// below for the actual production-reachable shape. This test earns its
// keep as a helper-contract/unit test of buildOperationalHTTPDispatcher's
// own kind-membership logic, and becomes production-reachable once
// CHAOS-5353 removes billing_notification from operationalHTTPDispatchedKinds
// (or splits it off the shared queue).
func TestBuildOperationalHTTPDispatcher_WebhookOnlyConstructsNone_HelperContract(t *testing.T) {
	specs := []jobruntime.HandlerSpec{{Kind: jobcontract.KindWebhookDelivery}}
	dispatcher, err := buildOperationalHTTPDispatcher(config.Config{}, specs, discardLogger())
	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}
	if dispatcher != nil {
		t.Fatalf("dispatcher = %#v, want nil for a webhook-only configuration", dispatcher)
	}
}

// TestBuildOperationalHTTPDispatcher_BillingAndWebhookTogetherConstructsOne
// is the production-reachable sibling of the helper-contract test above:
// contracts/jobs/v1/registry.json puts billing_notification and
// webhook_delivery on the SAME "webhooks" queue, so a real worker selecting
// that queue always presents buildOperationalHTTPDispatcher with BOTH kinds
// together, never webhook_delivery alone. Since CHAOS-5353 NEITHER of those
// two kinds routes through the HTTP bridge any more -- billing renders and
// sends natively, and CHAOS-5320 deleted the webhook leg -- so this shape must
// now construct NO dispatcher at all. That is the point of the lazy build: a
// worker serving only natively-handled kinds cannot fail on a bridge URL it
// never uses.
func TestBuildOperationalHTTPDispatcher_BillingAndWebhookTogetherConstructsNone(t *testing.T) {
	specs := []jobruntime.HandlerSpec{
		{Kind: jobcontract.KindBillingNotification},
		{Kind: jobcontract.KindWebhookDelivery},
	}
	cfg := config.Config{
		OperationalBridgeURL:     "https://internal.example.com",
		OperationalBridgeToken:   secrets.NewValue("test-token"),
		OperationalBridgeTimeout: time.Second,
	}
	dispatcher, err := buildOperationalHTTPDispatcher(cfg, specs, discardLogger())
	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}
	if dispatcher != nil {
		t.Fatalf("dispatcher = %#v, want nil: neither billing nor webhook uses "+
			"the HTTP bridge any more", dispatcher)
	}
}

// The counterpart to the above, and the reason it is safe: a billing-only
// worker must no longer fail on an EMPTY bridge URL, because it never touches
// the bridge. Before CHAOS-5353 this same configuration was required to fail
// fast; now requiring it would make a natively-served worker refuse to start
// over a setting it does not use.
func TestBuildOperationalHTTPDispatcher_BillingWithEmptyURLNoLongerFails(t *testing.T) {
	specs := []jobruntime.HandlerSpec{{Kind: jobcontract.KindBillingNotification}}
	cfg := config.Config{
		OperationalBridgeURL:     "",
		OperationalBridgeToken:   secrets.NewValue("test-token"),
		OperationalBridgeTimeout: time.Second,
	}
	dispatcher, err := buildOperationalHTTPDispatcher(cfg, specs, discardLogger())
	if err != nil {
		t.Fatalf("a billing-only worker must not fail on an unused bridge URL, got %v", err)
	}
	if dispatcher != nil {
		t.Fatalf("dispatcher = %#v, want nil", dispatcher)
	}
}

// The failure path CHAOS-5384 added is still live, just anchored on the kind
// that genuinely still needs the bridge: an enabled HEARTBEAT with an empty
// bridge URL must fail fast with a bounded reason rather than default to an
// empty, silently-invalid endpoint.
func TestBuildOperationalHTTPDispatcher_HeartbeatWithEmptyURLFails(t *testing.T) {
	specs := []jobruntime.HandlerSpec{{Kind: jobcontract.KindHeartbeat}}
	cfg := config.Config{
		OperationalBridgeURL:     "",
		OperationalBridgeToken:   secrets.NewValue("test-token"),
		OperationalBridgeTimeout: time.Second, // valid, so the failure isolates the empty URL
	}
	dispatcher, err := buildOperationalHTTPDispatcher(cfg, specs, discardLogger())
	if err == nil {
		t.Fatal("expected an error for an enabled heartbeat kind with an empty bridge URL")
	}
	if dispatcher != nil {
		t.Fatalf("dispatcher = %#v, want nil on failure", dispatcher)
	}
	var failure dependencyFailure
	if !errors.As(err, &failure) || failure.reason != "operational_http_dispatcher_misconfigured" {
		t.Fatalf("error = %v, want a named operational_http_dispatcher_misconfigured reason", err)
	}
}

// A heartbeat-only worker also still routes through the HTTP bridge (it is a
// separate Python-compatibility shim, unaffected by CHAOS-5320's webhook
// deletion), so it must construct a dispatcher when correctly configured.
func TestBuildOperationalHTTPDispatcher_HeartbeatConstructsOne(t *testing.T) {
	specs := []jobruntime.HandlerSpec{{Kind: jobcontract.KindHeartbeat}}
	cfg := config.Config{
		OperationalBridgeURL:     "https://internal.example.com",
		OperationalBridgeToken:   secrets.NewValue("test-token"),
		OperationalBridgeTimeout: time.Second, // within NewHTTPDispatcher's [100ms, 30s] bounds
	}
	dispatcher, err := buildOperationalHTTPDispatcher(cfg, specs, discardLogger())
	if err != nil {
		t.Fatalf("unexpected error = %v", err)
	}
	if dispatcher == nil {
		t.Fatal("dispatcher = nil, want a constructed dispatcher for an enabled heartbeat kind")
	}
}
