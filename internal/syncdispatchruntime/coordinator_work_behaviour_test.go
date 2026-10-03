package syncdispatchruntime

import (
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"strings"
	"testing"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	"github.com/riverqueue/rivercontrib/otelriver"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
)

// CHAOS-7896 (D4217): every coordinator Work of the registration set is DRIVEN through the real otelriver middleware with a
// failing dependency and, where the dependency is an interface, a panicking one, each carrying a plain marker. At River's
// received error and at an in-memory span exporter: no exported string (status, every attribute value, every event name and
// attribute, span names) and no byte of the error text holds the marker; the error text is the fixed prefix and a Go type.

type driveMode int

const (
	driveFail driveMode = iota
	drivePanic
	drivePanicNil
)

const behaviourMarker = "the planted detail of ticket 7896 behaviour"

const (
	behaviourOrg = "11111111-1111-4111-8111-111111111111"
	behaviourRun = "22222222-2222-4222-8222-222222222222"
	behaviourBox = "33333333-3333-4333-8333-333333333333"
)

type markerFailure struct{ text string }

func (failure *markerFailure) Error() string { return failure.text }

type failingImporter struct{ panics, panicNil bool }

func (importer failingImporter) TeamAutoImport(context.Context, DomainReference) error {
	if importer.panicNil {
		panic(nil)
	}
	if importer.panics {
		panic(behaviourMarker)
	}
	return fmt.Errorf("GET https://u:p@host.example.test/x: %w", &markerFailure{behaviourMarker})
}

type failingDeriver struct{ panics, panicNil bool }

func (deriver failingDeriver) DeriveWithStats(context.Context, string) (int, int, bool, map[string]int, providersync.TeamRepoOwnershipDerivationStats, error) {
	if deriver.panicNil {
		panic(nil)
	}
	if deriver.panics {
		panic(behaviourMarker)
	}
	return 0, 0, false, nil, providersync.TeamRepoOwnershipDerivationStats{}, fmt.Errorf("query failed: %w", &markerFailure{behaviourMarker})
}

func transport() TransportArgs {
	return TransportArgs{Version: ContractVersionV1, OrgID: behaviourOrg, RunID: behaviourRun, DispatchOutbox: behaviourBox, RouteGeneration: 1}
}

func envelope() (jobcontract.DomainLink, string) {
	return jobcontract.DomainLink{Type: "sync_run", ID: behaviourRun}, behaviourRun
}

// cases are keyed by the worker TYPE name; the registration set is derived from worker.go and every registered type must have a case.
func behaviourCases() map[string]func(mode driveMode) func(ctx context.Context) error {
	domain, run := envelope()
	return map[string]func(driveMode) func(context.Context) error{
		"dispatchWorker": func(driveMode) func(context.Context) error {
			return func(ctx context.Context) error {
				return (&dispatchWorker{service: &NativeDispatchSyncRunService{}}).Work(ctx, &river.Job[DispatchSyncRunArgs]{Args: DispatchSyncRunArgs{transport()}})
			}
		},
		"finalizeWorker": func(driveMode) func(context.Context) error {
			return func(ctx context.Context) error {
				return (&finalizeWorker{service: &NativeFinalizeSyncRunService{}}).Work(ctx, &river.Job[FinalizeSyncRunArgs]{Args: FinalizeSyncRunArgs{transport()}})
			}
		},
		"postSyncWorker": func(driveMode) func(context.Context) error {
			return func(ctx context.Context) error {
				return (&postSyncWorker{service: &NativePostSyncService{}}).Work(ctx, &river.Job[PostSyncArgs]{Args: PostSyncArgs{transport()}})
			}
		},
		"referenceDiscoveryWorker": func(driveMode) func(context.Context) error {
			return func(ctx context.Context) error {
				return (&referenceDiscoveryWorker{service: &NativeReferenceDiscoveryService{}}).Work(ctx, &river.Job[ReferenceDiscoveryArgs]{Args: ReferenceDiscoveryArgs{transport()}})
			}
		},
		"teamAutoimportWorker": func(mode driveMode) func(context.Context) error {
			return func(ctx context.Context) error {
				args := TeamAutoimportJobArgs{Version: ContractVersionV1, OrgID: behaviourOrg, CorrelationID: "c", Idempotency: "i", Domain: domain, Payload: jobcontract.TeamAutoimportPayload{SyncRunID: run}}
				return (&teamAutoimportWorker{bridge: failingImporter{panics: mode == drivePanic, panicNil: mode == drivePanicNil}}).Work(ctx, &river.Job[TeamAutoimportJobArgs]{Args: args})
			}
		},
		"teamRepoOwnershipDerivationWorker": func(mode driveMode) func(context.Context) error {
			return func(ctx context.Context) error {
				args := TeamRepoOwnershipDerivationJobArgs{Version: ContractVersionV1, OrgID: behaviourOrg, CorrelationID: "c", Idempotency: "i", Domain: domain, Payload: jobcontract.TeamRepoOwnershipDerivationPayload{SyncRunID: run}}
				return (&teamRepoOwnershipDerivationWorker{service: failingDeriver{panics: mode == drivePanic, panicNil: mode == drivePanicNil}}).Work(ctx, &river.Job[TeamRepoOwnershipDerivationJobArgs]{Args: args})
			}
		},
	}
}

// pool-backed services have no dependency a test can make carry a marker without a database; for them the failing path is the
// service's own unavailable error (a nil pool): the contract asserted is that whatever error River receives has the fixed form.
var poolBacked = map[string]bool{"dispatchWorker": true, "finalizeWorker": true, "postSyncWorker": true, "referenceDiscoveryWorker": true}

func exportedStrings(spans tracetest.SpanStubs) []string {
	var seen []string
	for _, span := range spans {
		seen = append(seen, span.Name, span.Status.Description)
		for _, kv := range span.Attributes {
			seen = append(seen, string(kv.Key), kv.Value.Emit())
		}
		for _, event := range span.Events {
			seen = append(seen, event.Name)
			for _, kv := range event.Attributes {
				seen = append(seen, string(kv.Key), kv.Value.Emit())
			}
		}
	}
	return seen
}

func TestEveryRegisteredCoordinatorWorkHandsRiverAFixedErrorAndExportsNoMarker(t *testing.T) {
	registered, _ := registeredCoordinatorWorkers(t)
	cases := behaviourCases()
	if len(registered) < 6 {
		t.Fatalf("derived %d registered workers", len(registered))
	}
	for name := range registered {
		build, ok := cases[name]
		if !ok {
			t.Fatalf("registered worker %s has no behaviour case: add one (a new Work without a driven test fails)", name)
		}
		t.Run(name+"/failing", func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
			middleware := otelriver.NewMiddleware(&otelriver.MiddlewareConfig{TracerProvider: provider})
			err := middleware.Work(context.Background(), &rivertype.JobRow{Kind: name}, build(driveFail))
			if err == nil {
				t.Fatal("Work returned nil for a failing dependency")
			}
			if !strings.HasPrefix(err.Error(), "coordinator job failed: ") || strings.Contains(err.Error(), behaviourMarker) || strings.Contains(err.Error(), "example.test") {
				t.Fatalf("River received %q, want the fixed prefix and a Go type", err)
			}
			if !poolBacked[name] {
				var marker *markerFailure
				if !errors.As(err, &marker) {
					t.Fatal("the cause is no longer reachable through errors.As")
				}
			}
			spans := exporter.GetSpans()
			if len(spans) == 0 {
				t.Fatal("no span was exported")
			}
			sawRiver := false
			for _, span := range spans {
				if span.Name == "river.work" {
					sawRiver = true
				}
			}
			if !sawRiver {
				t.Fatal("no river.work span was exported")
			}
			for _, text := range exportedStrings(spans) {
				if strings.Contains(text, behaviourMarker) || strings.Contains(text, "example.test") {
					t.Fatalf("an exported span string holds the marker: %q", text)
				}
			}
		})
		if poolBacked[name] {
			continue
		}
		t.Run(name+"/panicking", func(t *testing.T) {
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				_ = build(drivePanic)(context.Background())
			}()
			if recovered != behaviourMarker {
				t.Fatalf("the panic did not propagate: %v", recovered)
			}
			spans := exporter.GetSpans()
			if len(spans) == 0 {
				t.Fatal("no span was exported for a panicking Work")
			}
			failed := false
			for _, span := range spans {
				if span.Status.Code == 1 {
					failed = true
				}
			}
			if !failed {
				t.Fatal("a panicking Work exported no failed span")
			}
			for _, text := range exportedStrings(spans) {
				if strings.Contains(text, behaviourMarker) {
					t.Fatalf("the panic value is on an exported span: %q", text)
				}
			}
		})
		t.Run(name+"/panic-nil", func(t *testing.T) {
			t.Setenv("GODEBUG", "panicnil=1")
			exporter := tracetest.NewInMemoryExporter()
			provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			t.Cleanup(func() { otel.SetTracerProvider(previous); _ = provider.Shutdown(context.Background()) })
			returnedNormally := false
			func() {
				// a swallowed panic(nil) lets this closure go on; a propagated one skips the last line
				defer func() { _ = recover() }()
				_ = build(drivePanicNil)(context.Background())
				returnedNormally = true
			}()
			if returnedNormally {
				t.Fatal("panic(nil) was swallowed: Work returned normally")
			}
			failed := false
			for _, span := range exporter.GetSpans() {
				if span.Status.Code == 1 {
					failed = true
				}
			}
			if !failed {
				t.Fatal("a Work that panicked with nil exported no failed span")
			}
		})
	}
}
