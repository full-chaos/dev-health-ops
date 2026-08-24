package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/platform/lifecycle"
	"github.com/full-chaos/dev-health-ops/internal/streamhandlers"
	"github.com/full-chaos/dev-health-ops/internal/streamrunner"
)

// observingStreamStorage records the observer the composition root hands to
// Handler, which is the only place the answer is decidable. A test that
// inspected newExternalIngestMetrics directly would prove the constructor
// exists, not that anything reaches the handler -- and a registry-owned
// collector held by nobody is exactly the shape that reads as coverage while
// publishing nothing.
type observingStreamStorage struct {
	observers map[streamHandlerKind]streamhandlers.ExternalIngestObserver
	seen      []streamHandlerKind
}

func (*observingStreamStorage) ClickHouseReady(context.Context) error     { return nil }
func (*observingStreamStorage) DomainPostgresReady(context.Context) error { return nil }
func (*observingStreamStorage) ValkeyReady(context.Context) error         { return nil }

func (storage *observingStreamStorage) Handler(
	kind streamHandlerKind, observer streamhandlers.ExternalIngestObserver,
) (streamrunner.Handler, error) {
	if storage.observers == nil {
		storage.observers = map[streamHandlerKind]streamhandlers.ExternalIngestObserver{}
	}
	storage.observers[kind] = observer
	storage.seen = append(storage.seen, kind)
	return streamCommandHandler{}, nil
}

func (*observingStreamStorage) NewTransport() (streamrunner.Transport, error) {
	return &streamCommandTransport{}, nil
}
func (*observingStreamStorage) ControlComponents() []lifecycle.Component { return nil }
func (*observingStreamStorage) Close()                                   {}

// TestExternalProfileWiresReachableIngestCounters proves the CHAOS-4194
// counters are REACHABLE, not merely constructed: the external profile must
// hand the handler a live observer AND publish that same collector on the
// operator endpoint. Owning the constructor is not the same as the binary
// building it -- a sibling port satisfied a file:line acceptance bar while its
// worker family came back empty because a config switch was missing from the
// activation condition.
//
// The table walks every profile rather than asserting the external one alone,
// so a future profile that starts ingesting customer pushes without wiring the
// counters fails here instead of shipping blind.
func TestExternalProfileWiresReachableIngestCounters(t *testing.T) {
	for _, testCase := range []struct {
		profile      string
		kind         streamHandlerKind
		wantObserver bool
	}{
		{profile: "external", kind: externalIngestHandlerKind, wantObserver: true},
		{profile: "ingest", kind: internalIngestHandlerKind, wantObserver: false},
	} {
		t.Run(testCase.profile, func(t *testing.T) {
			storage := &observingStreamStorage{}
			registry := health.NewRegistry(100 * time.Millisecond)
			components, err := configureStreamRunnerDependenciesWithSources(
				context.Background(),
				config.Config{Profile: testCase.profile, StreamConfiguredReplicas: 1},
				registry,
				streamDependencySources{
					openStorage: func(context.Context, config.Config, *slog.Logger) (streamStorage, error) {
						return storage, nil
					},
				},
				nil,
			)
			if err != nil {
				t.Fatal(err)
			}
			if len(components) == 0 {
				t.Fatal("profile built no stream consumers")
			}
			observer, built := storage.observers[testCase.kind]
			if !built {
				t.Fatalf("%s handler was never constructed: %v", testCase.kind, storage.seen)
			}
			if !testCase.wantObserver {
				if observer != nil {
					t.Fatalf("%s was handed an observer it has nothing to report to", testCase.profile)
				}
				return
			}
			if observer == nil {
				t.Fatal("external ingest handler was constructed without an observer")
			}
			// A live observer that no scrape reads is the same as no observer,
			// so the counters must also appear in the operator exposition.
			if err := observer.ObserveExternalProjectMembershipsSunk("github", 1); err != nil {
				t.Fatal(err)
			}
			if err := observer.ObserveExternalKindRefused("github", "unsupported_kind_for_system"); err != nil {
				t.Fatal(err)
			}
			var exposition strings.Builder
			outcomes, err := registry.WriteMetricsPartial(&exposition)
			if err != nil {
				t.Fatal(err)
			}
			for _, outcome := range outcomes {
				if outcome.Source == "external_ingest_records" && outcome.Err != nil {
					t.Fatalf("external_ingest fragment failed to write: %v", outcome.Err)
				}
			}
			for _, want := range []string{
				`worker_external_project_memberships_sunk_total{provider="github"} 1`,
				`worker_external_record_refused_total{source_system="github",reason="unsupported_kind_for_system"} 1`,
			} {
				if !strings.Contains(exposition.String(), want) {
					t.Errorf("operator endpoint is missing %q\n%s", want, exposition.String())
				}
			}
		})
	}
}
