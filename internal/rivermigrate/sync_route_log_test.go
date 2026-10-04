package rivermigrate

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	syncdispatchv1 "github.com/full-chaos/dev-health-ops/contracts/sync-dispatch/v1"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/syncdispatchcontract"
)

func syncRouteEvents(t *testing.T, result riverstore.MigrationResult) []map[string]any {
	t.Helper()
	var output bytes.Buffer
	logSyncRouteMove(context.Background(), slog.New(slog.NewJSONHandler(&output, nil)), result)
	var events []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}

// TestLogSyncRouteMoveNamesEveryKind pins the migrate run's sync-dispatch
// route telemetry (CHAOS-8600): one line per kind. A moved row and a row
// already on river are Info. A row the run left in another state, and a kind
// with no row, are warnings that carry the state, because the reconciler's
// route fence stays closed for them. A database without the route table is
// one warning.
func TestLogSyncRouteMoveNamesEveryKind(t *testing.T) {
	events := syncRouteEvents(t, riverstore.MigrationResult{SyncRoutes: []riverstore.SyncRouteOutcome{
		{Kind: "dispatch_sync_run", Outcome: riverstore.SyncRouteMoved, Transport: "celery", RollbackTransport: "none", Generation: 2},
		{Kind: "finalize_sync_run", Outcome: riverstore.SyncRoutePresent, Transport: "river", RollbackTransport: "none", Generation: 3},
		{Kind: "post_sync", Outcome: riverstore.SyncRouteHeld, Transport: "celery", RollbackTransport: "none", Paused: true, Generation: 4, LiveClaims: 1},
		{Kind: "reference_discovery", Outcome: riverstore.SyncRouteMissing},
	}})
	if len(events) != 4 {
		t.Fatalf("events=%v", events)
	}
	moved, present, held, missing := events[0], events[1], events[2], events[3]
	if moved["level"] != "INFO" || moved["msg"] != "sync dispatch route moved" || moved["kind"] != "dispatch_sync_run" ||
		moved["from_transport"] != "celery" || moved["transport"] != "river" || moved["from_generation"] != float64(2) {
		t.Fatalf("moved event=%v", moved)
	}
	if present["level"] != "INFO" || present["msg"] != "sync dispatch route present" || present["kind"] != "finalize_sync_run" {
		t.Fatalf("present event=%v", present)
	}
	if held["level"] != "WARN" || held["msg"] != "sync dispatch route left as found" || held["kind"] != "post_sync" ||
		held["transport"] != "celery" || held["rollback_transport"] != "none" || held["paused"] != true || held["live_claims"] != float64(1) {
		t.Fatalf("held event=%v", held)
	}
	if missing["level"] != "WARN" || missing["msg"] != "sync dispatch route row missing" || missing["kind"] != "reference_discovery" {
		t.Fatalf("missing event=%v", missing)
	}
	absent := syncRouteEvents(t, riverstore.MigrationResult{SyncRouteTableAbsent: true})
	if len(absent) != 1 || absent[0]["level"] != "WARN" ||
		absent[0]["reason"] != "sync_dispatch_transport_routes_absent" {
		t.Fatalf("absent events=%v", absent)
	}
}

// TestEmbeddedPolicyYieldsTheSyncDispatchKinds pins what the binary moves
// from: the policy embedded at build time parses, and it names every frozen
// sync-dispatch kind (each is checked in at river with no rollback route). A
// kind that goes back to a rollback route leaves this set, and this test then
// says so before a migrate run silently stops looking at its row.
func TestEmbeddedPolicyYieldsTheSyncDispatchKinds(t *testing.T) {
	kinds, err := syncdispatchcontract.NativeRiverRouteKinds(syncdispatchv1.TransportRoutes)
	if err != nil {
		t.Fatal(err)
	}
	if want := syncdispatchcontract.Kinds(); !reflect.DeepEqual(kinds, want) {
		t.Fatalf("the embedded policy yields %v, the frozen kinds are %v", kinds, want)
	}
}
