package rivermigrate

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	jobsv1 "github.com/full-chaos/dev-health-ops/contracts/jobs/v1"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
)

func routeSeedEvents(t *testing.T, result riverstore.MigrationResult) []map[string]any {
	t.Helper()
	var output bytes.Buffer
	logRouteSeed(context.Background(), slog.New(slog.NewJSONHandler(&output, nil)), result)
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

// TestLogRouteSeedNamesEveryKind pins the migrate run's route telemetry: one
// line per kind saying whether this run created its row, and a warning when
// the route table does not exist yet, since every River-only kind then still
// lacks its row.
func TestLogRouteSeedNamesEveryKind(t *testing.T) {
	events := routeSeedEvents(t, riverstore.MigrationResult{
		SeededRoutes:  []string{"system.heartbeat"},
		PresentRoutes: []string{"sync.provider_unit"},
	})
	if len(events) != 2 ||
		events[0]["msg"] != "worker job route seeded" || events[0]["job_kind"] != "system.heartbeat" ||
		events[0]["level"] != "INFO" ||
		events[1]["msg"] != "worker job route present" || events[1]["job_kind"] != "sync.provider_unit" {
		t.Fatalf("events=%v", events)
	}
	absent := routeSeedEvents(t, riverstore.MigrationResult{RouteTableAbsent: true})
	if len(absent) != 1 || absent[0]["level"] != "WARN" ||
		absent[0]["reason"] != "worker_job_routes_absent" {
		t.Fatalf("absent events=%v", absent)
	}
}

// TestEmbeddedPolicyYieldsRiverOnlyKinds pins what the binary seeds from:
// the policy embedded at build time parses and names at least one kind.
func TestEmbeddedPolicyYieldsRiverOnlyKinds(t *testing.T) {
	kinds, err := jobcontract.NativeRiverRouteKinds(jobsv1.MigrationState)
	if err != nil || len(kinds) == 0 {
		t.Fatalf("kinds=%v err=%v", kinds, err)
	}
}
