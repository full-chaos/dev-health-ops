package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// fakeCreateDiscovery answers Discover with a fixed report or error and
// records the args it was called with.
type fakeCreateDiscovery struct {
	report schedsync.SourceDiscoveryReport
	err    error
	calls  []schedsync.SourceDiscoveryArgs
}

func (fake *fakeCreateDiscovery) Discover(_ context.Context, args schedsync.SourceDiscoveryArgs) (schedsync.SourceDiscoveryReport, error) {
	fake.calls = append(fake.calls, args)
	return fake.report, fake.err
}

// TestCreateDiscoveryOutcomeIsLoggedAtInfo holds the create path's Jira
// discovery to one log event per call: the Info outcome with its counts on
// success (a zero-project discovery included, which still answers 201), the
// Error event on failure, never both.
func TestCreateDiscoveryOutcomeIsLoggedAtInfo(t *testing.T) {
	integrationID := uuid.MustParse("00000000-0000-4000-8000-00000000a001")
	configID := uuid.MustParse("00000000-0000-4000-8000-00000000c001")
	created := &plannerCreated{config: &syncConfig{ID: configID, Provider: "jira"}, integrationID: integrationID}

	for _, testCase := range []struct {
		name      string
		discovery *fakeCreateDiscovery
		level     string
		message   string
		fields    map[string]any
	}{
		{"zero projects", &fakeCreateDiscovery{report: schedsync.SourceDiscoveryReport{Outcome: "created"}},
			"INFO", "jira_project_discovery_at_creation", map[string]any{"outcome": "created", "created": 0.0, "existing": 0.0}},
		{"projects", &fakeCreateDiscovery{report: schedsync.SourceDiscoveryReport{Outcome: "created", Created: 3, Existing: 2}},
			"INFO", "jira_project_discovery_at_creation", map[string]any{"outcome": "created", "created": 3.0, "existing": 2.0}},
		{"skipped", &fakeCreateDiscovery{report: schedsync.SourceDiscoveryReport{Outcome: "skipped"}},
			"INFO", "jira_project_discovery_at_creation", map[string]any{"outcome": "skipped", "created": 0.0, "existing": 0.0}},
		{"failed", &fakeCreateDiscovery{err: errors.New("jira unreachable")},
			"ERROR", "jira_project_discovery_at_creation_failed", map[string]any{"error": "jira unreachable"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			h := &handlers{logger: slog.New(slog.NewJSONHandler(&logs, nil)), discovery: testCase.discovery}
			h.discoverJiraProjects(context.Background(), "org-1", created, pyjson.NewObject())

			if len(testCase.discovery.calls) != 1 || testCase.discovery.calls[0].IntegrationID != integrationID.String() ||
				testCase.discovery.calls[0].ConfigID != configID.String() || !testCase.discovery.calls[0].PlannerManaged {
				t.Fatalf("discovery calls = %+v", testCase.discovery.calls)
			}
			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			if len(lines) != 1 {
				t.Fatalf("want exactly one log event, got %d:\n%s", len(lines), logs.String())
			}
			var event map[string]any
			if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
				t.Fatal(err)
			}
			if event["level"] != testCase.level || event["msg"] != testCase.message ||
				event["org_id"] != "org-1" || event["integration_id"] != integrationID.String() {
				t.Fatalf("event = %v", event)
			}
			for key, want := range testCase.fields {
				if event[key] != want {
					t.Errorf("%s = %v, want %v (event %v)", key, event[key], want, event)
				}
			}
		})
	}
}
