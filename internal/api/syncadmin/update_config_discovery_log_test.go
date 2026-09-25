package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// TestUpdateDiscoveryOutcomeIsLoggedAtInfo holds the update path's Jira
// discovery to one log event per run: jira_project_discovery_on_update at
// Info with the outcome and counts on success (zero projects included),
// jira_project_discovery_on_update_failed at Error on failure, never both.
func TestUpdateDiscoveryOutcomeIsLoggedAtInfo(t *testing.T) {
	args := schedsync.SourceDiscoveryArgs{OrgID: "org-1", IntegrationID: "int-1", Provider: "jira", ConfigID: "cfg-1", PlannerManaged: true}
	for _, testCase := range []struct {
		name      string
		discovery *fakeCreateDiscovery
		level     string
		message   string
		fields    map[string]any
	}{
		{"zero projects", &fakeCreateDiscovery{report: schedsync.SourceDiscoveryReport{Outcome: "created"}},
			"INFO", "jira_project_discovery_on_update", map[string]any{"integration_id": "int-1", "outcome": "created", "created": 0.0, "existing": 0.0}},
		{"projects", &fakeCreateDiscovery{report: schedsync.SourceDiscoveryReport{Outcome: "existing", Created: 1, Existing: 4}},
			"INFO", "jira_project_discovery_on_update", map[string]any{"outcome": "existing", "created": 1.0, "existing": 4.0}},
		{"failed", &fakeCreateDiscovery{err: errors.New("jira unreachable")},
			"ERROR", "jira_project_discovery_on_update_failed", map[string]any{"error": "jira unreachable"}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var logs bytes.Buffer
			h := &handlers{logger: slog.New(slog.NewJSONHandler(&logs, nil)), discovery: testCase.discovery}
			h.runIntegrationDiscovery(context.Background(), args, "jira_project_discovery_on_update", "config_id", "cfg-path")
			if len(testCase.discovery.calls) != 1 || testCase.discovery.calls[0].IntegrationID != "int-1" || testCase.discovery.calls[0].ConfigID != "cfg-1" {
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
			if event["level"] != testCase.level || event["msg"] != testCase.message || event["org_id"] != "org-1" || event["config_id"] != "cfg-path" {
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
