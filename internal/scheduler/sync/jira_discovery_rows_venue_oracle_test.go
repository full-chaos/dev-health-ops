package sync

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonJiraDiscoveryRowsProgram runs each case through the api's own
// discovery: discover_jira_projects over the case's project list (its Jira
// client's get_all_projects returns the list), then _tuples_to_source_dicts
// with a planner-managed config id. It prints each row's external_id,
// source_type, name, full_name and json.dumps(metadata).
const pythonJiraDiscoveryRowsProgram = `
import json, sys, uuid
from dev_health_ops.providers.jira import client as jira_client
from dev_health_ops.discovery.repos import discover_jira_projects
from dev_health_ops.sync.discovery import _tuples_to_source_dicts
from dev_health_ops.credentials.resolver import jira_credentials_from_mapping
out = []
for case in json.loads(sys.stdin.read()):
    jira_client.JiraClient.get_all_projects = lambda self, projects=case["projects"]: projects
    credentials = jira_credentials_from_mapping({"base_url": "https://example.atlassian.net", "email": "e@example.com", "api_token": "t"})
    tuples = discover_jira_projects(credentials, org_id="org", sync_options=case["options"])
    rows = _tuples_to_source_dicts("jira", tuples, org_id="org", integration_id=uuid.UUID(int=1),
                                   planner_managed_sync_config_id="cfg-1")
    out.append([[r["external_id"], r["source_type"], r["name"], r["full_name"], json.dumps(r["metadata_"])] for r in rows])
print(json.dumps(out))
`

type jiraRowsCase struct {
	Projects []any          `json:"projects"`
	Options  map[string]any `json:"options"`
}

func jiraRowsCases() []jiraRowsCase {
	projects := []any{
		map[string]any{"id": "10001", "key": "CHAOS", "name": "Chaos Engineering", "projectTypeKey": "software"},
		map[string]any{"id": " 10002 ", "key": " OPS ", "name": "Operations", "projectTypeKey": " Service_Desk "},
		map[string]any{"id": 10003, "key": "NUM", "name": "Numeric id"},
		map[string]any{"id": "", "key": "NOID", "name": "No id", "projectTypeKey": "business"},
		map[string]any{"key": "NONAME"},
		map[string]any{"id": "10006", "key": "", "name": "No key"},
		map[string]any{"id": "10007", "key": "JIRA", "name": "", "projectTypeKey": nil},
		map[string]any{"id": "10008", "key": "İDX", "name": "Dotted", "projectTypeKey": "SOFTWARE"},
		"not an object",
		[]any{"a"},
		map[string]any{"id": "10011", "key": "eng", "name": "Engineering (lower)"},
		// A capital sigma, 31 case-ignorable runes, then a letter: medial
		// in CPython at any distance (x/text alone stops looking at 31).
		map[string]any{"id": "10012", "key": "SIG", "name": "Sigma", "projectTypeKey": "AΣ" + strings.Repeat(".", 31) + "B"},
		map[string]any{"id": "10013", "key": "SIGF", "name": "Final sigma", "projectTypeKey": " Ops Σ" + strings.Repeat("'", 40) + " "},
	}
	options := []map[string]any{
		nil, {}, {"project_key": "CHAOS"}, {"project_key": " ops "}, {"project_key": "missing"},
		{"project_id": "10001"}, {"project_id": " 10002 "}, {"project_id": "10001", "project_key": "OPS"}, {"project_key": "ENG"},
	}
	var cases []jiraRowsCase
	for _, option := range options {
		cases = append(cases, jiraRowsCase{Projects: projects, Options: option})
	}
	return cases
}

// TestJiraDiscoveryRowsVenueOracleMatchesLivePython holds discoverJira's
// rows, as the upsert tags and writes them, to the api's own
// discover_jira_projects and _tuples_to_source_dicts over the same project
// list: the same external_id, source_type, name and full_name, and the same
// metadata text, in order.
func TestJiraDiscoveryRowsVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the jira discovery rows oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	cases := jiraRowsCases()
	input, _ := json.Marshal(cases)
	command := exec.Command(python, "-c", pythonJiraDiscoveryRowsProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want [][][5]string
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil || len(want) != len(cases) {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	rows := 0
	for index, c := range cases {
		page, _ := json.Marshal(map[string]any{"values": c.Projects, "isLast": true})
		doer := &fakeSourceDiscoveryDoer{t: t, body: string(page)}
		credential, err := providerfoundation.Credential{Provider: "jira"}.WithEphemeralSecret("email", secrets.NewValue("e@example.com"))
		if err != nil {
			t.Fatal(err)
		}
		if credential, err = credential.WithEphemeralSecret("api_token", secrets.NewValue("t")); err != nil {
			t.Fatal(err)
		}
		if credential, err = credential.WithEphemeralSecret("base_url", secrets.NewValue("https://example.atlassian.net")); err != nil {
			t.Fatal(err)
		}
		service := &NativeSourceDiscoveryService{doer: doer, retry: fastRetry(), telemetry: newSourceDiscoveryTelemetry(), now: time.Now}
		sources, err := service.discoverJira(context.Background(), credential, c.Options)
		if err != nil {
			t.Fatalf("case %d: %v", index, err)
		}
		var got [][5]string
		for _, source := range sources {
			metadata := cloneMetadata(source.Metadata)
			metadata.Set("planner_managed_sync_config_id", "cfg-1")
			text, err := encodeSourceMetadata(metadata)
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, [5]string{source.ExternalID, source.SourceType, source.Name, source.FullName, text})
		}
		if fmt.Sprint(got) != fmt.Sprint(want[index]) {
			t.Errorf("case %d (options %v):\n go     %v\n python %v", index, c.Options, got, want[index])
		}
		rows += len(got)
	}
	if rows == 0 {
		t.Fatal("no case produced a row")
	}
	t.Logf("%d cases, %d rows compared", len(cases), rows)
	venueoracle.WriteProof(t)
}
