package syncadmin

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pyoracle"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// pythonCreateWriteProgram runs every case through the api's own code:
// sync/datasets.planner_dataset_keys, the router's _planner_dataset_options
// and _non_git_source_rows, _create_planner_managed_config's GitHub parent
// option merge (with the case's GITHUB_* environment set), and
// sync/pagerduty_repair.pagerduty_provider_instance_id. Every result is
// json.dumps of the value or "raise <class>: <message>".
const pythonCreateWriteProgram = `
import json, os, sys, uuid
from dev_health_ops.sync.datasets import planner_dataset_keys
from dev_health_ops.api.admin.routers import sync as router
from dev_health_ops.providers.github.work_item_options import snapshot_github_work_item_runtime_options
from dev_health_ops.sync.pagerduty_repair import pagerduty_provider_instance_id
def attempt(fn):
    try:
        return json.dumps(fn())
    except Exception as exc:
        return "raise " + type(exc).__name__ + ": " + str(exc)
payload = json.loads(sys.stdin.read())
out = {"keys": [], "options": [], "parents": [], "sources": [], "pagerduty": []}
for case in payload["keys"]:
    out["keys"].append(attempt(lambda: planner_dataset_keys(case["provider"], case["targets"])))
for case in payload["options"]:
    parent = json.loads(case["parent"])
    out["options"].append(attempt(lambda: router._planner_dataset_options(case["provider"], case["key"], case["targets"], parent)))
ENV = ("GITHUB_FETCH_COMMENTS", "GITHUB_FETCH_MILESTONES", "GITHUB_COMMENTS_LIMIT")
for case in payload["parents"]:
    for name in ENV:
        os.environ.pop(name, None)
    os.environ.update(case["env"])
    options = json.loads(case["options"])
    def merge():
        if case["provider"].lower() == "github":
            return {**options, **snapshot_github_work_item_runtime_options(options)}
        return options
    out["parents"].append(attempt(merge))
for case in payload["sources"]:
    options = json.loads(case["options"])
    def rows():
        built = router._non_git_source_rows(case["provider"], options, case["name"], "org-1", uuid.UUID(int=1), uuid.UUID(case["config_id"]))
        return [[r.provider, r.source_type, r.external_id, r.name, r.full_name, r.metadata_, r.is_enabled] for r in built]
    out["sources"].append(attempt(rows))
for raw in payload["pagerduty"]:
    out["pagerduty"].append(attempt(lambda: pagerduty_provider_instance_id(json.loads(raw))))
print(json.dumps(out))
`

type keysCase struct {
	Provider string   `json:"provider"`
	Targets  []string `json:"targets"`
}

type optionsCase struct {
	Provider string   `json:"provider"`
	Key      string   `json:"key"`
	Targets  []string `json:"targets"`
	Parent   string   `json:"parent"`
}

type parentCase struct {
	Provider string            `json:"provider"`
	Options  string            `json:"options"`
	Env      map[string]string `json:"env"`
}

type sourcesCase struct {
	Provider string `json:"provider"`
	Options  string `json:"options"`
	Name     string `json:"name"`
	ConfigID string `json:"config_id"`
}

func createWriteCases() (keys []keysCase, options []optionsCase, parents []parentCase, sources []sourcesCase, pagerduty []string) {
	providers := []string{"github", "gitlab", "jira", "linear", "launchdarkly", "pagerduty", "GitHub", "PAGERDUTY", " jira", "bogus", ""}
	targetSets := [][]string{
		{}, {"git"}, {"prs"}, {"blame"}, {"git", "prs"}, {"cicd", "tests"}, {"deployments", "incidents"}, {"security"},
		{"work-items"}, {"feature-flags"}, {"operational"}, {"operational", "incidents"}, {"operational", "operational"},
		{"git", "prs", "cicd", "tests", "deployments", "incidents", "security", "work-items", "feature-flags", "operational"},
		{"GIT"}, {"unknown"}, {"work-items", "work-items"},
	}
	for _, provider := range providers {
		for _, targets := range targetSets {
			keys = append(keys, keysCase{Provider: provider, Targets: targets})
		}
	}
	parentShapes := []string{
		`{}`, `{"service_repository_mappings": {"svc": ["repo"]}}`, `{"service_repository_mappings": ["x"]}`,
		`{"service_repository_mappings": null}`, `{"fetch_comments": false, "comments_limit": 5}`,
		`{"fetch_comments": "no"}`, `{"fetch_milestones": 1}`, `{"comments_limit": -1}`, `{"comments_limit": 2.0}`,
		`{"comments_limit": true}`, `{"comments_limit": 123456789012345678901234567890}`, `{"fetch_comments": null, "comments_limit": null}`,
	}
	for _, provider := range []string{"github", "GitHub", "pagerduty", "PagerDuty", "gitlab"} {
		for _, key := range []string{"work-items", "services", "commits"} {
			for _, parent := range parentShapes {
				options = append(options, optionsCase{Provider: provider, Key: key, Targets: []string{"work-items", "git"}, Parent: parent})
			}
		}
	}
	envs := []map[string]string{
		{}, {"GITHUB_FETCH_COMMENTS": "0", "GITHUB_FETCH_MILESTONES": " OFF ", "GITHUB_COMMENTS_LIMIT": "25"},
		{"GITHUB_FETCH_COMMENTS": "yes", "GITHUB_FETCH_MILESTONES": "maybe", "GITHUB_COMMENTS_LIMIT": "abc"},
		{"GITHUB_COMMENTS_LIMIT": ""}, {"GITHUB_COMMENTS_LIMIT": " 1_000 "}, {"GITHUB_COMMENTS_LIMIT": "-5"},
		{"GITHUB_COMMENTS_LIMIT": "١٢"}, {"GITHUB_FETCH_COMMENTS": "TRUE", "GITHUB_FETCH_MILESTONES": "İ"},
	}
	for _, provider := range []string{"github", "GitHub", "gitlab"} {
		for _, shape := range parentShapes {
			for _, env := range envs {
				parents = append(parents, parentCase{Provider: provider, Options: shape, Env: env})
			}
		}
	}
	sourceOptions := []string{
		`{}`, `{"project_key": "ENG"}`, `{"project_key": "   "}`, `{"project_key": ""}`, `{"project_id": 10001}`, `{"project_id": 0}`,
		`{"project_id": false, "project_key": "X"}`, `{"team_id": "t-1"}`, `{"repo": "org/r"}`, `{"project_key": ["a"]}`,
		`{"project_key": {"a": 1}}`, `{"project_key": true}`, `{"project_key": 1.5}`, `{"project_key": "ENG", "full_name": "Engineering"}`,
		`{"full_name": 5}`, `{"full_name": ""}`, `{"project_id": null, "team_id": " x "}`,
	}
	for _, provider := range []string{"jira", "linear", "launchdarkly", "JIRA", "Linear", "other"} {
		for _, shape := range sourceOptions {
			sources = append(sources, sourcesCase{Provider: provider, Options: shape, Name: "My Config", ConfigID: "11111111-2222-4333-8444-555555555555"})
		}
	}
	pagerduty = []string{
		`null`, `{}`, `[]`, `""`, `0`, `false`, `["a"]`, `"x"`, `1`, `true`,
		`{"account_id": "acc", "subdomain": "sub"}`, `{"account_id": " acc ", "subdomain": "sub"}`, `{"account_id": "acc"}`,
		`{"subdomain": "sub"}`, `{"account_id": "  ", "subdomain": "sub"}`, `{"account_id": 5, "subdomain": "sub"}`,
		`{"account_id": "acc", "subdomain": null}`,
	}
	return
}

// TestCreateWriteEnginesVenueOracleMatchesLivePython holds the create
// path's pure pieces to the api's own code: the planner dataset keys and
// their ValueError, the dataset options, the GitHub parent option merge
// under every legacy-environment shape, the non-git source rows, and the
// PagerDuty account identity.
func TestCreateWriteEnginesVenueOracleMatchesLivePython(t *testing.T) {
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") != "1" {
		t.Skip("the create write oracle needs the full project Python environment; ci/check_go.sh venue-oracles runs it")
	}
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	python := pyoracle.Resolve(t, root)
	keys, options, parents, sources, pagerduty := createWriteCases()
	input, _ := json.Marshal(map[string]any{"keys": keys, "options": options, "parents": parents, "sources": sources, "pagerduty": pagerduty})
	command := exec.Command(python, "-c", pythonCreateWriteProgram)
	command.Env = append(os.Environ(), "PYTHONPATH="+filepath.Join(root, "src"))
	command.Stdin = strings.NewReader(string(input))
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("live python: %v", pyoracle.RunError(python, err, output))
	}
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	var want struct {
		Keys, Options, Parents, Sources, Pagerduty []string
	}
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &want); err != nil {
		t.Fatalf("decode: %v\n%s", err, output)
	}
	dumps := func(value pyjson.Value) string {
		text, err := pyjson.Dumps(value)
		if err != nil {
			t.Fatal(err)
		}
		return text
	}
	decode := func(raw string) pyjson.Value {
		value, err := pyjson.DecodeString(raw)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	raises := map[string]int{}
	check := func(kind string, index int, got, want string) {
		if got != want {
			t.Errorf("%s %d: go %s, python %s", kind, index, got, want)
		}
		if strings.HasPrefix(want, "raise ") {
			raises[kind]++
		}
	}
	for index, c := range keys {
		got, err := plannerDatasetKeys(c.Provider, c.Targets)
		text := ""
		if err != nil {
			text = "raise ValueError: " + err.Error()
		} else {
			list := make([]pyjson.Value, len(got))
			for position, key := range got {
				list[position] = key
			}
			text = dumps(list)
		}
		check("keys", index, text, want.Keys[index])
	}
	for index, c := range options {
		got, err := plannerDatasetOptions(c.Provider, c.Key, c.Targets, decode(c.Parent).(*pyjson.Object))
		text := ""
		if err != nil {
			text = "raise ValueError: " + err.Error()
		} else {
			text = dumps(got)
		}
		check("options", index, text, want.Options[index])
	}
	for index, c := range parents {
		env := c.Env
		lookup := func(name string) (string, bool) { value, ok := env[name]; return value, ok }
		got, err := plannerParentOptions(c.Provider, decode(c.Options).(*pyjson.Object), lookup)
		text := ""
		if err != nil {
			text = "raise ValueError: " + err.Error()
		} else {
			text = dumps(got)
		}
		check("parents", index, text, want.Parents[index])
	}
	for index, c := range sources {
		rows := []pyjson.Value{}
		for _, row := range nonGitSourceRows(c.Provider, decode(c.Options).(*pyjson.Object), c.Name, c.ConfigID) {
			rows = append(rows, []pyjson.Value{row.provider, row.sourceType, row.externalID, row.name, row.fullName, row.metadata, true})
		}
		check("sources", index, dumps(rows), want.Sources[index])
	}
	for index, raw := range pagerduty {
		got, err := pagerDutyProviderInstanceID(decode(raw))
		var text string
		switch {
		case err == errPagerDutyConfigNotMapping:
			text = "raise AttributeError"
		case err != nil:
			text = "raise PagerDutyOperationalTargetError"
		default:
			text = dumps(got)
		}
		wantText := want.Pagerduty[index]
		if strings.HasPrefix(wantText, "raise ") {
			wantText = strings.SplitN(wantText, ":", 2)[0]
		}
		check("pagerduty", index, text, wantText)
	}
	for _, kind := range []string{"keys", "options", "parents", "pagerduty"} {
		if raises[kind] == 0 {
			t.Errorf("no %s case reached a raise", kind)
		}
	}
	t.Logf("%d keys, %d options, %d parent merges, %d source cases, %d identities compared; raises %v",
		len(keys), len(options), len(parents), len(sources), len(pagerduty), fmt.Sprint(raises))
	venueoracle.WriteProof(t)
}
