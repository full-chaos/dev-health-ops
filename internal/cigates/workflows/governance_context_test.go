package workflows_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10376: guards ported from tests/test_governance_workflow.py. The governance workflow must compare the
// merge queue group's base and head commits, and its "Resolve governance context" step must resolve every queued
// pull request body through a paginated, slurped gh call that excludes entries queued behind the current one.

func governanceContextScript(wf yamlMap) (string, bool) {
	for _, step := range stepsOf(asMap(asMap(wf["jobs"])["governance"])) {
		if asString(step["id"]) == "governance-context" {
			return asString(step["run"]), true
		}
	}
	return "", false
}

func governanceViolations(t *testing.T, root string) []string {
	t.Helper()
	path := filepath.Join(root, ".github", "workflows", "governance.yml")
	text := readFile(t, path)
	var bad []string
	for _, want := range []string{
		"BASE_SHA: ${{ github.event.pull_request.base.sha || github.event.merge_group.base_sha }}",
		"HEAD_SHA: ${{ github.event.pull_request.head.sha || github.event.merge_group.head_sha }}",
	} {
		if !strings.Contains(text, want) {
			bad = append(bad, "governance.yml no longer compares the merge group shas: missing "+want)
		}
	}
	script, ok := governanceContextScript(parseYAML(t, text, "governance.yml"))
	if !ok || script == "" {
		return append(bad, "governance-context step has no run script")
	}
	dir := t.TempDir()
	pages := `[{"data":{"repository":{"pullRequest":{"mergeQueueEntry":{"position":2,` +
		`"mergeQueue":{"entries":{"nodes":[` +
		`{"position":1,"pullRequest":{"body":"first PR evidence"}},` +
		`{"position":2,"pullRequest":{"body":"second PR evidence"}},` +
		`{"position":3,"pullRequest":{"body":"queued-behind evidence"}}` +
		`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}}}]`
	argsFile, outFile := filepath.Join(dir, "gh-args.txt"), filepath.Join(dir, "github-output.txt")
	fake := "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$GH_ARGS_FILE\"\nprintf '%s\\n' '" + pages + "'\n"
	if err := os.WriteFile(filepath.Join(dir, "gh"), []byte(fake), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "-euo", "pipefail", "-c", script)
	cmd.Env = []string{
		"EVENT_NAME=merge_group",
		"MERGE_GROUP_HEAD_REF=refs/heads/gh-readonly-queue/main/pr-1262-deadbeef",
		"PR_BODY=",
		"GH_TOKEN=test-token",
		"GITHUB_REPOSITORY=full-chaos/dev-health-ops",
		"GITHUB_REPOSITORY_OWNER=full-chaos",
		"REPOSITORY_NAME=dev-health-ops",
		"GITHUB_OUTPUT=" + outFile,
		"GH_ARGS_FILE=" + argsFile,
		"PATH=" + dir + ":" + os.Getenv("PATH"),
	}
	if out, err := cmd.CombinedOutput(); err != nil {
		return append(bad, "the context script failed: "+err.Error()+" "+string(out))
	}
	output, _ := os.ReadFile(outFile)
	if !strings.Contains(string(output), "first PR evidence\nsecond PR evidence") {
		bad = append(bad, "the context script did not resolve the queued pull request bodies: "+string(output))
	}
	if strings.Contains(string(output), "queued-behind evidence") {
		bad = append(bad, "the queued-behind entry (position 3 > current 2) must be excluded")
	}
	inv, _ := os.ReadFile(argsFile)
	for _, want := range []string{"mergeQueue", "--paginate", "--slurp", "entries(first:100,after:$endCursor)", "pageInfo{hasNextPage endCursor}"} {
		if !strings.Contains(string(inv), want) {
			bad = append(bad, "gh invocation lacks "+want)
		}
	}
	if strings.Contains(string(inv), "--jq") {
		bad = append(bad, "gh rejects --slurp combined with --jq: the filter must run as a standalone jq stage")
	}
	if !strings.Contains(script, "position <= $current.position") {
		bad = append(bad, "the context script lost its position filter")
	}
	return bad
}

func TestGovernanceWorkflowResolvesMergeGroupContext(t *testing.T) {
	root := repoRoot(t)
	if v := governanceViolations(t, root); len(v) > 0 {
		t.Fatal(strings.Join(v, "\n"))
	}
}

func TestGovernanceWorkflowGuardFiresOnPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	raw := readFile(t, filepath.Join(root, ".github", "workflows", "governance.yml"))
	for _, c := range []struct{ name, old, repl, want string }{
		{"base sha loses the merge group fallback", "github.event.pull_request.base.sha || github.event.merge_group.base_sha", "github.event.pull_request.base.sha", "missing BASE_SHA"},
		{"position filter dropped", "position <= $current.position", "position >= 0", "position filter"},
		{"jq rides along on gh", "--slurp", "--slurp --jq .", "--jq"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if strings.Count(raw, c.old) < 1 {
				t.Fatalf("anchor %q not found in governance.yml", c.old)
			}
			tree := t.TempDir()
			dst := filepath.Join(tree, ".github", "workflows")
			if err := os.MkdirAll(dst, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dst, "governance.yml"), []byte(strings.Replace(raw, c.old, c.repl, 1)), 0o644); err != nil {
				t.Fatal(err)
			}
			if v := strings.Join(governanceViolations(t, tree), "\n"); !strings.Contains(v, c.want) {
				t.Fatalf("guard did not fire naming %q; got:\n%s", c.want, v)
			}
		})
	}
}
