package workflows_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// A ghcr.io registry login that times out in a merge_group job ejects the PR before any test runs. docker/login-action
// has no retry input, so each such job keeps the action (pin, logout) as continue-on-error with an id, retries with
// ci/lib/ghcr_login.sh only when the action failed, and always runs `docker logout ghcr.io` as its last step.

const (
	ghcrFallbackRun = "bash ci/lib/ghcr_login.sh"
	ghcrLogoutRun   = "docker logout ghcr.io"
)

// ghcrMergeGroupJobs are the jobs that run on merge_group and log in to ghcr.io.
var ghcrMergeGroupJobs = map[string][]string{
	"go.yml":            {"go-storage-integration-shard", "go-auth-compose-proof", "go-auth-web-proxy-proof"},
	"go-quality.yml":    {"go-quality-leg"},
	"docker-images.yml": {"go-build", "go-build-dho-arm64", "go-api-tools-build"},
}

// ghcrNoMergeGroupRun are ghcr.io logins in workflows with a merge_group trigger whose jobs never run in the queue.
var ghcrNoMergeGroupRun = map[string]string{
	"docker-images.yml/resolve":            "login step runs on push to main only",
	"docker-images.yml/go-merge":           "job runs on non-PR, non-merge_group events only",
	"docker-images.yml/go-api-tools-merge": "job runs on non-PR, non-merge_group events only",
	"docker-images.yml/fan-in-latest":      "job runs on non-PR, non-merge_group events only",
}

var pinnedAction = regexp.MustCompile(`@[0-9a-f]{40}$`)

func boolish(v any) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return strings.TrimSpace(strings.ToLower(fmt.Sprint(v))) == "true"
}

func squash(s string) string { return strings.Join(strings.Fields(s), " ") }

func isGhcrLogin(step yamlMap) bool {
	return isDockerLoginAction(step) && withString(step, "registry") == "ghcr.io"
}

// ghcrRetryOffenders returns what is wrong with one job's ghcr login retry shape; empty means the shape is right.
func ghcrRetryOffenders(job yamlMap) []string {
	steps := stepsOf(job)
	var out []string
	login := -1
	for i, s := range steps {
		if isGhcrLogin(s) {
			login = i
			break
		}
	}
	if login < 0 {
		return []string{"no ghcr.io docker/login-action step"}
	}
	ls := steps[login]
	if !pinnedAction.MatchString(asString(ls["uses"])) {
		out = append(out, "the login action is not pinned to a commit sha")
	}
	id := asString(ls["id"])
	if id == "" {
		out = append(out, "the login action has no id")
	}
	if !boolish(ls["continue-on-error"]) {
		out = append(out, "the login action is not continue-on-error")
	}
	fb := -1
	for i, s := range steps {
		if squash(asString(s["run"])) == ghcrFallbackRun {
			fb = i
			break
		}
	}
	switch {
	case fb < 0:
		out = append(out, "no fallback step running "+ghcrFallbackRun)
	default:
		s := steps[fb]
		if fb < login {
			out = append(out, "the fallback step runs before the login action")
		}
		if id != "" && squash(asString(s["if"])) != fmt.Sprintf("steps.%s.outcome == 'failure'", id) {
			out = append(out, fmt.Sprintf("the fallback step is not gated on steps.%s.outcome == 'failure' (got %q)", id, asString(s["if"])))
		}
		if boolish(s["continue-on-error"]) {
			out = append(out, "the fallback step is continue-on-error; a login that fails 3 times must fail the job")
		}
		env := asMap(s["env"])
		if !strings.Contains(asString(env["GHCR_TOKEN"]), "secrets.GITHUB_TOKEN") || !strings.Contains(asString(env["GHCR_USER"]), "github.actor") {
			out = append(out, "the fallback step must pass GHCR_USER and GHCR_TOKEN through env")
		}
		if strings.Contains(asString(s["run"]), "${{") {
			out = append(out, "the fallback run text holds an expression; the token must come from env only")
		}
	}
	last := steps[len(steps)-1]
	if squash(asString(last["run"])) != ghcrLogoutRun {
		out = append(out, "the last step is not `"+ghcrLogoutRun+"`")
	} else if squash(asString(last["if"])) != "always()" {
		out = append(out, "the final logout step is not `if: always()`")
	}
	return out
}

func TestMergeGroupGhcrLoginsRetryAndLogOut(t *testing.T) {
	root := rootOf(t)
	var offenders []string
	for _, wf := range sortedKeys(yamlMap{"go.yml": 1, "go-quality.yml": 1, "docker-images.yml": 1}) {
		jobs := jobsAsMaps(loadWf(t, root, wf))
		for _, jn := range ghcrMergeGroupJobs[wf] {
			job, ok := jobs[jn]
			if !ok {
				offenders = append(offenders, fmt.Sprintf("%s: job %q not found", wf, jn))
				continue
			}
			for _, o := range ghcrRetryOffenders(job) {
				offenders = append(offenders, fmt.Sprintf("%s/%s: %s", wf, jn, o))
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
}

func TestEveryGhcrLoginInAMergeGroupWorkflowIsCoveredOrExempt(t *testing.T) {
	root := rootOf(t)
	covered := map[string]bool{}
	for wf, jobs := range ghcrMergeGroupJobs {
		for _, j := range jobs {
			covered[wf+"/"+j] = true
		}
	}
	var offenders []string
	for _, wf := range workflowNames(t, root) {
		doc := loadWf(t, root, wf)
		if _, ok := asMap(doc["on"])["merge_group"]; !ok {
			continue
		}
		jobs := jobsAsMaps(doc)
		for _, jn := range sortedKeysOf(jobs) {
			login := false
			for _, s := range stepsOf(jobs[jn]) {
				login = login || isGhcrLogin(s)
			}
			key := wf + "/" + jn
			if login && !covered[key] && ghcrNoMergeGroupRun[key] == "" {
				offenders = append(offenders, key+": ghcr.io login in a merge_group workflow is neither retried nor exempt")
			}
		}
	}
	for key := range ghcrNoMergeGroupRun {
		if covered[key] {
			offenders = append(offenders, key+": listed as both covered and exempt")
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
}

func goodGhcrJob() yamlMap {
	return yamlMap{"steps": []any{
		yamlMap{"uses": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"},
		yamlMap{
			"name": "Log in", "id": "ghcr_login", "continue-on-error": true,
			"uses": "docker/login-action@dbcb813823bdd20940b903addbd779551569679f",
			"with": yamlMap{"registry": "ghcr.io"},
		},
		yamlMap{
			"name": "Retry", "if": "steps.ghcr_login.outcome == 'failure'", "run": ghcrFallbackRun,
			"env": yamlMap{"GHCR_USER": "${{ github.actor }}", "GHCR_TOKEN": "${{ secrets.GITHUB_TOKEN }}"},
		},
		yamlMap{"name": "work", "run": "make test"},
		yamlMap{"name": "Log out", "if": "always()", "run": ghcrLogoutRun},
	}}
}

func TestGhcrRetryShapeCheckerCatchesPlantedDefects(t *testing.T) {
	if got := ghcrRetryOffenders(goodGhcrJob()); len(got) != 0 {
		t.Fatalf("the good fixture is red: %v", got)
	}
	cases := []struct {
		name   string
		mutate func(steps []any) []any
		want   string
	}{
		{"no logout step", func(s []any) []any { return s[:len(s)-1] }, "last step is not"},
		{"logout not last", func(s []any) []any { return append(s, yamlMap{"run": "echo after"}) }, "last step is not"},
		{"logout without always()", func(s []any) []any { asMap(s[len(s)-1])["if"] = "success()"; return s }, "not `if: always()`"},
		{"no continue-on-error", func(s []any) []any { delete(asMap(s[1]), "continue-on-error"); return s }, "not continue-on-error"},
		{"no id", func(s []any) []any { delete(asMap(s[1]), "id"); return s }, "has no id"},
		{"continue-on-error but no fallback", func(s []any) []any { return append(s[:2:2], s[3:]...) }, "no fallback step"},
		{"fallback gated on success", func(s []any) []any { asMap(s[2])["if"] = "steps.ghcr_login.outcome == 'success'"; return s }, "not gated"},
		{"fallback gated on another id", func(s []any) []any { asMap(s[2])["if"] = "steps.other.outcome == 'failure'"; return s }, "not gated"},
		{"fallback ungated", func(s []any) []any { delete(asMap(s[2]), "if"); return s }, "not gated"},
		{"fallback swallows failure", func(s []any) []any { asMap(s[2])["continue-on-error"] = true; return s }, "must fail the job"},
		{"fallback before login", func(s []any) []any { s[1], s[2] = s[2], s[1]; return s }, "before the login"},
		{"token in run text", func(s []any) []any { asMap(s[2])["run"] = ghcrFallbackRun + " ${{ secrets.GITHUB_TOKEN }}"; return s }, "no fallback step"},
		{"token not from env", func(s []any) []any { delete(asMap(s[2]), "env"); return s }, "through env"},
		{"login action unpinned", func(s []any) []any { asMap(s[1])["uses"] = "docker/login-action@v4"; return s }, "not pinned"},
	}
	for _, c := range cases {
		job := goodGhcrJob()
		job["steps"] = c.mutate(job["steps"].([]any))
		got := strings.Join(ghcrRetryOffenders(job), "\n")
		if !strings.Contains(got, c.want) {
			t.Errorf("%s: want an offender containing %q, got %q", c.name, c.want, got)
		}
	}
}
