package workflows_test

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// CHAOS-13520: the auth-corpus-queue legs build their six images with docker/bake-action from a gha cache that only
// auth-corpus-image-cache (a push to main of this repository) writes. A pull request leg reads the cache and never writes it;
// the cache token stays inside the bake action (no shell step exports it); the corpus script starts the loaded images with
// --no-build (AUTH_CORPUS_PREBUILT_TAG = the tag the bake step loads); and both bake steps build the same targets, each with its
// own scope.

const bakeAction = "docker/bake-action@59cb1203cd42bcefd40a1b6d73b589e48c86040e"

func bakeSets(s yamlMap) (from, to map[string]string) {
	from, to = map[string]string{}, map[string]string{}
	for _, line := range strings.Split(withString(s, "set"), "\n") {
		line = strings.TrimSpace(line)
		if target, v, ok := strings.Cut(line, ".cache-from="); ok {
			from[target] = v
		}
		if target, v, ok := strings.Cut(line, ".cache-to="); ok {
			to[target] = v
		}
	}
	return from, to
}

func authCorpusCacheViolations(doc yamlMap) []string {
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	jobs := jobsOf(doc)
	check := func(jobName string, writes bool) []string {
		job := jobs[jobName]
		var b yamlMap
		for _, s := range stepsOf(job) {
			if strings.HasPrefix(asString(s["uses"]), "docker/bake-action@") {
				b = s
			}
			if strings.Contains(asString(s["run"]), "ACTIONS_RUNTIME_TOKEN") || strings.Contains(asString(s["run"]), "ACTIONS_CACHE_URL") ||
				strings.Contains(asString(s["run"]), "ACTIONS_RESULTS_URL") {
				add("%s: a shell step reads the actions cache token or URL", jobName)
			}
		}
		if b == nil {
			add("%s has no docker/bake-action step", jobName)
			return nil
		}
		if !strings.HasPrefix(asString(b["uses"]), bakeAction) {
			add("%s: the bake step must use %s, got %q", jobName, bakeAction, asString(b["uses"]))
		}
		if withString(b, "load") != "true" || withString(b, "source") != "." {
			add("%s: the bake step must build the checkout (source: .) and load the images (load: true)", jobName)
		}
		if tag := asString(asMap(b["env"])["AUTH_CORPUS_IMAGE_TAG"]); tag != "ci-prebuilt" {
			add("%s: the bake step must tag the stub images ci-prebuilt (AUTH_CORPUS_IMAGE_TAG), got %q", jobName, tag)
		}
		targets := strings.Fields(withString(b, "targets"))
		sort.Strings(targets)
		from, to := bakeSets(b)
		for _, target := range targets {
			if want := "type=gha,scope=authcorpus-" + target; from[target] != want {
				add("%s: target %s must read cache-from %q, got %q", jobName, target, want, from[target])
			}
			if writes {
				if want := "type=gha,scope=authcorpus-" + target + ",mode=max"; to[target] != want {
					add("%s: target %s must write cache-to %q, got %q", jobName, target, want, to[target])
				}
			}
		}
		if !writes && len(to) > 0 {
			add("%s: a pull request leg must never write the cache, got cache-to %v", jobName, to)
		}
		if len(from) != len(targets) || (writes && len(to) != len(targets)) {
			add("%s: the cache lines name targets the bake step does not build", jobName)
		}
		return targets
	}
	legTargets := check("auth-corpus-queue", false)
	seedTargets := check("auth-corpus-image-cache", true)
	if !reflect.DeepEqual(legTargets, seedTargets) {
		add("the seed job builds %v, the corpus legs %v: a leg would miss the cache of an image", seedTargets, legTargets)
	}
	if len(legTargets) != 6 {
		add("the corpus legs must bake the six images the corpus runs, got %v", legTargets)
	}
	if env := asString(asMap(asMap(jobs["auth-corpus-queue"])["env"])["AUTH_CORPUS_PREBUILT_TAG"]); env != "ci-prebuilt" {
		add("auth-corpus-queue must set AUTH_CORPUS_PREBUILT_TAG ci-prebuilt, so the script starts the baked images with --no-build; got %q", env)
	}
	if cond := asString(asMap(jobs["auth-corpus-image-cache"])["if"]); cond != "github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'" {
		add("auth-corpus-image-cache must run only on a push to main of this repository, got if: %q", cond)
	}
	seed := asMap(jobs["auth-corpus-image-cache"])
	if perms := asMap(seed["permissions"]); len(perms) != 2 || asString(perms["contents"]) != "read" || asString(perms["actions"]) != "write" {
		add("auth-corpus-image-cache must hold exactly contents: read and actions: write, got %v", seed["permissions"])
	}
	if strings.Contains(fmt.Sprint(seed), "secrets.") {
		add("auth-corpus-image-cache must read no secret")
	}
	if strings.Contains(fmt.Sprint(asMap(jobs["auth-gate"])["needs"]), "auth-corpus-image-cache") {
		add("auth-gate must not need the cache seed job")
	}
	return out
}

func TestAuthCorpusBuildCacheIsReadOnlyOnPullRequests(t *testing.T) {
	root := rootOf(t)
	if v := authCorpusCacheViolations(loadWf(t, root, "go-auth-service.yml")); len(v) > 0 {
		t.Fatal(strings.Join(v, "\n"))
	}
}

func TestAuthCorpusBuildCachePlantedDefectsAreCaught(t *testing.T) {
	root := rootOf(t)
	text := readFile(t, root+"/.github/workflows/go-auth-service.yml")
	for _, c := range []struct{ name, old, repl, want string }{
		{"a pull request leg writes the cache",
			"            smtpsink.cache-from=type=gha,scope=authcorpus-smtpsink\n      - name: Run the auth-flow corpus",
			"            smtpsink.cache-from=type=gha,scope=authcorpus-smtpsink\n            smtpsink.cache-to=type=gha,scope=authcorpus-smtpsink,mode=max\n      - name: Run the auth-flow corpus",
			"must never write the cache"},
		{"the seed job runs on pull requests", "  auth-corpus-image-cache:\n    if: github.event_name == 'push' && ", "  auth-corpus-image-cache:\n    if: github.event_name != 'merge_group' && ", "only on a push to main"},
		{"the seed job gets a wider token", "      contents: read\n      actions: write\n", "      contents: write\n      actions: write\n", "exactly contents: read"},
		{"the seed job reads a secret", "  auth-corpus-image-cache:\n    if: github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'\n",
			"  auth-corpus-image-cache:\n    if: github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'\n    env:\n      T: ${{ secrets.GITHUB_TOKEN }}\n", "must read no secret"},
		{"the legs rebuild instead of using the baked images", "      AUTH_CORPUS_PREBUILT_TAG: ci-prebuilt\n", "", "AUTH_CORPUS_PREBUILT_TAG"},
		{"a shell step reads the cache token", "      - name: Run the auth-flow corpus against the external go-api\n        if: matrix.leg != 'acp-nokeys'\n        run: ",
			"      - name: Run the auth-flow corpus against the external go-api\n        if: matrix.leg != 'acp-nokeys'\n        run: echo \"${ACTIONS_RUNTIME_TOKEN}\"; ", "reads the actions cache token"},
	} {
		if strings.Count(text, c.old) != 1 {
			t.Fatalf("%s: the plant anchor occurs %d times", c.name, strings.Count(text, c.old))
		}
		doc := parseYAML(t, strings.Replace(text, c.old, c.repl, 1), c.name)
		v := strings.Join(authCorpusCacheViolations(doc), "\n")
		if !strings.Contains(v, c.want) {
			t.Errorf("%s: want a violation naming %q, got:\n%s", c.name, c.want, v)
		}
	}
}
