package workflows_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// CHAOS-13520: the auth-corpus-queue legs build their six images with docker/bake-action (compose.yml, the corpus override and
// ci/auth_corpus.bake.hcl, group authcorpus) from a gha cache. Only auth-corpus-image-cache (a push to main of this repository,
// contents: read, no secret) sets AUTH_CORPUS_CACHE_WRITE=1, the one switch that turns on the cache-to lines of the bake file; a
// pull request leg reads the cache and never writes it. The cache token stays inside the bake action (no shell step reads it),
// and the corpus script starts the loaded images with --no-build (AUTH_CORPUS_PREBUILT_TAG = the tag the bake step loads).

const bakeAction = "docker/bake-action@59cb1203cd42bcefd40a1b6d73b589e48c86040e"

var (
	bakeGroupRe   = regexp.MustCompile(`(?s)group "authcorpus" \{\s*targets = \[([^\]]*)\]`)
	bakeTargetRe  = regexp.MustCompile(`(?s)target "([a-z0-9-]+)" \{(.*?)\n\}`)
	bakeFromRe    = regexp.MustCompile(`cache-from = \["type=gha,scope=authcorpus-([a-z0-9-]+)"\]`)
	bakeToGatedRe = regexp.MustCompile(`cache-to\s+= AUTH_CORPUS_CACHE_WRITE == "1" \? \["type=gha,scope=authcorpus-([a-z0-9-]+),mode=max"\] : \[\]`)
)

const seedIf = "github.event_name == 'push' && github.ref == 'refs/heads/main' && github.repository == 'full-chaos/dev-health-ops'"

func authCorpusCacheViolations(doc yamlMap, hcl string) []string {
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }

	var group []string
	if m := bakeGroupRe.FindStringSubmatch(hcl); m != nil {
		for _, f := range strings.Split(m[1], ",") {
			if f = strings.Trim(strings.TrimSpace(f), `"`); f != "" {
				group = append(group, f)
			}
		}
	}
	sort.Strings(group)
	if len(group) != 6 {
		add("ci/auth_corpus.bake.hcl: group authcorpus must hold the six images the corpus runs, got %v", group)
	}
	defined := map[string]bool{}
	for _, m := range bakeTargetRe.FindAllStringSubmatch(hcl, -1) {
		name, body := m[1], m[2]
		defined[name] = true
		from := bakeFromRe.FindStringSubmatch(body)
		if from == nil || from[1] != name {
			add("ci/auth_corpus.bake.hcl: target %s must read only its own scope authcorpus-%s", name, name)
		}
		to := bakeToGatedRe.FindStringSubmatch(body)
		if to == nil || to[1] != name || strings.Count(body, "cache-to") != 1 {
			add("ci/auth_corpus.bake.hcl: target %s must write cache-to only when AUTH_CORPUS_CACHE_WRITE is 1, to its own scope", name)
		}
	}
	for _, g := range group {
		if !defined[g] {
			add("ci/auth_corpus.bake.hcl: group target %s has no cache settings", g)
		}
	}
	if len(defined) != len(group) {
		add("ci/auth_corpus.bake.hcl: %d targets for a group of %d", len(defined), len(group))
	}

	jobs := jobsOf(doc)
	for _, jobName := range []string{"auth-corpus-queue", "auth-corpus-image-cache"} {
		var b yamlMap
		for _, s := range stepsOf(jobs[jobName]) {
			if strings.HasPrefix(asString(s["uses"]), "docker/bake-action@") {
				b = s
			}
			if r := asString(s["run"]); strings.Contains(r, "ACTIONS_RUNTIME_TOKEN") || strings.Contains(r, "ACTIONS_CACHE_URL") || strings.Contains(r, "ACTIONS_RESULTS_URL") {
				add("%s: a shell step reads the actions cache token or URL", jobName)
			}
		}
		if b == nil {
			add("%s has no docker/bake-action step", jobName)
			continue
		}
		if !strings.HasPrefix(asString(b["uses"]), bakeAction) {
			add("%s: the bake step must use %s, got %q", jobName, bakeAction, asString(b["uses"]))
		}
		if withString(b, "load") != "true" || withString(b, "source") != "." || withString(b, "targets") != "authcorpus" {
			add("%s: the bake step must build group authcorpus of the checkout (source: .) and load the images", jobName)
		}
		if files := strings.Fields(withString(b, "files")); strings.Join(files, " ") != "compose.yml ci/auth_corpus_external.override.yml ci/auth_corpus.bake.hcl" {
			add("%s: the bake files must be compose.yml, the corpus override and ci/auth_corpus.bake.hcl, got %v", jobName, files)
		}
		if withString(b, "set") != "" {
			add("%s: the bake step sets no value: the cache lines live in ci/auth_corpus.bake.hcl", jobName)
		}
		env := asMap(b["env"])
		if tag := asString(env["AUTH_CORPUS_IMAGE_TAG"]); tag != "ci-prebuilt" {
			add("%s: the bake step must tag the stub images ci-prebuilt (AUTH_CORPUS_IMAGE_TAG), got %q", jobName, tag)
		}
		write := asString(env["AUTH_CORPUS_CACHE_WRITE"])
		switch {
		case jobName == "auth-corpus-queue" && write != "":
			add("auth-corpus-queue: a pull request leg must never write the cache, got AUTH_CORPUS_CACHE_WRITE %q", write)
		case jobName == "auth-corpus-image-cache" && write != "1":
			add("auth-corpus-image-cache must write the cache (AUTH_CORPUS_CACHE_WRITE 1), got %q", write)
		}
		if strings.Contains(fmt.Sprint(asMap(jobs[jobName])["env"]), "AUTH_CORPUS_CACHE_WRITE") {
			add("%s: AUTH_CORPUS_CACHE_WRITE belongs on the seed job's bake step only", jobName)
		}
	}
	for _, s := range stepsOf(jobs["auth-corpus-queue"]) {
		if r := asString(s["run"]); strings.Contains(r, "ci/auth_corpus_external.sh") {
			if env := asString(asMap(s["env"])["AUTH_CORPUS_PREBUILT_TAG"]); env != "ci-prebuilt" {
				add("auth-corpus-queue step %q must set AUTH_CORPUS_PREBUILT_TAG ci-prebuilt, so the script starts the baked images with --no-build; got %q", asString(s["name"]), env)
			}
		}
	}
	seed := asMap(jobs["auth-corpus-image-cache"])
	if cond := asString(seed["if"]); cond != seedIf {
		add("auth-corpus-image-cache must run only on a push to main of this repository, got if: %q", cond)
	}
	if perms := asMap(seed["permissions"]); len(perms) != 1 || asString(perms["contents"]) != "read" {
		add("auth-corpus-image-cache must hold exactly contents: read, got %v", seed["permissions"])
	}
	if strings.Contains(fmt.Sprint(seed), "secrets.") {
		add("auth-corpus-image-cache must read no secret")
	}
	if strings.Contains(fmt.Sprint(asMap(jobs["auth-gate"])["needs"]), "auth-corpus-image-cache") {
		add("auth-gate must not need the cache seed job")
	}
	return out
}

func authCorpusCacheInputs(t *testing.T, root string) (string, string) {
	t.Helper()
	return readFile(t, filepath.Join(root, ".github/workflows/go-auth-service.yml")), readFile(t, filepath.Join(root, "ci/auth_corpus.bake.hcl"))
}

func TestAuthCorpusBuildCacheIsReadOnlyOnPullRequests(t *testing.T) {
	wf, hcl := authCorpusCacheInputs(t, rootOf(t))
	if v := authCorpusCacheViolations(parseYAML(t, wf, "go-auth-service.yml"), hcl); len(v) > 0 {
		t.Fatal(strings.Join(v, "\n"))
	}
}

func TestAuthCorpusBuildCachePlantedDefectsAreCaught(t *testing.T) {
	wf, hcl := authCorpusCacheInputs(t, rootOf(t))
	const legBake = "          AUTH_CORPUS_IMAGE_TAG: ci-prebuilt\n          # compose.yml's bugsink"
	for _, c := range []struct{ name, file, old, repl, want string }{
		{"a pull request leg writes the cache", "wf", legBake, "          AUTH_CORPUS_IMAGE_TAG: ci-prebuilt\n          AUTH_CORPUS_CACHE_WRITE: \"1\"\n          # compose.yml's bugsink", "must never write the cache"},
		{"the bake file writes unconditionally", "hcl", `cache-to   = AUTH_CORPUS_CACHE_WRITE == "1" ? ["type=gha,scope=authcorpus-smtpsink,mode=max"] : []`, `cache-to   = ["type=gha,scope=authcorpus-smtpsink,mode=max"]`, "only when AUTH_CORPUS_CACHE_WRITE is 1"},
		{"a target reads another scope", "hcl", `cache-from = ["type=gha,scope=authcorpus-go-api"]`, `cache-from = ["type=gha,scope=authcorpus-auth-service"]`, "only its own scope"},
		{"the seed job runs on pull requests", "wf", "  auth-corpus-image-cache:\n    if: github.event_name == 'push' && ", "  auth-corpus-image-cache:\n    if: github.event_name != 'merge_group' && ", "only on a push to main"},
		{"the seed job gets a wider token", "wf", "    permissions:\n      contents: read\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          persist-credentials: false\n",
			"    permissions:\n      contents: read\n      actions: write\n    steps:\n      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1\n        with:\n          persist-credentials: false\n", "exactly contents: read"},
		{"the seed job reads a secret", "wf", "  auth-corpus-image-cache:\n    if: " + seedIf + "\n", "  auth-corpus-image-cache:\n    if: " + seedIf + "\n    env:\n      T: ${{ secrets.GITHUB_TOKEN }}\n", "must read no secret"},
		{"the legs rebuild instead of using the baked images", "wf", "        env:\n          AUTH_CORPUS_PREBUILT_TAG: ci-prebuilt\n        run: bash ci/auth_corpus_external.sh", "        run: bash ci/auth_corpus_external.sh", "AUTH_CORPUS_PREBUILT_TAG"},
		{"a shell step reads the cache token", "wf", "      - name: Run the auth-flow corpus against the external go-api\n        if: matrix.leg != 'acp-nokeys'\n        run: ",
			"      - name: Run the auth-flow corpus against the external go-api\n        if: matrix.leg != 'acp-nokeys'\n        run: echo \"${ACTIONS_RUNTIME_TOKEN}\"; ", "reads the actions cache token"},
	} {
		w, h := wf, hcl
		target := &w
		if c.file == "hcl" {
			target = &h
		}
		if strings.Count(*target, c.old) != 1 {
			t.Fatalf("%s: the plant anchor occurs %d times", c.name, strings.Count(*target, c.old))
		}
		*target = strings.Replace(*target, c.old, c.repl, 1)
		v := strings.Join(authCorpusCacheViolations(parseYAML(t, w, c.name), h), "\n")
		if !strings.Contains(v, c.want) {
			t.Errorf("%s: want a violation naming %q, got:\n%s", c.name, c.want, v)
		}
	}
}
