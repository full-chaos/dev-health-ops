package cigates_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of the YAML-shape tests of tests/tooling/test_docker_images_fanin_gate.py (CHAOS-4947): the
// main-head decision for moving tags is made once, by a fan-in job, as a descendant check on the OCI revision label,
// serialised by a never-cancelled concurrency group. The behavioural tests of that file (the fan-in script executed
// against stand-in `docker buildx imagetools` / git) are MOVED-TO-C and stay in Python until PR-C replaces them.

func fanInJobs(doc yamlMap) map[string]yamlMap {
	out := map[string]yamlMap{}
	for name, j := range jobsOf(doc) {
		if strings.Contains(name, "fan-in") || strings.Contains(name, "fan_in") {
			out[name] = asMap(j)
		}
	}
	return out
}

func fanInRunText(jobs map[string]yamlMap) string {
	var parts []string
	for _, name := range sortedKeys(toAnyMap4(jobs)) {
		for _, s := range stepsOf(jobs[name]) {
			parts = append(parts, str(s["run"]))
		}
	}
	return strings.Join(parts, "\n")
}

func toAnyMap4(m map[string]yamlMap) yamlMap {
	o := yamlMap{}
	for k := range m {
		o[k] = nil
	}
	return o
}

func needsList(j yamlMap) []string {
	switch n := j["needs"].(type) {
	case string:
		return []string{n}
	case []any:
		var out []string
		for _, x := range n {
			out = append(out, asString(x))
		}
		return out
	}
	return nil
}

func contains(l []string, s string) bool {
	for _, x := range l {
		if x == s {
			return true
		}
	}
	return false
}

func fanInViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	doc := loadWf(t, root, "docker-images.yml")
	jobs := jobsOf(doc)
	fan := fanInJobs(doc)
	if len(fan) == 0 {
		add("no job with 'fan-in' in its name in docker-images.yml: the CHAOS-4947 fan-in job was removed or renamed")
		return out
	}
	for _, name := range sortedKeys(toAnyMap4(fan)) {
		n := needsList(fan[name])
		if !contains(n, "go-merge") || !contains(n, "go-api-tools-merge") {
			add("job %q must need BOTH go-merge and go-api-tools-merge so it sees every family's immutable tag before deciding for any; got needs=%v", name, n)
		}
		c := asMap(fan[name]["concurrency"])
		if c == nil || str(c["group"]) == "" {
			add("%s: no concurrency group: fan-in runs for different commits can race each other even with the ancestor check", name)
		} else if b, ok := c["cancel-in-progress"].(bool); !ok || b {
			add("%s: concurrency.cancel-in-progress must be explicitly false: cancelling a queued fan-in run drops that commit's moving-tag decision entirely", name)
		}
	}

	for _, jn := range []string{"go-merge", "go-api-tools-merge"} {
		job := asMap(jobs[jn])
		if job == nil {
			add("expected a %q job in docker-images.yml", jn)
			continue
		}
		for _, s := range stepsOf(job) {
			nm := str(s["name"])
			if strings.Contains(nm, "main's current head") || strings.Contains(strings.ToLower(nm), "mainhead") {
				add("%s: still has a per-leg main-head decision step %q: CHAOS-4947 moves this to the fan-in job", jn, nm)
			}
			if str(s["id"]) == "meta" {
				tags := str(asMap(s["with"])["tags"])
				if strings.Contains(tags, "type=raw,value=latest") {
					add("%s's Docker meta step still emits a `latest` tag directly: that decision belongs in the fan-in job only", jn)
				}
				if strings.Contains(tags, "type=ref,event=branch") {
					add("%s's Docker meta step still emits a branch-name tag directly: that decision belongs in the fan-in job too", jn)
				}
			}
		}
	}

	text := fanInRunText(fan)
	if !strings.Contains(text, "merge-base") || !strings.Contains(text, "--is-ancestor") {
		add("the fan-in job does not call `git merge-base --is-ancestor`: CHAOS-4947 requires a descendant check, not an equals-head comparison")
	}
	if !strings.Contains(text, "org.opencontainers.image.revision") {
		add("the fan-in job's ancestry base must come from the org.opencontainers.image.revision OCI label, not a sha-<7> tag string")
	}
	if strings.Contains(text, "${amd64_rev:-${arm64_rev}}") {
		add("found `${amd64_rev:-${arm64_rev}}`: this silently takes whichever platform's revision label exists when only one is present; both labels must be present and equal before trusting either")
	}
	if !strings.Contains(text, `[ -n "${amd64_rev}" ] && [ -n "${arm64_rev}" ]`) {
		add("no explicit both-platforms-present guard before the base_sha assignment: a family with one platform labeled must not be treated as agreeing")
	}
	if !strings.Contains(text, "git log --first-parent") {
		add("the unlabeled-family fallback must walk first-parent history from the built commit, not the label alone")
	}
	if !strings.Contains(text, "-n 50") && !strings.Contains(text, "-n50") {
		add("the fan-in history walk for unlabeled families must be bounded (-n 50): an unbounded walk is a runaway registry API-call risk")
	}

	// Dockerfile revision label and build args
	if job := asMap(jobs["go-build"]); job == nil {
		add("expected a \"go-build\" job in docker-images.yml")
	} else {
		var build yamlMap
		for _, s := range stepsOf(job) {
			if str(s["id"]) == "build" {
				build = s
				break
			}
		}
		if build == nil {
			add("go-build: no step with id: build found")
		} else {
			w := asMap(build["with"])
			if strings.Contains(str(w["labels"]), "org.opencontainers.image.revision=") {
				add("go-build's build step sets a `labels:` input containing org.opencontainers.image.revision: a second mechanism alongside the Dockerfile LABEL; the two can disagree")
			}
			if !strings.Contains(str(w["build-args"]), "COMMIT=") {
				add("go-build's build-args don't pass COMMIT: the Dockerfile's LABEL references it, but nothing supplies a value at build time")
			}
		}
	}
	df := readFile(t, filepath.Join(root, "docker/go-worker.Dockerfile"))
	if !strings.Contains(df, "org.opencontainers.image.revision=") {
		add("docker/go-worker.Dockerfile has no org.opencontainers.image.revision LABEL: the fan-in ancestry check would treat the family as a permanent bootstrap case")
	}

	// branch tag sanitiser: the full Docker tag grammar
	if !strings.Contains(text, "tr -c 'A-Za-z0-9_.-' '-'") {
		add("the branch-tag sanitizer's character-class translation is missing or changed: re-verify against the Docker tag grammar")
	}
	if !strings.Contains(text, "[A-Za-z0-9_]*") {
		add("no leading-character guard after the sanitizer: a branch like +foo sanitizes to an invalid leading '-'")
	}
	prefixPos := strings.Index(text, `safe_branch_tag="x${safe_branch_tag}"`)
	capPos := strings.Index(text, ":0:128")
	switch {
	case capPos < 0:
		add("no 128-character length cap (:0:128) on the sanitized branch tag: Docker's tag grammar caps length at 128")
	case prefixPos < 0:
		add("the leading-character prefix assignment (safe_branch_tag=\"x${safe_branch_tag}\") is missing")
	case capPos < prefixPos:
		add("the 128-char truncation appears before the leading-character prefix is added: truncating first then prefixing can yield a 129-character tag")
	}

	// the digest walk uses the single-attempt helper
	start := strings.Index(text, "while IFS= read -r candidate_sha")
	end := strings.Index(text, `done < <(git log --first-parent -n 50 --format=%H "${BUILT_SHA}")`)
	if start < 0 || end < start {
		add("the digest-walk candidate loop (while IFS= read -r candidate_sha ... git log --first-parent -n 50) was not found")
	} else {
		body := text[start:end]
		if !strings.Contains(body, "_inspect_once") {
			add("the digest-walk candidate loop no longer calls _inspect_once: a retrying helper pays for retries on an expected outcome (absence)")
		}
		if strings.Contains(body, `_inspect_retry "`) || strings.Contains(body, `_inspect_retry_classify "`) {
			add("the digest-walk candidate loop calls a multi-attempt helper (_inspect_retry / _inspect_retry_classify): absence is expected for most candidates, retrying turns a fast walk into a slow one")
		}
	}

	// :latest fails closed on unknown registry errors
	if !strings.Contains(text, "_inspect_classify") {
		add("no _inspect_classify helper: the :latest check must distinguish CONFIRMED ABSENT from UNKNOWN registry failures")
	}
	if !strings.Contains(text, `"${ref}: not found"`) {
		add("the classifier does not match the live-verified 'not found' stderr substring, the only pattern observed against ghcr.io")
	}
	if strings.Contains(text, "manifest unknown") || strings.Contains(text, "name unknown") {
		add("found an unverified guessed error-text pattern ('manifest unknown' or 'name unknown'): only the live-verified 'not found' may classify CONFIRMED ABSENT")
	}
	if !strings.Contains(text, "_inspect_retry_classify") {
		add("no _inspect_retry_classify helper: the :latest check must retry only the UNKNOWN outcome")
	}
	if pos := strings.Index(text, `_inspect_retry_classify "${image}:latest"`); pos < 0 {
		add("the :latest call site `_inspect_retry_classify \"${image}:latest\"` was not found")
	} else {
		tail := text[pos:min(len(text), pos+1200)]
		if !strings.Contains(tail, `"${latest_rc}" -eq 1`) {
			add("no branch on rc==1 (CONFIRMED ABSENT) near the :latest call site: the three-way outcome must be handled explicitly")
		}
		if !strings.Contains(tail, `"${latest_rc}" -eq 2`) {
			add("no branch on rc==2 (UNKNOWN) near the :latest call site: an unrecognized registry failure must fail closed, never bootstrap")
		}
	}
	return out
}

func TestDockerImagesFanInGate(t *testing.T) {
	expectNoViolations(t, fanInViolations(t, repoRoot(t)))
}

func TestDockerImagesFanInGatePlantedDefects(t *testing.T) {
	root := repoRoot(t)
	const wf, df = ".github/workflows/docker-images.yml", "docker/go-worker.Dockerfile"
	files := []string{wf, df}
	text := readFile(t, filepath.Join(root, wf))
	pick := func(sub string) string {
		if !strings.Contains(text, sub) {
			t.Fatalf("fixture string %q not in %s", sub, wf)
		}
		return sub
	}
	plant := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"equality replaces the ancestry check", []mutation{{wf, pick("--is-ancestor"), allMarker + "--is-equal"}}, []string{"merge-base --is-ancestor"}},
		{"revision label dropped from the Dockerfile", []mutation{{df, "org.opencontainers.image.revision=", "org.example.rev="}}, []string{"has no org.opencontainers.image.revision LABEL"}},
		{"single-platform fallback guesses", []mutation{{wf, pick(`[ -n "${amd64_rev}" ] && [ -n "${arm64_rev}" ]`), `[ -n "${amd64_rev}" ]`}}, []string{"both-platforms-present guard"}},
		{"digest walk becomes unbounded", []mutation{{wf, pick("git log --first-parent -n 50"), "git log --first-parent"}}, []string{"bounded", "candidate loop"}},
		{"sanitiser cap moves before the prefix", []mutation{{wf, pick(":0:128"), ":0:256"}}, []string{"128-character length cap"}},
		{"a guessed error text is trusted", []mutation{{wf, pick(`"${ref}: not found"`), `"${ref}: not found"|*"manifest unknown"*`}}, []string{"unverified guessed error-text"}},
		{"fan-in runs may be cancelled", []mutation{{wf, "cancel-in-progress: false", allMarker + "cancel-in-progress: true"}}, []string{"cancel-in-progress must be explicitly false"}},
	}
	for _, c := range plant {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, fanInViolations(t, tree), c.want...)
		})
	}
}
