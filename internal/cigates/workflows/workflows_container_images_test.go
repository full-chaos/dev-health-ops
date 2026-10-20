package workflows_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// Container and image guards over every workflow. The defect class: a CI job starts Testcontainers against images nobody
// warmed (a cold, anonymous Docker Hub pull), or pulls from a registry CI must not depend on. The guards FAIL CLOSED: a
// check_go.sh verb is container-running unless it is listed as safe, and an image is refused unless it provably resolves
// to an approved registry.

const (
	prepullCommand    = "ci/check_go.sh integration-prepull"
	dockerLoginAction = "docker/login-action@"
)

var nonContainerVerbs = map[string]bool{
	"fmt": true, "vet": true, "test": true, "race": true, "build": true, "contract": true, "integration-vet": true,
	"integration-shard-plan": true, "integration-prepull": true, "integration-coverage": true,
}

var (
	containerLeafPattern = regexp.MustCompile(`go test\b[^\n]*-tags=integration`)
	goTestWithTags       = regexp.MustCompile(`go test\b[^\n]*-tags[= ]`)
	checkGoInvocation    = regexp.MustCompile(`ci/check_go\.sh\s+(\S+)`)
	containerTool        = regexp.MustCompile(`\b(docker|podman|compose|nerdctl)\b`)
	tagsViaEnv           = regexp.MustCompile(`\b(GOFLAGS|GOEXPERIMENT)\b`)
	mirrorHost           = regexp.MustCompile(`(?i)(?:^|[^\w.:/-])ghcr\.io(?::\d+)?/`)
	verbShape            = regexp.MustCompile(`^[a-z0-9-]+$`)
)

func checkGoFunctions(t *testing.T, src string) map[string]string {
	t.Helper()
	bodies := map[string]string{}
	for _, m := range regexp.MustCompile(`(?m)^([a-z_][a-z0-9_]*)\(\) \{$`).FindAllStringSubmatchIndex(src, -1) {
		name := src[m[2]:m[3]]
		rest := src[m[1]:]
		end := strings.Index(rest, "\n}\n")
		if end == -1 {
			t.Fatalf("unterminated function %s", name)
		}
		bodies[name] = rest[:end]
	}
	if len(bodies) == 0 {
		t.Fatal("parsed no functions out of check_go.sh")
	}
	return bodies
}

func checkGoDispatch(t *testing.T, src string) map[string]string {
	t.Helper()
	arms := map[string]string{}
	for _, m := range regexp.MustCompile(`(?ms)^  ([a-z0-9|_-]+)\)\n(.*?)^    ;;$`).FindAllStringSubmatch(src, -1) {
		for _, verb := range strings.Split(m[1], "|") {
			arms[verb] = m[2]
		}
	}
	if _, ok := arms["ci"]; !ok {
		t.Fatal("did not parse check_go.sh's verb dispatch")
	}
	return arms
}

// verbsReachingAContainerLeaf is a cross-check only: static, so it under-reports; never used to grant safety.
func verbsReachingAContainerLeaf(t *testing.T, src string) map[string]bool {
	t.Helper()
	functions := checkGoFunctions(t, src)
	leaves := map[string]bool{}
	for name, body := range functions {
		if containerLeafPattern.MatchString(body) {
			leaves[name] = true
		}
	}
	if len(leaves) == 0 {
		t.Fatal("found no function running an integration-tagged go test")
	}
	var reaches func(body string, seen map[string]bool) bool
	reaches = func(body string, seen map[string]bool) bool {
		for name, fb := range functions {
			if seen[name] || !regexp.MustCompile(`\b`+regexp.QuoteMeta(name)+`\b`).MatchString(body) {
				continue
			}
			next := map[string]bool{name: true}
			for k := range seen {
				next[k] = true
			}
			if leaves[name] || reaches(fb, next) {
				return true
			}
		}
		return false
	}
	out := map[string]bool{}
	for verb, body := range checkGoDispatch(t, src) {
		if reaches(body, map[string]bool{}) {
			out[verb] = true
		}
	}
	return out
}

func TestNoVerbDeclaredSafeActuallyStartsContainers(t *testing.T) {
	src := readFile(t, filepath.Join(rootOf(t), "ci", "check_go.sh"))
	derived := verbsReachingAContainerLeaf(t, src)
	var overlap []string
	for v := range nonContainerVerbs {
		if derived[v] {
			overlap = append(overlap, v)
		}
	}
	if len(overlap) > 0 {
		sort.Strings(overlap)
		t.Errorf("verbs declared non-container reach an integration go test: %v", overlap)
	}
	for _, v := range []string{"ci", "all", "integration", "integration-shard"} {
		if !derived[v] {
			t.Errorf("the derivation no longer sees %q as container-starting: the cross-check has rotted", v)
		}
	}
	for _, v := range []string{"fmt", "vet", "build", "integration-shard-plan"} {
		if derived[v] {
			t.Errorf("the derivation sees %q as container-starting", v)
		}
	}
}

func runUsesGoTestHarness(command string) bool {
	if goTestWithTags.MatchString(command) || tagsViaEnv.MatchString(command) {
		return true
	}
	for _, m := range checkGoInvocation.FindAllStringSubmatch(command, -1) {
		if !verbShape.MatchString(m[1]) || !nonContainerVerbs[m[1]] {
			return true
		}
	}
	return false
}

func normaliseScheme(s string) string { return strings.ReplaceAll(s, "://", " ") }

func isDockerLoginAction(step yamlMap) bool {
	return strings.HasPrefix(strings.ToLower(asString(step["uses"])), dockerLoginAction)
}

func withString(step yamlMap, key string) string {
	v, _ := asMap(step["with"])[key]
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func stepKind(step yamlMap) string {
	if isDockerLoginAction(step) {
		if withString(step, "registry") == "ghcr.io" {
			return "login"
		}
		return ""
	}
	command := asString(step["run"])
	if command == "" {
		return ""
	}
	if regexp.MustCompile(`^(?:bash\s+)?` + regexp.QuoteMeta(prepullCommand) + `$`).MatchString(strings.TrimSpace(command)) {
		return "prepull"
	}
	if runUsesGoTestHarness(command) {
		return "harness"
	}
	if strings.Contains(command, "compose.compatibility.yml") || strings.Contains(command, "tests/compatibility/river") {
		return "mirror"
	}
	if mirrorHost.MatchString(normaliseScheme(command)) {
		return "mirror"
	}
	return ""
}

func jobsAsMaps(doc yamlMap) map[string]yamlMap {
	out := map[string]yamlMap{}
	for name, j := range jobsOf(doc) {
		if m := asMap(j); m != nil {
			out[name] = m
		}
	}
	return out
}

func jobStepKinds(job yamlMap) []string {
	var kinds []string
	for _, s := range stepsOf(job) {
		kinds = append(kinds, stepKind(s))
	}
	return kinds
}

func firstIndex(kinds []string, want ...string) int {
	for i, k := range kinds {
		for _, w := range want {
			if k == w {
				return i
			}
		}
	}
	return -1
}

func TestEveryWorkflowJobStartingContainersAuthenticatesAndPrePulls(t *testing.T) {
	var offenders []string
	for _, name := range sortedKeysOf(jobsAsMaps(loadWf(t, rootOf(t), "go.yml"))) {
		job := jobsAsMaps(loadWf(t, rootOf(t), "go.yml"))[name]
		kinds := jobStepKinds(job)
		firstPull := firstIndex(kinds, "harness", "mirror")
		if firstPull < 0 {
			continue
		}
		harness, login, prepull := firstIndex(kinds, "harness"), firstIndex(kinds, "login"), firstIndex(kinds, "prepull")
		if login < 0 {
			offenders = append(offenders, name+": pulls images with no ghcr.io login step")
		} else if login > firstPull {
			offenders = append(offenders, name+": logs in AFTER pulling images")
		}
		if harness >= 0 {
			switch {
			case prepull < 0:
				offenders = append(offenders, fmt.Sprintf("%s: starts Testcontainers with no `%s` step", name, prepullCommand))
			case prepull > harness:
				offenders = append(offenders, name+": pre-pulls AFTER starting Testcontainers")
			case login >= 0 && login > prepull:
				offenders = append(offenders, name+": logs in AFTER pre-pulling")
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
}

func sortedKeysOf(m map[string]yamlMap) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func allWorkflowDocs(t *testing.T, root string) map[string]yamlMap {
	t.Helper()
	docs := map[string]yamlMap{}
	for _, n := range workflowNames(t, root) {
		docs[n] = loadWf(t, root, n)
	}
	return docs
}

func TestCIHoldsNoDockerHubCredentials(t *testing.T) {
	root := rootOf(t)
	var offenders []string
	for _, name := range workflowNames(t, root) {
		if strings.Contains(readFile(t, filepath.Join(root, ".github", "workflows", name)), "DOCKERHUB_") {
			offenders = append(offenders, name+": references a DOCKERHUB_* secret")
		}
		doc := loadWf(t, root, name)
		for _, jn := range sortedKeysOf(jobsAsMaps(doc)) {
			for _, step := range stepsOf(jobsAsMaps(doc)[jn]) {
				if !isDockerLoginAction(step) {
					continue
				}
				if reg := strings.TrimSpace(withString(step, "registry")); reg != "ghcr.io" {
					if reg == "" {
						reg = "<unset>"
					}
					offenders = append(offenders, fmt.Sprintf("%s: job %q runs docker/login-action with registry=%s; an unset registry DEFAULTS to Docker Hub, which CI must never authenticate to", name, jn, reg))
				}
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
}

func TestGhcrLoginIsPresentAndNotForkGuarded(t *testing.T) {
	jobs := jobsAsMaps(loadWf(t, rootOf(t), "go.yml"))
	for _, name := range sortedKeysOf(jobs) {
		for _, step := range stepsOf(jobs[name]) {
			if isDockerLoginAction(step) && withString(step, "registry") == "ghcr.io" && strings.Contains(fmt.Sprint(step["if"]), "head.repo.full_name") {
				t.Errorf("%s: ghcr login is fork-guarded", name)
			}
		}
	}
	for _, name := range sortedKeysOf(jobs) {
		needsMirror, hasLogin := false, false
		for _, step := range stepsOf(jobs[name]) {
			c := asString(step["run"])
			if strings.Contains(c, prepullCommand) || strings.Contains(c, "compose.compatibility.yml") || strings.Contains(strings.ToLower(c), "river") {
				needsMirror = true
			}
			if isDockerLoginAction(step) && withString(step, "registry") == "ghcr.io" {
				hasLogin = true
			}
		}
		if needsMirror && !hasLogin {
			t.Errorf("%s: pulls the mirror with no ghcr.io login", name)
		}
	}
}

// ---- the image allowlist -------------------------------------------------------------------------------------------

var allowedImageRegistries = []string{"ghcr.io/full-chaos/", "mirror.gcr.io/", "gcr.io/"}

var imagePullingActions = []string{"docker/setup-buildx-action"}

type actionDebt struct{ workflow, job, action string }

// ticketed debt: actions that pull an image themselves; each must still be present so the list cannot rot.
var imagePullingActionDebt = map[actionDebt]bool{
	{"docker-images.yml", "go-build", "docker/setup-buildx-action"}:           true,
	{"docker-images.yml", "go-merge", "docker/setup-buildx-action"}:           true,
	{"docker-images.yml", "go-api-tools-build", "docker/setup-buildx-action"}: true,
	{"docker-images.yml", "go-api-tools-merge", "docker/setup-buildx-action"}: true,
	{"docker-images.yml", "fan-in-latest", "docker/setup-buildx-action"}:      true,
	{"arc-runner-image.yml", "build", "docker/setup-buildx-action"}:           true,
	{"docker-images.yml", "go-build-dho-arm64", "docker/setup-buildx-action"}: true,
}

func resolveImageExpressions(image string) string {
	return strings.NewReplacer("${{ github.repository_owner }}", "full-chaos", "${{github.repository_owner}}", "full-chaos").Replace(image)
}

func imageIsAllowed(image string) bool {
	ref := strings.TrimSpace(resolveImageExpressions(image))
	if strings.Contains(ref, "${{") || strings.Contains(ref, "${") {
		return false
	}
	folded := strings.ToLower(ref)
	for _, a := range allowedImageRegistries {
		if strings.HasPrefix(folded, strings.ToLower(a)) {
			return true
		}
	}
	return false
}

type imageDecl struct{ workflow, job, where, image string }

func allImageDeclarations(t *testing.T, root string) []imageDecl {
	t.Helper()
	var out []imageDecl
	docs := allWorkflowDocs(t, root)
	for _, wf := range sortedDocKeys(docs) {
		jobs := jobsAsMaps(docs[wf])
		for _, jn := range sortedKeysOf(jobs) {
			job := jobs[jn]
			if c := containerImage(job["container"]); c != "" {
				out = append(out, imageDecl{wf, jn, "container", c})
			}
			svcs := asMap(job["services"])
			for _, sn := range sortedKeys(svcs) {
				if img := containerImage(svcs[sn]); img != "" {
					out = append(out, imageDecl{wf, jn, "services." + sn, img})
				}
			}
		}
	}
	return out
}

func sortedDocKeys(m map[string]yamlMap) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func containerImage(v any) string {
	switch c := v.(type) {
	case string:
		return c
	case yamlMap:
		return asString(c["image"])
	}
	return ""
}

func TestEveryPulledImageResolvesToAnApprovedRegistry(t *testing.T) {
	var offenders []string
	for _, d := range allImageDeclarations(t, rootOf(t)) {
		if !imageIsAllowed(d.image) {
			offenders = append(offenders, fmt.Sprintf("%s: %s.%s pulls %q, which does not resolve to an approved registry %v. An unresolvable expression counts as unapproved.", d.workflow, d.job, d.where, d.image, allowedImageRegistries))
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
	for image, want := range map[string]bool{
		"ghcr.io/full-chaos/x:1": true, "GHCR.IO/full-chaos/x:1": true, "mirror.gcr.io/library/postgres:18": true,
		"postgres:16": false, "evil-ghcr.io/full-chaos/x": false, "ghcr.io/other/x": false,
		"${{ format('{0}/{1}/postgres:18-alpine', 'ghcr.io', github.repository_owner) }}": false,
		"ghcr.io/${{ github.repository_owner }}/x:1":                                      true,
	} {
		if got := imageIsAllowed(image); got != want {
			t.Errorf("imageIsAllowed(%q) = %v, want %v", image, got, want)
		}
	}
}

func TestMirrorPulledContainerImagesDeclareCredentials(t *testing.T) {
	root := rootOf(t)
	var offenders []string
	docs := allWorkflowDocs(t, root)
	for _, wf := range sortedDocKeys(docs) {
		jobs := jobsAsMaps(docs[wf])
		for _, jn := range sortedKeysOf(jobs) {
			check := func(where string, decl any) {
				var m yamlMap
				switch d := decl.(type) {
				case string:
					m = yamlMap{"image": d}
				case yamlMap:
					m = d
				}
				if m == nil || !mirrorHost.MatchString(normaliseScheme(asString(m["image"]))) {
					return
				}
				cred := asMap(m["credentials"])
				if asString(cred["username"]) == "" || asString(cred["password"]) == "" {
					offenders = append(offenders, fmt.Sprintf("%s: %s.%s pulls %q from the mirror without `credentials:`; the runner pulls it before any login step", wf, jn, where, asString(m["image"])))
				}
			}
			if c := jobs[jn]["container"]; c != nil {
				check("container", c)
			}
			svcs := asMap(jobs[jn]["services"])
			for _, sn := range sortedKeys(svcs) {
				check("services."+sn, svcs[sn])
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
}

func TestNoUnscannedLocalReusableWorkflowsOrCompositeActions(t *testing.T) {
	root := rootOf(t)
	var offenders []string
	docs := allWorkflowDocs(t, root)
	for _, wf := range sortedDocKeys(docs) {
		jobs := jobsAsMaps(docs[wf])
		for _, jn := range sortedKeysOf(jobs) {
			if u := asString(jobs[jn]["uses"]); strings.HasPrefix(u, "./") {
				offenders = append(offenders, fmt.Sprintf("%s: job %q calls local reusable workflow %q, which no guard here follows", wf, jn, u))
			}
			for _, s := range stepsOf(jobs[jn]) {
				if u := asString(s["uses"]); strings.HasPrefix(u, "./") {
					offenders = append(offenders, fmt.Sprintf("%s: %s uses local action %q, which no guard here follows", wf, jn, u))
				}
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n") + "\n\nTeach the image/login guards to follow this indirection before adding it.")
	}
}

func TestImagePullingActionStepsAreAllowlistedOrTicketed(t *testing.T) {
	root := rootOf(t)
	var offenders []string
	seen := map[actionDebt]bool{}
	docs := allWorkflowDocs(t, root)
	for _, wf := range sortedDocKeys(docs) {
		jobs := jobsAsMaps(docs[wf])
		for _, jn := range sortedKeysOf(jobs) {
			for _, s := range stepsOf(jobs[jn]) {
				uses := asString(s["uses"])
				if image, ok := strings.CutPrefix(uses, "docker://"); ok {
					if !imageIsAllowed(image) {
						offenders = append(offenders, fmt.Sprintf("%s: %s runs container action %q, which does not resolve to an approved registry %v", wf, jn, uses, allowedImageRegistries))
					}
					continue
				}
				action, _, _ := strings.Cut(uses, "@")
				for _, a := range imagePullingActions {
					if action != a {
						continue
					}
					key := actionDebt{wf, jn, action}
					if imagePullingActionDebt[key] {
						seen[key] = true
					} else {
						offenders = append(offenders, fmt.Sprintf("%s: %s uses %q, which pulls its own image from Docker Hub; mirror it or add it to the ticketed debt", wf, jn, action))
					}
				}
			}
		}
	}
	var stale []string
	for k := range imagePullingActionDebt {
		if !seen[k] {
			stale = append(stale, fmt.Sprintf("%v", k))
		}
	}
	if len(stale) > 0 {
		sort.Strings(stale)
		t.Errorf("ticketed action debt is stale -- these steps no longer exist; delete them so the list cannot rot into a silent allowlist:\n  %v", stale)
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
}

func TestExactlyOneWorkflowDeclaresTheRequiredGoQualityJob(t *testing.T) {
	root := rootOf(t)
	var declaring []string
	for n, doc := range allWorkflowDocs(t, root) {
		if _, ok := jobsOf(doc)["go-quality"]; ok {
			declaring = append(declaring, n)
		}
	}
	sort.Strings(declaring)
	if len(declaring) != 1 || declaring[0] != "go-quality.yml" {
		t.Fatalf("exactly one workflow may declare the required `go-quality` job; found %v. Two producers means the required context can be satisfied by whichever reports first, which is not necessarily the one that ran the gate.", declaring)
	}
}

func TestTheGoQualityJobAlwaysReports(t *testing.T) {
	doc := loadWf(t, rootOf(t), "go-quality.yml")
	on := asMap(doc["on"])
	if on == nil {
		t.Fatal("go-quality.yml has no `on` block")
	}
	for _, event := range []string{"pull_request", "push"} {
		trigger := asMap(on[event])
		if _, has := trigger["paths"]; has {
			t.Errorf("go-quality.yml must not filter %s by paths: a filtered required check never reports on the changes it filters out", event)
		}
		if _, has := trigger["paths-ignore"]; has {
			t.Errorf("go-quality.yml must not filter %s by paths-ignore either", event)
		}
	}
}

// ---- run steps that pull images --------------------------------------------------------------------------------------

var (
	dockerNonPullingSubcommands = setOf("build", "buildx", "tag", "push", "save", "load", "login", "logout", "version", "info", "inspect", "rm", "rmi", "stop", "kill", "ps", "logs", "exec", "cp", "system", "ls", "list", "history", "prune", "df", "stats", "top", "port", "start", "restart", "pause", "unpause", "rename", "wait", "diff", "attach", "events", "commit", "export")
	nounPullingVerbs            = map[string]map[string]bool{
		"": setOf("pull", "run", "create"), "image": setOf("pull"), "container": setOf("run", "create"), "service": setOf("create"), "plugin": setOf("install"),
	}
	nonPullingNouns      = setOf("volume", "network", "config", "secret", "node", "context", "builder", "trust", "system", "manifest", "swarm")
	composeLikeVerbs     = map[string]map[string]bool{"stack": setOf("deploy")}
	composeLikeSafeVerbs = map[string]map[string]bool{"stack": setOf("ls", "ps", "rm", "services")}
	composeBinaries      = setOf("docker-compose", "podman-compose", "nerdctl-compose")
	containerBinaries    = setOf("docker", "podman", "nerdctl")
	valueFlags           = setOf("-v", "--volume", "-e", "--env", "-p", "--publish", "--name", "--network", "-w", "--workdir", "--platform", "--entrypoint", "-u", "--user", "--label", "-l")
	managementNouns      = func() map[string]bool {
		out := map[string]bool{}
		for n := range nounPullingVerbs {
			if n != "" {
				out[n] = true
			}
		}
		for n := range nonPullingNouns {
			out[n] = true
		}
		for n := range composeLikeVerbs {
			out[n] = true
		}
		return out
	}()
)

func setOf(items ...string) map[string]bool {
	m := map[string]bool{}
	for _, i := range items {
		m[i] = true
	}
	return m
}

// shlexSplit splits a shell line into words, honouring single and double quotes and backslash escapes, and dropping
// a `#` comment. It reports false when a quote is left open.
func shlexSplit(line string) ([]string, bool) {
	var words []string
	var cur strings.Builder
	inWord := false
	runes := []rune(line)
	for i := 0; i < len(runes); i++ {
		c := runes[i]
		switch {
		case c == '\\' && i+1 < len(runes):
			i++
			cur.WriteRune(runes[i])
			inWord = true
		case c == '\'':
			j := i + 1
			for j < len(runes) && runes[j] != '\'' {
				j++
			}
			if j >= len(runes) {
				return nil, false
			}
			cur.WriteString(string(runes[i+1 : j]))
			i, inWord = j, true
		case c == '"':
			j := i + 1
			for j < len(runes) && runes[j] != '"' {
				if runes[j] == '\\' && j+1 < len(runes) {
					j++
				}
				cur.WriteRune(runes[j])
				j++
			}
			if j >= len(runes) {
				return nil, false
			}
			i, inWord = j, true
		case c == '#' && !inWord:
			i = len(runes)
		case c == ' ' || c == '\t':
			if inWord {
				words = append(words, cur.String())
				cur.Reset()
				inWord = false
			}
		default:
			cur.WriteRune(c)
			inWord = true
		}
	}
	if inWord {
		words = append(words, cur.String())
	}
	return words, true
}

func trunc(s string) string {
	if len(s) > 60 {
		return s[:60]
	}
	return s
}

// runPulledImages: the images a run text pulls, and the reasons it could not be determined. An undeterminable image is
// refused, not assumed safe.
func runPulledImages(command string) (images, unknown []string) {
	joined := regexp.MustCompile(`\\\n\s*`).ReplaceAllString(command, " ")
	for _, raw := range regexp.MustCompile(`[\n;]|&&|\|\|`).Split(joined, -1) {
		line := strings.TrimSpace(raw)
		if !containerTool.MatchString(line) {
			continue
		}
		tokens, ok := shlexSplit(line)
		if !ok {
			unknown = append(unknown, "unparseable shell: "+trunc(line))
			continue
		}
		compose := false
		for _, tk := range tokens {
			if composeBinaries[filepath.Base(tk)] {
				compose = true
			}
		}
		if compose {
			unknown = append(unknown, "`docker-compose` invocation: "+trunc(line))
			continue
		}
		var tools []int
		for i, tk := range tokens {
			if containerBinaries[filepath.Base(tk)] {
				tools = append(tools, i)
			}
		}
		for _, start := range tools {
			rest := tokens[start+1:]
			var words []string
			for _, tk := range rest {
				if !strings.HasPrefix(tk, "-") {
					words = append(words, tk)
				}
			}
			if len(words) == 0 {
				unknown = append(unknown, "container tool with no subcommand: "+trunc(line))
				continue
			}
			sub, noun := words[0], ""
			if managementNouns[sub] {
				if len(words) < 2 {
					unknown = append(unknown, fmt.Sprintf("`docker %s` with no verb: %s", sub, trunc(line)))
					continue
				}
				noun, sub = sub, words[1]
				if composeLikeVerbs[noun][sub] {
					unknown = append(unknown, fmt.Sprintf("`docker %s %s` deploys from a compose file whose images this cannot see: %s", noun, sub, trunc(line)))
					continue
				}
				if _, isComposeLike := composeLikeVerbs[noun]; isComposeLike {
					if composeLikeSafeVerbs[noun][sub] {
						continue
					}
					unknown = append(unknown, fmt.Sprintf("unrecognised verb `docker %s %s`: %s", noun, sub, trunc(line)))
					continue
				}
				if nonPullingNouns[noun] {
					continue
				}
			}
			if sub == "compose" {
				unknown = append(unknown, "`docker compose` invocation: "+trunc(line))
				continue
			}
			if dockerNonPullingSubcommands[sub] {
				continue
			}
			if !nounPullingVerbs[noun][sub] {
				where := "`docker " + sub + "`"
				if noun != "" {
					where = fmt.Sprintf("`docker %s %s`", noun, sub)
				}
				unknown = append(unknown, fmt.Sprintf("unrecognised subcommand %s: %s", where, trunc(line)))
				continue
			}
			idx := 0
			for i, tk := range rest {
				if tk == sub {
					idx = i
					break
				}
			}
			after := rest[idx+1:]
			image, skipNext := "", false
			for _, tk := range after {
				if skipNext {
					skipNext = false
					continue
				}
				if strings.HasPrefix(tk, "-") {
					if !strings.Contains(tk, "=") && valueFlags[tk] {
						skipNext = true
					}
					continue
				}
				image = tk
				break
			}
			if image == "" {
				unknown = append(unknown, fmt.Sprintf("`docker %s` with no image argument: %s", sub, trunc(line)))
			} else {
				images = append(images, image)
			}
		}
	}
	return images, unknown
}

func refusedRun(command string) bool {
	images, unknown := runPulledImages(command)
	if len(unknown) > 0 {
		return true
	}
	for _, i := range images {
		if !imageIsAllowed(i) {
			return true
		}
	}
	return false
}

func TestRunStepsDoNotPullFromUnapprovedRegistries(t *testing.T) {
	root := rootOf(t)
	var offenders []string
	docs := allWorkflowDocs(t, root)
	for _, wf := range sortedDocKeys(docs) {
		jobs := jobsAsMaps(docs[wf])
		for _, jn := range sortedKeysOf(jobs) {
			for i, s := range stepsOf(jobs[jn]) {
				command := asString(s["run"])
				if command == "" {
					continue
				}
				images, unknown := runPulledImages(command)
				for _, image := range images {
					if !imageIsAllowed(image) {
						offenders = append(offenders, fmt.Sprintf("%s: %s step %d pulls %q, which does not resolve to %v", wf, jn, i, image, allowedImageRegistries))
					}
				}
				for _, reason := range unknown {
					offenders = append(offenders, fmt.Sprintf("%s: %s step %d: %s -- an image that cannot be determined is refused, not assumed safe", wf, jn, i, reason))
				}
			}
		}
	}
	if len(offenders) > 0 {
		t.Fatal(strings.Join(offenders, "\n"))
	}
}

func TestRunScannerRefusalCases(t *testing.T) {
	for _, c := range []struct {
		command string
		refused bool
	}{
		{"docker image pull postgres:16", true}, {"podman image pull postgres:16", true},
		{"docker container run postgres:16", true}, {"docker container create postgres:16", true},
		{"docker image pull ghcr.io/full-chaos/postgres:18-alpine", false}, {"docker container run ghcr.io/full-chaos/postgres:18-alpine", false},
		{"docker image rm stale", false}, {"docker image ls", false}, {"docker image", true},
		{"docker volume create myvol", false}, {"docker network create ci-net", false}, {"docker volume create --label keep=1 myvol", false},
		{"docker secret create tls ./cert.pem", false}, {"docker config create app ./app.conf", false},
		{"docker create postgres:16", true}, {"docker create ghcr.io/full-chaos/postgres:18-alpine", false},
		{"docker container frobnicate x", true},
		{"docker stack deploy -c compose.yml mystack", true}, {"docker stack deploy --compose-file x.yml svc", true},
		{"docker stack ls", false}, {"docker stack ps mystack", false}, {"docker stack rm mystack", false}, {"docker stack services mystack", false},
		{"docker stack frobnicate mystack", true}, {"docker stack config -c compose.yml", true},
		{"docker compose up -d", true}, {"docker-compose up -d", true}, {"docker-compose -f tests/compose.yml up", true},
		{"podman-compose up", true}, {"nerdctl-compose up", true},
		{"docker pull postgres:16", true},
	} {
		if got := refusedRun(c.command); got != c.refused {
			images, unknown := runPulledImages(c.command)
			t.Errorf("%q: refused=%v, want %v (images=%v unknown=%v)", c.command, got, c.refused, images, unknown)
		}
	}
}

func TestStepKindClassificationDecoys(t *testing.T) {
	for _, c := range []struct {
		name string
		step yamlMap
		want string
	}{
		{"the real prepull", yamlMap{"run": "bash ci/check_go.sh integration-prepull"}, "prepull"},
		{"a no-op that merely contains the prepull text", yamlMap{"run": ": bash ci/check_go.sh integration-prepull"}, ""},
		{"a prepull with a leftover subset argument is not the prepull step", yamlMap{"run": "bash ci/check_go.sh integration-prepull postgres"}, ""},
		{"a ghcr login", yamlMap{"uses": "docker/login-action@v4", "with": yamlMap{"registry": "ghcr.io"}}, "login"},
		{"a login with the registry omitted (Docker Hub)", yamlMap{"uses": "Docker/Login-Action@v4"}, ""},
		{"an unsafe check_go verb", yamlMap{"run": "bash ci/check_go.sh brand-new-verb"}, "harness"},
		{"a safe check_go verb", yamlMap{"run": "bash ci/check_go.sh vet"}, ""},
		{"go test with tags through an env var", yamlMap{"run": "GOFLAGS=-tags=integration go test ./..."}, "harness"},
		{"a mirror host reference", yamlMap{"run": "docker pull ghcr.io/full-chaos/x:1"}, "mirror"},
		{"a look-alike host", yamlMap{"run": "echo evil-ghcr.io/x"}, ""},
		{"a host in a path segment", yamlMap{"run": "echo https://registry.example/ghcr.io/x"}, ""},
		{"the river compose", yamlMap{"run": "docker compose -f compose.compatibility.yml up"}, "mirror"},
	} {
		if got := stepKind(c.step); got != c.want {
			t.Errorf("%s: stepKind = %q, want %q", c.name, got, c.want)
		}
	}
}
