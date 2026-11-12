package workflows_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The immutable sha- tag of a docker-images publish must name the commit that was BUILT, not the commit the workflow was
// dispatched from. A dispatch from main with inputs.ref=X builds X; metadata-action's `type=sha` reads the workflow
// context sha (the dispatch head), so the build of X was pushed as sha-<main head> and overwrote main's own tag.
// The fix resolves the build commit once (resolve job, step build_commit), checks every build leg out at it, and tags
// the merge jobs with it. These cells pin that, and that the push-to-main tag set is unchanged.

const (
	shaTagWorkflow  = ".github/workflows/docker-images.yml"
	shaTagResolve   = "resolve"
	shaTagStepID    = "build_commit"
	shaTagBuildExpr = "needs.resolve.outputs.build_sha"
)

var shaTagBuildJobs = []string{"go-build", "go-build-dho-arm64", "go-api-tools-build"}
var shaTagMergeJobs = []string{"go-merge", "go-api-tools-merge"}

func stepByID(job any, id string) yamlMap {
	for _, s := range stepsOf(job) {
		if asString(s["id"]) == id {
			return s
		}
	}
	return nil
}

func metaTagsInput(job any) (string, bool) {
	for _, s := range stepsOf(job) {
		if strings.HasPrefix(asString(s["uses"]), "docker/metadata-action@") {
			return asString(asMap(s["with"])["tags"]), true
		}
	}
	return "", false
}

func dockerImagesShaTagViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	doc := loadWf(t, root, "docker-images.yml")
	jobs := jobsOf(doc)

	for _, name := range shaTagMergeJobs {
		tags, ok := metaTagsInput(jobs[name])
		if !ok {
			add("%s has no docker/metadata-action step: the sha- tag source is not where the guard reads it", name)
			continue
		}
		for _, line := range strings.Split(tags, "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "type=sha") {
				add("%s tags with type=sha, which reads the workflow context sha (the dispatch head), not the built commit: a dispatch with inputs.ref pushes the build of that ref as sha-<dispatch head>", name)
			}
		}
		if strings.Contains(tags, "github.sha") {
			add("%s tags read github.sha: that is the dispatch head, not the built commit", name)
		}
		if !strings.Contains(tags, "type=raw,value=sha-${{ needs.resolve.outputs.tag_short }}") {
			add("%s has no type=raw,value=sha-${{ needs.resolve.outputs.tag_short }} tag: its sha- tag does not come from the resolved build commit", name)
		}
		if !regexp.MustCompile(`type=raw,value=sha-\$\{\{ needs\.resolve\.outputs\.tag_short \}\},priority=100,`).MatchString(tags) {
			add("%s sha- raw tag has no priority=100: metadata-action would rank it (default 200) above or beside other raw tags and could change the version output on push", name)
		}
	}

	for _, name := range shaTagBuildJobs {
		found := false
		for _, s := range stepsOf(jobs[name]) {
			if !strings.HasPrefix(asString(s["uses"]), "actions/checkout@") {
				continue
			}
			found = true
			ref := asString(asMap(s["with"])["ref"])
			if !strings.HasPrefix(strings.TrimSpace(ref), "${{ "+shaTagBuildExpr+" ||") {
				add("%s checkout ref %q does not start with %s: each build leg resolves inputs.ref on its own, so a moving branch or a tag can build different commits in one run, and the sha tag cannot name them", name, ref, shaTagBuildExpr)
			}
		}
		if !found {
			add("%s has no actions/checkout step", name)
		}
	}

	res := stepByID(jobs[shaTagResolve], shaTagStepID)
	switch {
	case res == nil:
		add("the resolve job has no step with id %s: the build commit is never resolved once", shaTagStepID)
	default:
		if _, bad := res["continue-on-error"]; bad {
			add("resolve step %s sets continue-on-error: an unresolvable inputs.ref must refuse the run", shaTagStepID)
		}
		outs := asMap(asMap(jobs[shaTagResolve])["outputs"])
		for _, o := range []string{"build_sha", "tag_short"} {
			if asString(outs[o]) == "" {
				add("resolve job has no %s output", o)
			}
		}
	}

	// moving tags must stay blocked for a dispatch that names a ref: the sha tag is now right, and latest/main must not follow it
	for _, s := range stepsOf(jobs["fan-in-latest"]) {
		env := asMap(s["env"])
		if env == nil || env["IS_MAIN_REF"] == nil {
			continue
		}
		for _, k := range []string{"IS_MAIN_REF", "REF_IS_BRANCH"} {
			if !strings.Contains(asString(env[k]), "github.event.inputs.ref == ''") {
				add("fan-in %s no longer requires an empty inputs.ref: a dispatch of another ref could move latest/main or a branch tag", k)
			}
		}
	}
	return out
}

// ---- push byte-equality: render the metadata-action tag inputs under an event ----

type renderCtx struct {
	event, sha, buildSHA string
	hotfix               bool
	tree                 string
}

func (c renderCtx) tagShort() string {
	s := c.sha
	if c.event == "workflow_dispatch" && c.buildSHA != "" {
		s = c.buildSHA
	}
	return s[:7]
}

var exprRe = regexp.MustCompile(`\$\{\{\s*(.*?)\s*\}\}`)

func (c renderCtx) eval(expr string) (string, error) {
	switch expr {
	case "needs.resolve.outputs.tag_short":
		return c.tagShort(), nil
	case "needs.resolve.outputs.tree":
		return c.tree, nil
	case "needs.resolve.outputs.hotfix == 'true'":
		return strconv.FormatBool(c.hotfix), nil
	case "(github.event_name != 'pull_request' && github.event_name != 'merge_group')":
		return strconv.FormatBool(c.event != "pull_request" && c.event != "merge_group"), nil
	}
	return "", fmt.Errorf("the render cell cannot evaluate expression %q", expr)
}

type renderedTags struct {
	set     []string
	version string
}

// renderMeta models docker/metadata-action v6.2.0 (dc802804100637a589fabce1cb79ff13a1411302) for the type=raw, type=sha and
// type=semver lines only: tags sorted by priority descending, stable (src/tag.ts:69-85; defaults raw 200, sha 100, semver
// 900: tag.ts:43-52); disabled lines skipped; the version output is the first enabled value (src/meta.ts:46-103,357-366);
// type=sha,format=short is the first 7 chars of the context sha (meta.ts:343-354, defaultShortShaLength=7, meta.ts:16).
// The semver line is not modelled (it yields nothing on a branch push) and must be byte-equal between old and new input.
func renderMeta(input string, c renderCtx) (renderedTags, error) {
	type tg struct {
		typ, value string
		prio       int
		enabled    bool
	}
	var tags []tg
	for _, line := range strings.Split(input, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var evalErr error
		line = exprRe.ReplaceAllStringFunc(line, func(m string) string {
			v, err := c.eval(exprRe.FindStringSubmatch(m)[1])
			if err != nil {
				evalErr = err
			}
			return v
		})
		if evalErr != nil {
			return renderedTags{}, evalErr
		}
		attrs := map[string]string{}
		for _, kv := range strings.Split(line, ",") {
			k, v, _ := strings.Cut(kv, "=")
			attrs[k] = v
		}
		t := tg{typ: attrs["type"], enabled: attrs["enable"] != "false"}
		switch t.typ {
		case "raw":
			t.value, t.prio = attrs["value"], 200
		case "sha":
			t.value, t.prio = "sha-"+c.sha[:7], 100
			if attrs["format"] != "short" {
				return renderedTags{}, fmt.Errorf("type=sha without format=short is not modelled: %q", line)
			}
		case "semver":
			continue
		default:
			return renderedTags{}, fmt.Errorf("tag type %q is not modelled", t.typ)
		}
		if p, ok := attrs["priority"]; ok {
			n, err := strconv.Atoi(p)
			if err != nil {
				return renderedTags{}, err
			}
			t.prio = n
		}
		tags = append(tags, t)
	}
	sort.SliceStable(tags, func(i, j int) bool { return tags[i].prio > tags[j].prio })
	var r renderedTags
	for _, t := range tags {
		if !t.enabled || t.value == "" {
			continue
		}
		if r.version == "" {
			r.version = t.value
		}
		r.set = append(r.set, t.value)
	}
	return r, nil
}

// oldMergeTags is the tags input of go-merge and go-api-tools-merge on main 2936e068 (docker-images.yml:776-779, :1042-1045).
const oldMergeTags = `type=raw,value=tree-${{ needs.resolve.outputs.tree }},enable=${{ needs.resolve.outputs.hotfix == 'true' }}
type=semver,pattern={{version}}
type=sha,format=short,enable=${{ (github.event_name != 'pull_request' && github.event_name != 'merge_group') }}`

const (
	shaDispatchHead = "d1977b3e40fcb963a601b9be95e77c2ff424056f"
	shaBuiltRef     = "5d97b9e91aa43aa1abd271c587a1e2bc8c78c1ee"
)

func tagRenderViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	jobs := jobsOf(loadWf(t, root, "docker-images.yml"))
	oldSemver := "type=semver,pattern={{version}}"
	for _, name := range shaTagMergeJobs {
		now, ok := metaTagsInput(jobs[name])
		if !ok {
			add("%s has no docker/metadata-action step", name)
			continue
		}
		if !strings.Contains(now, oldSemver) {
			add("%s changed its type=semver line: release tags are out of this guard's byte-equality proof", name)
		}
		same := []renderCtx{
			{event: "push", sha: shaDispatchHead},
			{event: "release", sha: shaDispatchHead},
			{event: "workflow_dispatch", sha: shaDispatchHead},
			{event: "pull_request", sha: shaDispatchHead},
			{event: "pull_request", sha: shaDispatchHead, hotfix: true, tree: "abc123"},
			{event: "merge_group", sha: shaDispatchHead},
		}
		for _, c := range same {
			want, err := renderMeta(oldMergeTags, c)
			if err != nil {
				add("%s: old tags do not render: %v", name, err)
				continue
			}
			got, err := renderMeta(now, c)
			if err != nil {
				add("%s: tags do not render: %v", name, err)
				continue
			}
			if strings.Join(want.set, " ") != strings.Join(got.set, " ") || want.version != got.version {
				add("%s event=%s hotfix=%v: tags %v (version %q) differ from main's %v (version %q): a push would change what is pushed", name, c.event, c.hotfix, got.set, got.version, want.set, want.version)
			}
		}
		c := renderCtx{event: "workflow_dispatch", sha: shaDispatchHead, buildSHA: shaBuiltRef}
		got, err := renderMeta(now, c)
		if err != nil {
			add("%s: dispatch tags do not render: %v", name, err)
			continue
		}
		if len(got.set) != 1 || got.set[0] != "sha-"+shaBuiltRef[:7] {
			add("%s dispatch with inputs.ref=%s (dispatch head %s) renders %v, want only [sha-%s]", name, shaBuiltRef[:7], shaDispatchHead[:7], got.set, shaBuiltRef[:7])
		}
	}
	return out
}

// ---- the resolve step, executed with a stand-in gh ----

func resolveStepViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	step := stepByID(jobsOf(loadWf(t, root, "docker-images.yml"))[shaTagResolve], shaTagStepID)
	if step == nil {
		return []string{"the resolve job has no step with id " + shaTagStepID}
	}
	run := asString(step["run"])
	if run == "" {
		return []string{"resolve step " + shaTagStepID + " has no run script"}
	}
	dir := t.TempDir()
	bin := filepath.Join(dir, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	stub := "#!/usr/bin/env bash\necho \"$*\" >> \"$GH_LOG\"\ncase \"$GH_MODE\" in\n  ok) echo \"$GH_OUT\" ;;\n  fail) exit 1 ;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "gh"), []byte(stub), 0o755); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(dir, "step.sh")
	if err := os.WriteFile(script, []byte(run), 0o644); err != nil {
		t.Fatal(err)
	}
	type tc struct {
		name, event, ref, mode, ghOut string
		wantFail                      bool
		wantBuild, wantShort          string
		wantGH                        string
	}
	cases := []tc{
		{name: "dispatch with a branch ref", event: "workflow_dispatch", ref: "feature/x", mode: "ok", ghOut: shaBuiltRef, wantBuild: shaBuiltRef, wantShort: shaBuiltRef[:7], wantGH: "repos/o/r/commits/feature/x"},
		{name: "dispatch with a ref that does not resolve", event: "workflow_dispatch", ref: "nope", mode: "fail", wantFail: true},
		{name: "dispatch with a ref that resolves to junk", event: "workflow_dispatch", ref: "x", mode: "ok", ghOut: "not-a-sha", wantFail: true},
		{name: "dispatch with a ref that resolves to nothing", event: "workflow_dispatch", ref: "x", mode: "ok", ghOut: "", wantFail: true},
		{name: "dispatch without a ref", event: "workflow_dispatch", ref: "", mode: "fail", wantBuild: "", wantShort: shaDispatchHead[:7]},
		{name: "push", event: "push", ref: "", mode: "fail", wantBuild: "", wantShort: shaDispatchHead[:7]},
	}
	for _, c := range cases {
		ghLog, outFile := filepath.Join(dir, "gh.log"), filepath.Join(dir, "out.txt")
		_ = os.Remove(ghLog)
		_ = os.Remove(outFile)
		for _, f := range []string{ghLog, outFile} {
			if err := os.WriteFile(f, nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		env := inheritedEnv([]string{"GITHUB_OUTPUT", "GITHUB_SHA"},
			"PATH="+bin+string(os.PathListSeparator)+os.Getenv("PATH"),
			"GITHUB_OUTPUT="+outFile, "GITHUB_SHA="+shaDispatchHead, "EVENT_NAME="+c.event, "INPUT_REF="+c.ref,
			"REPO=o/r", "GH_TOKEN=x", "GH_LOG="+ghLog, "GH_MODE="+c.mode, "GH_OUT="+c.ghOut)
		res := cmdSpec{dir: dir, env: env}.run("bash", script)
		outText := readFile(t, outFile)
		if c.wantFail {
			if res.code == 0 {
				add("%s: the step exited 0; an unresolvable build commit must refuse the run", c.name)
			}
			if strings.Contains(outText, "tag_short=") {
				add("%s: the step wrote tag_short before refusing", c.name)
			}
			continue
		}
		if res.code != 0 {
			add("%s: the step exited %d: %s", c.name, res.code, res.stderr)
			continue
		}
		if !strings.Contains(outText, "build_sha="+c.wantBuild+"\n") {
			add("%s: output lacks build_sha=%s: %q", c.name, c.wantBuild, outText)
		}
		if !strings.Contains(outText, "tag_short="+c.wantShort+"\n") {
			add("%s: output lacks tag_short=%s: %q", c.name, c.wantShort, outText)
		}
		gh := readFile(t, ghLog)
		if c.wantGH == "" && gh != "" {
			add("%s: gh was called (%q) where no ref is named", c.name, gh)
		}
		if c.wantGH != "" && !strings.Contains(gh, c.wantGH) {
			add("%s: gh was not asked for %s: %q", c.name, c.wantGH, gh)
		}
	}
	return out
}

func TestDockerImagesShaTagIsTheBuiltCommit(t *testing.T) {
	expectNoViolations(t, dockerImagesShaTagViolations(t, repoRoot(t)))
}

func TestDockerImagesShaTagRenderEqualsMainOnPush(t *testing.T) {
	expectNoViolations(t, tagRenderViolations(t, repoRoot(t)))
}

func TestDockerImagesResolveStepRefusesWhatItCannotResolve(t *testing.T) {
	expectNoViolations(t, resolveStepViolations(t, repoRoot(t)))
}

func TestDockerImagesShaTagPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	files := []string{shaTagWorkflow}
	const tagsLine = "            type=raw,value=sha-${{ needs.resolve.outputs.tag_short }},priority=100,enable=${{ (github.event_name != 'pull_request' && github.event_name != 'merge_group') }}\n"
	const oldLine = "            type=sha,format=short,enable=${{ (github.event_name != 'pull_request' && github.event_name != 'merge_group') }}\n"
	const coRef = "          ref: ${{ needs.resolve.outputs.build_sha || github.event.pull_request.head.sha || github.sha }}\n"
	const oldRef = "          ref: ${{ github.event.inputs.ref || github.event.pull_request.head.sha || github.sha }}\n"
	type cell struct {
		name string
		fn   func(*testing.T, string) []string
		muts []mutation
		want []string
	}
	cases := []cell{
		{"M1 both merge jobs back to type=sha", dockerImagesShaTagViolations, []mutation{{shaTagWorkflow, tagsLine, allMarker + oldLine}}, []string{"go-merge tags with type=sha", "go-api-tools-merge tags with type=sha"}},
		{"M1r go-merge sha tag loses priority=100", tagRenderViolations, []mutation{{shaTagWorkflow, tagsLine, allMarker + strings.Replace(tagsLine, ",priority=100", "", 1)}}, []string{"differ from main's"}},
		{"M3 a build checkout reads inputs.ref again", dockerImagesShaTagViolations, []mutation{{shaTagWorkflow, coRef, oldRef}}, []string{"does not start with " + shaTagBuildExpr}},
		{"M6 resolve step does not refuse an unresolvable ref", resolveStepViolations, []mutation{{shaTagWorkflow, "|| { echo \"::error::inputs.ref", "|| { true; echo \"::error::inputs.ref"}}, []string{"must refuse the run"}},
		{"M7 tag_short of a dispatch comes from the dispatch head", resolveStepViolations, []mutation{{shaTagWorkflow, "tag_sha=\"${build_sha}\"", "tag_sha=\"${GITHUB_SHA}\""}}, []string{"tag_short="}},
		{"M8 fan-in IS_MAIN_REF accepts a dispatch with a ref", dockerImagesShaTagViolations, []mutation{{shaTagWorkflow, "(github.event_name == 'workflow_dispatch' && github.event.inputs.ref == '' && github.ref == 'refs/heads/main') }}", "(github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/main') }}"}}, []string{"IS_MAIN_REF no longer requires an empty inputs.ref"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, c.fn(t, tree), c.want...)
		})
	}
}
