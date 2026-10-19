package cigates_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// CHAOS-10177: Go port of tests/tooling/test_workflow_concurrency_groups.py (CHAOS-4814, 4928, 4906).
// The guard does not hard-code the right expression: it EVALUATES each workflow's concurrency group and
// cancel-in-progress per event with a small GitHub-expression evaluator and fails when the modelled events collide
// or when main / merge_group runs could be cancelled. An expression outside the grammar fails loudly (never
// assumed safe).

type ctxMap map[string]string

func eventContext(event, ref, headRef, refName, prNumber, sha string) ctxMap {
	return ctxMap{
		"github.head_ref": headRef, "github.ref_name": refName, "github.ref": ref,
		"github.event.pull_request.number": prNumber, "github.event.number": prNumber,
		"github.workflow": "W", "github.event_name": event, "github.sha": sha, "github.run_id": "1",
		// `inputs.*` only exists in a workflow_dispatch payload; no modelled event carries one.
		"github.event.inputs.ref": "",
	}
}

var (
	pushCtx     = eventContext("push", "refs/heads/topic-branch", "", "topic-branch", "", "abc123")
	prCtx       = eventContext("pull_request", "refs/pull/7/merge", "topic-branch", "7/merge", "7", "def456")
	scheduleCtx = eventContext("schedule", "refs/heads/main", "", "main", "", "abc123")
	dispatchCtx = eventContext("workflow_dispatch", "refs/heads/topic-branch", "", "topic-branch", "", "ghi789")
	pushMainCtx = eventContext("push", "refs/heads/main", "", "main", "", "mainsha1")
	mergeGrpCtx = eventContext("merge_group", "refs/heads/gh-readonly-queue/main/pr-7-abcdef", "", "gh-readonly-queue/main/pr-7-abcdef", "", "mergegroupsha1")
)

func withInputsAction(c ctxMap) ctxMap {
	out := ctxMap{"inputs.action": ""}
	for k, v := range c {
		out[k] = v
	}
	return out
}

// exprValue models a JS-like value: a comparison yields a real bool, everything else a string.
type exprValue struct {
	isBool bool
	b      bool
	s      string
}

func (v exprValue) truthy() bool {
	if v.isBool {
		return v.b
	}
	return v.s != ""
}

var exprToken = regexp.MustCompile(`^\s*(?:(\|\||&&|==|!=|\(|\))|('[^']*')|([A-Za-z_][\w.]*))`)

func tokenizeExpr(expr string) ([]string, error) {
	var tokens []string
	rest := expr
	for strings.TrimSpace(rest) != "" {
		m := exprToken.FindStringSubmatchIndex(rest)
		if m == nil || m[1] == 0 {
			return nil, fmt.Errorf("concurrency expression uses a shape this guard cannot tokenize at %q (full expression %q); extend the tokenizer rather than assuming it is safe", rest, expr)
		}
		tok := ""
		for g := 1; g <= 3; g++ {
			if m[2*g] >= 0 {
				tok = rest[m[2*g]:m[2*g+1]]
			}
		}
		tokens = append(tokens, tok)
		rest = rest[m[1]:]
	}
	return tokens, nil
}

type exprParser struct {
	tokens []string
	ctx    ctxMap
	src    string
	pos    int
}

func (p *exprParser) peek() string {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return ""
}

func (p *exprParser) parseOr() (exprValue, error) {
	v, err := p.parseAnd()
	for err == nil && p.peek() == "||" {
		p.pos++
		var rhs exprValue
		if rhs, err = p.parseAnd(); err == nil && !v.truthy() {
			v = rhs
		}
	}
	return v, err
}

func (p *exprParser) parseAnd() (exprValue, error) {
	v, err := p.parseComparison()
	for err == nil && p.peek() == "&&" {
		p.pos++
		var rhs exprValue
		if rhs, err = p.parseComparison(); err == nil && v.truthy() {
			v = rhs
		}
	}
	return v, err
}

func (p *exprParser) parseComparison() (exprValue, error) {
	lhs, err := p.parseAtom()
	if err != nil {
		return lhs, err
	}
	if op := p.peek(); op == "==" || op == "!=" {
		p.pos++
		rhs, err := p.parseAtom()
		if err != nil {
			return rhs, err
		}
		same := lhs == rhs
		return exprValue{isBool: true, b: same == (op == "==")}, nil
	}
	return lhs, nil
}

func (p *exprParser) parseAtom() (exprValue, error) {
	if p.pos >= len(p.tokens) {
		return exprValue{}, fmt.Errorf("concurrency expression %q ended where a value was expected", p.src)
	}
	tok := p.tokens[p.pos]
	p.pos++
	switch {
	case tok == "(":
		v, err := p.parseOr()
		if err != nil {
			return v, err
		}
		if p.pos >= len(p.tokens) || p.tokens[p.pos] != ")" {
			return v, fmt.Errorf("concurrency expression %q is missing a closing paren", p.src)
		}
		p.pos++
		return v, nil
	case strings.HasPrefix(tok, "'") && strings.HasSuffix(tok, "'") && len(tok) >= 2:
		return exprValue{s: tok[1 : len(tok)-1]}, nil
	}
	val, ok := p.ctx[tok]
	if !ok {
		return exprValue{}, fmt.Errorf("concurrency expression %q references %q, which this guard's context does not model; extend the context rather than assuming it is safe", p.src, tok)
	}
	return exprValue{s: val}, nil
}

func evalExpr(expr string, ctx ctxMap) (exprValue, error) {
	toks, err := tokenizeExpr(expr)
	if err != nil {
		return exprValue{}, err
	}
	p := &exprParser{tokens: toks, ctx: ctx, src: expr}
	v, err := p.parseOr()
	if err != nil {
		return v, err
	}
	if p.pos != len(toks) {
		return v, fmt.Errorf("concurrency expression %q has trailing tokens this guard cannot model: %v", expr, toks[p.pos:])
	}
	return v, nil
}

var exprBlock = regexp.MustCompile(`(?s)\$\{\{(.+?)\}\}`)

// renderGroup renders a group key; a top-level bool is a modelling bug, not a render.
func renderGroup(group string, ctx ctxMap) (string, error) {
	var firstErr error
	out := exprBlock.ReplaceAllStringFunc(group, func(m string) string {
		inner := exprBlock.FindStringSubmatch(m)[1]
		v, err := evalExpr(inner, ctx)
		if err == nil && v.isBool {
			err = fmt.Errorf("concurrency expression %q rendered to a bare boolean at the top level; group keys must resolve to a string", inner)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return v.s
	})
	return out, firstErr
}

// cancels evaluates cancel-in-progress, a bare YAML bool or an ${{ }} expression.
func cancels(cancel any, ctx ctxMap) (bool, error) {
	if b, ok := cancel.(bool); ok {
		return b, nil
	}
	s := fmt.Sprint(cancel)
	inner := strings.TrimSpace(s)
	if m := exprBlock.FindStringSubmatch(s); m != nil {
		inner = strings.TrimSpace(m[1])
	}
	v, err := evalExpr(inner, ctx)
	return v.truthy(), err
}

// triggerEvents models `on:` as a map, a list or a scalar; the config of an event may be nil (no filter).
func triggerEvents(doc yamlMap) map[string]any {
	events := map[string]any{}
	switch on := doc["on"].(type) {
	case yamlMap:
		for k, v := range on {
			events[k] = v
		}
	case []any:
		for _, e := range on {
			events[asString(e)] = nil
		}
	case string:
		events[on] = nil
	}
	return events
}

func branchPatterns(events map[string]any, event string) []string {
	cfg, ok := events[event]
	if !ok {
		return nil
	}
	branches, has := asMap(cfg)["branches"]
	if !has {
		return []string{"**"} // no filter means every branch
	}
	var out []string
	for _, b := range asList(branches) {
		out = append(out, fmt.Sprint(b))
	}
	return out
}

func firesOnFeatureBranches(patterns []string) bool {
	for _, p := range patterns {
		if p != "main" && p != "master" {
			return true
		}
	}
	return false
}

func workflowGlob(t *testing.T, root string) []string { return workflowNames(t, root) }

func concurrencyOf(doc yamlMap) (group string, cancel any) {
	c := asMap(doc["concurrency"])
	if c == nil {
		return "", nil
	}
	if g, ok := c["group"]; ok && g != nil {
		group = fmt.Sprint(g)
	}
	return group, c["cancel-in-progress"]
}

func concurrencyViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }

	// (1) a shared group needs at most one event firing on a feature branch (CHAOS-4814)
	sharedChecked := 0
	for _, name := range workflowGlob(t, root) {
		doc := loadWf(t, root, name)
		ev := triggerEvents(doc)
		_, hasPush := ev["push"]
		_, hasPR := ev["pull_request"]
		group, _ := concurrencyOf(doc)
		if !hasPush || !hasPR || group == "" {
			continue
		}
		sharedChecked++
		rp, err1 := renderGroup(group, pushCtx)
		rr, err2 := renderGroup(group, prCtx)
		if err1 != nil || err2 != nil {
			add("%s: %v %v", name, err1, err2)
			continue
		}
		if !strings.EqualFold(rp, rr) {
			continue
		}
		var firing []string
		for _, e := range []string{"push", "pull_request"} {
			if firesOnFeatureBranches(branchPatterns(ev, e)) {
				firing = append(firing, e)
			}
		}
		if len(firing) > 1 {
			add("%s: %v all fire on feature branches AND share the concurrency group %q (both render %q): the two runs race to cancel each other and leave a cancelled required-context check-run on the head commit; split the group or restrict a trigger", name, firing, group, rp)
		}
	}
	if sharedChecked == 0 {
		add("no workflow declares both push and pull_request with a concurrency group: the shared-group guard would pass vacuously")
	}

	// (2) mirror-test-images: push+schedule share one group, pull_request and workflow_dispatch are isolated (CHAOS-4928)
	mirror := loadWf(t, root, "mirror-test-images.yml")
	mgroup, _ := concurrencyOf(mirror)
	rendered := map[string]string{}
	for ev, c := range map[string]ctxMap{"push": pushCtx, "schedule": scheduleCtx, "pull_request": prCtx, "workflow_dispatch": dispatchCtx} {
		r, err := renderGroup(mgroup, c)
		if err != nil {
			add("mirror-test-images.yml: %v", err)
		}
		rendered[ev] = strings.ToLower(r)
	}
	if rendered["push"] != rendered["schedule"] {
		add("mirror-test-images.yml: push and schedule must share one serialised group (got %q vs %q)", rendered["push"], rendered["schedule"])
	}
	for _, ev := range []string{"pull_request", "workflow_dispatch"} {
		if rendered[ev] == rendered["push"] {
			add("mirror-test-images.yml: %s's group must not collide with the shared push/schedule group (both rendered %q): an unrelated push or the weekly schedule could evict this event's pending run (CHAOS-4928)", ev, rendered["push"])
		}
	}
	if rendered["pull_request"] == rendered["workflow_dispatch"] {
		add("mirror-test-images.yml: pull_request and workflow_dispatch must not share a group (both rendered %q)", rendered["pull_request"])
	}

	// (3) a PR-numbered group on a pull_request workflow must cancel a stale PR run (CHAOS-4906)
	prChecked := 0
	for _, name := range workflowGlob(t, root) {
		doc := loadWf(t, root, name)
		if _, ok := triggerEvents(doc)["pull_request"]; !ok {
			continue
		}
		group, cancel := concurrencyOf(doc)
		if !strings.Contains(group, "github.event.pull_request.number") {
			continue
		}
		prChecked++
		if b, ok := cancel.(bool); ok {
			if !b {
				add("%s: cancel-in-progress is hardcoded false while this workflow triggers on pull_request and keys its group by PR number: a re-push will not cancel its stale run", name)
			}
			continue
		}
		got, err := cancels(cancel, prCtx)
		if err != nil {
			add("%s: %v", name, err)
		} else if !got {
			add("%s: cancel-in-progress (%v) does not evaluate true for a pull_request event: a re-push will not cancel its stale run", name, cancel)
		}
	}
	if prChecked == 0 {
		add("no workflow has both a pull_request trigger and a PR-numbered concurrency group: the cancel guard would pass vacuously")
	}

	// (4) the canonical shape fleet-wide: never cancel main or merge_group (CHAOS-4906)
	for _, name := range workflowGlob(t, root) {
		if name == "mirror-test-images.yml" {
			continue
		}
		doc := loadWf(t, root, name)
		ev := triggerEvents(doc)
		_, hasPR := ev["pull_request"]
		_, hasPush := ev["push"]
		_, hasMG := ev["merge_group"]
		if !hasPR && !hasPush {
			continue
		}
		group, cancel := concurrencyOf(doc)
		if group == "" {
			add("%s: triggers on pull_request or push but declares no concurrency group at all", name)
			continue
		}
		if cancel == nil {
			add("%s: triggers on pull_request or push but declares no cancel-in-progress at all", name)
			continue
		}
		check := func(label string, ctx ctxMap, wantCancel bool, why string) string {
			got, err := cancels(cancel, withInputsAction(ctx))
			if err != nil {
				add("%s: %v", name, err)
			} else if got != wantCancel {
				add("%s: %s", name, why)
			}
			r, err := renderGroup(group, withInputsAction(ctx))
			if err != nil {
				add("%s: %v", name, err)
			}
			return r
		}
		if hasPR {
			g := check("pull_request", prCtx, true, "cancel-in-progress does not cancel a stale pull_request run: a re-push will pile up behind it")
			if !strings.Contains(g, "7") {
				add("%s: pull_request group %q is not keyed by PR number", name, g)
			}
			if strings.Contains(g, prCtx["github.sha"]) {
				add("%s: pull_request group %q is keyed by commit sha instead of PR number: a re-push would never cancel the run it superseded", name, g)
			}
		}
		if hasPush {
			g := check("push", pushCtx, true, "cancel-in-progress does not cancel a stale push to a non-main branch")
			if strings.Contains(g, pushCtx["github.sha"]) {
				add("%s: a push to a non-main branch is keyed by commit sha (%q) instead of ref", name, g)
			}
			g = check("push-main", pushMainCtx, false, "cancel-in-progress cancels a push to main: reopens CHAOS-3948/CHAOS-4921/CHAOS-4946")
			if !strings.Contains(g, pushMainCtx["github.sha"]) {
				add("%s: a push to main is not keyed by commit sha (got %q): two commits pushed close together could cancel or displace each other", name, g)
			}
			if strings.Contains(g, pushMainCtx["github.ref"]) {
				add("%s: a push to main's group %q still contains the literal ref: every main push would share ONE group", name, g)
			}
		}
		if hasMG {
			g := check("merge_group", mergeGrpCtx, false, "cancel-in-progress cancels a merge_group entry: a burst of merges would lose every result but the last (CHAOS-3948)")
			if !strings.Contains(g, mergeGrpCtx["github.sha"]) {
				add("%s: a merge_group entry is not keyed by commit sha (got %q)", name, g)
			}
		}
	}
	return out
}

func TestWorkflowConcurrencyGroups(t *testing.T) {
	expectNoViolations(t, concurrencyViolations(t, repoRoot(t)))
}

func TestConcurrencyExpressionEvaluator(t *testing.T) {
	cases := []struct {
		expr string
		ctx  ctxMap
		want string
	}{
		{"github.head_ref || github.ref_name", pushCtx, "topic-branch"},
		{"github.head_ref || github.ref_name", prCtx, "topic-branch"},
		{"github.event.pull_request.number || ((github.ref == 'refs/heads/main' || github.event_name == 'merge_group') && github.sha) || github.ref", pushMainCtx, "mainsha1"},
		{"github.event.pull_request.number || ((github.ref == 'refs/heads/main' || github.event_name == 'merge_group') && github.sha) || github.ref", pushCtx, "refs/heads/topic-branch"},
		{"github.event.pull_request.number || ((github.ref == 'refs/heads/main' || github.event_name == 'merge_group') && github.sha) || github.ref", prCtx, "7"},
	}
	for _, c := range cases {
		v, err := evalExpr(c.expr, c.ctx)
		if err != nil || v.s != c.want {
			t.Errorf("%q = %q, %v; want %q", c.expr, v.s, err, c.want)
		}
	}
	for _, bad := range []string{"github.unknown", "github.sha ?? 'x'", "(github.sha", "github.sha github.ref"} {
		if _, err := evalExpr(bad, pushCtx); err == nil {
			t.Errorf("%q: evaluator accepted an expression outside its grammar", bad)
		}
	}
	if _, err := renderGroup("${{ github.sha == 'x' }}", pushCtx); err == nil {
		t.Error("a top-level boolean group key was rendered instead of refused")
	}
}

func TestWorkflowConcurrencyPlantedDefects(t *testing.T) {
	root := repoRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	const goYml = ".github/workflows/go.yml"
	const mirror = ".github/workflows/mirror-test-images.yml"
	const codeql = ".github/workflows/codeql-analysis.yml"
	const dockerImages = ".github/workflows/docker-images.yml"
	canonGroup := "group: ${{ github.workflow }}-${{ github.event.pull_request.number || ((github.ref == 'refs/heads/main' || github.event_name == 'merge_group') && github.sha) || github.ref }}"
	cancelExpr := "cancel-in-progress: ${{ github.event_name == 'pull_request' || (github.event_name == 'push' && github.ref != 'refs/heads/main') }}"
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"CHAOS-4814: push and pull_request twins share a group and both fire on branches",
			[]mutation{{goYml, canonGroup, "group: ${{ github.workflow }}-${{ github.head_ref || github.ref_name }}"}, {goYml, "branches: [main]", "branches: ['**']"}},
			[]string{"go.yml", "race to cancel each other"}},
		{"CHAOS-4928: mirror pull_request collides with the shared push group",
			[]mutation{{mirror, "group: mirror-test-images-${{ github.event.pull_request.number || (github.event_name == 'workflow_dispatch' && github.sha) || 'main' }}", "group: mirror-test-images-main"}},
			[]string{"mirror-test-images.yml", "must not collide"}},
		{"CHAOS-4906: PR-numbered group with cancel-in-progress false",
			[]mutation{{codeql, cancelExpr, "cancel-in-progress: false"}},
			[]string{"codeql-analysis.yml", "hardcoded false"}},
		{"CHAOS-4906: cancel-in-progress true also cancels main pushes",
			[]mutation{{dockerImages, cancelExpr, "cancel-in-progress: true"}},
			[]string{"docker-images.yml", "cancels a push to main"}},
		{"CHAOS-4906: main push keyed by ref instead of commit sha",
			[]mutation{{codeql, canonGroup, "group: ${{ github.workflow }}-${{ github.event.pull_request.number || github.ref }}"}},
			[]string{"codeql-analysis.yml", "not keyed by commit sha"}},
		{"CHAOS-4906: workflow drops its concurrency group",
			[]mutation{{codeql, canonGroup, "# group dropped"}},
			[]string{"codeql-analysis.yml", "no concurrency group"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, concurrencyViolations(t, tree), c.want...)
		})
	}
}

// A bare `push:` / `pull_request:` (no body) means EVERY branch. The Python helper (_branch_patterns in
// test_workflow_concurrency_groups.py) returned no branch for a non-mapping trigger, so a shared group on two bare
// triggers passed falsely.
func TestBareTriggersCountAsEveryBranch(t *testing.T) {
	root := repoRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	tree := plantTree(t, root, files)
	bare := "on:\n  push:\n  pull_request:\nconcurrency:\n  group: ${{ github.workflow }}-${{ github.head_ref || github.ref_name }}\n  cancel-in-progress: true\njobs:\n  j:\n    runs-on: ubuntu-latest\n    steps:\n      - run: true\n"
	if err := os.WriteFile(filepath.Join(tree, ".github/workflows/bare.yml"), []byte(bare), 0o644); err != nil {
		t.Fatal(err)
	}
	expectViolation(t, concurrencyViolations(t, tree), "bare.yml", "race to cancel each other")
}
