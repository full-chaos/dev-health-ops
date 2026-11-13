package workflows_test

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13711: a pull request only READS the Actions caches. The repository cache is capped at 10 GB; buildkit mode=max
// blobs and setup-go entries written by pull request runs evicted the go-quality leg caches main saved, so pull request legs
// ran cold. Every step that can write an Actions cache (actions/cache, actions/cache/save, a cache-to of a build or bake
// action, a setup action's cache, a run step with --cache-to) must be switched off on a pull_request or pull_request_target
// event, by the job's if, the step's if, or the value of the switch itself. The expressions are evaluated with the event name
// known and everything else unknown, so a switch passes only when it is off on that event whatever the rest holds.
// A setup action (setup-go, setup-node, setup-uv, ...) saves in its post step on a key miss and has no restore-only switch, so
// on a job a pull request can run its cache must be LITERALLY off (no expression); such a job reads the cache through an
// actions/cache/restore step and saves it with an actions/cache/save step only a push to main or the merge queue runs.

var prCacheEvents = []string{"pull_request", "pull_request_target"}

func TestNoStepWritesAnActionsCacheOnAPullRequest(t *testing.T) {
	expectNoViolations(t, prCacheWriteViolations(t, authProofRoot(t)))
}

func prCacheWriteViolations(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	checked := 0
	for _, name := range workflowNames(t, root) {
		doc := loadWf(t, root, name)
		ev := triggerEvents(doc)
		for _, event := range prCacheEvents {
			if _, ok := ev[event]; !ok {
				continue
			}
			jobs := jobsOf(doc)
			for _, jn := range sortedKeys(jobs) {
				job := asMap(jobs[jn])
				if !mayRunOn(job["if"], event) {
					continue
				}
				if uses := asString(job["uses"]); uses != "" {
					out = append(out, fmt.Sprintf("%s: job %s calls the reusable workflow %s on %s, which this guard does not follow", name, jn, uses, event))
				}
				for i, s := range stepsOf(job) {
					label := fmt.Sprintf("#%d %s", i+1, firstNonEmpty(asString(s["name"]), asString(s["id"]), asString(s["uses"])))
					if strings.HasPrefix(asString(s["uses"]), "./") {
						out = append(out, fmt.Sprintf("%s: job %s step %s runs the local action %s on %s, which this guard does not follow", name, jn, label, asString(s["uses"]), event))
						continue
					}
					why, switches := cacheWrite(root, doc, job, s)
					if why == "" {
						continue
					}
					checked++
					if !mayRunOn(s["if"], event) {
						continue
					}
					on := len(switches) == 0
					for _, sw := range switches {
						if lit, ok := sw.(literalSwitch); ok {
							on = on || !literalOff(lit.v)
						} else if switchMayBeOn(sw, event) {
							on = true
						}
					}
					if on {
						out = append(out, fmt.Sprintf("%s: job %s step %s can write an Actions cache on a %s event: %s", name, jn, label, event, why))
					}
				}
			}
		}
	}
	if checked == 0 {
		out = append(out, "no cache-writing step found in any pull_request workflow: the guard would pass vacuously")
	}
	sort.Strings(out)
	return out
}

func firstNonEmpty(s ...string) string {
	for _, v := range s {
		if v != "" {
			return v
		}
	}
	return ""
}

// literalSwitch is a setup action's cache input: only a literal off value switches it off.
type literalSwitch struct{ v any }

func literalOff(v any) bool {
	switch c := v.(type) {
	case nil:
		return true
	case bool:
		return !c
	}
	s := strings.TrimSpace(str(v))
	return s == "" || strings.EqualFold(s, "false")
}

var bakeCacheToRe = regexp.MustCompile(`(?m)^\s*cache-to\s*=\s*(.+?)\s*$`)
var bakeGatedToRe = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*) == "1" \? \[.*\] : \[\]$`)

// cacheWrite says whether a step can write an Actions cache, and which input values switch the write (all must be off to
// switch it off). No switch means only an if can keep it from writing.
func cacheWrite(root string, doc, job, s yamlMap) (string, []any) {
	uses := asString(s["uses"])
	action, _, _ := strings.Cut(strings.ToLower(uses), "@")
	with := asMap(s["with"])
	input := func(k string) (any, bool) {
		v, ok := with[k]
		return v, ok
	}
	switch {
	case action == "actions/cache" || action == "actions/cache/save":
		return action, nil
	case action == "actions/setup-go":
		if v, ok := input("cache"); ok {
			return "setup-go cache: " + str(v), []any{literalSwitch{v}}
		}
		return "setup-go (cache defaults on)", []any{literalSwitch{true}}
	case action == "astral-sh/setup-uv":
		if v, ok := input("enable-cache"); ok {
			return "setup-uv enable-cache: " + str(v), []any{literalSwitch{v}}
		}
		return "setup-uv (enable-cache defaults to auto)", []any{literalSwitch{true}}
	case strings.HasPrefix(action, "actions/setup-") || action == "ruby/setup-ruby":
		for _, k := range []string{"cache", "bundler-cache"} {
			if v, ok := input(k); ok {
				return action + " " + k + ": " + str(v), []any{literalSwitch{v}}
			}
		}
	}
	if v, ok := input("cache-to"); ok {
		return "cache-to: " + str(v), []any{v}
	}
	for _, line := range strings.Split(str(with["set"]), "\n") {
		if _, val, ok := strings.Cut(strings.TrimSpace(line), ".cache-to="); ok {
			return "bake set " + strings.TrimSpace(line), []any{val}
		}
	}
	if action == "docker/bake-action" {
		for _, f := range strings.Fields(str(with["files"])) {
			raw, err := os.ReadFile(filepath.Join(root, f))
			if err != nil {
				continue
			}
			for _, m := range bakeCacheToRe.FindAllStringSubmatch(string(raw), -1) {
				rhs := m[1]
				if rhs == "[]" {
					continue
				}
				if g := bakeGatedToRe.FindStringSubmatch(rhs); g != nil {
					v, set := envValue(g[1], asMap(s["env"]), asMap(job["env"]), asMap(doc["env"]))
					if !set {
						continue
					}
					return fmt.Sprintf("bake file %s writes cache-to when %s is \"1\"", f, g[1]), []any{v}
				}
				return fmt.Sprintf("bake file %s: cache-to = %s", f, rhs), nil
			}
		}
	}
	if strings.Contains(shellCode(asString(s["run"])), "--cache-to") {
		return "run text holds --cache-to", nil
	}
	return "", nil
}

func envValue(key string, scopes ...yamlMap) (any, bool) {
	for _, m := range scopes {
		if v, ok := m[key]; ok {
			return v, true
		}
	}
	return nil, false
}

// mayRunOn: an absent if runs; a YAML boolean is what it says; an expression runs unless it is falsy on the event.
func mayRunOn(cond any, event string) bool {
	switch c := cond.(type) {
	case nil:
		return true
	case bool:
		return c
	}
	expr := strings.TrimSpace(str(cond))
	if m := wholeExprRe.FindStringSubmatch(expr); m != nil {
		expr = m[1]
	}
	v, err := evalAbstract(expr, event)
	return err != nil || v.mayBeTruthy()
}

var wholeExprRe = regexp.MustCompile(`(?s)^\$\{\{(.*)\}\}$`)

// switchMayBeOn: an input switch is off when it is empty or false; a value made of literal text around an expression is on.
func switchMayBeOn(v any, event string) bool {
	switch c := v.(type) {
	case nil:
		return false
	case bool:
		return c
	}
	s := strings.TrimSpace(str(v))
	m := wholeExprRe.FindStringSubmatch(s)
	if m == nil || strings.Contains(m[1], "${{") {
		return s != "" && !strings.EqualFold(s, "false")
	}
	val, err := evalAbstract(m[1], event)
	if err != nil {
		return true
	}
	for _, a := range val {
		if a.k == kStr && strings.EqualFold(a.s, "false") {
			continue
		}
		if ct, _ := a.truth(); ct {
			return true
		}
	}
	return false
}

// The abstract evaluator: a value is the set of what an expression can be. github.event_name is known; every other context
// read, status function and unknown function is any value, so a result is off only when every possibility is falsy.

type atomKind int

const (
	kStr atomKind = iota
	kBool
	kNull
	kNum
	kArr
	kAny
	kAnyTruthy
	kAnyFalsy
)

type atom struct {
	k   atomKind
	s   string
	b   bool
	n   float64
	arr []string
}

type absVal []atom

func (a atom) truth() (canTrue, canFalse bool) {
	switch a.k {
	case kStr:
		return a.s != "", a.s == ""
	case kBool:
		return a.b, !a.b
	case kNull:
		return false, true
	case kNum:
		return a.n != 0, a.n == 0
	case kArr, kAnyTruthy:
		return true, false
	case kAnyFalsy:
		return false, true
	}
	return true, true
}

func (a atom) concrete() bool { return a.k == kStr || a.k == kBool || a.k == kNull || a.k == kNum }

func (v absVal) mayBeTruthy() bool {
	for _, a := range v {
		if ct, _ := a.truth(); ct {
			return true
		}
	}
	return false
}

func anyVal() absVal            { return absVal{{k: kAny}} }
func boolsVal(t, f bool) absVal { return absVal(nil).addBool(t, f) }

func (v absVal) addBool(t, f bool) absVal {
	if t {
		v = append(v, atom{k: kBool, b: true})
	}
	if f {
		v = append(v, atom{k: kBool, b: false})
	}
	return v
}

func (a atom) num() float64 {
	switch a.k {
	case kNum:
		return a.n
	case kBool:
		if a.b {
			return 1
		}
		return 0
	case kNull:
		return 0
	case kStr:
		if strings.TrimSpace(a.s) == "" {
			return 0
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(a.s), 64); err == nil {
			return f
		}
	}
	return math.NaN()
}

func atomsEqual(x, y atom) bool {
	if x.k == y.k {
		switch x.k {
		case kStr:
			return strings.EqualFold(x.s, y.s)
		case kBool:
			return x.b == y.b
		case kNull:
			return true
		}
	}
	return x.num() == y.num()
}

type absParser struct {
	toks  []string
	pos   int
	event string
}

var absTokenRe = regexp.MustCompile(`^(?:\s+|'(?:[^']|'')*'|&&|\|\||==|!=|<=|>=|[()!<>,]|-?[0-9]+(?:\.[0-9]+)?\b|[A-Za-z_][A-Za-z0-9_\-]*(?:(?:\.[A-Za-z0-9_\-*]+)|(?:\[[^\]]*\]))*|\.[A-Za-z0-9_\-*]+|\[[^\]]*\])`)

func evalAbstract(expr, event string) (absVal, error) {
	var toks []string
	for rest := expr; rest != ""; {
		m := absTokenRe.FindString(rest)
		if m == "" {
			return nil, fmt.Errorf("cannot read %q at %q", expr, rest)
		}
		if strings.TrimSpace(m) != "" {
			toks = append(toks, m)
		}
		rest = rest[len(m):]
	}
	p := &absParser{toks: toks, event: event}
	v, err := p.or()
	if err == nil && p.pos != len(toks) {
		err = fmt.Errorf("trailing tokens in %q: %v", expr, toks[p.pos:])
	}
	return v, err
}

func (p *absParser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return ""
}

func (p *absParser) or() (absVal, error) {
	l, err := p.and()
	for err == nil && p.peek() == "||" {
		p.pos++
		var r absVal
		if r, err = p.and(); err != nil {
			break
		}
		var out absVal
		for _, a := range l {
			ct, cf := a.truth()
			if ct {
				if a.k == kAny {
					out = append(out, atom{k: kAnyTruthy})
				} else {
					out = append(out, a)
				}
			}
			if cf {
				out = append(out, r...)
			}
		}
		l = out
	}
	return l, err
}

func (p *absParser) and() (absVal, error) {
	l, err := p.equality()
	for err == nil && p.peek() == "&&" {
		p.pos++
		var r absVal
		if r, err = p.equality(); err != nil {
			break
		}
		var out absVal
		for _, a := range l {
			ct, cf := a.truth()
			if cf {
				if a.k == kAny {
					out = append(out, atom{k: kAnyFalsy})
				} else {
					out = append(out, a)
				}
			}
			if ct {
				out = append(out, r...)
			}
		}
		l = out
	}
	return l, err
}

func (p *absParser) equality() (absVal, error) {
	l, err := p.relational()
	for err == nil && (p.peek() == "==" || p.peek() == "!=") {
		op := p.toks[p.pos]
		p.pos++
		var r absVal
		if r, err = p.relational(); err != nil {
			break
		}
		var t, f bool
		for _, a := range l {
			for _, b := range r {
				if !a.concrete() || !b.concrete() {
					t, f = true, true
					continue
				}
				eq := atomsEqual(a, b) == (op == "==")
				t, f = t || eq, f || !eq
			}
		}
		l = boolsVal(t, f)
	}
	return l, err
}

func (p *absParser) relational() (absVal, error) {
	l, err := p.unary()
	for err == nil && (p.peek() == "<" || p.peek() == ">" || p.peek() == "<=" || p.peek() == ">=") {
		p.pos++
		if _, err = p.unary(); err != nil {
			break
		}
		l = boolsVal(true, true)
	}
	return l, err
}

func (p *absParser) unary() (absVal, error) {
	if p.peek() == "!" {
		p.pos++
		v, err := p.unary()
		var t, f bool
		for _, a := range v {
			ct, cf := a.truth()
			t, f = t || cf, f || ct
		}
		return boolsVal(t, f), err
	}
	return p.primary()
}

func (p *absParser) primary() (absVal, error) {
	tok := p.peek()
	if tok == "" {
		return nil, fmt.Errorf("expression ended where a value was expected")
	}
	p.pos++
	var v absVal
	switch {
	case tok == "(":
		var err error
		if v, err = p.or(); err != nil {
			return nil, err
		}
		if p.peek() != ")" {
			return nil, fmt.Errorf("missing )")
		}
		p.pos++
	case strings.HasPrefix(tok, "'"):
		v = absVal{{k: kStr, s: strings.ReplaceAll(tok[1:len(tok)-1], "''", "'")}}
	case tok == "true" || tok == "false":
		v = absVal{{k: kBool, b: tok == "true"}}
	case tok == "null":
		v = absVal{{k: kNull}}
	case tok[0] == '-' || (tok[0] >= '0' && tok[0] <= '9'):
		n, _ := strconv.ParseFloat(tok, 64)
		v = absVal{{k: kNum, n: n}}
	case p.peek() == "(":
		p.pos++
		var args []absVal
		for p.peek() != ")" {
			a, err := p.or()
			if err != nil {
				return nil, err
			}
			args = append(args, a)
			if p.peek() == "," {
				p.pos++
			} else if p.peek() != ")" {
				return nil, fmt.Errorf("bad argument list of %s", tok)
			}
		}
		p.pos++
		v = callAbstract(strings.ToLower(tok), args)
	case strings.ContainsAny(tok[:1], "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"):
		if strings.EqualFold(tok, "github.event_name") {
			v = absVal{{k: kStr, s: p.event}}
		} else {
			v = anyVal()
		}
	default:
		return nil, fmt.Errorf("unexpected %q", tok)
	}
	for strings.HasPrefix(p.peek(), ".") || strings.HasPrefix(p.peek(), "[") {
		p.pos++
		v = anyVal()
	}
	return v, nil
}

func callAbstract(fn string, args []absVal) absVal {
	switch {
	case fn == "fromjson" && len(args) == 1:
		var out absVal
		for _, a := range args[0] {
			if a.k != kStr {
				return anyVal()
			}
			var x any
			if json.Unmarshal([]byte(a.s), &x) != nil {
				return anyVal()
			}
			switch y := x.(type) {
			case string:
				out = append(out, atom{k: kStr, s: y})
			case bool:
				out = append(out, atom{k: kBool, b: y})
			case nil:
				out = append(out, atom{k: kNull})
			case float64:
				out = append(out, atom{k: kNum, n: y})
			case []any:
				arr := atom{k: kArr}
				for _, e := range y {
					s, ok := e.(string)
					if !ok {
						return anyVal()
					}
					arr.arr = append(arr.arr, s)
				}
				out = append(out, arr)
			default:
				return anyVal()
			}
		}
		return out
	case (fn == "contains" || fn == "startswith" || fn == "endswith") && len(args) == 2:
		var t, f bool
		for _, h := range args[0] {
			for _, n := range args[1] {
				if n.k != kStr || (h.k != kStr && !(fn == "contains" && h.k == kArr)) {
					t, f = true, true
					continue
				}
				var r bool
				hs, ns := strings.ToLower(h.s), strings.ToLower(n.s)
				switch {
				case h.k == kArr:
					for _, e := range h.arr {
						r = r || strings.EqualFold(e, n.s)
					}
				case fn == "contains":
					r = strings.Contains(hs, ns)
				case fn == "startswith":
					r = strings.HasPrefix(hs, ns)
				default:
					r = strings.HasSuffix(hs, ns)
				}
				t, f = t || r, f || !r
			}
		}
		return boolsVal(t, f)
	}
	return anyVal()
}

func TestPRCacheExpressionEvaluator(t *testing.T) {
	cases := []struct {
		expr string
		run  bool
	}{
		{"github.event_name != 'pull_request'", false},
		{"github.event_name == 'push' && github.ref == 'refs/heads/main'", false},
		{"github.event_name == 'push' || github.event_name == 'merge_group'", false},
		{"!(github.event_name == 'pull_request')", false},
		{"!(github.event_name == 'pull_request' || (vars.X == 'enabled' && matrix.y))", false},
		{"contains(fromJSON('[\"push\",\"merge_group\"]'), github.event_name)", false},
		{"GITHUB.EVENT_NAME == 'PUSH'", false},
		{"success() && github.event_name != 'pull_request'", false},
		{"github.event_name != 'pull_request' || always()", true},
		{"github.event.inputs.ref == ''", true},
		{"!(vars.SELF_HOSTED_RUNNERS == 'enabled')", true},
		{"contains(fromJSON('[\"push\",\"pull_request\"]'), github.event_name)", true},
		{"(github.event_name == 'pull_request' && github.event.pull_request.draft) == false", true},
		{"steps.x.outputs.y != '0'", true},
	}
	for _, c := range cases {
		if got := mayRunOn(c.expr, "pull_request"); got != c.run {
			t.Errorf("mayRunOn(%q) = %v, want %v", c.expr, got, c.run)
		}
	}
	switches := []struct {
		v  any
		on bool
	}{
		{false, false},
		{true, true},
		{"", false},
		{"type=gha,mode=max", true},
		{"${{ github.event_name != 'pull_request' }}", false},
		{"${{ github.event_name != 'pull_request' && 'pnpm' || '' }}", false},
		{"${{ github.event_name != 'pull_request' && 'pnpm' || 'npm' }}", true},
		{"${{ github.event.inputs.ref == '' && 'type=gha,mode=max' || '' }}", true},
		{"${{ github.event.inputs.ref == '' && github.event_name != 'pull_request' && 'type=gha,mode=max' || '' }}", false},
		{"${{ !(github.event_name == 'pull_request' || (vars.A == 'enabled')) }}", false},
		{"${{ !(vars.A == 'enabled') }}", true},
		{"${{ github.event_name == 'pull_request' && 'false' || 'true' }}", false},
		{"type=gha,scope=${{ github.event_name }}", true},
		{"${{ this is not an expression ]] }}", true},
	}
	for _, c := range switches {
		if got := switchMayBeOn(c.v, "pull_request"); got != c.on {
			t.Errorf("switchMayBeOn(%v) = %v, want %v", c.v, got, c.on)
		}
	}
}

func TestPRCacheWriteGuardPlantedDefectsAreCaught(t *testing.T) {
	root := authProofRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	files = append(files, "ci/auth_corpus.bake.hcl")
	const (
		dock = ".github/workflows/docker-images.yml"
		gq   = ".github/workflows/go-quality.yml"
		goY  = ".github/workflows/go.yml"
		gas  = ".github/workflows/go-auth-service.yml"
	)
	cases := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a buildkit cache-to without the event condition", []mutation{{dock, "cache-to: ${{ github.event.inputs.ref == '' && github.event_name != 'pull_request' && github.event_name != 'release' && 'type=gha,mode=max' || '' }}", "cache-to: ${{ github.event.inputs.ref == '' && 'type=gha,mode=max' || '' }}"}}, []string{"docker-images.yml: job go-build", "cache-to:"}},
		{"a literal cache-to", []mutation{{".github/workflows/arc-runner-image.yml", "cache-to: ${{ github.event_name != 'pull_request' && 'type=gha,mode=max' || '' }}", "cache-to: type=gha,mode=max"}}, []string{"arc-runner-image.yml: job build", "cache-to: type=gha,mode=max"}},
		{"an actions/cache/save without the event condition", []mutation{{gq, "          && github.event_name == 'push' && github.ref == 'refs/heads/main' && steps.go-cache.outputs.cache-hit != 'true'", "          && steps.go-cache.outputs.cache-hit != 'true'"}}, []string{"go-quality.yml: job go-quality-leg", "actions/cache/save"}},
		{"a combined actions/cache", []mutation{{gq, "        id: go-cache\n        if: (steps.relevance.outputs.relevant == 'true' || steps.relevance.outputs.warm == 'true') && runner.environment == 'github-hosted'\n        uses: actions/cache/restore@", "        id: go-cache\n        if: (steps.relevance.outputs.relevant == 'true' || steps.relevance.outputs.warm == 'true') && runner.environment == 'github-hosted'\n        uses: actions/cache@"}}, []string{"go-quality.yml: job go-quality-leg", ": actions/cache"}},
		{"setup-go back on its default cache", []mutation{{".github/workflows/docs-drift.yml", "          cache: false\n", ""}}, []string{"docs-drift.yml: job docs-drift", "cache defaults on"}},
		{"setup-go cache as an expression that is off on a pull request", []mutation{{".github/workflows/test.yml", "          cache: false\n", "          cache: ${{ github.event_name != 'pull_request' }}\n"}}, []string{"test.yml: job chart-started", "setup-go cache: ${{"}},
		{"setup-node writes the pnpm cache", []mutation{{goY, "          node-version: 22\n      - name: Locate the pnpm store", "          node-version: 22\n          cache: pnpm\n      - name: Locate the pnpm store"}}, []string{"go.yml: job go-auth-web-proxy-proof", "actions/setup-node cache: pnpm"}},
		{"setup-node pnpm cache as an expression", []mutation{{goY, "          node-version: 22\n      - name: Locate the pnpm store", "          node-version: 22\n          cache: ${{ github.event_name != 'pull_request' && 'pnpm' || '' }}\n      - name: Locate the pnpm store"}}, []string{"go.yml: job go-auth-web-proxy-proof", "actions/setup-node cache: ${{"}},
		{"the Go cache save loses its event condition", []mutation{{".github/workflows/docs-inventory-review.yml", "        if: (github.event_name == 'merge_group' || (github.event_name == 'push' && github.ref == 'refs/heads/main')) && steps.go-cache.outcome", "        if: steps.go-cache.outcome"}}, []string{"docs-inventory-review.yml: job inventory-archive", "actions/cache/save"}},
		{"the pnpm save keeps only the merge-queue half of its condition", []mutation{{goY, "        if: (github.event_name == 'merge_group' || (github.event_name == 'push' && github.ref == 'refs/heads/main')) && steps.pnpm-cache.outcome", "        if: (github.event_name == 'merge_group' || github.ref == 'refs/heads/main') && steps.pnpm-cache.outcome"}}, []string{"go.yml: job go-auth-web-proxy-proof", "actions/cache/save"}},
		{"the bake write switch on a pull request job", []mutation{{gas, "          AUTH_CORPUS_STUB_DIR: ${{ runner.temp }}/corpus-stub", "          AUTH_CORPUS_CACHE_WRITE: \"1\"\n          AUTH_CORPUS_STUB_DIR: ${{ runner.temp }}/corpus-stub"}}, []string{"go-auth-service.yml: job auth-corpus-queue", "AUTH_CORPUS_CACHE_WRITE"}},
		{"the seeding job loses its main-push if", []mutation{{gas, "  auth-corpus-image-cache:\n    if: github.event_name == 'push' && github.ref == 'refs/heads/main' && ", "  auth-corpus-image-cache:\n    if: "}}, []string{"go-auth-service.yml: job auth-corpus-image-cache"}},
		{"a run step exports a buildx cache", []mutation{{goY, "      - name: Install web dependencies\n        working-directory: dev-health-web\n", "      - name: Planted\n        run: docker buildx build --cache-to type=gha,mode=max .\n      - name: Install web dependencies\n        working-directory: dev-health-web\n"}}, []string{"go.yml: job go-auth-web-proxy-proof", "--cache-to"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tree := plantTree(t, root, files, c.muts...)
			expectViolation(t, prCacheWriteViolations(t, tree), c.want...)
		})
	}
}

func TestPRCacheWriteGuardFailsClosed(t *testing.T) {
	cases := []struct {
		name, yml, want string
	}{
		{"a local action", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/cache/save@x\n        if: github.event_name == 'push'\n      - uses: ./.github/actions/build\n", "local action"},
		{"a reusable workflow", "on: pull_request\njobs:\n  a:\n    uses: ./.github/workflows/other.yml\n  b:\n    runs-on: x\n    steps:\n      - uses: actions/cache/save@x\n        if: github.event_name == 'push'\n", "reusable workflow"},
		{"an expression it cannot read", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/cache/save@x\n        if: github.event_name == 'push' ]]\n", "actions/cache/save"},
		{"pull_request_target", "on: pull_request_target\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/setup-go@x\n", "pull_request_target"},
		{"nothing to check", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: true\n", "vacuously"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, ".github", "workflows")
			if err := os.MkdirAll(dir, 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "plant.yml"), []byte(c.yml), 0o644); err != nil {
				t.Fatal(err)
			}
			expectViolation(t, prCacheWriteViolations(t, root), c.want)
		})
	}
}
