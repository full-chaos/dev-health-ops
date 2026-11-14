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
// An action that caches by itself (setup-go, setup-node, setup-uv, trivy-action, setup-qemu, setup-buildx, ...) saves on a key
// miss and has no restore-only switch, so on a step a pull request can run EVERY cache input of it must be set LITERALLY off
// (no expression but the one main-only gate, mainOnlyGate; an absent input fails even when it defaults off); such a job reads
// the cache through an
// actions/cache/restore step and saves it with an actions/cache/save step only a push to main or the merge queue runs.
// The guard fails closed: every action of a pull_request workflow must be pinned to a full 40-hex sha with a `# vX.Y.Z`
// comment on its uses: line, and owner/repo[/path] at that major must be in prCacheActions; a local action or a reusable
// workflow fails, and so does a bake file it cannot read.

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
		raw := usesComments(readFile(t, filepath.Join(root, ".github", "workflows", name)))
		ev := triggerEvents(doc)
		for _, event := range prCacheEvents {
			if _, ok := ev[event]; !ok {
				continue
			}
			jobs := jobsOf(doc)
			for _, jn := range sortedKeys(jobs) {
				job := asMap(jobs[jn])
				jobRuns := mayRunOn(job["if"], event)
				if uses := asString(job["uses"]); uses != "" && jobRuns {
					out = append(out, fmt.Sprintf("%s: job %s calls the reusable workflow %s on %s, which this guard does not follow", name, jn, uses, event))
				}
				for i, s := range stepsOf(job) {
					label := fmt.Sprintf("#%d %s", i+1, firstNonEmpty(asString(s["name"]), asString(s["id"]), asString(s["uses"])))
					at := fmt.Sprintf("%s: job %s step %s", name, jn, label)
					uses := asString(s["uses"])
					// Fail closed on every step of a pull_request workflow, reachable or not: an action not read at its pinned ref
					// may write an Actions cache through an input default or a post step.
					var key string
					if uses != "" {
						if strings.HasPrefix(uses, "./") {
							out = append(out, fmt.Sprintf("%s runs the local action %s, which this guard does not follow", at, uses))
							continue
						}
						k, problem := actionKey(uses, raw[uses])
						if problem != "" {
							out = append(out, fmt.Sprintf("%s uses %s, %s", at, uses, problem))
							continue
						}
						if _, known := prCacheActions[k]; !known {
							out = append(out, fmt.Sprintf("%s uses %s (%s), an action at a major this guard has not read: read its cache inputs, defaults and post step at that sha and add it to prCacheActions", at, uses, k))
							continue
						}
						key = k
					}
					why, switches, broken := cacheWrite(root, doc, job, s, key)
					if broken != "" {
						out = append(out, at+": "+broken)
						continue
					}
					if why == "" {
						continue
					}
					checked++
					if !jobRuns || !mayRunOn(s["if"], event) {
						continue
					}
					on := len(switches) == 0
					for _, sw := range switches {
						if lit, ok := sw.(literalSwitch); ok {
							on = on || !lit.off(event)
						} else if switchMayBeOn(sw, event) {
							on = true
						}
					}
					if on {
						out = append(out, fmt.Sprintf("%s can write an Actions cache on a %s event: %s", at, event, why))
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

type actionKind int

const (
	nonWriter     actionKind = iota // no Actions cache read or write at this ref (artifacts and the runner's local tool cache are not one)
	alwaysWriter                    // writes whenever it runs: only an if keeps it off a pull request
	inputWriter                     // writes unless each listed input is literally off
	cacheToWriter                   // a buildx build: the cache-to value is the switch
	bakeWriter                      // a bake: set lines and the bake files' cache-to are the switch
)

// cacheInput is an input that turns an action's own Actions cache on. On a step a pull request can run it must be set
// literally off: false, "false", "" or a value in off, or be exactly ${{ mainOnlyGate }}. defaultOn only words the message
// of an absent input.
type cacheInput struct {
	name      string
	defaultOn bool
	off       []string
}

type actionPolicy struct {
	kind   actionKind
	inputs []cacheInput
}

// prCacheActions is every action a pull_request workflow may use, keyed on owner/repo[/path]@v<major>, the major read from
// the `# vX.Y.Z` comment of its uses: line (for v0 the major is 0.Y: a 0.x minor is breaking). Each entry was read at the sha
// its comment names (action.yml inputs, defaults and post step; the source where the caching is conditional), so a sha bump
// inside a major keeps its entry. A bot PR that bumps one is read with this one command (OWNER/REPO[/PATH], OLD and NEW from
// the diff; action.yaml in place of action.yml for aquasecurity/*):
//
//	diff <(curl -fsSL https://raw.githubusercontent.com/OWNER/REPO/OLD/PATH/action.yml) <(curl -fsSL https://raw.githubusercontent.com/OWNER/REPO/NEW/PATH/action.yml)
//
// Residual: a minor or patch bump that ADDS a new default-on cache input passes this guard unseen; that diff is the check.
var prCacheActions = map[string]actionPolicy{
	// read at 3d3c42e5aac5ba805825da76410c181273ba90b1 (v7.0.1): cache inputs = []
	"actions/checkout@v7": {kind: nonWriter},
	// read at 9000827ccba6bdab643e8b6fd33ac0654aef8333 (v8): cache inputs = []
	"actions/download-artifact@v8": {kind: nonWriter},
	// read at cf430e030ddbb5b0abf93d22962f4752f3646cd9 (v7): cache inputs = []
	"actions/upload-artifact@v7": {kind: nonWriter},
	// read at 4d101475d8b20a2381f78447822ac1eab6504dd8 (v4.2.2): cache inputs = []
	"actions/attest-build-provenance@v4": {kind: nonWriter},
	// read at 55cc8345863c7cc4c66a329aec7e433d2d1c52a9 (v6.1.0): cache inputs = [] (restores only)
	"actions/cache/restore@v6": {kind: nonWriter},
	// read at 55cc8345863c7cc4c66a329aec7e433d2d1c52a9 (v6.1.0): cache inputs = [] (its post step always saves)
	"actions/cache@v6": {kind: alwaysWriter},
	// read at 55cc8345863c7cc4c66a329aec7e433d2d1c52a9 (v6.1.0): cache inputs = [] (always saves)
	"actions/cache/save@v6": {kind: alwaysWriter},
	// read at b7ad1dad31e06c5925ef5d2fc7ad053ef454303e (v7.0.0): cache inputs = [cache (default true)]
	"actions/setup-go@v7": {kind: inputWriter, inputs: []cacheInput{{name: "cache", defaultOn: true}}},
	// read at 949feb2413d6458794dcd2491c4babbbce0c15c1 (v7.1.0): cache inputs = [cache (no default), package-manager-cache (default true)]
	"actions/setup-node@v7": {kind: inputWriter, inputs: []cacheInput{{name: "cache"}, {name: "package-manager-cache", defaultOn: true}}},
	// read at 6d4fd22b20465ccc0f7354476588e8df7c9ca0e0 (v7.1.0): cache inputs = [cache (no default)]
	"actions/setup-python@v7": {kind: inputWriter, inputs: []cacheInput{{name: "cache"}}},
	// read at 68559e99cc807c1eb7e9238fd43a15bba460eec2 (v10.3.3): cache inputs = [enable-cache (default auto)]; restore-cache
	// and save-cache act only when enable-cache is on
	"astral-sh/setup-uv@v10": {kind: inputWriter, inputs: []cacheInput{{name: "enable-cache", defaultOn: true}}},
	// read at 73e46992e05b19abd31f2f3c20e0e7370d762115 (v6.1.1): cache inputs = [cache (default false)]
	"pnpm/action-setup@v6": {kind: inputWriter, inputs: []cacheInput{{name: "cache"}}},
	// read at ed142fd0673e97e23eac54620cfb913e5ce36c25 (v0.36.0): cache inputs = [cache (default true)]; it drives both the
	// actions/cache of the DB and the cache of the setup-trivy the action runs when skip-setup-trivy is false
	"aquasecurity/trivy-action@v0.36": {kind: inputWriter, inputs: []cacheInput{{name: "cache", defaultOn: true}}},
	// read at 3fb12ec12f41e471780db15c232d5dd185dcb514 (v0.2.6): cache inputs = [cache (default false)]
	"aquasecurity/setup-trivy@v0.2": {kind: inputWriter, inputs: []cacheInput{{name: "cache"}}},
	// read at 99012661954931238ded8c8b007157a8430204e1 (v4.4.0): cache inputs = [cache-image (default true)]
	"docker/setup-qemu-action@v4": {kind: inputWriter, inputs: []cacheInput{{name: "cache-image", defaultOn: true}}},
	// read at 594f3bf4285d9ea8dc53c9a0c9c4092420091003 (v4.4.0): cache inputs = [cache-binary (default true)]
	"docker/setup-buildx-action@v4": {kind: inputWriter, inputs: []cacheInput{{name: "cache-binary", defaultOn: true}}},
	// read at c3c9e263c25d99ce0380d002d59b67737d91b0dc (v7.4.0): cache inputs = [cache-to (no default)]
	"docker/build-push-action@v7": {kind: cacheToWriter},
	// read at 59cb1203cd42bcefd40a1b6d73b589e48c86040e (v7.5.0): cache inputs = [set *.cache-to, the bake files' cache-to]
	"docker/bake-action@v7": {kind: bakeWriter},
	// read at 8aa5831fb8d48d8f76594c7ddde61ae58019f550 (v4.8.0): cache inputs = []
	"docker/login-action@v4": {kind: nonWriter},
	// read at dc802804100637a589fabce1cb79ff13a1411302 (v6.2.0): cache inputs = []
	"docker/metadata-action@v6": {kind: nonWriter},
	// read at ceb8a2b8f2d89434be7ff52d3de7ec3738c5cc9d (v4.0.3): cache inputs = []
	"dorny/paths-filter@v4": {kind: nonWriter},
	// read at 8a0cd651e5b2365bd640854a3fa5dfc70e4875f3 (v3.0.4): cache inputs = []
	"softprops/action-gh-release@v3": {kind: nonWriter},
	// read at 9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310 (v5.0.1): cache inputs = [] (the runner's local tool cache only)
	"azure/setup-helm@v5": {kind: nonWriter},
	// read at 66cbf4bc1f1c0d2edc94016e65bc221b6bb0ad6c (v0.24.3): cache inputs = [] (the runner's local tool cache only)
	"anchore/sbom-action@v0.24": {kind: nonWriter},
	// read at 66cbf4bc1f1c0d2edc94016e65bc221b6bb0ad6c (v0.24.3): cache inputs = [] (the runner's local tool cache only)
	"anchore/sbom-action/download-syft@v0.24": {kind: nonWriter},
	// read at 33d7b85da1a04171e211befe2c739303b9a4c066 (v4.39.3): cache inputs = [dependency-caching (default none),
	// trap-caching (no default)]. TRAP caches are uploaded only when the default branch is analysed
	// (src/trap-caching.ts uploadTrapCaches), the overlay-base database only in overlay-base mode, which only the default
	// branch gets (src/config-utils.ts), and dependency caches only when dependency-caching is on (src/caching-utils.ts).
	"github/codeql-action/init@v4": {kind: inputWriter, inputs: []cacheInput{
		{name: "dependency-caching", off: []string{"none", "off", "restore"}}, {name: "trap-caching"}}},
	// read at 33d7b85da1a04171e211befe2c739303b9a4c066 (v4.39.3): cache inputs = []
	"github/codeql-action/autobuild@v4": {kind: nonWriter},
	// read at 33d7b85da1a04171e211befe2c739303b9a4c066 (v4.39.3): cache inputs = []
	"github/codeql-action/analyze@v4": {kind: nonWriter},
	// read at 33d7b85da1a04171e211befe2c739303b9a4c066 (v4.39.3): cache inputs = []
	"github/codeql-action/upload-sarif@v4": {kind: nonWriter},
}

var (
	usesLineRe   = regexp.MustCompile(`^\s*(?:-\s+)?uses:\s*(?:"([^"]*)"|'([^']*)'|([^\s#"']+))\s*(?:#\s*(.*?))?\s*$`)
	pinnedUsesRe = regexp.MustCompile(`^([^@\s]+)@[0-9a-f]{40}$`)
	versionRe    = regexp.MustCompile(`^v(0|[1-9][0-9]*)(?:\.(0|[1-9][0-9]*)(?:\.(0|[1-9][0-9]*))?)?$`)
)

// usesComments maps each uses: value of a workflow's raw text to the comments of its lines: the YAML loader drops comments.
func usesComments(text string) map[string][]string {
	out := map[string][]string{}
	for _, line := range strings.Split(text, "\n") {
		m := usesLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		v := m[1] + m[2] + m[3]
		out[v] = append(out[v], strings.TrimSpace(m[4]))
	}
	return out
}

// actionKey keys a pinned action on owner/repo[/path] and the major of the version comments of its uses: lines, which must
// all name one major. problem says what keeps it from being keyed.
func actionKey(uses string, comments []string) (key, problem string) {
	m := pinnedUsesRe.FindStringSubmatch(uses)
	if m == nil {
		return "", "which is not pinned to a full 40-hex commit sha"
	}
	if len(comments) == 0 {
		return "", "whose uses: line the guard cannot find in the raw workflow text"
	}
	for _, c := range comments {
		if c == "" {
			return "", "whose uses: line has no `# vX.Y.Z` version comment"
		}
		v := versionRe.FindStringSubmatch(c)
		if v == nil {
			return "", fmt.Sprintf("whose version comment %q is not vX, vX.Y or vX.Y.Z", c)
		}
		major := v[1]
		if major == "0" {
			if v[2] == "" {
				return "", fmt.Sprintf("whose version comment %q has no minor: the major of a v0 action is 0.Y", c)
			}
			major += "." + v[2]
		}
		k := m[1] + "@v" + major
		if key != "" && k != key {
			return "", fmt.Sprintf("whose uses: lines name two majors (%s, %s)", key, k)
		}
		key = k
	}
	return key, ""
}

// literalSwitch is a cache input of an action: only a literal off value (no expression) switches it off.
type literalSwitch struct {
	v   any
	set bool
	in  cacheInput
}

// mainOnlyGate is the one expression a cache input may hold: the push-to-main / merge-queue gate of the actions/cache/save
// steps, so the input is on where the saves run and off on every other event.
const mainOnlyGate = "github.event_name == 'merge_group' || (github.event_name == 'push' && github.ref == 'refs/heads/main')"

// mainOnlyGatedValue holds when v is exactly ${{ mainOnlyGate }} (blanks folded, one pair of outer parentheses allowed).
func mainOnlyGatedValue(v string) bool {
	m := wholeExprRe.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return false
	}
	e := strings.Join(strings.Fields(m[1]), " ")
	if inner, ok := strings.CutPrefix(e, "("); ok {
		if inner, ok = strings.CutSuffix(inner, ")"); ok {
			e = strings.TrimSpace(inner)
		}
	}
	return e == mainOnlyGate
}

func (l literalSwitch) off(event string) bool {
	if s, ok := l.v.(string); ok && mainOnlyGatedValue(s) {
		return !switchMayBeOn(s, event)
	}
	switch c := l.v.(type) {
	case nil:
		return false
	case bool:
		return !c
	}
	s := strings.TrimSpace(str(l.v))
	if s == "" || strings.EqualFold(s, "false") {
		return true
	}
	for _, o := range l.in.off {
		if strings.EqualFold(s, o) {
			return true
		}
	}
	return false
}

var bakeCacheToRe = regexp.MustCompile(`(?m)^\s*cache[-_]to\s*[=:]\s*(.+?)\s*$`)
var bakeGatedToRe = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*) == "1" \? \[.*\] : \[\]$`)

// cacheWrite says whether a step can write an Actions cache, and which values switch the write (all must be off to switch
// it off; no switch means only an if keeps it from writing). broken names a step the guard cannot judge.
func cacheWrite(root string, doc, job, s yamlMap, key string) (why string, switches []any, broken string) {
	uses := asString(s["uses"])
	with := asMap(s["with"])
	if uses == "" {
		if strings.Contains(shellCode(asString(s["run"])), "--cache-to") {
			return "run text holds --cache-to", nil, ""
		}
		return "", nil, ""
	}
	action, _, _ := strings.Cut(uses, "@")
	p := prCacheActions[key]
	switch p.kind {
	case alwaysWriter:
		return action, nil, ""
	case inputWriter:
		var parts []string
		for _, in := range p.inputs {
			v, set := with[in.name]
			sw := literalSwitch{v: v, set: set, in: in}
			switches = append(switches, sw)
			if set {
				parts = append(parts, in.name+": "+str(v))
			} else if in.defaultOn {
				parts = append(parts, in.name+" defaults on")
			} else {
				parts = append(parts, in.name+" unset")
			}
		}
		return action + " (" + strings.Join(parts, ", ") + ")", switches, ""
	case cacheToWriter:
		if v, ok := with["cache-to"]; ok {
			return "cache-to: " + str(v), []any{v}, ""
		}
	case bakeWriter:
		for _, line := range strings.Split(str(with["set"]), "\n") {
			if _, val, ok := strings.Cut(strings.TrimSpace(line), ".cache-to="); ok {
				return "bake set " + strings.TrimSpace(line), []any{val}, ""
			}
		}
		files := strings.Fields(str(with["files"]))
		if len(files) == 0 {
			return "", nil, "a bake step without files: the guard cannot read its bake definition"
		}
		for _, f := range files {
			raw, err := os.ReadFile(filepath.Join(root, f))
			if err != nil {
				return "", nil, fmt.Sprintf("the bake file %s cannot be read (%v)", f, err)
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
					return fmt.Sprintf("bake file %s writes cache-to when %s is \"1\"", f, g[1]), []any{v}, ""
				}
				return fmt.Sprintf("bake file %s: cache-to = %s", f, rhs), nil, ""
			}
		}
	}
	return "", nil, ""
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
	files = append(files, "ci/auth_corpus.bake.hcl", "compose.yml", "ci/auth_corpus_external.override.yml")
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
		{"setup-go cache as an expression that is off on a pull request", []mutation{{".github/workflows/test.yml", "          cache: false\n", "          cache: ${{ github.event_name != 'pull_request' }}\n"}}, []string{"test.yml: job chart-started", "actions/setup-go (cache: ${{"}},
		{"setup-node writes the pnpm cache", []mutation{{goY, "          cache: ''\n          package-manager-cache: false\n          node-version: 22\n      - name: Locate the pnpm store", "          cache: pnpm\n          package-manager-cache: false\n          node-version: 22\n      - name: Locate the pnpm store"}}, []string{"go.yml: job go-auth-web-proxy-proof", "actions/setup-node (cache: pnpm"}},
		{"setup-node pnpm cache as an expression", []mutation{{goY, "          cache: ''\n          package-manager-cache: false\n          node-version: 22\n      - name: Locate the pnpm store", "          cache: ${{ github.event_name != 'pull_request' && 'pnpm' || '' }}\n          package-manager-cache: false\n          node-version: 22\n      - name: Locate the pnpm store"}}, []string{"go.yml: job go-auth-web-proxy-proof", "actions/setup-node (cache: ${{"}},
		{"the Go cache save loses its event condition", []mutation{{".github/workflows/docs-inventory-review.yml", "        if: (github.event_name == 'merge_group' || (github.event_name == 'push' && github.ref == 'refs/heads/main')) && steps.go-cache.outcome", "        if: steps.go-cache.outcome"}}, []string{"docs-inventory-review.yml: job inventory-archive", "actions/cache/save"}},
		{"the pnpm save keeps only the merge-queue half of its condition", []mutation{{goY, "        if: (github.event_name == 'merge_group' || (github.event_name == 'push' && github.ref == 'refs/heads/main')) && steps.pnpm-cache.outcome", "        if: (github.event_name == 'merge_group' || github.ref == 'refs/heads/main') && steps.pnpm-cache.outcome"}}, []string{"go.yml: job go-auth-web-proxy-proof", "actions/cache/save"}},
		{"the bake write switch on a pull request job", []mutation{{gas, "          AUTH_CORPUS_STUB_DIR: ${{ runner.temp }}/corpus-stub", "          AUTH_CORPUS_CACHE_WRITE: \"1\"\n          AUTH_CORPUS_STUB_DIR: ${{ runner.temp }}/corpus-stub"}}, []string{"go-auth-service.yml: job auth-corpus-queue", "AUTH_CORPUS_CACHE_WRITE"}},
		{"the seeding job loses its main-push if", []mutation{{gas, "  auth-corpus-image-cache:\n    if: github.event_name == 'push' && github.ref == 'refs/heads/main' && ", "  auth-corpus-image-cache:\n    if: "}}, []string{"go-auth-service.yml: job auth-corpus-image-cache"}},
		{"a run step exports a buildx cache", []mutation{{goY, "      - name: Install web dependencies\n        working-directory: dev-health-web\n", "      - name: Planted\n        run: docker buildx build --cache-to type=gha,mode=max .\n      - name: Install web dependencies\n        working-directory: dev-health-web\n"}}, []string{"go.yml: job go-auth-web-proxy-proof", "--cache-to"}},
		{"a bake file the guard cannot read", []mutation{{gas, "            ci/auth_corpus_external.override.yml\n            ci/auth_corpus.bake.hcl\n          targets: authcorpus\n          load: true\n      - name: Run the auth-flow corpus", "            ci/auth_corpus_external.override.yml\n            ci/missing.bake.hcl\n          targets: authcorpus\n          load: true\n      - name: Run the auth-flow corpus"}}, []string{"go-auth-service.yml: job auth-corpus-queue", "the bake file ci/missing.bake.hcl cannot be read"}},
		{"a bake step without files", []mutation{{gas, "          files: |\n            compose.yml\n            ci/auth_corpus_external.override.yml\n            ci/auth_corpus.bake.hcl\n          targets: authcorpus\n          load: true\n      - name: Run the auth-flow corpus", "          targets: authcorpus\n          load: true\n      - name: Run the auth-flow corpus"}}, []string{"go-auth-service.yml: job auth-corpus-queue", "a bake step without files"}},
		{"trivy-action back on its own cache", []mutation{{goY, "          cache: false\n          skip-setup-trivy: true\n", "          skip-setup-trivy: true\n"}}, []string{"go.yml: job go-container-security", "aquasecurity/trivy-action (cache defaults on)"}},
		{"setup-qemu back on its image cache", []mutation{{dock, "          cache-image: false\n", ""}}, []string{"docker-images.yml: job go-build", "docker/setup-qemu-action (cache-image defaults on)"}},
		{"setup-buildx back on its binary cache", []mutation{{".github/workflows/arc-runner-image.yml", "          cache-binary: false\n", ""}}, []string{"arc-runner-image.yml: job build", "docker/setup-buildx-action (cache-binary defaults on)"}},
		{"setup-node back on its package-manager cache", []mutation{{".github/workflows/auth-contracts-ajv.yml", "          package-manager-cache: false\n", ""}}, []string{"auth-contracts-ajv.yml", "package-manager-cache defaults on"}},
		{"codeql dependency caching on", []mutation{{".github/workflows/codeql-analysis.yml", "          dependency-caching: none\n", "          dependency-caching: true\n"}}, []string{"codeql-analysis.yml", "github/codeql-action/init (dependency-caching: true,"}},
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
		{"a local action", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0\n        if: github.event_name == 'push'\n      - uses: ./.github/actions/build\n", "local action"},
		{"a reusable workflow", "on: pull_request\njobs:\n  a:\n    uses: ./.github/workflows/other.yml\n  b:\n    runs-on: x\n    steps:\n      - uses: actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0\n        if: github.event_name == 'push'\n", "reusable workflow"},
		{"an expression it cannot read", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0\n        if: github.event_name == 'push' ]]\n", "actions/cache/save"},
		{"pull_request_target", "on: pull_request_target\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e # v7.0.0\n", "pull_request_target"},
		{"nothing to check", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    steps:\n      - run: true\n", "vacuously"},
		{"an unknown action on a step a pull request never runs", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    if: github.event_name == 'push'\n    steps:\n      - uses: actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0\n      - uses: someone/cache-everything@v1\n", "someone/cache-everything@v1, which is not pinned to a full 40-hex commit sha"},
		{"a docker step", "on: pull_request\njobs:\n  a:\n    runs-on: x\n    steps:\n      - uses: actions/cache/save@55cc8345863c7cc4c66a329aec7e433d2d1c52a9 # v6.1.0\n        if: github.event_name == 'push'\n      - uses: docker://alpine:3\n", "docker://alpine:3, which is not pinned to a full 40-hex commit sha"},
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

// TestPRCacheGuardKeysOnActionMajor: an action is keyed on owner/repo[/path] and the major of its version comment, so a
// bot PR that bumps a sha inside a major passes and a new major or a new action fails; a cache input must be set literally
// off even when it defaults off.
func TestPRCacheGuardKeysOnActionMajor(t *testing.T) {
	root := authProofRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	files = append(files, "ci/auth_corpus.bake.hcl", "compose.yml", "ci/auth_corpus_external.override.yml")
	const (
		drift    = ".github/workflows/docs-drift.yml"
		goY      = ".github/workflows/go.yml"
		sec      = ".github/workflows/security-scan.yml"
		checkout = "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1"
		helm     = "azure/setup-helm@9bc31f4ebc9c6b171d7bfbaa5d006ae7abdb4310 # v5.0.1"
		trivy    = "aquasecurity/setup-trivy@3fb12ec12f41e471780db15c232d5dd185dcb514 # v0.2.6"
	)
	passes := []struct {
		name string
		muts []mutation
	}{
		{"a minor bump", []mutation{
			{".github/workflows/test.yml", helm, "azure/setup-helm@2f5e7bc67f1f9972ab16657bb795f36d8f04de0e # v5.1.0"},
			{".github/workflows/go-quality.yml", helm, "azure/setup-helm@2f5e7bc67f1f9972ab16657bb795f36d8f04de0e # v5.1.0"}}},
		{"a v0 patch bump", []mutation{{goY, trivy, "aquasecurity/setup-trivy@0123456789abcdef0123456789abcdef01234567 # v0.2.7"}}},
	}
	for _, c := range passes {
		t.Run(c.name+" passes", func(t *testing.T) {
			expectNoViolations(t, prCacheWriteViolations(t, plantTree(t, root, files, c.muts...)))
		})
	}
	fails := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"a major bump", []mutation{{drift, checkout, "actions/checkout@0123456789abcdef0123456789abcdef01234567 # v8.0.0"}}, []string{"docs-drift.yml: job docs-drift", "(actions/checkout@v8), an action at a major this guard has not read"}},
		{"a v0 minor bump", []mutation{{goY, trivy, "aquasecurity/setup-trivy@81e514348e19b6112ce2a7e3ecbafe19c1e1f567 # v0.3.1"}}, []string{"go.yml: job go-container-security", "(aquasecurity/setup-trivy@v0.3), an action at a major this guard has not read"}},
		{"a new action", []mutation{{drift, checkout, "someone/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1"}}, []string{"docs-drift.yml: job docs-drift", "(someone/checkout@v7), an action at a major this guard has not read"}},
		{"a missing version comment", []mutation{{drift, checkout, "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1"}}, []string{"docs-drift.yml: job docs-drift", "has no `# vX.Y.Z` version comment"}},
		{"an unparseable version comment", []mutation{{drift, checkout, "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # version 7"}}, []string{"docs-drift.yml: job docs-drift", `version comment "version 7" is not vX, vX.Y or vX.Y.Z`}},
		{"a v0 comment without its minor", []mutation{{goY, trivy, "aquasecurity/setup-trivy@3fb12ec12f41e471780db15c232d5dd185dcb514 # v0"}}, []string{"go.yml: job go-container-security", "has no minor: the major of a v0 action is 0.Y"}},
		{"two majors on one pin", []mutation{{goY, checkout, "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v8.0.0"}}, []string{"go.yml: job", "name two majors (actions/checkout@v"}},
		{"a uses that is not a full sha", []mutation{{drift, checkout, "actions/checkout@v7 # v7.0.1"}}, []string{"docs-drift.yml: job docs-drift", "actions/checkout@v7, which is not pinned to a full 40-hex commit sha"}},
		{"a uses with a short sha", []mutation{{drift, checkout, "actions/checkout@3d3c42e # v7.0.1"}}, []string{"docs-drift.yml: job docs-drift", "actions/checkout@3d3c42e, which is not pinned to a full 40-hex commit sha"}},
		{"a default-off cache input unset", []mutation{{sec, "          cache: ''\n", ""}}, []string{"security-scan.yml: job semgrep", "actions/setup-python (cache unset)"}},
		{"a default-off cache input set by an expression", []mutation{{sec, "          cache: ''\n", "          cache: ${{ github.event_name != 'pull_request' && 'pip' || '' }}\n"}}, []string{"security-scan.yml: job semgrep", "actions/setup-python (cache: ${{"}},
		{"codeql trap caching unset", []mutation{{".github/workflows/codeql-analysis.yml", codeqlTrapGated, ""}}, []string{"codeql-analysis.yml", "trap-caching unset"}},
		{"pnpm cache unset", []mutation{{goY, "          cache: false\n          package_json_file: dev-health-web/package.json\n", "          package_json_file: dev-health-web/package.json\n"}}, []string{"go.yml: job go-auth-web-proxy-proof", "pnpm/action-setup (cache unset)"}},
	}
	for _, c := range fails {
		t.Run(c.name+" fails", func(t *testing.T) {
			expectViolation(t, prCacheWriteViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}

const codeqlTrapGated = "          trap-caching: ${{ " + mainOnlyGate + " }}\n"

// TestPRCacheGuardMainOnlyGate: besides a literal off value, a cache input may hold only the main-only gate of the cache saves.
func TestPRCacheGuardMainOnlyGate(t *testing.T) {
	root := authProofRoot(t)
	var files []string
	for _, n := range workflowNames(t, root) {
		files = append(files, ".github/workflows/"+n)
	}
	files = append(files, "ci/auth_corpus.bake.hcl", "compose.yml", "ci/auth_corpus_external.override.yml")
	const (
		cq    = ".github/workflows/codeql-analysis.yml"
		drift = ".github/workflows/docs-drift.yml"
	)
	t.Run("a cache input with the main-only gate passes", func(t *testing.T) {
		tree := plantTree(t, root, files, mutation{drift, "          cache: false\n", "          cache: ${{ (" + mainOnlyGate + ") }}\n"})
		expectNoViolations(t, prCacheWriteViolations(t, tree))
	})
	fails := []struct {
		name string
		muts []mutation
		want []string
	}{
		{"another expression that is off on a pull request", []mutation{{cq, codeqlTrapGated, "          trap-caching: ${{ github.event_name != 'pull_request' }}\n"}}, []string{"codeql-analysis.yml", "github/codeql-action/init (dependency-caching: none, trap-caching: ${{ github.event_name != 'pull_request' }})"}},
		{"the gate widened by a term", []mutation{{cq, codeqlTrapGated, "          trap-caching: ${{ " + mainOnlyGate + " || github.event_name == 'pull_request' }}\n"}}, []string{"codeql-analysis.yml", "github/codeql-action/init (dependency-caching: none, trap-caching: ${{"}},
		{"the gate unset", []mutation{{cq, codeqlTrapGated, ""}}, []string{"codeql-analysis.yml", "github/codeql-action/init (dependency-caching: none, trap-caching unset)"}},
	}
	for _, c := range fails {
		t.Run(c.name+" fails", func(t *testing.T) {
			expectViolation(t, prCacheWriteViolations(t, plantTree(t, root, files, c.muts...)), c.want...)
		})
	}
}
