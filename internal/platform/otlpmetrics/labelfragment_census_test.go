package otlpmetrics

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The fragment census reads every non-test Go source that writes a Prometheus-text family ("# TYPE name kind") and works out,
// per family, the label keys its sample lines carry today. labels.tsv must hold a row per family with exactly those keys: the
// Gatherer applies that table to the health-registry fragments, so a row narrower than the code would drop a label that is
// emitted today, and a family with no row loses every label. A family or label block the scan cannot read (a name taken from
// a %s argument, a label block assembled at run time) is a failure naming file:line, never a skip, unless the site is in
// manualFragments, which pins its names and keys and is checked against the source.

var (
	typeLiteralPattern = regexp.MustCompile(`# TYPE ([A-Za-z_:][A-Za-z0-9_:]*) (counter|gauge|histogram|summary|untyped)`)
	typeConstPattern   = regexp.MustCompile(`# TYPE "\s*\+\s*([A-Za-z_][A-Za-z0-9_]*)\s*\+\s*" (counter|gauge|histogram|summary|untyped)`)
	typeDynamicPattern = regexp.MustCompile(`# TYPE (%s|"\s*\+)`)
	labelPairPattern   = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_]*)=(?:\\"" ?\+ ?[^+"]+ ?\+ ?"\\"|\\?"(?:[^"\\]|\\.)*?\\?"|%[qsvdx])`)
	commentPattern     = regexp.MustCompile(`(?m)^[ \t]*\/\/.*$`)
	// the keys the text parser turns into a bucket bound or a quantile, never a label of the exported point
	parserKeys = map[string]bool{"le": true, "quantile": true}
)

// allowedDynamicSites is, per file, the number of "# TYPE" sites whose family name is not a literal or a package constant. Each
// is covered below (helperSpecs, pinnedFragments, the job runtime reader). A new such site, or a file that gains one, fails
// the census until the count and its coverage are updated.
var allowedDynamicSites = map[string]int{
	"internal/joboutbox/loop.go": 1, "internal/scheduler/sync/loop.go": 1, "internal/scheduler/fixed/loop.go": 1, "internal/streamrunner/runner.go": 1,
	"internal/providerfoundation/budget.go": 6, "internal/platform/poolstat/poolstat.go": 3, "internal/jobruntime/telemetry.go": 1,
	"internal/syncrunrollup/dispatch_wake.go": 1,
}

type fragmentSite struct {
	file   string
	line   int
	family string
	kind   string
}

type fragmentCensus struct {
	keys       map[string]map[string]bool
	sites      map[string]fragmentSite
	unreadable []string
	typeFiles  int
	typeLines  int
}

func lineOf(src string, offset int) int { return strings.Count(src[:offset], "\n") + 1 }

// packageConsts is every `name = "literal"` of the non-test files of one directory.
func packageConsts(dirSources []string) map[string]string {
	pattern := regexp.MustCompile(`(?m)^\s*(?:const |var )?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*"([^"\n]+)"`)
	out := map[string]string{}
	for _, src := range dirSources {
		for _, m := range pattern.FindAllStringSubmatch(src, -1) {
			out[m[1]] = m[2]
		}
	}
	return out
}

// labelKeysOfBlocks returns the keys of every `<name>{...}` block in src, and the blocks it cannot read.
func labelKeysOfBlocks(src, name string, relatedSuffixes bool) (keys map[string]bool, unreadable []string, found bool) {
	keys = map[string]bool{}
	suffix := ""
	if relatedSuffixes {
		suffix = `(?:_bucket|_sum|_count)?`
	}
	block := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + suffix + `\{([^}]*)\}`)
	for _, m := range block.FindAllStringSubmatchIndex(src, -1) {
		found = true
		text := src[m[2]:m[3]]
		rest := labelPairPattern.ReplaceAllString(text, "")
		if strings.Trim(rest, ", \\") != "" {
			unreadable = append(unreadable, fmt.Sprintf("line %d: label block %q", lineOf(src, m[2]), text))
			continue
		}
		for _, pair := range labelPairPattern.FindAllStringSubmatch(text, -1) {
			if !parserKeys[pair[1]] {
				keys[pair[1]] = true
			}
		}
	}
	return keys, unreadable, found
}

func buildFragmentCensus(t *testing.T, root string) fragmentCensus {
	t.Helper()
	sources := map[string]string{}
	byDir := map[string][]string{}
	for _, f := range loadSources(t, root) {
		raw, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f.rel)))
		if err != nil {
			t.Fatal(err)
		}
		text := commentPattern.ReplaceAllString(string(raw), "")
		sources[f.rel] = text
		byDir[f.pkg] = append(byDir[f.pkg], text)
	}
	c := fragmentCensus{keys: map[string]map[string]bool{}, sites: map[string]fragmentSite{}}
	files := make([]string, 0, len(sources))
	for rel := range sources {
		files = append(files, rel)
	}
	sort.Strings(files)
	for _, rel := range files {
		src := sources[rel]
		if !strings.Contains(src, "# TYPE") {
			continue
		}
		if _, skip := operatorPullOnly[rel]; skip {
			c.typeFiles++
			c.typeLines += strings.Count(src, "# TYPE")
			continue
		}
		c.typeFiles++
		dir := filepath.ToSlash(filepath.Dir(rel))
		consts := packageConsts(byDir[dir])
		c.typeLines += strings.Count(src, "# TYPE")
		type family struct {
			name, ident, kind string
			offset            int
		}
		var families []family
		for _, m := range typeLiteralPattern.FindAllStringSubmatchIndex(src, -1) {
			families = append(families, family{name: src[m[2]:m[3]], kind: src[m[4]:m[5]], offset: m[0]})
		}
		for _, m := range typeConstPattern.FindAllStringSubmatchIndex(src, -1) {
			ident := src[m[2]:m[3]]
			value, ok := consts[ident]
			if !ok {
				c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: family named by %s, which is not a string constant of its package", rel, lineOf(src, m[0]), ident))
				continue
			}
			families = append(families, family{name: value, ident: ident, kind: src[m[4]:m[5]], offset: m[0]})
		}
		resolved := map[int]bool{}
		for _, f := range families {
			resolved[f.offset] = true
		}
		dynamic := 0
		for _, m := range typeDynamicPattern.FindAllStringIndex(src, -1) {
			if !resolved[m[0]] {
				dynamic++
			}
		}
		if dynamic != allowedDynamicSites[rel] {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s: %d \"# TYPE\" sites name their family by a %%s argument or a run-time value, %d are covered (allowedDynamicSites); name the new site", rel, dynamic, allowedDynamicSites[rel]))
		}
		if rel == "internal/jobruntime/telemetry.go" {
			c.jobruntimeFamilies(rel, src, consts)
		}
		for _, f := range families {
			line := lineOf(src, f.offset)
			keys := map[string]bool{}
			var anyFound bool
			scopes := []string{src}
			for _, other := range byDir[dir] {
				if other != src {
					scopes = append(scopes, other)
				}
			}
			for _, scope := range scopes {
				k, bad, found := labelKeysOfBlocks(scope, f.name, true)
				anyFound = anyFound || found
				for key := range k {
					keys[key] = true
				}
				for _, b := range bad {
					c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: family %s, %s", rel, line, f.name, b))
				}
				if f.ident != "" {
					for _, m := range regexp.MustCompile(`%s\{([^}]*)\}[^"\n]*"\s*,\s*(\w+)\b`).FindAllStringSubmatch(scope, -1) {
						if m[2] != f.ident {
							continue
						}
						anyFound = true
						rest := labelPairPattern.ReplaceAllString(m[1], "")
						if strings.Trim(rest, ", \\") != "" {
							c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: family %s, label block %q", rel, line, f.name, m[1]))
							continue
						}
						for _, pair := range labelPairPattern.FindAllStringSubmatch(m[1], -1) {
							if !parserKeys[pair[1]] {
								keys[pair[1]] = true
							}
						}
					}
					for _, m := range regexp.MustCompile(`\b`+regexp.QuoteMeta(f.ident)+`\s*\+\s*"\{([^}]*)\}`).FindAllStringSubmatch(scope, -1) {
						anyFound = true
						rest := labelPairPattern.ReplaceAllString(m[1], "")
						if strings.Trim(rest, ", \\") != "" {
							c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: family %s, label block %q", rel, line, f.name, m[1]))
							continue
						}
						for _, pair := range labelPairPattern.FindAllStringSubmatch(m[1], -1) {
							if !parserKeys[pair[1]] {
								keys[pair[1]] = true
							}
						}
					}
				}
			}
			_ = anyFound
			if _, ok := c.keys[f.name]; !ok {
				c.keys[f.name] = map[string]bool{}
			}
			for key := range keys {
				c.keys[f.name][key] = true
			}
			c.sites[f.name] = fragmentSite{file: rel, line: line, family: f.name, kind: f.kind}
		}
	}
	c.helperFamilies(sources, byDir)
	return c
}

// helperSpecs are the writer helpers that take the family name (and sometimes a label name) as an argument: every call of the
// helper in its package is a family. The fixed keys are the ones the helper's own sample line spells; each must appear there.
var helperSpecs = []struct {
	file, helper string
	nameArg      int
	fixed        []string
	keyArgs      []int
}{
	{"internal/joboutbox/loop.go", "writeReconcilerCounter", 1, nil, nil},
	{"internal/scheduler/sync/loop.go", "writeLoopCounter", 1, nil, nil},
	{"internal/scheduler/fixed/loop.go", "writeCounter", 1, nil, nil},
	{"internal/streamrunner/runner.go", "writeCounter", 1, nil, nil},
	{"internal/providerfoundation/budget.go", "writeProviderDatasetCounter", 1, []string{"dataset", "provider"}, nil},
	{"internal/providerfoundation/budget.go", "writeProviderDatasetComponentCounter", 1, []string{"cause", "component", "dataset", "provider"}, nil},
	{"internal/providerfoundation/budget.go", "writeProviderDatasetReasonCounter", 1, []string{"dataset", "provider"}, []int{3}},
	{"internal/providerfoundation/budget.go", "writeLabeledCounter", 1, nil, []int{3}},
	{"internal/providerfoundation/budget.go", "writeTwoLabelCounter", 1, nil, []int{3, 4}},
	{"internal/providerfoundation/budget.go", "writeProviderLabeledCounter", 1, []string{"provider"}, []int{3}},
}

// pinnedFragments are the sites whose names are not in the source as one literal: poolstat names its three gauges from the
// metric a caller passes, dispatch_wake writes one family through a constant. Each pin is checked: the file holds the
// constant or the caller's literal, and every key is spelled in the file.
var pinnedFragments = []struct {
	file, anchor string
	families     []string
	keys         []string
}{
	{"internal/platform/poolstat/poolstat.go", "# TYPE %s gauge", nil, []string{"pool"}},
	{"internal/syncrunrollup/dispatch_wake.go", "dispatchWakeMetricName", []string{"dev_health_sync_dispatch_woken_total"}, []string{"outcome", "path"}},
}

// poolstatCallers name the metric passed to poolstat.New: each yields <metric>, <metric>_acquired_conns, <metric>_max_conns.
var poolstatCallers = []struct{ file, metric string }{
	{"internal/schedulerservice/dependencies.go", "scheduler_database_pool_saturation_ratio"},
	{"internal/reconcilerservice/dependencies.go", "reconciler_database_pool_saturation_ratio"},
}

// operatorPullOnly are the files that write a family only on the operator /metrics pull path of the query API, never through
// a health.Registry fragment: the Gatherer does not see them.
var operatorPullOnly = map[string]string{
	"internal/queryapi/server/operator_compat.go": "target_info of the old query listener, written into the /metrics answer",
}

// splitCallArgs returns the top-level arguments of the call whose "(" is at open.
func splitCallArgs(src string, open int) []string {
	var args []string
	depth, start := 0, open+1
	var quote byte
	for i := open; i < len(src); i++ {
		c := src[i]
		if quote != 0 {
			if c == '\\' && quote != '`' {
				i++
			} else if c == quote {
				quote = 0
			}
			continue
		}
		switch c {
		case '"', '`', '\'':
			quote = c
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
			if depth == 0 {
				return append(args, strings.TrimSpace(src[start:i]))
			}
		case ',':
			if depth == 1 {
				args = append(args, strings.TrimSpace(src[start:i]))
				start = i + 1
			}
		}
	}
	return nil
}

var stringArg = regexp.MustCompile(`^"([^"\\]*)"$`)

// stringValue is the literal or the package constant an argument names, or false.
func stringValue(arg string, consts map[string]string) (string, bool) {
	if m := stringArg.FindStringSubmatch(arg); m != nil {
		return m[1], true
	}
	v, ok := consts[arg]
	return v, ok
}

func (c *fragmentCensus) add(name, file string, line int, kind string, keys ...string) {
	if c.keys[name] == nil {
		c.keys[name] = map[string]bool{}
	}
	for _, k := range keys {
		c.keys[name][k] = true
	}
	c.sites[name] = fragmentSite{file: file, line: line, family: name, kind: kind}
}

// jobruntimeFamilies reads telemetry.go of the job runtime: families come from writeMetadata calls, labels from the
// write*Sample and writeHistogram calls of the family. A call whose name or label argument it cannot read is unreadable, and so
// is a sample call of a family with no writeMetadata call.
func (c *fragmentCensus) jobruntimeFamilies(rel, src string, consts map[string]string) {
	labelFuncs := map[string][]string{}
	for _, m := range regexp.MustCompile(`func (\w+MetricLabels)\([^)]*\) \[\]metricLabel \{\s*return \[\]metricLabel\{([^}]*(?:\{[^}]*\}[^}]*)*)\}\s*\}`).FindAllStringSubmatch(src, -1) {
		labelFuncs[m[1]] = labelKeysIn(m[2])
	}
	declared := map[string]bool{}
	for _, loc := range regexp.MustCompile(`\bwriteMetadata\(`).FindAllStringIndex(src, -1) {
		if strings.HasSuffix(src[:loc[0]], "func ") {
			continue
		}
		args := splitCallArgs(src, loc[1]-1)
		line := lineOf(src, loc[0])
		if len(args) != 4 {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: writeMetadata call with %d arguments", rel, line, len(args)))
			continue
		}
		name, ok := stringValue(args[1], consts)
		kind, kindOK := stringValue(args[3], consts)
		if !ok || !kindOK {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: writeMetadata family name %q or type %q is neither a literal nor a package constant", rel, line, args[1], args[3]))
			continue
		}
		declared[name] = true
		c.add(name, rel, line, kind)
	}
	for _, loc := range regexp.MustCompile(`\bwrite(?:Int|Uint|Float)Sample\(|\bwriteHistogram\(`).FindAllStringIndex(src, -1) {
		if strings.HasSuffix(src[:loc[0]], "func ") {
			continue
		}
		args := splitCallArgs(src, loc[1]-1)
		line := lineOf(src, loc[0])
		if len(args) < 4 {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: sample call with %d arguments", rel, line, len(args)))
			continue
		}
		if wrapper := args[1]; wrapper == `name+"_bucket"` || wrapper == `name+"_sum"` || wrapper == `name+"_count"` {
			continue
		}
		name, ok := stringValue(args[1], consts)
		if !ok || !declared[name] {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: sample of %q: not a declared family name", rel, line, args[1]))
			continue
		}
		expr := args[2]
		if expr == "nil" {
			continue
		}
		keys := keysOfExpr(expr, labelFuncs)
		if len(keys) == 0 {
			assign := regexp.MustCompile(`\b`+regexp.QuoteMeta(expr)+` :?= ([^\n]*)`).FindAllStringSubmatch(src[:loc[0]], -1)
			if len(assign) > 0 {
				keys = keysOfExpr(assign[len(assign)-1][1], labelFuncs)
			}
		}
		if len(keys) == 0 {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: family %s is written with labels %q the census cannot read", rel, line, name, expr))
			continue
		}
		c.add(name, rel, line, c.sites[name].kind, keys...)
	}
}

func labelKeysIn(text string) []string {
	var keys []string
	for _, m := range regexp.MustCompile(`\{(?:name: *)?"([a-z_][a-z0-9_]*)",|\{name: "([a-z_][a-z0-9_]*)",`).FindAllStringSubmatch(text, -1) {
		keys = append(keys, m[1]+m[2])
	}
	return keys
}

// helperFamilies adds the families written through helperSpecs, and checks the pins.
func (c *fragmentCensus) helperFamilies(sources map[string]string, byDir map[string][]string) {
	for _, spec := range helperSpecs {
		src, ok := sources[spec.file]
		if !ok {
			c.unreadable = append(c.unreadable, spec.file+": helper spec for a file that does not exist")
			continue
		}
		dir := filepath.ToSlash(filepath.Dir(spec.file))
		consts := packageConsts(byDir[dir])
		decl := strings.Index(src, "func "+spec.helper+"(")
		if decl < 0 {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s: helper %s is no longer declared", spec.file, spec.helper))
			continue
		}
		body := src[decl:]
		if next := strings.Index(body[1:], "\nfunc "); next >= 0 {
			body = body[:next+1]
		}
		for _, key := range spec.fixed {
			if !strings.Contains(body, key+"=") {
				c.unreadable = append(c.unreadable, fmt.Sprintf("%s: helper %s no longer spells the label %s in its sample line", spec.file, spec.helper, key))
			}
		}
		calls := 0
		for rel, text := range sources {
			if filepath.ToSlash(filepath.Dir(rel)) != dir {
				continue
			}
			for _, loc := range regexp.MustCompile(`\b`+spec.helper+`\(`).FindAllStringIndex(text, -1) {
				if strings.HasSuffix(text[:loc[0]], "func ") {
					continue
				}
				args := splitCallArgs(text, loc[1]-1)
				line := lineOf(text, loc[0])
				if len(args) <= spec.nameArg {
					c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: call of %s with %d arguments", rel, line, spec.helper, len(args)))
					continue
				}
				name, ok := stringValue(args[spec.nameArg], consts)
				if !ok {
					c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: %s family name %q is neither a literal nor a package constant", rel, line, spec.helper, args[spec.nameArg]))
					continue
				}
				keys := append([]string{}, spec.fixed...)
				for _, index := range spec.keyArgs {
					key, ok := "", false
					if index < len(args) {
						key, ok = stringValue(args[index], consts)
					}
					if !ok {
						c.unreadable = append(c.unreadable, fmt.Sprintf("%s:%d: %s label name argument %d is neither a literal nor a package constant", rel, line, spec.helper, index))
						continue
					}
					keys = append(keys, key)
				}
				c.add(name, rel, line, "counter", keys...)
				calls++
			}
		}
		if calls == 0 {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s: helper %s has no call: the spec backs nothing", spec.file, spec.helper))
		}
	}
	for _, caller := range poolstatCallers {
		if !strings.Contains(sources[caller.file], `poolstat.New("`+caller.metric+`"`) {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s: no longer calls poolstat.New(%q)", caller.file, caller.metric))
			continue
		}
		for _, name := range []string{caller.metric, caller.metric + "_acquired_conns", caller.metric + "_max_conns"} {
			c.add(name, "internal/platform/poolstat/poolstat.go", 0, "gauge", "pool")
		}
	}
	for _, pin := range pinnedFragments {
		src := sources[pin.file]
		if !strings.Contains(src, pin.anchor) {
			c.unreadable = append(c.unreadable, fmt.Sprintf("%s: the pinned site %q is gone", pin.file, pin.anchor))
		}
		for _, key := range pin.keys {
			if !strings.Contains(src, key+"=") {
				c.unreadable = append(c.unreadable, fmt.Sprintf("%s: the pinned key %s is not spelled in the file", pin.file, key))
			}
		}
		for _, name := range pin.families {
			if !strings.Contains(src, `"`+name+`"`) {
				c.unreadable = append(c.unreadable, fmt.Sprintf("%s: the pinned family %s is not in the file", pin.file, name))
			}
			c.add(name, pin.file, 0, "counter", pin.keys...)
		}
	}
}

func (c fragmentCensus) rows() map[string][]string {
	out := make(map[string][]string, len(c.keys))
	for name, set := range c.keys {
		keys := make([]string, 0, len(set))
		for key := range set {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out[name] = keys
	}
	return out
}

func TestFragmentCensusProbe(t *testing.T) {
	c := buildFragmentCensus(t, filepath.Join("..", "..", ".."))
	t.Logf("files=%d typeLines=%d families=%d unreadable=%d", c.typeFiles, c.typeLines, len(c.keys), len(c.unreadable))
	for _, u := range c.unreadable {
		t.Log("UNREADABLE", u)
	}
	rows := c.rows()
	names := make([]string, 0, len(rows))
	for n := range rows {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		t.Logf("ROW\t%s\t%s\tcode: %s", n, strings.Join(rows[n], ","), c.sites[n].file)
	}
}

// keysOfExpr reads the label keys of a label-slice expression: literal pairs and the label helpers it calls.
func keysOfExpr(expr string, labelFuncs map[string][]string) []string {
	keys := labelKeysIn(expr)
	for _, m := range regexp.MustCompile(`(\w+MetricLabels)\(`).FindAllStringSubmatch(expr, -1) {
		keys = append(keys, labelFuncs[m[1]]...)
	}
	return keys
}

// TestFragmentLabelsTableEqualsTheCensus: labels.tsv holds one row per family a health-registry fragment writes, with exactly
// the label keys its sample lines carry today. A site or label block the census cannot read is a failure naming file:line.
func TestFragmentLabelsTableEqualsTheCensus(t *testing.T) {
	c := buildFragmentCensus(t, filepath.Join("..", "..", ".."))
	for _, problem := range c.unreadable {
		t.Errorf("%s", problem)
	}
	table, err := ParseLabelRows(embeddedLabels)
	if err != nil {
		t.Fatal(err)
	}
	have := map[string]LabelRow{}
	for _, row := range table {
		have[row.Instrument] = row
	}
	want := c.rows()
	for name, keys := range want {
		row, ok := have[name]
		if !ok {
			t.Errorf("fragment family %s (%s:%d) has no row in labels.tsv; its keys today: %v", name, c.sites[name].file, c.sites[name].line, keys)
			continue
		}
		if strings.Join(row.Keys, ",") != strings.Join(keys, ",") {
			t.Errorf("fragment family %s: labels.tsv has %v, the code emits %v (%s)", name, row.Keys, keys, c.sites[name].file)
		}
	}
	if c.typeFiles < 39 || c.typeLines < 122 || len(want) < 280 {
		t.Errorf("the census read %d files, %d TYPE lines, %d families: fewer than the 39 files and 122 lines of the base", c.typeFiles, c.typeLines, len(want))
	}
	t.Logf("%d files with # TYPE, %d TYPE lines, %d families", c.typeFiles, c.typeLines, len(want))
}

// The census fails closed: each unreadable form below is reported, never skipped.
func TestFragmentCensusReportsWhatItCannotRead(t *testing.T) {
	_, bad, _ := labelKeysOfBlocks(`"planted_total{%s} 1\n"`, "planted_total", false)
	if len(bad) != 1 {
		t.Errorf("a run-time label block must be unreadable, got %v", bad)
	}
	_, bad, _ = labelKeysOfBlocks(`"planted_total{kind=%q,id=\"x\"} 1\n"`, "planted_total", false)
	if len(bad) != 0 {
		t.Errorf("literal and %%q pairs must be readable, got %v", bad)
	}
	keys, _, _ := labelKeysOfBlocks(`"planted_seconds_bucket{kind=\"a\",le=\"1\"} 1\nplanted_seconds_sum{kind=\"a\"} 1\n"`, "planted_seconds", true)
	if len(keys) != 1 || !keys["kind"] {
		t.Errorf("histogram series must give kind only (le is the bucket bound), got %v", keys)
	}
	if got := splitCallArgs(`f(a, "b,c", g(d, e), [2]int{1, 2}, h)`, 1); len(got) != 5 || got[1] != `"b,c"` || got[3] != `[2]int{1, 2}` {
		t.Errorf("splitCallArgs = %q", got)
	}
}
