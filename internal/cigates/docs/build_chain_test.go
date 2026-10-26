package docs_test

import (
	"fmt"
	"strings"
	"testing"
)

// The docs build is three command lines, in this order: `docs-build pre`, MkDocs, `docs-build post`. These tests pin the
// order, the shared --mode and --full-check, and the log redirection in every Makefile target and in docs-cloudflare.yml.

const (
	chainPre  = "go run -trimpath -mod=readonly ./cmd/cigate docs-build pre"
	chainMk   = "$(PYTHON) -m mkdocs build --strict --config-file mkdocs.yml"
	chainPost = "go run -trimpath -mod=readonly ./cmd/cigate docs-build post"
)

type chainTarget struct {
	name  string
	mode  string
	full  bool
	build string // the recipe text as written in the Makefile
}

func chainRecipe(mode string, full bool) string {
	flag := ""
	if full {
		flag = " --full-check"
	}
	return "\t" + chainPre + " --mode " + mode + flag + "\n\t" + chainMk + "\n\t" + chainPost + " --mode " + mode + flag + "\n"
}

var chainTargets = []chainTarget{
	{`docs\:check`, "preview", true, ""},
	{`docs\:build-preview`, "preview", false, ""},
	{`docs\:version`, "preview", true, ""},
	{`docs\:deploy`, "production", true, ""},
}

func flagsOf(line string) (mode string, full bool) {
	fields := strings.Fields(line)
	for i, f := range fields {
		if f == "--mode" && i+1 < len(fields) {
			mode = fields[i+1]
		}
		if f == "--full-check" {
			full = true
		}
	}
	return mode, full
}

// lineIndexes returns the positions of the lines that start with prefix.
func lineIndexes(lines []string, prefix string) []int {
	var out []int
	for i, l := range lines {
		if strings.HasPrefix(l, prefix) {
			out = append(out, i)
		}
	}
	return out
}

// chainFindings checks one ordered list of command lines (a Makefile recipe or a workflow run block): each of the three
// steps runs exactly once, in the order pre, mkdocs, post, and pre and post share --mode and --full-check.
func chainFindings(where string, lines []string, mkPrefix string, wantMode string, wantFull bool) ([]string, int, int, int) {
	var out []string
	pre, mk, post := lineIndexes(lines, chainPre), lineIndexes(lines, mkPrefix), lineIndexes(lines, chainPost)
	for _, c := range []struct {
		name string
		at   []int
	}{{"docs-build pre", pre}, {"mkdocs", mk}, {"docs-build post", post}} {
		if len(c.at) != 1 {
			out = append(out, fmt.Sprintf("%s: runs %s %d times, want exactly 1", where, c.name, len(c.at)))
		}
	}
	if len(pre) != 1 || len(mk) != 1 || len(post) != 1 {
		return out, -1, -1, -1
	}
	if !(pre[0] < mk[0] && mk[0] < post[0]) {
		out = append(out, where+": the build lines must run docs-build pre, then mkdocs, then docs-build post")
	}
	preMode, preFull := flagsOf(lines[pre[0]])
	postMode, postFull := flagsOf(lines[post[0]])
	if preMode != postMode {
		out = append(out, fmt.Sprintf("%s: pre and post must run with the same --mode (pre %q, post %q)", where, preMode, postMode))
	}
	if preFull != postFull {
		out = append(out, where+": pre and post must run with the same --full-check")
	}
	if wantMode != "" && (preMode != wantMode || postMode != wantMode) {
		out = append(out, fmt.Sprintf("%s: must run docs-build pre and post with --mode %s", where, wantMode))
	}
	if wantMode != "" && preFull != wantFull {
		out = append(out, fmt.Sprintf("%s: --full-check must be %t on pre and post", where, wantFull))
	}
	return out, pre[0], mk[0], post[0]
}

func makefileChainGuard(d docTree) []string {
	makefile, err := d.read(makefilePath)
	if err != nil {
		return []string{err.Error()}
	}
	var out []string
	for _, tg := range chainTargets {
		lines := makeRecipe(makefile, tg.name, map[string]bool{})
		findings, _, _, _ := chainFindings(tg.name, lines, chainMk, tg.mode, tg.full)
		out = append(out, findings...)
	}
	return out
}

// teeOf returns the redirection of a command line: "tee" or "tee -a", and the log path.
func teeOf(line string) (kind, log string) {
	switch {
	case strings.Contains(line, "| tee -a "):
		kind = "tee -a"
		log = line[strings.Index(line, "| tee -a ")+len("| tee -a "):]
	case strings.Contains(line, "| tee "):
		kind = "tee"
		log = line[strings.Index(line, "| tee ")+len("| tee "):]
	}
	return kind, strings.TrimSpace(log)
}

func workflowChainGuard(d docTree) []string {
	step, err := cloudflareStep(d, "preview-or-build", "Build and prepare validated preview assets")
	if err != nil {
		return []string{err.Error()}
	}
	lines := commandLines(asString(step["run"]))
	var cmds []string
	for _, l := range lines {
		if strings.HasPrefix(l, "go run") || strings.HasPrefix(l, "python -m mkdocs") {
			cmds = append(cmds, l)
		}
	}
	out, pre, mk, post := chainFindings("docs-cloudflare.yml preview build", cmds, mkdocsCmdLine, "preview", true)
	if pre < 0 {
		return out
	}
	want := map[int]string{pre: "tee", mk: "tee -a", post: "tee -a"}
	names := map[int]string{pre: "pre must start the log with `| tee`", mk: "mkdocs must append with `| tee -a`", post: "post must append with `| tee -a`"}
	first := ""
	for _, i := range []int{pre, mk, post} {
		kind, log := teeOf(cmds[i])
		if kind != want[i] {
			out = append(out, "docs-cloudflare.yml preview build: "+names[i])
		}
		if first == "" {
			first = log
		} else if log != first {
			out = append(out, fmt.Sprintf("docs-cloudflare.yml preview build: all three steps must write the same log (%q vs %q)", first, log))
		}
	}
	return out
}

func TestMakefileDocsBuildChain(t *testing.T) {
	check := func(old, repl string) func(*testing.T, docTree) docTree {
		return func(t *testing.T, d docTree) docTree { return d.edit(t, makefilePath, old, repl) }
	}
	pv := chainRecipe("preview", false)
	pf := chainRecipe("preview", true)
	pd := chainRecipe("production", true)
	swapped := func(mode string, full bool) string {
		flag := ""
		if full {
			flag = " --full-check"
		}
		return "\t" + chainPre + " --mode " + mode + flag + "\n\t" + chainPost + " --mode " + mode + flag + "\n\t" + chainMk + "\n"
	}
	assertGuard(t, makefileChainGuard, []plantCase{
		{"docs:check runs post before mkdocs", check("check-freshness\n"+pf, "check-freshness\n"+swapped("preview", true)), `docs\:check: the build lines must run docs-build pre, then mkdocs, then docs-build post`},
		{"docs:build-preview runs post before mkdocs", check(`docs\:build-preview:`+"\n"+pv, `docs\:build-preview:`+"\n"+swapped("preview", false)), `docs\:build-preview: the build lines must run`},
		{"docs:version runs mkdocs before pre", check(pf+"\t$(WRANGLER) versions upload", "\t"+chainMk+"\n\t"+chainPre+" --mode preview --full-check\n\t"+chainPost+" --mode preview --full-check\n\t$(WRANGLER) versions upload"), `docs\:version: the build lines must run`},
		{"docs:deploy runs mkdocs first", check(`docs\:deploy:`+"\n"+pd, `docs\:deploy:`+"\n\t"+chainMk+"\n\t"+chainPre+" --mode production --full-check\n\t"+chainPost+" --mode production --full-check\n"), `docs\:deploy: the build lines must run`},
		{"docs:deploy post --mode drifts from pre", check(`docs\:deploy:`+"\n"+pd, `docs\:deploy:`+"\n"+strings.Replace(pd, chainPost+" --mode production", chainPost+" --mode preview", 1)), `docs\:deploy: pre and post must run with the same --mode`},
		{"docs:version post drops --full-check", check(pf+"\t$(WRANGLER) versions upload", strings.Replace(pf, chainPost+" --mode preview --full-check", chainPost+" --mode preview", 1)+"\t$(WRANGLER) versions upload"), `docs\:version: pre and post must run with the same --full-check`},
		{"docs:deploy runs in preview mode on both steps", check(`docs\:deploy:`+"\n"+pd, `docs\:deploy:`+"\n"+chainRecipe("preview", true)), `docs\:deploy: must run docs-build pre and post with --mode production`},
		{"docs:build-preview drops mkdocs", check(`docs\:build-preview:`+"\n"+pv, `docs\:build-preview:`+"\n\t"+chainPre+" --mode preview\n\t"+chainPost+" --mode preview\n"), `docs\:build-preview: runs mkdocs 0 times`},
		{"docs:version drops pre", check(pf+"\t$(WRANGLER) versions upload", "\t"+chainMk+"\n\t"+chainPost+" --mode preview --full-check\n\t$(WRANGLER) versions upload"), `docs\:version: runs docs-build pre 0 times`},
		{"docs:deploy drops post", check(`docs\:deploy:`+"\n"+pd, `docs\:deploy:`+"\n\t"+chainPre+" --mode production --full-check\n\t"+chainMk+"\n"), `docs\:deploy: runs docs-build post 0 times`},
	})
}

func TestCloudflarePreviewBuildChain(t *testing.T) {
	const log = ".build/docs-cloudflare-build.log"
	edit := func(old, repl string) func(*testing.T, docTree) docTree {
		return func(t *testing.T, d docTree) docTree { return d.edit(t, cloudflarePath, old, repl) }
	}
	assertGuard(t, workflowChainGuard, []plantCase{
		{"mkdocs line loses tee -a", edit(mkdocsCmdLine+" \\\n            2>&1 | tee -a "+log, mkdocsCmdLine+" \\\n            2>&1 | tee "+log), "mkdocs must append with `| tee -a`"},
		{"post line loses tee -a", edit(postCmd+" --mode preview --full-check \\\n            2>&1 | tee -a "+log, postCmd+" --mode preview --full-check \\\n            2>&1 | tee "+log), "post must append with `| tee -a`"},
		{"pre line appends instead of creating", edit(preCmd+" --mode preview --full-check \\\n            2>&1 | tee "+log, preCmd+" --mode preview --full-check \\\n            2>&1 | tee -a "+log), "pre must start the log with `| tee`"},
		{"post tees to another log", edit(postCmd+" --mode preview --full-check \\\n            2>&1 | tee -a "+log, postCmd+" --mode preview --full-check \\\n            2>&1 | tee -a .build/other.log"), "must write the same log"},
		{"post --mode drifts from pre", edit(postCmd+" --mode preview", postCmd+" --mode production"), "pre and post must run with the same --mode"},
		{"post drops --full-check", edit(postCmd+" --mode preview --full-check", postCmd+" --mode preview"), "pre and post must run with the same --full-check"},
	})
}
