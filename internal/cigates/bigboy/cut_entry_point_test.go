package bigboy_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func cutCheckRootLayoutRefused(g *guard) {
	root := cutTemp(g, "not-a-bigboy-tree")
	r := cutRunCut(g, cutOpts{root: root})
	g.expect(r.code != 0, "a root with no bigboy layout must be refused, got rc=0: %s", r.stdout)
	g.expect(strings.Contains(r.stderr, "BIGBOY_ROOT="+root), "the refusal must name the resolved root BIGBOY_ROOT=%s: %s", root, r.stderr)
	g.expect(len(r.calls) == 0, "a root with no bigboy layout must be refused before anything runs, but %d call(s) ran: %s", len(r.calls), r.dump())
}

func cutCheckNoCIUnderRoot(g *guard) {
	r := cutRunCut(g, cutOpts{realTools: true})
	g.expect(!exists(filepath.Join(r.root, "ci")), "the fixture root must hold no ci/")
	g.expect(!strings.Contains(r.stderr, "missing ci/bigboy"), "the cut demanded ci/bigboy under the root: %s", r.stderr)
	want := "root=" + r.root + " tools=" + g.path("ci/bigboy")
	g.expect(strings.Contains(r.stdout, want), "the cut did not start from a root with no ci/bigboy (want %q in its start line): %s", want, r.dump())
}

func cutCheckToolsDirOverride(g *guard) {
	tools := cutTemp(g, "tools")
	cutWrite(g, filepath.Join(tools, "bigboy-repin.sh"), "#!/usr/bin/env bash\n", 0o755)
	cutWrite(g, filepath.Join(tools, "arm64-child-digest.sh"), "#!/usr/bin/env bash\n", 0o755)
	r := cutRunCut(g, cutOpts{realTools: true, env: []string{"BIGBOY_TOOLS_DIR=" + tools}})
	g.expect(strings.Contains(r.stdout, "tools="+tools), "BIGBOY_TOOLS_DIR=%s did not decide where the tools come from: %s", tools, r.dump())
}

func cutCheckToolsDirRefused(g *guard) {
	empty := cutTemp(g, "empty")
	for _, c := range []struct{ tools, reason string }{
		{filepath.Join(g.tmp, "does-not-exist"), "is not a directory"},
		{empty, "has no bigboy-repin.sh"},
	} {
		r := cutRunCut(g, cutOpts{realTools: true, env: []string{"BIGBOY_TOOLS_DIR=" + c.tools}})
		g.expect(r.code != 0, "BIGBOY_TOOLS_DIR=%s must be refused, got rc=0", c.tools)
		g.expect(strings.Contains(r.stderr, c.reason), "the refusal of BIGBOY_TOOLS_DIR=%s must say %q: %s", c.tools, c.reason, r.stderr)
		g.expect(!strings.Contains(r.stdout, "cut start"), "BIGBOY_TOOLS_DIR=%s: the cut started before refusing it: %s", c.tools, r.stdout)
	}
}

func cutCheckComposeFilesResolveFromRoot(g *guard) {
	r := cutRunCut(g, cutOpts{realTools: true})
	g.expect(!exists(filepath.Join(r.root, "ci")), "the fixture root must hold no ci/")
	var seen int
	for _, c := range r.calls {
		if !c.compose() {
			continue
		}
		for _, f := range c.flagValues("-f") {
			seen++
			p := f
			if !filepath.IsAbs(p) {
				p = filepath.Join(r.root, p)
			}
			if info, err := os.Stat(p); err != nil || info.IsDir() {
				g.failf("-f entry %q does not resolve from the root %s: %s", f, r.root, p)
			}
		}
	}
	g.expect(seen > 0, "the cut handed compose no -f: %s", r.dump())
}

func TestCutEntryPoint(t *testing.T) {
	runReal(t,
		check{"root_is_printed_and_layout_is_verified", cutCheckRootLayoutRefused},
		check{"the_cut_needs_no_ci_bigboy_under_the_root", cutCheckNoCIUnderRoot},
		check{"bigboy_tools_dir_overrides_where_the_tools_come_from", cutCheckToolsDirOverride},
		check{"a_tools_dir_that_is_not_the_bigboy_tools_is_refused", cutCheckToolsDirRefused},
		check{"every_compose_file_the_cut_exports_resolves_without_ci_under_the_root", cutCheckComposeFilesResolveFromRoot},
	)
}

// A planted copy loses the execute bits the cut needs to run its siblings.
func cutExecBits(t *testing.T, root string) {
	for _, p := range walkFiles(filepath.Join(root, "ci", "bigboy")) {
		if strings.HasSuffix(p, ".sh") {
			if err := os.Chmod(p, 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
}

const cutHereLine = `HERE=$(cd "${BIGBOY_TOOLS_DIR:-$(dirname "$0")}"`

func TestCutEntryPointPlanted(t *testing.T) {
	cp := []string{"ci/bigboy"}
	runPlanted(t, []planted{
		{
			name: "layout check removed", copy: cp, check: cutCheckRootLayoutRefused,
			mutate: mut(cutScript, `for need in _records/images.env _records; do`, `for need in ; do`),
			want:   "BIGBOY_ROOT=",
		},
		{
			name: "tools default under the root", copy: cp, check: cutCheckNoCIUnderRoot,
			mutate: all(cutExecBits, mut(cutScript, cutHereLine, `HERE=$(cd "${BIGBOY_TOOLS_DIR:-$R/ci/bigboy}"`)),
			want:   "did not start from a root with no ci/bigboy",
		},
		{
			name: "BIGBOY_TOOLS_DIR ignored", copy: cp, check: cutCheckToolsDirOverride,
			mutate: all(cutExecBits, mut(cutScript, cutHereLine, `HERE=$(cd "$(dirname "$0")"`)),
			want:   "did not decide where the tools come from",
		},
		{
			name: "tools dir not-a-directory refusal removed", copy: cp, check: cutCheckToolsDirRefused,
			mutate: all(cutExecBits, mut(cutScript, ` || { echo "FAIL: BIGBOY_TOOLS_DIR=${BIGBOY_TOOLS_DIR:-} is not a directory" >&2; exit 1; }`, "")),
			want:   `must say "is not a directory"`,
		},
		{
			name: "tools dir repin check removed", copy: cp, check: cutCheckToolsDirRefused,
			mutate: all(cutExecBits, mut(cutScript, `[ -f "$HERE/bigboy-repin.sh" ] ||`, `true ||`)),
			want:   `must say "has no bigboy-repin.sh"`,
		},
		{
			name: "compose file named relative to the root", copy: cp, check: cutCheckComposeFilesResolveFromRoot,
			mutate: all(cutExecBits, mut(cutScript, `-f "$HERE/compose.bigboy.yml")`, `-f ci/bigboy/compose.bigboy.yml)`)),
			want:   "does not resolve from the root",
		},
	})
}
