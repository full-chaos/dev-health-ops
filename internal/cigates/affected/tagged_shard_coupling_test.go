package affected_test

import (
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/affected"
)

// --tagged integration drops a row only when a shard cannot run its read. That rests on two facts of ci/check_go.sh;
// these cells fail when either moves, so a change there cannot make the plan miss a test without a red cell.

// K8: every package a shard can run unfiltered (gotestfuncs H records over the tag-only test files, before reviewed
// rows remove any) is in the planner's hook superset, and the planner reads every tag-only test file.
func TestTaggedPlanHookSupersetCoversTheShardHooks(t *testing.T) {
	t.Parallel()
	root := repoRoot(t)
	tagged, hooks, _, imports, err := affected.TagHooks(root, "integration")
	if err != nil {
		t.Fatal(err)
	}
	env := inheritedEnv([]string{"GOFLAGS"}, "GOWORK=off", "GOFLAGS=")
	list := func(tags ...string) map[string]bool {
		args := append([]string{"list", "-e", "-mod=readonly"}, tags...)
		args = append(args, "-f={{.Dir}}{{range .TestGoFiles}}\t{{.}}{{end}}{{range .XTestGoFiles}}\t{{.}}{{end}}", "./...")
		res := cmdSpec{dir: root, env: env}.run("go", args...)
		if res.code != 0 {
			t.Fatalf("go list %v: %s%s", tags, res.stdout, res.stderr)
		}
		out := map[string]bool{}
		for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
			f := strings.Split(l, "\t")
			rel, err := filepath.Rel(root, f[0])
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range f[1:] {
				out[filepath.ToSlash(filepath.Join(rel, name))] = true
			}
		}
		return out
	}
	withTag, plain := list("-tags=integration"), list()
	var tagOnly []string
	for f := range withTag {
		if !plain[f] {
			tagOnly = append(tagOnly, f)
		}
	}
	sort.Strings(tagOnly)
	if len(tagOnly) == 0 {
		t.Fatal("go list finds no tag-only test file: the listing is broken")
	}
	for _, f := range tagOnly {
		if !slices.Contains(tagged, f) {
			t.Errorf("tag-only test file %s is not in the planner's tagged set", f)
		}
	}
	bin := filepath.Join(t.TempDir(), "gotestfuncs")
	if res := (cmdSpec{dir: root, env: env}).run("go", "build", "-mod=readonly", "-o", bin, scriptPath(t, "ci/tools/gotestfuncs/main.go")); res.code != 0 {
		t.Fatalf("go build gotestfuncs: %s%s", res.stdout, res.stderr)
	}
	var args []string
	for _, f := range tagged {
		args = append(args, filepath.Join(root, filepath.FromSlash(f)))
	}
	res := cmdSpec{dir: root, env: env}.run(bin, args...)
	if res.code != 0 {
		t.Fatalf("gotestfuncs: exit %d: %s%s", res.code, res.stdout, res.stderr)
	}
	shard := 0
	for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
		f := strings.Split(l, "\x1f")
		if len(f) != 3 || f[0] != "H" {
			continue
		}
		shard++
		dir, err := filepath.Rel(root, filepath.Dir(f[1]))
		if err != nil {
			t.Fatal(err)
		}
		if !hooks[filepath.ToSlash(dir)] {
			t.Errorf("a shard can run %s unfiltered (%s %s), but the planner's hook superset misses it", dir, f[1], f[2])
		}
	}
	if shard == 0 {
		t.Fatal("gotestfuncs prints no H record over the tagged files: the cell checks nothing")
	}
	res = cmdSpec{dir: root, env: env}.run(bin, "-deny-list")
	if res.code != 0 {
		t.Fatalf("gotestfuncs -deny-list: exit %d: %s%s", res.code, res.stdout, res.stderr)
	}
	deny := map[string]bool{}
	for _, l := range strings.Split(strings.TrimSpace(res.stdout), "\n") {
		path, _, _ := strings.Cut(l, "\t")
		deny[path] = true
	}
	if len(deny) == 0 || len(deny) != len(imports) {
		t.Errorf("gotestfuncs deny list %v, planner hook imports %v: they must be equal", deny, imports)
	}
	for p := range deny {
		if !imports[p] {
			t.Errorf("gotestfuncs denies %s, the planner's hook imports miss it", p)
		}
	}
}

// The shard filter: a generic shard runs `-run '^(names)$'` with the tagged test names only, untagged names join only
// in a hook package, and every split planner runs a test only by split_test_runs_here.
func TestTaggedPlanShardFilterIsInPlace(t *testing.T) {
	t.Parallel()
	text := readText(t, scriptPath(t, "ci/check_go.sh"))
	for _, want := range []string{
		`    select_names["${pkg}"]="${INTEGRATION_TAGGED_TESTS[${pkg}]-}"`,
		`    if [ -n "${INTEGRATION_HOOK[${pkg}]+set}" ]; then` + "\n" + `      select_names["${pkg}"]+="${skip_names[${pkg}]}"`,
		`    group_regex[group]="^(${group_regex[${group}]})\$"`,
		`    run_pkgs=(-run "${group_regex[${group}]}" "${run_pkgs[@]}")`,
		"split_test_runs_here() {\n  [ \"$1\" = \"integration\" ] || [ -n \"$2\" ]\n}",
	} {
		if strings.Count(text, want) != 1 {
			t.Errorf("ci/check_go.sh holds %d copies of the shard filter line, want 1:\n%s", strings.Count(text, want), want)
		}
	}
	if n := strings.Count(text, `select_names["${pkg}"]+=`); n != 1 {
		t.Errorf("ci/check_go.sh adds names to a shard's -run set in %d places, want 1 (the hook branch)", n)
	}
	for _, split := range []string{"PROVIDER", "DAILY", "WORKERSERVICE", "QAPISERVER", "REMAINING"} {
		if !strings.Contains(text, `split_test_runs_here "${`+split+`_TEST_CLASS[${test_name}]}" "${`+split+`_UNFILTERED}"`) {
			t.Errorf("the %s split planner no longer runs a test by split_test_runs_here", split)
		}
	}
}
