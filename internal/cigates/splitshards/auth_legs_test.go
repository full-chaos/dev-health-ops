//go:build !race

package splitshards_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-13520: auth-test, auth-race and auth-race-integration run as COUNT legs (`check_go.sh auth KIND-leg LEG COUNT`). Over the
// COUNT legs of the workflow, every package of the kind's list runs whole in exactly one leg or is split, and every name `go test
// -list` (the kind's own build flags, current tree) reports for a split package runs in exactly one leg. The run line's COUNT is
// the length of the job's leg list.

type authKind struct {
	job, kind string
	pkgs      []string // the kind's package patterns (check_go.sh AUTH_PACKAGES / AUTH_RACE_PACKAGES)
	listFlags []string
}

var authPackages = []string{"./cmd/auth-service/...", "./cmd/auth-migrate/...", "./internal/auth/...", "./contracts/auth/v1/...", "./internal/audit/...",
	"./internal/storage/postgres/authschema/...", "./internal/authzenforce/...", "./internal/jobs/pushexpiry/...", "./internal/workerservice/...", "./internal/token/..."}

var authKinds = []authKind{
	{"auth-test", "test", authPackages, nil},
	{"auth-race", "race", authPackages, []string{"-race"}},
	{"auth-race-integration", "race-integration", []string{"./internal/auth/session", "./internal/auth/identity", "./internal/auth/identity/local", "./internal/audit", "./internal/auth/keyrotation"}, []string{"-race", "-tags=integration"}},
}

func jobBlock(t *testing.T, wf, job string) string {
	t.Helper()
	start := strings.Index(wf, "\n  "+job+":\n")
	if start < 0 {
		t.Fatalf("workflow has no job %s", job)
	}
	rest := wf[start+1:]
	if end := regexp.MustCompile(`\n  [a-z][a-z0-9-]*:\n`).FindStringIndex(rest[1:]); end != nil {
		rest = rest[:end[0]+1]
	}
	return rest
}

func authLegCount(t *testing.T, root string, k authKind) int {
	t.Helper()
	block := jobBlock(t, readText(t, filepath.Join(root, ".github/workflows/go-auth-service.yml")), k.job)
	m := regexp.MustCompile(`bash ci/check_go\.sh auth ` + regexp.QuoteMeta(k.kind) + `-leg "\$\{\{ matrix\.leg \}\}" ([0-9]+)`).FindStringSubmatch(block)
	if m == nil {
		t.Fatalf("%s runs no `check_go.sh auth %s-leg LEG COUNT` line", k.job, k.kind)
	}
	n, _ := strconv.Atoi(m[1])
	want := make([]string, n)
	for i := range want {
		want[i] = strconv.Itoa(i + 1)
	}
	if !strings.Contains(block, "leg: ["+strings.Join(want, ", ")+"]") {
		t.Fatalf("%s: the leg list is not [1..%d], the COUNT of its run line", k.job, n)
	}
	return n
}

// authAllLegs runs `check_go.sh auth KIND-leg all COUNT` (AUTH_LEG_LIST=1: every leg's list from one listing, no test run)
// and returns what precedes the first leg header and, per leg, the lines after its header; the headers must be legs 1..COUNT
// in order, each once.
func authAllLegs(t *testing.T, root string, env []string, k authKind, n int) (string, map[int]string) {
	t.Helper()
	r := freeSpec{dir: root, env: append(append([]string{}, env...), "AUTH_LEG_LIST=1")}.run("bash", "ci/check_go.sh", "auth", k.kind+"-leg", "all", strconv.Itoa(n))
	if r.code != 0 {
		t.Fatalf("auth %s-leg all %d: exit %d: %s%s", k.kind, n, r.code, r.stdout, tail(r.stderr))
	}
	header := regexp.MustCompile(`(?m)^==> auth ` + regexp.QuoteMeta(k.kind) + `-leg ([0-9]+) ` + strconv.Itoa(n) + `\n`)
	loc := header.FindAllStringSubmatchIndex(r.stdout, -1)
	if len(loc) != n {
		t.Fatalf("auth %s-leg all %d prints %d leg header(s), want %d", k.kind, n, len(loc), n)
	}
	parts := map[int]string{}
	for i, m := range loc {
		leg, _ := strconv.Atoi(r.stdout[m[2]:m[3]])
		if leg != i+1 {
			t.Fatalf("auth %s-leg all %d: header %d names leg %d, want legs 1..%d in order, each once", k.kind, n, i+1, leg, n)
		}
		end := len(r.stdout)
		if i+1 < len(loc) {
			end = loc[i+1][0]
		}
		parts[leg] = r.stdout[m[1]:end]
	}
	return r.stdout[:loc[0][0]], parts
}

// TestTheAuthAllLegsFormPrintsEachLegsOwnList: per kind, the leg listing the most split-package names lists, on its own
// (`auth KIND-leg LEG COUNT`, AUTH_LEG_LIST=1), exactly the all-legs form's opening lines plus its part, byte for byte.
// Without AUTH_LEG_LIST=1 the all-legs form is refused: it never runs a test.
func TestTheAuthAllLegsFormPrintsEachLegsOwnList(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	for _, k := range authKinds {
		t.Run(k.kind, func(t *testing.T) {
			t.Parallel()
			n := authLegCount(t, root, k)
			prefix, parts := authAllLegs(t, root, env, k, n)
			leg, most := 0, -1
			for l := 1; l <= n; l++ {
				c := 0
				for _, line := range runLines(parts[l], "AUTH-LEG-RUN "+k.kind+" ") {
					if !strings.HasSuffix(line, " *") {
						c++
					}
				}
				if c > most {
					leg, most = l, c
				}
			}
			r := freeSpec{dir: root, env: append(append([]string{}, env...), "AUTH_LEG_LIST=1")}.run("bash", "ci/check_go.sh", "auth", k.kind+"-leg", strconv.Itoa(leg), strconv.Itoa(n))
			if r.code != 0 {
				t.Fatalf("auth %s-leg %d %d: exit %d: %s%s", k.kind, leg, n, r.code, r.stdout, tail(r.stderr))
			}
			if want := prefix + parts[leg]; r.stdout != want {
				a, b := strings.Split(r.stdout, "\n"), strings.Split(want, "\n")
				i := 0
				for i < len(a) && i < len(b) && a[i] == b[i] {
					i++
				}
				g, e := "<end>", "<end>"
				if i < len(a) {
					g = a[i]
				}
				if i < len(b) {
					e = b[i]
				}
				t.Errorf("auth %s-leg %d %d differs from its part of the all-legs form at line %d: %q, all-legs form %q", k.kind, leg, n, i+1, g, e)
			}
			refused := freeSpec{dir: root, env: env}.run("bash", "ci/check_go.sh", "auth", k.kind+"-leg", "all", strconv.Itoa(n))
			if refused.code == 0 || !strings.Contains(refused.stderr, "needs AUTH_LEG_LIST=1") {
				t.Errorf("auth %s-leg all %d without AUTH_LEG_LIST=1: exit %d, want a refusal: %s", k.kind, n, refused.code, tail(refused.stderr))
			}
		})
	}
}

func TestEveryAuthLegNameRunsInExactlyOneLeg(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	for _, k := range authKinds {
		t.Run(k.kind, func(t *testing.T) {
			t.Parallel()
			n := authLegCount(t, root, k)
			whole := map[string]int{}
			names := map[string]map[string]int{}
			// One all-legs listing gives every leg (TestTheAuthAllLegsFormPrintsEachLegsOwnList holds it equal to a leg's own).
			_, parts := authAllLegs(t, root, env, k, n)
			for leg := 1; leg <= n; leg++ {
				for _, l := range runLines(parts[leg], "AUTH-LEG-RUN "+k.kind+" ") {
					pkg, name, ok := strings.Cut(l, " ")
					if !ok {
						t.Errorf("leg %d: malformed line %q", leg, l)
						continue
					}
					if name == "*" {
						whole[pkg]++
						continue
					}
					if names[pkg] == nil {
						names[pkg] = map[string]int{}
					}
					names[pkg][name]++
				}
			}
			if t.Failed() {
				return
			}
			listed := freeSpec{dir: root, env: env}.run("go", append([]string{"list", "-mod=readonly"}, k.pkgs...)...)
			if listed.code != 0 {
				t.Fatalf("go list: %s", listed.stderr)
			}
			var problems []string
			for _, p := range strings.Fields(listed.stdout) {
				_, split := names[p]
				switch {
				case split && whole[p] > 0:
					problems = append(problems, p+" runs whole and split")
				case !split && whole[p] != 1:
					problems = append(problems, fmt.Sprintf("%s runs whole in %d legs", p, whole[p]))
				}
			}
			for p := range whole {
				if !strings.Contains("\n"+listed.stdout, "\n"+p+"\n") {
					problems = append(problems, p+" runs but is not in the kind's package list")
				}
			}
			for p, got := range names {
				args := append(append([]string{"test", "-mod=readonly", "-trimpath"}, k.listFlags...), "-list", ".*", p)
				r := freeSpec{dir: root, env: env}.run("go", args...)
				if r.code != 0 {
					t.Fatalf("go test -list %s: %s%s", p, r.stdout, r.stderr)
				}
				want := map[string]bool{}
				for _, l := range strings.Split(r.stdout, "\n") {
					if strings.HasPrefix(l, "Test") || strings.HasPrefix(l, "Example") || strings.HasPrefix(l, "Fuzz") {
						want[strings.TrimSpace(l)] = true
					}
				}
				for w := range want {
					if got[w] != 1 {
						problems = append(problems, fmt.Sprintf("%s %s runs in %d legs", p, w, got[w]))
					}
				}
				for g := range got {
					if !want[g] {
						problems = append(problems, fmt.Sprintf("%s %s runs but go test -list does not list it", p, g))
					}
				}
			}
			sort.Strings(problems)
			if len(problems) > 0 {
				t.Errorf("%d problem(s): %v", len(problems), problems[:min(10, len(problems))])
			}
		})
	}
}
