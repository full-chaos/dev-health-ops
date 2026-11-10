//go:build !race

package splitshards_test

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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

func TestEveryAuthLegNameRunsInExactlyOneLeg(t *testing.T) {
	t.Parallel()
	root := rootOf(t)
	env := inheritedEnv(nil, "GOTOOLCHAIN="+pinnedGoToolchain(t), "GOWORK=off")
	for _, k := range authKinds {
		t.Run(k.kind, func(t *testing.T) {
			t.Parallel()
			n := authLegCount(t, root, k)
			var mu sync.Mutex
			whole := map[string]int{}
			names := map[string]map[string]int{}
			var wg sync.WaitGroup
			for leg := 1; leg <= n; leg++ {
				wg.Add(1)
				go func(leg int) {
					defer wg.Done()
					r := freeSpec{dir: root, env: append(append([]string{}, env...), "AUTH_LEG_LIST=1")}.run("bash", "ci/check_go.sh", "auth", k.kind+"-leg", strconv.Itoa(leg), strconv.Itoa(n))
					if r.code != 0 {
						t.Errorf("auth %s-leg %d %d: exit %d: %s%s", k.kind, leg, n, r.code, r.stdout, tail(r.stderr))
						return
					}
					mu.Lock()
					defer mu.Unlock()
					for _, l := range runLines(r.stdout, "AUTH-LEG-RUN "+k.kind+" ") {
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
				}(leg)
			}
			wg.Wait()
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
