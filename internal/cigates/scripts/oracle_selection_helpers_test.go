//go:build !race

package scripts_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Scratch trees for the venue-oracles verb of ci/check_go.sh (shard slices, cost-balanced plan, changed-file selection):
// the real script and awks run against a tiny Go tree and its registry, with a stand-in `go test` that records the -run
// set and writes the proof a real oracle writes. Test names here carry no "VenueOracle": the registry guard discovers
// venue tests by name.

var oracleCIFiles = []string{
	"ci/check_go.sh", "ci/check_venue_oracle_registry.sh", "ci/venue_oracle_discovery.awk", "ci/venue_oracle_names.awk",
	"ci/venue_oracle_proof.awk", "ci/venue_oracle_shard.awk", "ci/lib/venue_oracle_registry.sh",
}

const oracleStandInGo = `#!/usr/bin/env bash
if [ "$1" != "test" ]; then exec "%s" "$@"; fi
pattern=""; pkg=""
while [ "$#" -gt 0 ]; do
  case "$1" in -run) pattern="$2"; shift ;; ./*) pkg="$1" ;; esac
  shift
done
printf '%%s %%s\n' "${pkg}" "${pattern}" >> "${FAKE_GO_LOG}"
if [ -n "${FAKE_GO_SLEEP:-}" ]; then sleep "${FAKE_GO_SLEEP}"; fi
names="${pattern#^(}"; names="${names%%)\$}"
IFS='|' read -ra list <<< "${names}"
for n in "${list[@]}"; do
  printf 'executed' > "${DEV_HEALTH_LIVE_PYTHON_ORACLE_PROOF_DIR}/${n}"
done
exit 0
`

type otree struct {
	t   *testing.T
	dir string
}

type pkgTests struct {
	pkg   string
	tests []string
}

func (o *otree) write(rel, text string) {
	o.t.Helper()
	p := filepath.Join(o.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		o.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(text), 0o644); err != nil {
		o.t.Fatal(err)
	}
}

func (o *otree) mkdir(rel string) {
	o.t.Helper()
	if err := os.MkdirAll(filepath.Join(o.dir, filepath.FromSlash(rel)), 0o755); err != nil {
		o.t.Fatal(err)
	}
}

func oracleGoTest(pkg string, funcs []string, local string) string {
	body := "package " + pkg + "\n\nimport (\n\t" + venueHarness + "\n)\n\n"
	for _, name := range funcs {
		if name == local {
			body += "//venueoracle:local-only needs a key CI does not hold\n"
		}
		body += fmt.Sprintf("func %s(t *testing.T) {\n\tvenueoracle.Diff(t)\n}\n\n", name)
	}
	return body
}

func scratchOTree(t *testing.T, pkgs []pkgTests, local ...string) *otree {
	t.Helper()
	root := rootOf(t)
	o := &otree{t: t, dir: t.TempDir()}
	for _, f := range oracleCIFiles {
		o.write(f, readText(t, filepath.Join(root, filepath.FromSlash(f))))
	}
	o.write("go.mod", "module example.com/x\n")
	localTest := ""
	if len(local) > 0 {
		localTest = local[0]
	}
	for _, p := range pkgs {
		o.write(p.pkg+"/x_test.go", oracleGoTest(filepath.Base(p.pkg), p.tests, localTest))
		var rows strings.Builder
		tests := append([]string(nil), p.tests...)
		sort.Strings(tests)
		for _, tn := range tests {
			mode := "run"
			for _, l := range local {
				if tn == l {
					mode = "local"
				}
			}
			fmt.Fprintf(&rows, "%s\t%s\t%s\n", p.pkg, tn, mode)
		}
		o.write("ci/venue_oracle_registry.d/"+strings.ReplaceAll(p.pkg, "/", "__")+".tsv", rows.String())
	}
	return o
}

func realOTree(t *testing.T) *otree {
	t.Helper()
	root := rootOf(t)
	o := &otree{t: t, dir: t.TempDir()}
	for _, n := range []string{"go.mod", "ci", "internal", "cmd", "src"} {
		src := filepath.Join(root, n)
		st, err := os.Stat(src)
		if err != nil {
			continue
		}
		if st.IsDir() {
			if err := os.Symlink(src, filepath.Join(o.dir, n)); err != nil {
				t.Fatal(err)
			}
		} else {
			o.write(n, readText(t, src))
		}
	}
	return o
}

type oracleRun struct {
	res cmdResult
	ran [][2]string
}

func (o *otree) verb(verb string, env []string, args ...string) oracleRun {
	o.t.Helper()
	realGo, err := exec.LookPath("go")
	if err != nil {
		o.t.Fatal(err)
	}
	o.write("bin/go", fmt.Sprintf(oracleStandInGo, realGo))
	if err := os.Chmod(filepath.Join(o.dir, "bin", "go"), 0o755); err != nil {
		o.t.Fatal(err)
	}
	log := filepath.Join(o.dir, "fake_go.log")
	o.write("fake_go.log", "")
	o.mkdir("tmp")
	e := inheritedEnv(nil, append([]string{
		"PATH=" + filepath.Join(o.dir, "bin") + string(os.PathListSeparator) + os.Getenv("PATH"), "FAKE_GO_LOG=" + log,
		"TMPDIR=" + filepath.Join(o.dir, "tmp"), "DEV_HEALTH_GO_CACHE=" + filepath.Join(o.dir, "tmp", "gocache"),
	}, env...)...)
	res := freeSpec{dir: o.dir, env: e}.run("bash", append([]string{filepath.Join(o.dir, "ci", "check_go.sh"), verb}, args...)...)
	var ran [][2]string
	for _, l := range strings.Split(readText(o.t, log), "\n") {
		if l == "" {
			continue
		}
		pkg, pattern, _ := strings.Cut(l, " ")
		for _, n := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(pattern, "^("), ")$"), "|") {
			ran = append(ran, [2]string{strings.TrimPrefix(pkg, "./"), n})
		}
	}
	return oracleRun{res: res, ran: ran}
}

func (o *otree) venueOracles(env []string, args ...string) oracleRun {
	return o.verb("venue-oracles", env, args...)
}

func sortedRows(r [][2]string) [][2]string {
	out := append([][2]string(nil), r...)
	sort.Slice(out, func(i, j int) bool {
		if out[i][0] != out[j][0] {
			return out[i][0] < out[j][0]
		}
		return out[i][1] < out[j][1]
	})
	return out
}

func rowsEqual(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (o *otree) registryRunRows() [][2]string {
	o.t.Helper()
	files, _ := filepath.Glob(filepath.Join(o.dir, "ci", "venue_oracle_registry.d", "*.tsv"))
	sort.Strings(files)
	var rows [][2]string
	for _, f := range files {
		for _, l := range strings.Split(readText(o.t, f), "\n") {
			if l == "" || strings.HasPrefix(l, "#") {
				continue
			}
			parts := strings.Split(l, "\t")
			if len(parts) == 3 && parts[2] == "run" {
				rows = append(rows, [2]string{parts[0], parts[1]})
			}
		}
	}
	return sortedRows(rows)
}

func oracleWorkflowJob(t *testing.T, workflow, job string) yamlAny {
	t.Helper()
	var doc map[string]any
	if err := yaml.Unmarshal([]byte(readText(t, filepath.Join(rootOf(t), ".github", "workflows", workflow))), &doc); err != nil {
		t.Fatal(err)
	}
	jobs, _ := doc["jobs"].(map[string]any)
	j, _ := jobs[job].(map[string]any)
	if j == nil {
		t.Fatalf("%s: job %s not found", workflow, job)
	}
	return j
}

type yamlAny = map[string]any

func nested(m yamlAny, keys ...string) any {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}
