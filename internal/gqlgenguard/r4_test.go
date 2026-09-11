package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestGoEnvironmentInputDomain: the generator child's environment is BUILT from
// an allowlist, never inherited (childEnvKeys), so every cell here sets a
// setting in the GUARD's environment -- directly or through a GOENV file -- and
// the contract is that it does not reach the child: generation proceeds and
// nothing outside the private copy moves. The only inherited values are the
// three cache locations, and a location inside the module is refused. Round 4
// executed `GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod` writing into the
// working tree while check-drift passed.
func TestGoEnvironmentInputDomain(t *testing.T) {
	goAvailable(t)
	type cell struct {
		shape   string
		env     func(t *testing.T, f *fixture) map[string]string
		wantErr string
	}
	inTree := func(f *fixture, rel string) string { return filepath.Join(f.dir, rel) }
	goenvFile := func(t *testing.T, line string) string {
		p := filepath.Join(t.TempDir(), "goenv")
		if err := os.WriteFile(p, []byte(line+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	set := func(kv ...string) func(*testing.T, *fixture) map[string]string {
		return func(*testing.T, *fixture) map[string]string {
			m := map[string]string{}
			for i := 0; i+1 < len(kv); i += 2 {
				m[kv[i]] = kv[i+1]
			}
			return m
		}
	}
	cells := []cell{
		{shape: "canonical (nothing unusual inherited)", env: set()},
		{shape: "GOFLAGS=-modfile=<tree>/alternate.mod (not inherited)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOFLAGS": "-modfile=" + inTree(f, "alternate.mod")}
		}},
		{shape: "GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod (round 4's shape; not inherited)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOFLAGS": "-mod=mod -modfile=" + inTree(f, "alternate.mod")}
		}},
		{shape: "a GOENV file carrying GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod (GOENV=off for the child)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOFLAGS": "", "GOENV": goenvFile(t, "GOFLAGS=-mod=mod -modfile="+inTree(f, "alternate.mod"))}
		}},
		{shape: "GOTMPDIR inside the working tree (not inherited: the child's temp is scratch)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOTMPDIR": inTree(f, ".gotmp")}
		}},
		{shape: "GOTOOLCHAIN=go1.99.0 (not inherited: local)", env: set("GOTOOLCHAIN", "go1.99.0")},
		{shape: "GOWORK=<a workspace> (not inherited: off)", env: func(t *testing.T, f *fixture) map[string]string {
			w := filepath.Join(t.TempDir(), "go.work")
			if err := os.WriteFile(w, []byte("go 1.27.0\n\nuse "+f.dir+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			return map[string]string{"GOWORK": w}
		}},
		{shape: "a setting Go does not know yet (GO_FUTURE_KNOB; not inherited)", env: set("GO_FUTURE_KNOB", "1", "GOEXPERIMENT", "")},
		{shape: "GOCACHE inside the working tree (a shared location: refused)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOCACHE": inTree(f, ".gocache")}
		}, wantErr: `GOCACHE="`},
		{shape: "GOMODCACHE inside the working tree (a shared location: refused)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOMODCACHE": inTree(f, ".modcache")}
		}, wantErr: `GOMODCACHE="`},
		{shape: "GOPATH inside the working tree (a shared location: refused)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOPATH": inTree(f, ".gopath"), "GOMODCACHE": ""}
		}, wantErr: `GOPATH="`},
		{shape: "a GOENV file carrying GOCACHE inside the working tree (read for the shared location: refused)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"GOCACHE": "", "GOENV": goenvFile(t, "GOCACHE="+inTree(f, ".gocache"))}
		}, wantErr: `GOCACHE="`},
		{shape: "the module has no go.mod and one sits ABOVE the private copy (the go command would adopt it)", env: set(), wantErr: "outside the private copy"},
	}
	for i, c := range cells {
		t.Run(cellID("G12", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			opts := guardOptions(f, nil)
			if strings.HasPrefix(c.shape, "the module has no go.mod") {
				if err := os.Remove(filepath.Join(f.dir, "go.mod")); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(opts.TempParent, "go.mod"), []byte("module example.com/above\n\ngo 1.25\n"), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			for k, v := range c.env(t, f) {
				t.Setenv(k, v)
			}
			if c.wantErr != "" && strings.Contains(f.dir, c.wantErr) {
				t.Fatalf("wantErr %q also occurs in the fixture path %s", c.wantErr, f.dir)
			}
			before := f.digests()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			opts.Generator = gen
			var report strings.Builder
			opts.Report = &report
			_, err := CheckDrift(context.Background(), opts)
			line := ""
			for _, l := range strings.Split(report.String(), "\n") {
				if strings.HasPrefix(l, "generator env (allowlist") {
					line = l
				}
			}
			logCell(t, err, line)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("want a refusal naming %q, got %v", c.wantErr, err)
				}
				if gen.ran {
					t.Fatal("the generator ran with a shared location inside the module")
				}
				assertUnchanged(t, before, f.digests(), c.shape)
				return
			}
			// check-drift refuses for lack of a record here; what the cell is
			// about is the environment the generator was handed and the tree.
			if !gen.ran {
				t.Fatalf("the generator never ran: %v\n%s", err, report.String())
			}
			assertAllowlistedEnv(t, gen.env)
			for _, want := range []string{"GOFLAGS=-mod=readonly", `GOENV=""`, "GOWORK=off", "GOTOOLCHAIN=local"} {
				if !strings.Contains(line, want) {
					t.Fatalf("the go command's effective environment for the generator lacks %q:\n%s", want, line)
				}
			}
			assertUnchanged(t, before, f.digests(), c.shape)
		})
	}
}

// assertAllowlistedEnv fails unless env holds exactly the allowlisted keys, each
// once -- the property that makes "a setting a future Go release adds cannot
// reach the child" true by construction.
func assertAllowlistedEnv(t *testing.T, env []string) {
	t.Helper()
	var keys []string
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		keys = append(keys, k)
	}
	if strings.Join(keys, ",") != strings.Join(childEnvKeys, ",") {
		t.Fatalf("the generator's environment has keys %v, want exactly %v", keys, childEnvKeys)
	}
}

// TestTheRealChildSeesOnlyTheAllowlist executes the claim in a real child
// process: a `go run` of a program that prints its own environment's keys, run
// by the guard exactly as it runs gqlgen, with extra settings in the guard's
// environment that must not arrive.
func TestTheRealChildSeesOnlyTheAllowlist(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	f.write("envdump/main.go", "package main\n\nimport (\n\t\"fmt\"\n\t\"os\"\n\t\"sort\"\n\t\"strings\"\n)\n\nfunc main() {\n\tvar k []string\n\tfor _, kv := range os.Environ() {\n\t\tk = append(k, strings.SplitN(kv, \"=\", 2)[0])\n\t}\n\tsort.Strings(k)\n\tfmt.Println(\"CHILD-ENV-KEYS:\", strings.Join(k, \",\"))\n}\n")
	t.Setenv("GO_FUTURE_KNOB", "1")
	t.Setenv("GOFLAGS", "-mod=mod -modfile="+filepath.Join(f.dir, "alternate.mod"))
	var report strings.Builder
	opts := guardOptions(f, &GoRunGenerator{Package: "./envdump", Stdout: &report, Stderr: &report})
	opts.Report = &report
	_, _ = CheckDrift(context.Background(), opts)
	var got string
	for _, l := range strings.Split(report.String(), "\n") {
		if v, ok := strings.CutPrefix(l, "CHILD-ENV-KEYS: "); ok {
			got = v
		}
	}
	want := append([]string(nil), childEnvKeys...)
	sortStrings(want)
	if got != strings.Join(want, ",") {
		t.Fatalf("the real child's environment keys are %q, want exactly %q\n%s", got, strings.Join(want, ","), report.String())
	}
	t.Logf("CELL-OUTPUT: accepted: child environment keys = %s", got)
}

// TestTheReviewersModfileReproWritesNothing is round 4's P1-1 run for real:
// a committed record, then check-drift with the reviewer's GOFLAGS -- and the
// same through a GOENV file. Before the allowlist the child wrote
// alternate.mod/alternate.sum in the working tree and check-drift still passed.
func TestTheReviewersModfileReproWritesNothing(t *testing.T) {
	goAvailable(t)
	for _, via := range []string{"GOFLAGS", "GOENV"} {
		t.Run(via, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			if _, err := UpdateDriftRecord(context.Background(), guardOptions(f, nil)); err != nil {
				t.Fatalf("write the record: %v", err)
			}
			f.write("alternate.mod", f.read("go.mod"))
			f.write("alternate.sum", "")
			flags := "-mod=mod -modfile=" + filepath.Join(f.dir, "alternate.mod")
			if via == "GOFLAGS" {
				t.Setenv("GOFLAGS", flags)
			} else {
				p := filepath.Join(t.TempDir(), "goenv")
				if err := os.WriteFile(p, []byte("GOFLAGS="+flags+"\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				t.Setenv("GOFLAGS", "")
				t.Setenv("GOENV", p)
			}
			before := f.digests()
			_, err := CheckDrift(context.Background(), guardOptions(f, nil))
			logCell(t, err, "drift matches; the tree did not move")
			if err != nil {
				t.Fatalf("check-drift: %v", err)
			}
			assertUnchanged(t, before, f.digests(), "check-drift under the reviewer's "+via)
		})
	}
}

// TestGoModReplaceInputDomain: a filesystem `replace` in go.mod is an input of
// the generation -- the generator loads that directory's packages to bind
// models -- so it is confined like every other input. Round 4 executed an
// absolute replace outside the module whose package, edited alone, changed the
// output copied back. Version replaces resolve through the module cache and
// go.sum, like any dependency.
func TestGoModReplaceInputDomain(t *testing.T) {
	goAvailable(t)
	cells := []struct {
		shape   string
		replace func(t *testing.T, f *fixture) string
		wantErr string
	}{
		{shape: "canonical (no replace)", replace: func(t *testing.T, f *fixture) string { return "" }},
		{shape: "a replace to a directory INSIDE the module", replace: func(t *testing.T, f *fixture) string {
			f.write("inmod/go.mod", "module example.com/inmod\n\ngo 1.25\n")
			return "replace example.com/inmod => ./inmod\n"
		}},
		{shape: "a version replace (module cache, go.sum)", replace: func(t *testing.T, f *fixture) string {
			return "replace example.com/versioned => example.com/other v1.0.0\n"
		}},
		{shape: "an ABSOLUTE replace outside the module (round 4's shape)", replace: func(t *testing.T, f *fixture) string {
			ext := t.TempDir()
			if err := os.WriteFile(filepath.Join(ext, "go.mod"), []byte("module example.com/ext\n\ngo 1.25\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			return "replace example.com/ext => " + ext + "\n"
		}, wantErr: "go.mod replaces example.com/"},
		{shape: "a RELATIVE ../ replace outside the module", replace: func(t *testing.T, f *fixture) string {
			return "replace example.com/ext => ../external-domain\n"
		}, wantErr: "go.mod replaces example.com/"},
		{shape: "an absolute replace naming the REAL tree (outside the private copy)", replace: func(t *testing.T, f *fixture) string {
			f.write("inmod/go.mod", "module example.com/inmod\n\ngo 1.25\n")
			return "replace example.com/inmod => " + filepath.Join(f.dir, "inmod") + "\n"
		}, wantErr: "go.mod replaces example.com/"},
		{shape: "a replace through a directory link out of the module", replace: func(t *testing.T, f *fixture) string {
			ext := t.TempDir()
			if err := os.WriteFile(filepath.Join(ext, "go.mod"), []byte("module example.com/ext\n\ngo 1.25\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(ext, filepath.Join(f.dir, "extlink")); err != nil {
				t.Fatal(err)
			}
			return "replace example.com/ext => ./extlink\n"
		}, wantErr: "go.mod replaces example.com/"},
	}
	for i, c := range cells {
		t.Run(cellID("G13", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			if r := c.replace(t, f); r != "" {
				f.write("go.mod", f.read("go.mod")+"\n"+r)
			}
			if c.wantErr != "" && strings.Contains(f.dir, c.wantErr) {
				t.Fatalf("wantErr %q also occurs in the fixture path %s", c.wantErr, f.dir)
			}
			before := f.digests()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			_, err := UpdateDriftRecord(context.Background(), guardOptions(f, gen))
			logCell(t, err, "go.mod accepted")
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want a refusal naming %q, got %v", c.wantErr, err)
			}
			if gen.ran {
				t.Fatal("the generator ran over a go.mod replace outside the module")
			}
			assertUnchanged(t, before, f.digests(), c.shape)
		})
	}
}

// TestConfigPathInputDomain: the config path is confined BEFORE anything is
// changed into or read. Round 4 executed `-config ../outside.yml` being parsed
// from outside the module before the refusal.
func TestConfigPathInputDomain(t *testing.T) {
	cells := []struct {
		shape, path string
		wantErr     string
	}{
		{"canonical", "gqlgen.yml", ""},
		{"a ../ that stays inside the module", "gen/../gqlgen.yml", ""},
		{"../ out of the module (round 4's shape)", "../outside.yml", "leaves the module"},
		{"a deeper escape", "gen/../../outside.yml", "leaves the module"},
		{"absolute", "/etc/hostname", "module-relative"},
		{"the module root itself", ".", "leaves the module"},
	}
	for i, c := range cells {
		t.Run(cellID("G14", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t)
			// A sentinel beside the module that a pre-check read would parse.
			sentinel := filepath.Join(filepath.Dir(f.dir), "outside.yml")
			if err := os.WriteFile(sentinel, []byte("REVIEW_OUTSIDE_CONFIG_SENTINEL: true\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := EnumerateOutputs(f.dir, c.path)
			logCell(t, err, "config accepted")
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v", c.wantErr, err)
			}
			if strings.Contains(err.Error(), "REVIEW_OUTSIDE_CONFIG_SENTINEL") || strings.Contains(err.Error(), "unable to parse config") {
				t.Fatalf("the config outside the module was READ before the refusal: %v", err)
			}
		})
	}
}

// TestTheGuardsOwnInputsAreInBothParsedPathLists is round 4's P3: the path
// filters are asserted as YAML values, so a pattern that survives only in a
// comment is missing.
func TestTheGuardsOwnInputsAreInBothParsedPathLists(t *testing.T) {
	var wf struct {
		On struct {
			Push struct {
				Paths []string `yaml:"paths"`
			} `yaml:"push"`
			PullRequest struct {
				Paths []string `yaml:"paths"`
			} `yaml:"pull_request"`
		} `yaml:"on"`
	}
	if err := yaml.Unmarshal([]byte(readRepoFile(t, ".github/workflows/go.yml")), &wf); err != nil {
		t.Fatalf("parse go.yml: %v", err)
	}
	for _, want := range []string{"**/*.go", "cmd/query-api/gqlgen.yml", "contracts/**", ".github/workflows/go-quality.yml", ".github/workflows/go.yml", "**/go.mod", "**/go.sum", "ci/go_relevance.py"} {
		for event, list := range map[string][]string{"push": wf.On.Push.Paths, "pull_request": wf.On.PullRequest.Paths} {
			found := false
			for _, p := range list {
				if p == want {
					found = true
				}
			}
			if !found {
				t.Errorf("go.yml's %s paths do not contain %q as a value (a comment does not count)", event, want)
			}
		}
	}
}

func sortStrings(s []string) { sort.Strings(s) }
