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
// three cache locations, and a location inside the module is refused. An
// inherited `GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod` once made the
// child write into the working tree while check-drift passed.
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
		{shape: "GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod (writes the tree when inherited; not inherited)", env: func(t *testing.T, f *fixture) map[string]string {
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
		{shape: "GOINSECURE/GONOSUMDB/GOPRIVATE=* (not inherited)", env: set("GOINSECURE", "*", "GONOSUMDB", "*", "GOPRIVATE", "*")},
		{shape: "GOPROXY=off (not inherited)", env: set("GOPROXY", "off")},
		{shape: "HOME inside the working tree, cache locations set explicitly (HOME not inherited: the child's is scratch)", env: func(t *testing.T, f *fixture) map[string]string {
			loc, err := goEnv(context.Background(), t.TempDir(), os.Environ(), sharedLocations...)
			if err != nil {
				t.Fatal(err)
			}
			return map[string]string{"HOME": inTree(f, ".home"), "GOCACHE": loc["GOCACHE"], "GOMODCACHE": loc["GOMODCACHE"], "GOPATH": loc["GOPATH"]}
		}},
		{shape: "HOME inside the working tree, cache locations left to their HOME defaults (a shared location: refused)", env: func(t *testing.T, f *fixture) map[string]string {
			return map[string]string{"HOME": inTree(f, ".home"), "GOCACHE": "", "GOMODCACHE": "", "GOPATH": "", "XDG_CACHE_HOME": ""}
		}, wantErr: "is inside the module, so the go command would write there"},
		{shape: "the go command on PATH resolves INSIDE the module (a wrapper at <tree>/bin/go: refused before it runs)", env: func(t *testing.T, f *fixture) map[string]string {
			goBin, err := goBinary()
			if err != nil {
				t.Fatal(err)
			}
			f.write("bin/go", "#!/bin/sh\necho ran >> "+filepath.Join(t.TempDir(), "ran.log")+"\nexec "+goBin+" \"$@\"\n")
			if err := os.Chmod(filepath.Join(f.dir, "bin", "go"), 0o755); err != nil {
				t.Fatal(err)
			}
			return map[string]string{"PATH": filepath.Join(f.dir, "bin") + string(os.PathListSeparator) + os.Getenv("PATH")}
		}, wantErr: "the go command resolves to"},
		{shape: "the user's DEFAULT go env file (no GOENV set) carrying GOCACHE inside the working tree (still read for the shared location: refused)", env: func(t *testing.T, f *fixture) map[string]string {
			home := t.TempDir()
			if err := os.MkdirAll(filepath.Join(home, ".config", "go"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(home, ".config", "go", "env"), []byte("GOCACHE="+inTree(f, ".gocache")+"\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			loc, err := goEnv(context.Background(), t.TempDir(), os.Environ(), sharedLocations...)
			if err != nil {
				t.Fatal(err)
			}
			return map[string]string{"HOME": home, "XDG_CONFIG_HOME": "", "GOENV": "", "GOCACHE": "", "GOMODCACHE": loc["GOMODCACHE"], "GOPATH": loc["GOPATH"]}
		}, wantErr: `GOCACHE="`},
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
			if c.wantErr != "" || !gen.ran {
				logCell(t, err, line)
			} else {
				// The cell is about what reached the generator, not about
				// check-drift's verdict (the fixture has no record), so the
				// output is the child's key set and the go command's view.
				var keys []string
				for _, kv := range gen.env {
					k, _, _ := strings.Cut(kv, "=")
					keys = append(keys, k)
				}
				_, eff, _ := strings.Cut(line, "nothing inherited): ")
				t.Logf("CELL-OUTPUT: child env keys %s; go env: %s", strings.Join(keys, ","), eff)
			}
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
	// The values that are not the fixed ones: PATH is the go binary's own
	// directory, and HOME/TMPDIR are the guard's scratch, never the parent's.
	val := map[string]string{}
	for _, kv := range env {
		k, v, _ := strings.Cut(kv, "=")
		val[k] = v
	}
	goBin, err := goBinary()
	if err != nil {
		t.Fatal(err)
	}
	if val["PATH"] != filepath.Dir(goBin) {
		t.Fatalf("the generator's PATH is %q, want only the go binary's directory %q", val["PATH"], filepath.Dir(goBin))
	}
	for _, k := range []string{"HOME", "TMPDIR"} {
		if val[k] == os.Getenv(k) || !strings.Contains(val[k], "gqlgen-guard-env-") {
			t.Fatalf("the generator's %s is %q, want the guard's scratch (the guard's own is %q)", k, val[k], os.Getenv(k))
		}
	}
	for k, want := range map[string]string{"GOFLAGS": "-mod=readonly", "GOENV": "off", "GOWORK": "off", "GOTOOLCHAIN": "local"} {
		if val[k] != want {
			t.Fatalf("the generator's %s is %q, want %q", k, val[k], want)
		}
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

// TestAnInheritedModfileFlagWritesNothingInTheTree runs the -modfile escape
// for real: a committed record, then check-drift with GOFLAGS naming a modfile
// in the tree -- and the same through a GOENV file. Before the allowlist the child wrote
// alternate.mod/alternate.sum in the working tree and check-drift still passed.
func TestAnInheritedModfileFlagWritesNothingInTheTree(t *testing.T) {
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
			assertUnchanged(t, before, f.digests(), "check-drift under an inherited -modfile via "+via)
		})
	}
}

// TestGoModReplaceInputDomain: a filesystem `replace` in go.mod is an input of
// the generation -- the generator loads that directory's packages to bind
// models -- so it is confined like every other input: an absolute replace
// outside the module lets a package edited outside it change the output that
// is copied back. Version replaces resolve through the module cache and
// go.sum, like any dependency.
func TestGoModReplaceInputDomain(t *testing.T) {
	goAvailable(t)
	cells := []struct {
		shape   string
		replace func(t *testing.T, f *fixture, beside string) string
		wantErr string
	}{
		{shape: "canonical (no replace)", replace: func(t *testing.T, f *fixture, beside string) string { return "" }},
		{shape: "a replace to a directory INSIDE the module", replace: func(t *testing.T, f *fixture, beside string) string {
			f.write("inmod/go.mod", "module example.com/inmod\n\ngo 1.25\n")
			return "replace example.com/inmod => ./inmod\n"
		}},
		{shape: "a version replace (module cache, go.sum)", replace: func(t *testing.T, f *fixture, beside string) string {
			return "replace example.com/versioned => example.com/other v1.0.0\n"
		}},
		{shape: "an ABSOLUTE replace outside the module (its package changes the output)", replace: func(t *testing.T, f *fixture, beside string) string {
			ext := t.TempDir()
			if err := os.WriteFile(filepath.Join(ext, "go.mod"), []byte("module example.com/ext\n\ngo 1.25\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			return "replace example.com/ext => " + ext + "\n"
		}, wantErr: "go.mod replaces example.com/"},
		{shape: "a RELATIVE ../ replace outside the module", replace: func(t *testing.T, f *fixture, beside string) string {
			return "replace example.com/ext => ../external-domain\n"
		}, wantErr: "go.mod replaces example.com/"},
		{shape: "an absolute replace naming the REAL tree (outside the private copy)", replace: func(t *testing.T, f *fixture, beside string) string {
			f.write("inmod/go.mod", "module example.com/inmod\n\ngo 1.25\n")
			return "replace example.com/inmod => " + filepath.Join(f.dir, "inmod") + "\n"
		}, wantErr: "go.mod replaces example.com/"},
		{shape: "a replace through a directory link out of the module", replace: func(t *testing.T, f *fixture, beside string) string {
			ext := t.TempDir()
			if err := os.WriteFile(filepath.Join(ext, "go.mod"), []byte("module example.com/ext\n\ngo 1.25\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(ext, filepath.Join(f.dir, "extlink")); err != nil {
				t.Fatal(err)
			}
			return "replace example.com/ext => ./extlink\n"
		}, wantErr: "go.mod replaces example.com/"},
		{shape: "a RELATIVE ../ replace whose target EXISTS beside the private copy (the confinement check, not the resolution one)", replace: func(t *testing.T, f *fixture, beside string) string {
			if err := os.MkdirAll(filepath.Join(beside, "ext-beside"), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(beside, "ext-beside", "go.mod"), []byte("module example.com/ext\n\ngo 1.25\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			return "replace example.com/ext => ../ext-beside\n"
		}, wantErr: "a directory outside the module"},
	}
	for i, c := range cells {
		t.Run(cellID("G13", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			opts := guardOptions(f, gen)
			if r := c.replace(t, f, opts.TempParent); r != "" {
				f.write("go.mod", f.read("go.mod")+"\n"+r)
			}
			if c.wantErr != "" && strings.Contains(f.dir, c.wantErr) {
				t.Fatalf("wantErr %q also occurs in the fixture path %s", c.wantErr, f.dir)
			}
			before := f.digests()
			_, err := UpdateDriftRecord(context.Background(), opts)
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
// changed into or read: otherwise `-config ../outside.yml` is parsed from
// outside the module before any refusal.
func TestConfigPathInputDomain(t *testing.T) {
	cells := []struct {
		shape, path string
		wantErr     string
	}{
		{"canonical", "gqlgen.yml", ""},
		{"a ../ after a directory name, lexically inside the module (refused)", "gen/../gqlgen.yml", "has a `..` after a directory name"},
		{"../ out of the module", "../outside.yml", "leaves the module"},
		{"a deeper escape", "gen/../../outside.yml", "leaves the module"},
		{"absolute", "/etc/hostname", "module-relative"},
		{"the module root itself", ".", "leaves the module"},
	}
	for i, c := range cells {
		t.Run(cellID("G14", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t)
			// A sentinel beside the module that a pre-check read would parse.
			sentinel := filepath.Join(filepath.Dir(f.dir), "outside.yml")
			if err := os.WriteFile(sentinel, []byte("OUTSIDE_CONFIG_SENTINEL: true\n"), 0o644); err != nil {
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
			if strings.Contains(err.Error(), "OUTSIDE_CONFIG_SENTINEL") || strings.Contains(err.Error(), "unable to parse config") {
				t.Fatalf("the config outside the module was READ before the refusal: %v", err)
			}
		})
	}
}

// TestTheGuardsOwnInputsAreInBothParsedPathLists: the path filters are
// asserted as YAML values, so a pattern that survives only in a
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

// TestChildGoEnvAssertionInputDomain is the input domain of the assertion over
// the child's effective go settings (checkChildGoEnv), executed directly: the
// allowlist means no guard input reaches these values, so each field's wrong
// shapes are fed in here.
func TestChildGoEnvAssertionInputDomain(t *testing.T) {
	copyDir := t.TempDir()
	module := t.TempDir()
	inside := func(p string) bool { return within(module, p) || within(copyDir, p) }
	good := func() map[string]string {
		return map[string]string{"GOMOD": filepath.Join(copyDir, "go.mod"), "GOFLAGS": "-mod=readonly", "GOENV": "", "GOWORK": "off", "GOTOOLCHAIN": "local", "GOMODCACHE": "/cache/mod", "GOCACHE": "/cache/build", "GOTMPDIR": ""}
	}
	cells := []struct {
		shape   string
		edit    func(m map[string]string)
		wantErr string
	}{
		{"canonical", func(map[string]string) {}, ""},
		{"GOMOD absent (no module)", func(m map[string]string) { m["GOMOD"] = "" }, ""},
		{"GOMOD os.DevNull (module mode, no go.mod)", func(m map[string]string) { m["GOMOD"] = os.DevNull }, ""},
		{"GOMOD outside the copy", func(m map[string]string) { m["GOMOD"] = filepath.Join(module, "..", "x", "go.mod") }, "outside the private copy"},
		{"GOFLAGS empty", func(m map[string]string) { m["GOFLAGS"] = "" }, "does not honour"},
		{"GOFLAGS with -modfile added", func(m map[string]string) { m["GOFLAGS"] = "-mod=readonly -modfile=/x.mod" }, "does not honour"},
		{"GOENV a file path", func(m map[string]string) { m["GOENV"] = "/home/u/.config/go/env" }, "does not honour"},
		{"GOENV off (literal)", func(m map[string]string) { m["GOENV"] = "off" }, ""},
		{"GOWORK empty", func(m map[string]string) { m["GOWORK"] = "" }, "does not honour"},
		{"GOWORK a go.work path", func(m map[string]string) { m["GOWORK"] = "/w/go.work" }, "does not honour"},
		{"GOTOOLCHAIN auto", func(m map[string]string) { m["GOTOOLCHAIN"] = "auto" }, "does not honour"},
		{"GOTOOLCHAIN local+path (a valid local form)", func(m map[string]string) { m["GOTOOLCHAIN"] = "local+path" }, ""},
		{"GOMODCACHE inside the module", func(m map[string]string) { m["GOMODCACHE"] = filepath.Join(module, "mod") }, `GOMODCACHE="`},
		{"GOCACHE inside the copy", func(m map[string]string) { m["GOCACHE"] = filepath.Join(copyDir, "c") }, `GOCACHE="`},
		{"GOTMPDIR inside the module", func(m map[string]string) { m["GOTMPDIR"] = filepath.Join(module, "t") }, `GOTMPDIR="`},
	}
	for i, c := range cells {
		t.Run(cellID("G15", i)+" "+c.shape, func(t *testing.T) {
			m := good()
			c.edit(m)
			err := checkChildGoEnv(m, copyDir, inside)
			logCell(t, err, "accepted")
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("cell REFUSED but its contract says accept: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v", c.wantErr, err)
			}
		})
	}
}

// TestAGoCommandThatIgnoresTheEnvironmentIsRefused drives the assertion at its
// call site with a real process: the guard's PATH resolves `go` to a wrapper
// that runs the real go command but misreports one effective setting on the
// query made with the child's environment (the one naming GOMOD). The contract
// is a refusal before the generator runs, and an untouched tree.
func TestAGoCommandThatIgnoresTheEnvironmentIsRefused(t *testing.T) {
	goAvailable(t)
	realGo, err := goBinary()
	if err != nil {
		t.Fatal(err)
	}
	cells := []struct {
		shape   string
		sed     func(f *fixture) string
		wantErr string
	}{
		{"GOFLAGS reported as -mod=mod", func(*fixture) string { return `s/"GOFLAGS": "-mod=readonly"/"GOFLAGS": "-mod=mod"/` }, "does not honour"},
		{"GOWORK reported as a workspace", func(*fixture) string { return `s#"GOWORK": "off"#"GOWORK": "/w/go.work"#` }, "does not honour"},
		{"GOTOOLCHAIN reported as auto", func(*fixture) string { return `s/"GOTOOLCHAIN": "local"/"GOTOOLCHAIN": "auto"/` }, "does not honour"},
		{"GOCACHE reported inside the module", func(f *fixture) string {
			return `s#"GOCACHE": "[^"]*"#"GOCACHE": "` + filepath.Join(f.dir, ".gocache") + `"#`
		}, `GOCACHE="`},
		{"GOTMPDIR reported inside the module", func(f *fixture) string {
			return `s#"GOTMPDIR": "[^"]*"#"GOTMPDIR": "` + filepath.Join(f.dir, ".gotmp") + `"#`
		}, `GOTMPDIR="`},
	}
	for i, c := range cells {
		t.Run(cellID("G16", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			if strings.Contains(f.dir, c.wantErr) {
				t.Fatalf("wantErr %q also occurs in the fixture path %s", c.wantErr, f.dir)
			}
			bin := t.TempDir()
			script := "#!/usr/bin/sh\ncase \" $* \" in\n*\" GOMOD \"*) " + realGo + " \"$@\" | /usr/bin/sed '" + c.sed(f) + "'; exit ;;\nesac\nexec " + realGo + " \"$@\"\n"
			if err := os.WriteFile(filepath.Join(bin, "go"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			opts := guardOptions(f, nil)
			before := f.digests()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			opts.Generator = gen
			var report strings.Builder
			opts.Report = &report
			_, err := CheckDrift(context.Background(), opts)
			logCell(t, err, "accepted")
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("want a refusal containing %q, got %v\n%s", c.wantErr, err, report.String())
			}
			if gen.ran {
				t.Fatal("the generator ran under a go command that does not honour its environment")
			}
			assertUnchanged(t, before, f.digests(), c.shape)
		})
	}
}

// TestTheGoCommandIsTheVettedBinary: every go command the guard runs is
// constructed from the literal name "go", which exec resolves through the
// guard's PATH. A `go` shim put first on PATH AFTER the guard vetted the go
// binary -- inside the module, logging each run -- must be refused before it
// runs, by comparing what exec resolved with the vetted binary.
func TestTheGoCommandIsTheVettedBinary(t *testing.T) {
	goAvailable(t)
	f := guardFixture(t).withModuleFiles()
	realGo, err := goBinary()
	if err != nil {
		t.Fatal(err)
	}
	ran := filepath.Join(t.TempDir(), "ran.log")
	f.write("bin/go", "#!/bin/sh\necho \"$@\" >> "+ran+"\nexec "+realGo+" \"$@\"\n")
	if err := os.Chmod(filepath.Join(f.dir, "bin", "go"), 0o755); err != nil {
		t.Fatal(err)
	}
	fired := false
	testStageHook = func(stage, _ string) {
		if stage == "go-command" && !fired {
			fired = true
			t.Setenv("PATH", filepath.Join(f.dir, "bin")+string(os.PathListSeparator)+os.Getenv("PATH"))
		}
	}
	t.Cleanup(func() { testStageHook = nil })
	opts := guardOptions(f, &fakeGenerator{fn: rewriteOutputs("generated")})
	before := f.digests()
	_, err = CheckDrift(context.Background(), opts)
	logCell(t, err, "accepted")
	if !fired {
		t.Fatal("no go command was constructed, so the cell proves nothing")
	}
	if b, rerr := os.ReadFile(ran); rerr == nil && len(b) > 0 {
		t.Fatalf("the shim ran: %q", b)
	}
	if err == nil || !strings.Contains(err.Error(), "not the vetted go binary") {
		t.Fatalf("want a refusal naming the unvetted go binary, got %v", err)
	}
	delete(before, "bin/go")
	after := f.digests()
	delete(after, "bin/go")
	assertUnchanged(t, before, after, "a go shim put on PATH after vetting")
}
