package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestGoEnvironmentInputDomain is the go-command environment the generator
// child inherits, as a family: every setting that decides where the go command
// reads a module from or writes anything to. Each cell sets one -- through the
// environment or through a GOENV file, the go command's two sources -- and the
// guard asks the go command itself (`go env -json`, in the private copy, with
// the child's exact environment) for the EFFECTIVE value before generating.
// Round 4 executed `GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod` writing
// into the working tree while check-drift passed.
func TestGoEnvironmentInputDomain(t *testing.T) {
	goAvailable(t)
	type cell struct {
		shape   string
		env     func(f *fixture) map[string]string
		wantErr string
	}
	inTree := func(f *fixture, rel string) string { return filepath.Join(f.dir, rel) }
	goenvFile := func(t *testing.T, f *fixture, line string) string {
		p := filepath.Join(t.TempDir(), "goenv")
		if err := os.WriteFile(p, []byte(line+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	var tt *testing.T
	cells := []cell{
		{shape: "canonical (the inherited environment, GOFLAGS cleared)", env: func(f *fixture) map[string]string { return map[string]string{"GOFLAGS": ""} }},
		{shape: "GOFLAGS=-p=2 (a benign flag, the review harness's own)", env: func(f *fixture) map[string]string { return map[string]string{"GOFLAGS": "-p=2"} }},
		{shape: "GOFLAGS=-mod=mod (writes go.mod -- in the copy only)", env: func(f *fixture) map[string]string { return map[string]string{"GOFLAGS": "-mod=mod"} }},
		{shape: "GOFLAGS=-modfile=<tree>/alternate.mod", env: func(f *fixture) map[string]string {
			return map[string]string{"GOFLAGS": "-modfile=" + inTree(f, "alternate.mod")}
		}, wantErr: "-modfile"},
		{shape: "GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod (round 4's shape)", env: func(f *fixture) map[string]string {
			return map[string]string{"GOFLAGS": "-mod=mod -modfile=" + inTree(f, "alternate.mod")}
		}, wantErr: "-modfile"},
		{shape: "a GOENV file carrying GOFLAGS=-mod=mod -modfile=<tree>/alternate.mod", env: func(f *fixture) map[string]string {
			return map[string]string{"GOFLAGS": "", "GOENV": goenvFile(tt, f, "GOFLAGS=-mod=mod -modfile="+inTree(f, "alternate.mod"))}
		}, wantErr: "-modfile"},
		{shape: "GOCACHE inside the working tree", env: func(f *fixture) map[string]string { return map[string]string{"GOCACHE": inTree(f, ".gocache")} }, wantErr: "GOCACHE"},
		{shape: "GOMODCACHE inside the working tree", env: func(f *fixture) map[string]string { return map[string]string{"GOMODCACHE": inTree(f, ".modcache")} }, wantErr: "GOMODCACHE"},
		{shape: "GOPATH inside the working tree", env: func(f *fixture) map[string]string {
			return map[string]string{"GOPATH": inTree(f, ".gopath"), "GOMODCACHE": ""}
		}, wantErr: "GOPATH"},
		{shape: "GOTMPDIR inside the working tree", env: func(f *fixture) map[string]string { return map[string]string{"GOTMPDIR": inTree(f, ".gotmp")} }, wantErr: "GOTMPDIR"},
		{shape: "a GOENV file carrying GOCACHE inside the working tree", env: func(f *fixture) map[string]string {
			return map[string]string{"GOCACHE": "", "GOENV": goenvFile(tt, f, "GOCACHE="+inTree(f, ".gocache"))}
		}, wantErr: "GOCACHE"},
	}
	cells = append(cells, cell{shape: "the module has no go.mod and one sits ABOVE the private copy (the go command would adopt it)", env: func(f *fixture) map[string]string {
		return map[string]string{"GOFLAGS": ""}
	}, wantErr: "outside the private copy"})
	for i, c := range cells {
		t.Run(cellID("G12", i)+" "+c.shape, func(t *testing.T) {
			tt = t
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
			for k, v := range c.env(f) {
				t.Setenv(k, v)
			}
			before := f.digests()
			gen := &fakeGenerator{fn: rewriteOutputs("generated")}
			opts.Generator = gen
			_, err := UpdateDriftRecord(context.Background(), opts)
			logCell(t, err, "go environment accepted")
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
				t.Fatal("the generator ran with a go environment that reaches the working tree")
			}
			assertUnchanged(t, before, f.digests(), c.shape)
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
		}, wantErr: "replace"},
		{shape: "a RELATIVE ../ replace outside the module", replace: func(t *testing.T, f *fixture) string {
			return "replace example.com/ext => ../external-domain\n"
		}, wantErr: "replace"},
		{shape: "an absolute replace naming the REAL tree (outside the private copy)", replace: func(t *testing.T, f *fixture) string {
			f.write("inmod/go.mod", "module example.com/inmod\n\ngo 1.25\n")
			return "replace example.com/inmod => " + filepath.Join(f.dir, "inmod") + "\n"
		}, wantErr: "replace"},
		{shape: "a replace through a directory link out of the module", replace: func(t *testing.T, f *fixture) string {
			ext := t.TempDir()
			if err := os.WriteFile(filepath.Join(ext, "go.mod"), []byte("module example.com/ext\n\ngo 1.25\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(ext, filepath.Join(f.dir, "extlink")); err != nil {
				t.Fatal(err)
			}
			return "replace example.com/ext => ./extlink\n"
		}, wantErr: "replace"},
	}
	for i, c := range cells {
		t.Run(cellID("G13", i)+" "+c.shape, func(t *testing.T) {
			f := guardFixture(t).withModuleFiles()
			if r := c.replace(t, f); r != "" {
				f.write("go.mod", f.read("go.mod")+"\n"+r)
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
