package gqlgenguard

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestTheGeneratorRefusesToRunWithoutABuiltEnvironment: the child's whole
// environment is the allowlist, and the alternative to refusing a nil one is
// exec's default -- the guard's ENTIRE environment, which is the single thing
// the allowlist exists to prevent. It is driven directly because no caller can
// produce it, and a guard nothing can reach is a guard nothing pins.
func TestTheGeneratorRefusesToRunWithoutABuiltEnvironment(t *testing.T) {
	g := NewGoRunGenerator(vettedGoTool(t), nil, nil)
	err := g.Generate(context.Background(), t.TempDir(), "gqlgen.yml", nil)
	logCell(t, err, "accepted")
	if err == nil || !strings.Contains(err.Error(), "no environment was built for the generator") {
		t.Fatalf("want a refusal naming the missing environment, got %v", err)
	}
}

// TestCancellingTheGeneratorKillsTheWholeProcessTree: `go run` compiles the
// generator and EXECS it as a grandchild. Killing only the direct child leaves
// that grandchild running against a directory the guard is about to remove, so
// the command runs in its own process group and cancellation signals the whole
// group. The grandchild here announces itself, then writes a second marker
// after a delay; that second marker appearing is the leak.
func TestCancellingTheGeneratorKillsTheWholeProcessTree(t *testing.T) {
	goAvailable(t)
	dir := t.TempDir()
	markers := t.TempDir()
	started := filepath.Join(markers, "grandchild-started")
	survived := filepath.Join(markers, "grandchild-survived")
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/slowgen\n\ngo 1.25\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	src := "package main\n\nimport (\n\t\"os\"\n\t\"time\"\n)\n\nfunc main() {\n\t_ = os.WriteFile(`" + started + "`, []byte(\"x\"), 0o644)\n\ttime.Sleep(3 * time.Second)\n\t_ = os.WriteFile(`" + survived + "`, []byte(\"x\"), 0o644)\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}

	// A real child, with the allowlist environment the guard would build minus
	// the parts that need a private copy: only the go command and its caches.
	tool := vettedGoTool(t)
	env := []string{
		"PATH=" + filepath.Dir(tool.Path()),
		"HOME=" + t.TempDir(),
		"TMPDIR=" + t.TempDir(),
		"GOFLAGS=-mod=mod",
		"GOWORK=off",
		"GOTOOLCHAIN=local",
	}
	for _, k := range sharedLocations {
		env = append(env, k+"="+os.Getenv(k))
	}

	var out strings.Builder
	g := &GoRunGenerator{Tool: tool, Package: ".", Stdout: &out, Stderr: &out}
	ctx, cancel := context.WithCancel(context.Background())
	// Cancel only once the GRANDCHILD is running: cancelling while `go run` is
	// still compiling would kill the compile and prove nothing about the tree.
	go func() {
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(started); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		cancel()
	}()
	err := g.Generate(ctx, dir, "gqlgen.yml", env)
	logCell(t, err, "accepted")
	if _, statErr := os.Stat(started); statErr != nil {
		t.Fatalf("the grandchild never started, so the cell proves nothing: %v\n%s", err, out.String())
	}
	if err == nil || !strings.Contains(err.Error(), "cancelled") {
		t.Fatalf("want the guard's own cancellation error, got %v\n%s", err, out.String())
	}

	// Well past the grandchild's own sleep: if the group was not killed it has
	// written by now.
	time.Sleep(6 * time.Second)
	if _, statErr := os.Stat(survived); statErr == nil {
		t.Fatalf("the generator's grandchild outlived the cancelled run and wrote %s", survived)
	}
}
