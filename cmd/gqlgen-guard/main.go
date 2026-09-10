// Command gqlgen-guard runs the gqlgen code generator inside a private copy of
// this module and reports, or applies, exactly what it produced.
//
//	gqlgen-guard check-drift   compare a fresh generation with the checked-in
//	                           files; write nothing; exit non-zero on drift
//	gqlgen-guard generate      regenerate and copy the outputs back
//
// The generator never runs in the working tree. Every read and write the guard
// performs goes through an *os.Root opened on the module root, so a path that
// resolves outside the module -- an absolute `resolver.dir`, a
// `filename_template` carrying `../`, a symbolic link pointing away -- is
// refused by path resolution itself rather than by a check that has to
// enumerate the ways out.
//
// Interrupting the guard at any point before the copy-back phase leaves the
// working tree byte for byte as it was: the private copy is discarded and
// nothing was ever written.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/full-chaos/dev-health-ops/internal/gqlgenguard"
)

const usage = `gqlgen-guard runs gqlgen inside a private copy of this module.

usage:
  gqlgen-guard check-drift [flags]   compare a fresh generation with the checked-in files
  gqlgen-guard generate    [flags]   regenerate and copy the declared outputs back

flags:
  -module string   module root (default: the module containing the working directory)
  -config string   module-relative gqlgen config (default %q)
  -record string   module-relative expected-drift record (default %q)
  -update          check-drift only: rewrite the expected-drift record instead of
                   comparing against it. Never used by CI; a test asserts that.

exit status:
  0  the operation succeeded
  1  a refusal, drift, or a failure -- the reason is printed and named
  2  the command line itself was wrong
`

const (
	exitOK      = 0
	exitRefused = 1
	exitUsage   = 2
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr *os.File) int {
	if len(args) == 0 {
		fmt.Fprintf(stderr, usage, gqlgenguard.DefaultConfigPath, gqlgenguard.DefaultDriftPath)
		return exitUsage
	}
	verb := args[0]

	fs := flag.NewFlagSet("gqlgen-guard "+verb, flag.ContinueOnError)
	fs.SetOutput(stderr)
	moduleDir := fs.String("module", "", "module root (default: the module containing the working directory)")
	configPath := fs.String("config", gqlgenguard.DefaultConfigPath, "module-relative gqlgen config")
	recordPath := fs.String("record", gqlgenguard.DefaultDriftPath, "module-relative expected-drift record")
	update := fs.Bool("update", false, "check-drift only: rewrite the expected-drift record")
	fs.Usage = func() { fmt.Fprintf(stderr, usage, gqlgenguard.DefaultConfigPath, gqlgenguard.DefaultDriftPath) }

	if err := fs.Parse(args[1:]); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "gqlgen-guard: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}

	root := *moduleDir
	if root == "" {
		var err error
		root, err = discoverModuleRoot()
		if err != nil {
			fmt.Fprintf(stderr, "gqlgen-guard: %v\n", err)
			return exitRefused
		}
	}

	// Every signal that can reach this process is caught rather than left to
	// the default disposition. Catching it cancels the context, which kills the
	// generator child and returns before anything is copied back -- so an
	// interrupt at any point up to the copy-back phase leaves the tree
	// untouched, and the private copy is removed on the way out.
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)
	defer stop()

	opts := gqlgenguard.Options{
		ModuleDir:  root,
		ConfigPath: *configPath,
		DriftPath:  *recordPath,
		Report:     stdout,
	}

	var (
		res *gqlgenguard.Result
		err error
	)
	switch verb {
	case "check-drift":
		if *update {
			res, err = gqlgenguard.UpdateDriftRecord(ctx, opts)
		} else {
			res, err = gqlgenguard.CheckDrift(ctx, opts)
		}
	case "generate":
		if *update {
			fmt.Fprintln(stderr, "gqlgen-guard: -update applies to check-drift, not generate")
			return exitUsage
		}
		res, err = gqlgenguard.Generate(ctx, opts)
	default:
		fmt.Fprintf(stderr, "gqlgen-guard: unknown verb %q\n", verb)
		fmt.Fprintf(stderr, usage, gqlgenguard.DefaultConfigPath, gqlgenguard.DefaultDriftPath)
		return exitUsage
	}

	if err != nil {
		fmt.Fprintf(stderr, "gqlgen-guard: %v\n", err)
		return exitRefused
	}

	printDigests(stdout, res)
	return exitOK
}

// printDigests is the success output: every declared output path with the
// digest on each side, so a successful run says what it saw rather than only
// that it was happy.
func printDigests(out *os.File, res *gqlgenguard.Result) {
	fmt.Fprintf(out, "%d declared output path(s):\n", len(res.Changes))
	for _, c := range res.Changes {
		fmt.Fprintf(out, "  %-12s %s\n    tree      %s\n    generated %s\n",
			statusWord(c), c.Path, dashIfEmpty(c.TreeDigest), dashIfEmpty(c.GeneratedDigest))
	}
}

func statusWord(c gqlgenguard.Change) string {
	switch {
	case c.TreeDigest == "" && c.GeneratedDigest == "":
		return "absent"
	case c.TreeDigest == "":
		return "added"
	case c.GeneratedDigest == "":
		return "not-written"
	case !c.Drifted():
		return "same"
	default:
		return "drift"
	}
}

func dashIfEmpty(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// discoverModuleRoot asks the go command where the module containing the
// working directory begins, rather than walking up looking for a go.mod, so a
// nested module or a workspace resolves the same way the toolchain would.
func discoverModuleRoot() (string, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return "", fmt.Errorf("locate the module root: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return "", fmt.Errorf("locate the module root: %w", err)
	}
	dir := strings.TrimSpace(string(out))
	if dir == "" {
		return "", errors.New("locate the module root: `go list -m` returned no directory")
	}
	return filepath.Clean(dir), nil
}
