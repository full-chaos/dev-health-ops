// Command gqlgen-guard runs the gqlgen code generator inside a private copy of
// this module and reports, or applies, exactly what it produced.
//
//	gqlgen-guard check-drift   compare a fresh generation with the checked-in
//	                           files; write nothing; exit non-zero on drift
//	gqlgen-guard generate      regenerate and copy the outputs back
//
// The write verbs fail closed on the RECORD STATE, not on the verb. `generate`
// refuses, and writes nothing, unless the expected-drift record is present,
// parses, and describes every file it is about to overwrite as it now stands;
// -revert-recorded is the only override and names every file it reverts.
// `check-drift -update` never replaces a destination that is not already a
// record.
//
// The generator never runs in the working tree. Every read and write the guard
// itself performs goes through an *os.Root opened on the module root, so an
// output path that resolves outside the module -- an absolute `resolver.dir`,
// a `filename_template` carrying `../` -- is refused by path resolution itself
// rather than by a check that has to enumerate the ways out.
//
// The generator is an ordinary child process that *os.Root does not confine,
// so the private copy it runs in carries no way out: a symbolic link whose
// target is absolute or does not resolve inside the module is not reproduced
// -- its name stays as an inert link to itself, and it is reported -- and a
// schema pattern that would pass through one is refused before generating.
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
  -revert-recorded generate only: overwrite outputs even though the expected-drift
                   record describes them as deliberate hand-edits, or cannot
                   account for them at all. Without it, generate refuses and
                   writes nothing.

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
	revertRecorded := fs.Bool("revert-recorded", false, "generate only: overwrite outputs the record describes as deliberate hand-edits")
	fs.Usage = func() { fmt.Fprintf(stderr, usage, gqlgenguard.DefaultConfigPath, gqlgenguard.DefaultDriftPath) }

	if err := fs.Parse(args[1:]); err != nil {
		return exitUsage
	}
	if fs.NArg() != 0 {
		fmt.Fprintf(stderr, "gqlgen-guard: unexpected argument %q\n", fs.Arg(0))
		return exitUsage
	}

	// Every signal that can reach this process is caught rather than left to
	// the default disposition, from before the FIRST command runs. Catching it
	// cancels the context, which kills the generator child and returns before
	// anything is copied back -- so an interrupt at any point up to the
	// copy-back phase leaves the tree untouched, and the private copy is
	// removed on the way out.
	ctx, stop := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGPIPE)
	defer stop()

	// The go command is resolved ONCE, here, before anything is executed --
	// including the `go list -m` that locates the module. The module root is
	// not known yet when it has to be discovered, so the boundary for this
	// first refusal is the module that lexically encloses the working
	// directory; the root the guard settles on is checked again inside the
	// guard. Without this, a `go` shim committed in the tree ran the discovery
	// query before any check could refuse it.
	tool, err := gqlgenguard.ResolveGoTool(confinementDir(*moduleDir))
	if err != nil {
		fmt.Fprintf(stderr, "gqlgen-guard: %v\n", err)
		return exitRefused
	}

	root := *moduleDir
	if root == "" {
		root, err = discoverModuleRoot(ctx, tool)
		if err != nil {
			fmt.Fprintf(stderr, "gqlgen-guard: %v\n", err)
			return exitRefused
		}
	}

	opts := gqlgenguard.Options{
		ModuleDir:  root,
		ConfigPath: *configPath,
		DriftPath:  *recordPath,
		GoTool:     tool,
		Report:     stdout,

		RevertRecorded: *revertRecorded,
	}

	var res *gqlgenguard.Result
	switch verb {
	case "check-drift":
		if *revertRecorded {
			fmt.Fprintln(stderr, "gqlgen-guard: -revert-recorded applies to generate, not check-drift")
			return exitUsage
		}
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
//
// In workspace mode (a go.work, or GOWORK) `go list -m` prints one directory
// per workspace module. The module wanted is the one containing the working
// directory -- the deepest listed directory that is the working directory or
// one of its ancestors -- and a working directory inside none of them is a
// refusal that says so, never a multi-line string used as a path.
func discoverModuleRoot(ctx context.Context, tool gqlgenguard.GoTool) (string, error) {
	// Through the run's own go command, like every other invocation: this is
	// the FIRST thing that executes, so a `go` the run did not vet must be
	// refused here rather than after it has already answered. It is bound to
	// ctx like every other command, so an interrupt during the query ends the
	// run instead of being ignored until the query returns.
	cmd, err := tool.Command(ctx, "list", "-m", "-f", "{{.Dir}}")
	if err != nil {
		return "", err
	}
	// Not the bare environment: see GuardQueryEnv (a HOME inside the module
	// would otherwise receive the go command's telemetry counters).
	cmd.Env = gqlgenguard.GuardQueryEnv()
	out, err := cmd.Output()
	if err != nil {
		var ee *exec.ExitError
		if errors.As(err, &ee) && len(ee.Stderr) > 0 {
			return "", fmt.Errorf("locate the module root: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return "", fmt.Errorf("locate the module root: %w", err)
	}
	var dirs []string
	for _, line := range strings.Split(string(out), "\n") {
		if d := strings.TrimSpace(line); d != "" {
			dirs = append(dirs, filepath.Clean(d))
		}
	}
	if len(dirs) == 0 {
		return "", errors.New("locate the module root: `go list -m` returned no directory")
	}
	if len(dirs) == 1 {
		return dirs[0], nil
	}
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("locate the module root: read the working directory: %w", err)
	}
	wd = resolved(wd)
	best := ""
	for _, d := range dirs {
		rel, err := filepath.Rel(resolved(d), wd)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			continue
		}
		if len(d) > len(best) {
			best = d
		}
	}
	if best == "" {
		return "", fmt.Errorf("locate the module root: workspace mode lists %d modules and the working directory is inside none of them; pass -module", len(dirs))
	}
	return best, nil
}

// confinementDir is the directory the go command must not live inside, chosen
// before the module root is known. With -module it is that root. Otherwise it
// is the nearest enclosing directory holding a go.mod, found LEXICALLY -- the
// one thing available without running the go command. It is only the first of
// two refusals: the root the guard settles on is checked again, so a workspace
// whose module is elsewhere is still covered.
func confinementDir(moduleFlag string) string {
	if moduleFlag != "" {
		return moduleFlag
	}
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func resolved(p string) string {
	if r, err := filepath.EvalSymlinks(p); err == nil {
		return r
	}
	return p
}
