package gqlgenguard

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// cancelGraceperiod bounds how long a cancelled generator is given to exit
// before its whole process group is killed. `go run` execs the compiled binary
// as a GRANDCHILD, so killing only the direct child would leave the generator
// itself running against a directory that is about to be removed.
const cancelGracePeriod = 3 * time.Second

// Generator runs the code generator inside workDir, which is always a
// directory inside the private module copy.
//
// It is an interface so the guard's own behaviour -- what it refuses, what it
// copies back, what it does when the generator fails or writes somewhere it
// should not -- can be driven directly in tests instead of by mutating a
// script on disk and reading the result back out. The production
// implementation runs the real gqlgen binary.
type Generator interface {
	// Generate runs in workDir with configFile -- a path relative to workDir
	// -- as the ONLY configuration it may read (it must never fall back to
	// discovering one), and env as its ENTIRE environment: the guard builds env
	// from an allowlist (see childEnvironment) and nothing else of the
	// parent's environment may reach the child.
	Generate(ctx context.Context, workDir, configFile string, env []string) error
	// Describe returns a short human-readable identification of the generator,
	// printed in the guard's report so a run says what actually generated.
	Describe() string
}

// GoRunGenerator runs `go run github.com/99designs/gqlgen generate`.
//
// The gqlgen CLI is reachable because tools.go blank-imports it, which is what
// puts its own dependencies in go.sum. Without that file the command fails
// before doing anything, which is the state that let the checked-in generated
// files carry hand-edits unguarded for as long as they did.
type GoRunGenerator struct {
	// Package is the generator's import path.
	Package string
	// Args are passed to the generator after the package.
	Args []string
	// Stdout and Stderr receive the generator's output. A nil writer discards.
	Stdout io.Writer
	Stderr io.Writer
}

// NewGoRunGenerator returns the generator the guard uses in production.
func NewGoRunGenerator(stdout, stderr io.Writer) *GoRunGenerator {
	return &GoRunGenerator{
		Package: "github.com/99designs/gqlgen",
		Args:    []string{"generate"},
		Stdout:  stdout,
		Stderr:  stderr,
	}
}

func (g *GoRunGenerator) Describe() string {
	return fmt.Sprintf("go run %s %v", g.Package, g.Args)
}

// Generate runs the generator with its working directory inside the copy.
//
// The command is bound to ctx, so a cancelled context -- which is what an
// interrupt produces -- kills the child. Nothing has been written to the
// working tree at this point and nothing will be, because the caller only
// reaches the copy-back phase on a nil error.
func (g *GoRunGenerator) Generate(ctx context.Context, workDir, configFile string, env []string) error {
	// --config names the validated file. Without it gqlgen DISCOVERS a
	// config -- `.gqlgen.yml` before `gqlgen.yml` before `gqlgen.yaml`, in the
	// working directory and every parent, else its built-in default -- so a
	// shadow file beside the validated one, or a custom name, made the
	// generator run a configuration the guard never checked.
	args := append(append([]string{"run", g.Package}, g.Args...), "--config", configFile)
	if env == nil {
		return fmt.Errorf("`%s`: no environment was built for the generator; refusing to fall back to the guard's own", g.Describe())
	}
	// The go binary is resolved by the guard, once, and the child's PATH
	// holds only its directory (childEnvironment), so the child runs exactly
	// the toolchain the guard vetted.
	goBin, err := goBinary()
	if err != nil {
		return err
	}
	cmd := exec.CommandContext(ctx, goBin, args...)
	cmd.Dir = workDir
	cmd.Stdout = g.Stdout
	cmd.Stderr = g.Stderr
	cmd.Env = env
	if g.Stdout != nil {
		fmt.Fprintf(g.Stdout, "generator command: go %s\n", strings.Join(args, " "))
	}
	// The generator runs in its own process group so cancellation reaches the
	// whole tree -- `go run` plus the binary it execs -- rather than only the
	// `go` process, and so a signal sent to the guard cannot be delivered to
	// the generator by a route the guard does not control.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = cancelGracePeriod
	if err := cmd.Run(); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return fmt.Errorf("`%s` cancelled: %w", g.Describe(), ctxErr)
		}
		return fmt.Errorf("`%s`: %w", g.Describe(), err)
	}
	return nil
}
