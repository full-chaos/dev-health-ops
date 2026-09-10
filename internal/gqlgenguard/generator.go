package gqlgenguard

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
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
	Generate(ctx context.Context, workDir string) error
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
	// Env, when non-nil, replaces the child's environment entirely.
	Env []string
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
func (g *GoRunGenerator) Generate(ctx context.Context, workDir string) error {
	args := append([]string{"run", g.Package}, g.Args...)
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Dir = workDir
	cmd.Stdout = g.Stdout
	cmd.Stderr = g.Stderr
	if g.Env != nil {
		cmd.Env = g.Env
	} else {
		cmd.Env = os.Environ()
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
