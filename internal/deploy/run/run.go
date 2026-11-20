// Package run is the exec seam of `dho deploy`: a Runner takes a fixed argv
// (never a shell string), the real Runner starts only allow-listed tools with
// the fixed child env, a Step turns one exec into a typed result, and State
// keeps the per-RUN_TAG markers and keys. It is a correctness seam, not a
// sandbox (pm threat model: no OS boundary against the agent uid).
package run

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Exit codes of a failed gate; equal to the internal/cli contract.
const (
	CodeStop    = 1
	CodeRefused = 3
)

// Error is a gate result: Stop (a step or gate failed after the run started)
// or Refused (inputs, order, age; nothing was written to the cluster).
type Error struct {
	Code int
	Msg  string
}

func (e *Error) Error() string { return e.Msg }

// Stop is a failed step or gate (exit 1).
func Stop(format string, a ...any) *Error {
	return &Error{Code: CodeStop, Msg: fmt.Sprintf(format, a...)}
}

// Refuse is a refusal before any cluster write (exit 3).
func Refuse(format string, a ...any) *Error {
	return &Error{Code: CodeRefused, Msg: fmt.Sprintf(format, a...)}
}

// Code is the exit code of err: 0 for nil, the gate code for an *Error, and
// Stop for any other error.
func Code(err error) int {
	if err == nil {
		return 0
	}
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return CodeStop
}

// Cmd is one exec: argv[0] is a bare tool name, the rest are its words.
// Stdin nil = /dev/null.
type Cmd struct {
	Argv  []string
	Stdin []byte
}

// Output is what one exec gave back. Both streams stay in memory; a Step
// writes only its typed record to disk.
type Output struct {
	Stdout []byte
	Stderr []byte
	RC     int
}

// Runner runs one Cmd. An error means it did not run to an exit code.
type Runner interface {
	Run(ctx context.Context, c Cmd) (Output, error)
}

const fixedPATH = "/usr/sbin:/usr/bin:/sbin:/bin"

// FixedEnv is the whole child env (the kit's `env -i` + CHILD_ENV). Nothing of
// the caller's env reaches a child.
func FixedEnv() []string {
	return []string{
		"PATH=" + fixedPATH,
		"HOME=/home/ubuntu",
		"LANG=C.UTF-8",
		"LC_ALL=C",
		"KUBECONFIG=/etc/rancher/k3s/k3s.yaml",
	}
}

// Tools is the closed set of binaries the real Runner starts.
var Tools = []string{"git", "helm", "kubectl"}

// Exec is the real Runner. Env is the whole child env; argv[0] must be one of
// Tools and is resolved on the PATH of Env (never the caller's PATH). The
// child runs in "/".
type Exec struct {
	Env []string
}

// Real is the prod Runner: the fixed env.
func Real() Exec { return Exec{Env: FixedEnv()} }

// command builds the exec.Cmd of one closed-set binary: each case names its
// binary as a literal, the words come from the Step's argv. Any other name is
// refused before anything starts.
func command(ctx context.Context, name string, args []string) (*exec.Cmd, error) {
	switch name {
	case "git":
		return exec.CommandContext(ctx, "git", args...), nil
	case "helm":
		return exec.CommandContext(ctx, "helm", args...), nil
	case "kubectl":
		return exec.CommandContext(ctx, "kubectl", args...), nil
	}
	return nil, Refuse("exec: argv[0] is not one of git, helm, kubectl")
}

// Run starts c.Argv with no shell.
func (e Exec) Run(ctx context.Context, c Cmd) (Output, error) {
	if len(c.Argv) == 0 {
		return Output{}, Refuse("exec: empty argv")
	}
	name := c.Argv[0]
	cmd, err := command(ctx, name, c.Argv[1:])
	if err != nil {
		return Output{}, err
	}
	path, err := lookPath(name, e.Env)
	if err != nil {
		return Output{}, Stop("exec: %s is not on the fixed PATH", name)
	}
	// The binary is the one on the PATH of Env, not the one exec found on the
	// caller's PATH (or failed to find: that error is dropped with it).
	cmd.Path, cmd.Err = path, nil
	cmd.Env = append([]string{}, e.Env...)
	cmd.Dir = "/"
	if c.Stdin != nil {
		cmd.Stdin = bytes.NewReader(c.Stdin)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 10 * time.Second
	err = cmd.Run()
	out := Output{Stdout: stdout.Bytes(), Stderr: stderr.Bytes()}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		return out, nil
	case errors.As(err, &exitErr) && exitErr.Exited():
		out.RC = exitErr.ExitCode()
		return out, nil
	default:
		return out, Stop("exec: %s did not run to an exit code", name)
	}
}

// lookPath resolves a bare name on the PATH of env, never os PATH.
func lookPath(name string, env []string) (string, error) {
	if name == "" || strings.ContainsRune(name, '/') {
		return "", errors.New("not a bare name")
	}
	var dirs string
	for _, kv := range env {
		if v, ok := strings.CutPrefix(kv, "PATH="); ok {
			dirs = v
		}
	}
	for _, d := range filepath.SplitList(dirs) {
		if !filepath.IsAbs(d) {
			continue
		}
		p := filepath.Join(d, name)
		if fi, err := os.Stat(p); err == nil && fi.Mode().IsRegular() && fi.Mode().Perm()&0o111 != 0 {
			return p, nil
		}
	}
	return "", errors.New("not found")
}

// Harden is the kit's `umask 077` + `cd /` for a run that writes state.
func Harden() error {
	syscall.Umask(0o077)
	return os.Chdir("/")
}
