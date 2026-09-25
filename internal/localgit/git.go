// Package localgit reads a local git working tree the way `dev-hops sync
// git|prs --provider local` does (processors/local.py), with exec-git: the
// git binary is the source, exactly the commands GitPython runs, and no go-git.
//
// Each function documents the GitPython behaviour it reproduces. The
// differential oracle (localgit_oracle_integration_test.go) runs the real
// Python producer against the same repository and compares the ClickHouse rows.
package localgit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// ErrNoRepository is `not (repo_root / ".git").exists()`: Python logs an error
// and returns, so the verb ends 0 having written nothing.
var ErrNoRepository = errors.New("no git repository found")

// Repo is one local working tree.
type Repo struct {
	// Root is Path(repo_path).resolve(): absolute, symlinks resolved.
	Root string
	// Git is the git binary; empty means "git" on PATH.
	Git string
}

// Open resolves path like pathlib.Path(path).resolve() and checks for .git.
func Open(path string) (Repo, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return Repo{}, err
	}
	root := absolute
	if resolved, err := filepath.EvalSymlinks(absolute); err == nil {
		root = resolved
	}
	if _, err := os.Lstat(filepath.Join(root, ".git")); err != nil {
		return Repo{Root: root}, fmt.Errorf("%w at %s", ErrNoRepository, root)
	}
	return Repo{Root: root}, nil
}

// Name is Path(repo_root).name.
func (r Repo) Name() string { return filepath.Base(r.Root) }

// run executes git in the working tree with GitPython's locale settings
// (LANGUAGE=C, LC_ALL=C: git's own messages are not read, but a decimal
// separator or quoting locale must not change the output shape).
func (r Repo) run(ctx context.Context, args ...string) ([]byte, error) {
	bin := r.Git
	if bin == "" {
		bin = "git"
	}
	command := exec.CommandContext(ctx, bin, args...)
	command.Dir = r.Root
	command.Env = append(os.Environ(), "LANGUAGE=C", "LC_ALL=C")
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}
