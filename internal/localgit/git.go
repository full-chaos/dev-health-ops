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
	"regexp"
	"strings"
	"unicode/utf8"
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
	// Path.exists() follows symlinks: a dangling .git symlink does not exist.
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
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
	command.Env = r.env()
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return stdout.Bytes(), fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return stdout.Bytes(), nil
}

// env is the environment of every git subprocess: the process environment with
// GitPython's locale settings and, exactly as Repo.__init__ does, the repository's
// OWN git dir pinned over an ambient GIT_DIR / GIT_COMMON_DIR (and an absolute
// GIT_OBJECT_DIRECTORY), so a variable that names another repository cannot
// redirect the sync. Every other GIT_* variable reaches git unchanged, as it
// reaches GitPython's git.
func (r Repo) env() []string {
	env := append(os.Environ(), "LANGUAGE=C", "LC_ALL=C")
	gitDir, ok := r.gitDir()
	if !ok {
		return env
	}
	if common, set := os.LookupEnv("GIT_COMMON_DIR"); set {
		if abs, err := filepath.Abs(common); err == nil {
			common = abs
		}
		env = append(env, "GIT_DIR="+gitDir, "GIT_COMMON_DIR="+common)
	} else if _, set := os.LookupEnv("GIT_DIR"); set {
		env = append(env, "GIT_DIR="+gitDir)
	}
	if object, set := os.LookupEnv("GIT_OBJECT_DIRECTORY"); set {
		if abs, err := filepath.Abs(object); err == nil {
			object = abs
		}
		env = append(env, "GIT_OBJECT_DIRECTORY="+object)
	}
	return env
}

// gitDir is Repo.git_dir for a working tree, discovered exactly like Repo.__init__
// does for `<root>/.git`: `.git` itself when is_git_dir accepts it, else the target
// of a `gitdir: <path>` file (a regular file of at most 1 MiB; a relative path is
// relative to the directory holding it) when that is a git directory. false is
// InvalidGitRepositoryError (or WorkTreeRepositoryUnsupported): the run cannot
// construct the repository object.
func (r Repo) gitDir() (string, bool) {
	dotgit := filepath.Join(r.Root, ".git")
	if isGitDir(dotgit) {
		return dotgit, true
	}
	info, err := os.Stat(dotgit)
	if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
		return "", false
	}
	data, err := os.ReadFile(dotgit)
	if err != nil || len(data) != int(info.Size()) || !utf8.Valid(data) {
		return "", false
	}
	content := strings.TrimRight(string(data), "\r\n")
	if len(content) < 9 || !strings.HasPrefix(content, "gitdir: ") {
		return "", false
	}
	target := content[8:]
	if !filepath.IsAbs(target) {
		target = filepath.Clean(filepath.Join(filepath.Dir(dotgit), target))
	}
	if !isGitDir(target) {
		return "", false
	}
	return target, true
}

var reHeadSHA = regexp.MustCompile(`^(?:[0-9A-Fa-f]{64}|[0-9A-Fa-f]{40})`)

// isGitDir is git.repo.fun.is_git_dir (setup.c's is_git_directory): the directory
// has a HEAD that is a `refs/...` symlink, a `ref: refs/...` file or a sha, an
// objects directory (GIT_OBJECT_DIRECTORY, as given and relative to the working
// directory, when the environment has one) and a refs directory in the common dir
// (GIT_COMMON_DIR as given, else the `commondir` file resolved against the directory,
// else the directory itself). An empty GIT_COMMON_DIR rejects every directory.
func isGitDir(dir string) bool {
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return false
	}
	head := filepath.Join(dir, "HEAD")
	validHead := false
	if info, err := os.Lstat(head); err == nil && info.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(head)
		validHead = err == nil && strings.HasPrefix(target, "refs/")
	} else if file, err := os.Open(head); err == nil {
		buf := make([]byte, 256)
		n, _ := file.Read(buf)
		_ = file.Close()
		buf = buf[:n]
		validHead = (bytes.HasPrefix(buf, []byte("ref:")) && bytes.HasPrefix(bytes.TrimLeft(buf[4:], " \t\n\r\x0b\x0c"), []byte("refs/"))) || reHeadSHA.Match(buf)
	}
	common, set := os.LookupEnv("GIT_COMMON_DIR")
	if set && common == "" {
		return false
	}
	if !set {
		commondirFile := filepath.Join(dir, "commondir")
		data, err := os.ReadFile(commondirFile)
		switch {
		case err == nil:
			common = strings.TrimRight(string(data), "\r\n")
			if common == "" || !utf8.Valid(data) {
				return false
			}
			resolved, err := filepath.EvalSymlinks(filepath.Join(dir, common))
			if err != nil {
				resolved = filepath.Clean(filepath.Join(dir, common)) // realpath(strict=False)
			}
			common = resolved
		case errors.Is(err, os.ErrNotExist):
			if _, lerr := os.Lstat(commondirFile); lerr == nil {
				return false // a dangling commondir symlink
			}
			common = dir
		default:
			return false
		}
	}
	objects, set := os.LookupEnv("GIT_OBJECT_DIRECTORY")
	if !set {
		objects = filepath.Join(common, "objects")
	}
	if validHead && isDirectory(objects) && isDirectory(filepath.Join(common, "refs")) {
		return true
	}
	return false
}

func isDirectory(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
