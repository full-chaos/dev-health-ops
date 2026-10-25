package affected

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The reach store (CHAOS-11421): `plan-reads --reach --reach-cache DIR` keeps the rows reachRows computed for a tree under
// DIR/reach-<key>.tsv, written only after a computed run found no defect. The key hashes the content of every input of
// the computation: each Go file, go.mod/go.sum/go.work, every other file in a directory that holds a Go file (embedded
// files, the generated tables, cgo and assembly sources), every file under a //go:embed path, the build tag sets, and the
// toolchain environment. A change outside those (docs, workflows, web) leaves the key alone. A stored result is still
// compared with reach.tsv like a computed one, and any doubt (no git, unreadable or foreign store) recomputes.
const reachCacheSchema = "reach-cache-v1"

var embedLine = regexp.MustCompile(`(?m)^//go:embed[ \t]+(.+)$`)

func reachStoreFile(dir, key string) string { return filepath.Join(dir, "reach-"+key+".tsv") }

// reachKey is the hex sha256 of every input of reachRows under root.
func reachKey(root string) (string, error) {
	if _, err := os.Stat(filepath.Join(root, ".git")); err != nil {
		return "", errors.New("no .git: the inputs of the call graph cannot be listed")
	}
	listed, err := exec.Command("git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard").Output()
	if err != nil {
		return "", fmt.Errorf("git ls-files: %w", err)
	}
	var files []string
	for _, f := range strings.Split(string(listed), "\x00") {
		if f == "" {
			continue
		}
		if fi, err := os.Stat(filepath.Join(root, filepath.FromSlash(f))); err == nil && fi.Mode().IsRegular() {
			files = append(files, f)
		}
	}
	sort.Strings(files)
	goDirs := map[string]bool{}
	embedDirs := map[string]bool{}
	for _, f := range files {
		if !strings.HasSuffix(f, ".go") {
			continue
		}
		goDirs[path.Dir(f)] = true
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f)))
		if err != nil {
			return "", err
		}
		for _, m := range embedLine.FindAllSubmatch(src, -1) {
			for _, field := range strings.Fields(string(m[1])) {
				p := strings.Trim(field, "\"`")
				p = strings.TrimPrefix(p, "all:")
				if i := strings.IndexAny(p, "*?[\\"); i >= 0 {
					p = p[:i]
					if j := strings.LastIndex(p, "/"); j >= 0 {
						p = p[:j+1]
					} else {
						p = ""
					}
				}
				embedDirs[path.Join(path.Dir(f), p)] = true
			}
		}
	}
	input := func(f string) bool {
		base := path.Base(f)
		switch {
		case strings.HasSuffix(f, ".go"), base == "go.mod", base == "go.sum", base == "go.work", base == "go.work.sum":
			return true
		case goDirs[path.Dir(f)]:
			return true
		}
		for d := range embedDirs {
			if f == d || strings.HasPrefix(f, strings.TrimSuffix(d, "/")+"/") {
				return true
			}
		}
		return false
	}
	h := sha256.New()
	fmt.Fprintf(h, "%s\nrun-tags %v\n", reachCacheSchema, reachTags)
	env, err := goEnvLine(root)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(h, "go-env %s\n", env)
	for _, f := range files {
		if !input(f) {
			continue
		}
		b, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(f)))
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(b)
		fmt.Fprintf(h, "%s\x00%x\n", f, sum)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// goEnvLine is the toolchain environment the type-check and the build constraints depend on.
func goEnvLine(root string) (string, error) {
	cmd := exec.Command("go", "env", "GOVERSION", "GOOS", "GOARCH", "CGO_ENABLED", "GOEXPERIMENT", "GOFLAGS", "GOWORK", "GOTOOLCHAIN")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go env: %w", err)
	}
	return strings.Join(strings.Split(strings.TrimSpace(string(out)), "\n"), "|"), nil
}

func reachStoreHeader(key string) string { return "# " + reachCacheSchema + " key " + key }

// loadReachStore returns the rows stored for key, or false when there is no usable store (absent, foreign header, unreadable).
func loadReachStore(dir, key string) (map[string]bool, bool) {
	b, err := os.ReadFile(reachStoreFile(dir, key))
	if err != nil {
		return nil, false
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) == 0 || lines[0] != reachStoreHeader(key) {
		return nil, false
	}
	rows := map[string]bool{}
	for _, l := range lines[1:] {
		if l != "" {
			rows[l] = true
		}
	}
	return rows, true
}

// writeReachStore writes the rows for key atomically.
func writeReachStore(dir, key string, rows map[string]bool) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString(reachStoreHeader(key) + "\n")
	for _, r := range sortedKeys(rows) {
		b.WriteString(r + "\n")
	}
	tmp, err := os.CreateTemp(dir, "reach-tmp-*")
	if err != nil {
		return err
	}
	if _, err := tmp.WriteString(b.String()); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return err
	}
	return os.Rename(tmp.Name(), reachStoreFile(dir, key))
}
