package affected

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/fs"
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
// the computation: every file the go command selects (go list, IgnoredGoFiles included), every Go file on disk, go.mod and
// go.sum, every other file in a directory that holds a Go file, every //go:embed path, the build tag sets, and the whole
// `go env` minus volatileGoEnv. A change outside those (docs, workflows, web) leaves the key alone. The header binds the
// rows to the key with a digest; the digest catches accident and is NOT authentication: CI therefore lets only pull
// requests read a store (--reach-cache-fresh on every other event), and the stored result is compared with reach.tsv
// like a computed one. Any doubt (a go command failing, a replace outside the repository, a foreign store) recomputes.
const reachCacheSchema = "reach-cache-v2"

var embedLine = regexp.MustCompile(`(?m)^//go:embed[ \t]+(.+)$`)

func reachStoreFile(dir, key string) string { return filepath.Join(dir, "reach-"+key+".tsv") }

// listFields are the file lists of `go list -json` that select what a build reads.
const listFields = "ImportPath,Dir,GoFiles,CgoFiles,CFiles,CXXFiles,MFiles,HFiles,FFiles,SFiles,SwigFiles,SwigCXXFiles,SysoFiles," +
	"IgnoredGoFiles,IgnoredOtherFiles,EmbedFiles,TestGoFiles,XTestGoFiles,TestEmbedFiles,XTestEmbedFiles"

type listedPackage struct {
	ImportPath, Dir                                                                   string
	GoFiles, CgoFiles, CFiles, CXXFiles, MFiles, HFiles, FFiles, SFiles               []string
	SwigFiles, SwigCXXFiles, SysoFiles, IgnoredGoFiles, IgnoredOtherFiles, EmbedFiles []string
	TestGoFiles, XTestGoFiles, TestEmbedFiles, XTestEmbedFiles                        []string
}

func (p listedPackage) lists() [][]string {
	return [][]string{p.GoFiles, p.CgoFiles, p.CFiles, p.CXXFiles, p.MFiles, p.HFiles, p.FFiles, p.SFiles, p.SwigFiles, p.SwigCXXFiles,
		p.SysoFiles, p.IgnoredGoFiles, p.IgnoredOtherFiles, p.EmbedFiles, p.TestGoFiles, p.XTestGoFiles, p.TestEmbedFiles, p.XTestEmbedFiles}
}

// reachKey is the hex sha256 of every input of reachRows under root. An error means the inputs cannot be bounded (a
// local replace outside the repository, a failing go command): the caller computes fresh and stores nothing.
func reachKey(root string) (string, error) {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	if err := checkLocalReplaces(realRoot); err != nil {
		return "", err
	}
	var files []string
	err = filepath.WalkDir(realRoot, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			if fi, err := os.Stat(p); err != nil || !fi.Mode().IsRegular() {
				return nil
			}
		}
		rel, err := filepath.Rel(realRoot, p)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Strings(files)
	goDirs := map[string]bool{}
	embedDirs := map[string]bool{}
	for _, f := range files {
		if !strings.HasSuffix(f, ".go") {
			continue
		}
		goDirs[path.Dir(f)] = true
		src, err := os.ReadFile(filepath.Join(realRoot, filepath.FromSlash(f)))
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
	env, err := goEnvLine(realRoot)
	if err != nil {
		return "", err
	}
	fmt.Fprintf(h, "go-env %s\n", env)
	hashFile := func(rel string) error {
		b, err := os.ReadFile(filepath.Join(realRoot, filepath.FromSlash(rel)))
		if err != nil {
			fmt.Fprintf(h, "%s\x00missing\n", rel)
			return nil
		}
		sum := sha256.Sum256(b)
		fmt.Fprintf(h, "%s\x00%x\n", rel, sum)
		return nil
	}
	for _, f := range files {
		if input(f) {
			if err := hashFile(f); err != nil {
				return "", err
			}
		}
	}
	// The files the go command selects for each tag set (IgnoredGoFiles included), by name and content.
	for _, tags := range reachTags {
		pkgs, err := listPackages(realRoot, tags)
		if err != nil {
			return "", err
		}
		for _, p := range pkgs {
			dir, err := filepath.Rel(realRoot, p.Dir)
			if err != nil || strings.HasPrefix(dir, "..") {
				return "", fmt.Errorf("package %s lies outside the repository", p.ImportPath)
			}
			dir = filepath.ToSlash(dir)
			fmt.Fprintf(h, "list %s %s\n", strings.Join(tags, ","), dir)
			for i, names := range p.lists() {
				fmt.Fprintf(h, "  field %d %s\n", i, strings.Join(names, " "))
				for _, n := range names {
					if err := hashFile(path.Join(dir, n)); err != nil {
						return "", err
					}
				}
			}
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// listPackages runs `go list -e -json` over the module for one tag set.
func listPackages(root string, tags []string) ([]listedPackage, error) {
	cmd := exec.Command("go", "list", "-e", "-tags="+strings.Join(tags, ","), "-json="+listFields, "./...")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}
	var pkgs []listedPackage
	dec := json.NewDecoder(bytes.NewReader(out))
	for dec.More() {
		var p listedPackage
		if err := dec.Decode(&p); err != nil {
			return nil, fmt.Errorf("go list output: %w", err)
		}
		pkgs = append(pkgs, p)
	}
	sort.Slice(pkgs, func(i, j int) bool { return pkgs[i].Dir < pkgs[j].Dir })
	return pkgs, nil
}

// checkLocalReplaces refuses a go.mod replace that points at a directory outside the repository: its sources are inputs the
// key does not hash.
func checkLocalReplaces(root string) error {
	cmd := exec.Command("go", "mod", "edit", "-json")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("go mod edit: %w", err)
	}
	var mod struct {
		Replace []struct {
			Old, New struct{ Path, Version string }
		}
	}
	if err := json.Unmarshal(out, &mod); err != nil {
		return fmt.Errorf("go mod edit output: %w", err)
	}
	for _, r := range mod.Replace {
		if r.New.Version != "" {
			continue
		}
		target := r.New.Path
		if !filepath.IsAbs(target) {
			target = filepath.Join(root, filepath.FromSlash(target))
		}
		if resolved, err := filepath.EvalSymlinks(target); err == nil {
			target = resolved
		}
		if rel, err := filepath.Rel(root, target); err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fmt.Errorf("go.mod replaces %s with %s outside the repository", r.Old.Path, r.New.Path)
		}
	}
	return nil
}

// volatileGoEnv names the `go env` keys left out of the key; every other key counts. Each is a path or a network or
// telemetry setting that moves between runners without changing which files the build selects or how it types them.
var volatileGoEnv = map[string]string{
	"GOCACHE":        "build cache location",
	"GOCACHEPROG":    "build cache backend program",
	"GOMODCACHE":     "module cache location; go.sum pins the module content",
	"GOTMPDIR":       "temp directory",
	"GOENV":          "location of the go env file",
	"GOPATH":         "workspace location",
	"GOROOT":         "toolchain location; GOVERSION names the toolchain",
	"GOTOOLDIR":      "path under GOROOT",
	"GOMOD":          "absolute path of go.mod; go.mod is hashed",
	"GOGCCFLAGS":     "holds a temp directory path",
	"GOTELEMETRY":    "telemetry mode",
	"GOTELEMETRYDIR": "telemetry location",
	"GOPROXY":        "module download source; go.sum pins the content",
	"GOSUMDB":        "checksum database; go.sum pins the content",
	"GONOSUMDB":      "checksum database scope",
	"GONOPROXY":      "module download scope",
	"GOPRIVATE":      "module download scope",
	"GOAUTH":         "download credentials",
	"GOBIN":          "install location",
}

// goEnvLine is the whole `go env -json` minus volatileGoEnv, as sorted key=value pairs.
func goEnvLine(root string) (string, error) {
	cmd := exec.Command("go", "env", "-json")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go env: %w", err)
	}
	env := map[string]string{}
	if err := json.Unmarshal(out, &env); err != nil {
		return "", fmt.Errorf("go env output: %w", err)
	}
	var parts []string
	for _, k := range sortedKeys(env) {
		if _, skip := volatileGoEnv[k]; skip {
			continue
		}
		parts = append(parts, k+"="+env[k])
	}
	return strings.Join(parts, "|"), nil
}

func reachRowsText(rows map[string]bool) string {
	var b strings.Builder
	for _, r := range sortedKeys(rows) {
		b.WriteString(r + "\n")
	}
	return b.String()
}

// reachStoreHeader binds the rows to the key: a store whose rows were edited after it was written fails the digest.
func reachStoreHeader(key, rows string) string {
	sum := sha256.Sum256([]byte(rows))
	return "# " + reachCacheSchema + " key " + key + " rows-sha256 " + hex.EncodeToString(sum[:])
}

// loadReachStore returns the rows stored for key. ok is false when there is no usable store; reason names why a file that
// exists was refused (the empty reason: no file).
func loadReachStore(dir, key string) (rows map[string]bool, reason string, ok bool) {
	b, err := os.ReadFile(reachStoreFile(dir, key))
	if err != nil {
		return nil, "", false
	}
	head, body, found := strings.Cut(string(b), "\n")
	if !found || head != reachStoreHeader(key, body) {
		return nil, "its header does not bind its rows to this key", false
	}
	rows = map[string]bool{}
	for _, l := range strings.Split(body, "\n") {
		if l != "" {
			rows[l] = true
		}
	}
	return rows, "", true
}

// writeReachStore writes the rows for key atomically.
func writeReachStore(dir, key string, rows map[string]bool) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	body := reachRowsText(rows)
	var b strings.Builder
	b.WriteString(reachStoreHeader(key, body) + "\n" + body)
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
