package frozengoldens

import (
	"bufio"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// This file is the guard's only way to read anything: a file or directory of the tree under root, an object at the
// base commit, the embedded manifest, or a git diff of guarded paths. Every read is recorded, and digest hashes
// exactly the recorded set, so the exemption's key is the guard's own read set (TestEveryGuardReadGoesThroughTheRecorder
// holds the other sources of the package to that).

//go:embed frozen_goldens.tsv
var manifestFile []byte

// Load parses the embedded manifest.
func Load() (Manifest, error) { return Parse(strings.NewReader(string(manifestFile))) }

// MergeBase is the commit this change branched from: the merge base of HEAD and origin/main in dir.
func MergeBase(dir string) (string, error) {
	out, err := exec.Command("git", "-C", dir, "merge-base", "HEAD", "origin/main").Output()
	if err != nil {
		return "", fmt.Errorf("no merge base of HEAD and origin/main: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}

const (
	atHead = "HEAD"
	atBase = "base"
)

// read is one recorded read: a file, a directory listing (dir), the entries of a directory matching a pattern (glob)
// or every file under a path (tree), at HEAD (the tree under root) or at the base commit.
type read struct{ rev, kind, path string }

type reads struct {
	root, base string
	seen       map[read]bool
}

func newReads(root, base string) *reads { return &reads{root: root, base: base, seen: map[read]bool{}} }

func (r *reads) record(rev, kind, rel string) { r.seen[read{rev, kind, rel}] = true }

// manifest records the manifest the guard runs with: the embedded copy is the file at ManifestPath.
func (r *reads) manifest() { r.record(atHead, "file", ManifestPath) }

func (r *reads) file(rel string) ([]byte, error) {
	r.record(atHead, "file", rel)
	return os.ReadFile(filepath.Join(r.root, filepath.FromSlash(rel)))
}

func (r *reads) dir(rel string) ([]os.DirEntry, error) {
	r.record(atHead, "dir", rel)
	return os.ReadDir(filepath.Join(r.root, filepath.FromSlash(rel)))
}

// glob lists the entries of dir whose names match pattern, as slash paths relative to root.
func (r *reads) glob(dir, pattern string) []string {
	r.record(atHead, "glob", dir+"/"+pattern)
	files, _ := filepath.Glob(filepath.Join(r.root, filepath.FromSlash(dir), pattern))
	var rels []string
	for _, file := range files {
		rel, _ := filepath.Rel(r.root, file)
		rels = append(rels, filepath.ToSlash(rel))
	}
	return rels
}

// fileAtBase is rel at the base commit; ok is false when the base does not hold it.
func (r *reads) fileAtBase(rel string) (data []byte, ok bool, err error) {
	r.record(atBase, "file", rel)
	out, err := exec.Command("git", "-C", r.root, "show", r.base+":"+rel).Output()
	if err == nil {
		return out, true, nil
	}
	if exec.Command("git", "-C", r.root, "cat-file", "-e", r.base+":"+rel).Run() != nil {
		return nil, false, nil
	}
	return nil, false, fmt.Errorf("git show %s:%s: %w", r.base, rel, err)
}

// diff lists the paths among paths that differ between base and HEAD under filter (a git --diff-filter).
func (r *reads) diff(filter string, paths []string) ([]string, error) {
	for _, rel := range paths {
		r.record(atBase, "tree", rel)
		r.record(atHead, "tree", rel)
	}
	args := append([]string{"-C", r.root, "diff", "--name-only", "--diff-filter=" + filter, r.base + "...HEAD", "--"}, paths...)
	out, err := exec.Command("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, args...)...).Output()
	if err != nil {
		return nil, fmt.Errorf("git diff %s...HEAD: %w", r.base, err)
	}
	var changed []string
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			changed = append(changed, line)
		}
	}
	return changed, nil
}

func (r *reads) commit(rev string) string {
	if rev == atBase {
		return r.base
	}
	return "HEAD"
}

// lsTree lists the paths git holds at rev under the given pathspecs, recursive or one level down.
func (r *reads) lsTree(rev string, recursive bool, specs []string) ([]string, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	args := []string{"-C", r.root, "ls-tree", "-z", "--name-only", "--full-tree"}
	if recursive {
		args = append(args, "-r")
	}
	out, err := exec.Command("git", append([]string{"-c", "maintenance.auto=false", "-c", "gc.auto=0"}, append(append(args, r.commit(rev), "--"), specs...)...)...).Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-tree %s: %w", rev, err)
	}
	var paths []string
	for _, p := range strings.Split(string(out), "\x00") {
		if p != "" {
			paths = append(paths, p)
		}
	}
	return paths, nil
}

// digest is the sha256 over "<rev>\t<path>\t<object type>\t<sha256 of the object>\n" (or "<rev>\t<path>\tabsent\n")
// for every path the recorded reads cover, sorted. A recorded directory, glob or tree covers the entries git holds
// there at that commit, so a file added, deleted or renamed among them changes the digest, as does any byte of one.
func (r *reads) digest() (string, error) {
	entries := map[read]bool{}
	for _, rev := range []string{atHead, atBase} {
		var trees, dirs []string
		globs := map[string][]string{}
		for rec := range r.seen {
			if rec.rev != rev {
				continue
			}
			switch rec.kind {
			case "file":
				entries[read{rev: rev, path: rec.path}] = true
			case "tree":
				trees = append(trees, rec.path)
			case "dir":
				dirs = append(dirs, rec.path+"/")
			case "glob":
				dir, pattern := path.Split(rec.path)
				globs[dir] = append(globs[dir], pattern)
			}
		}
		listed, err := r.lsTree(rev, true, trees)
		if err != nil {
			return "", err
		}
		inDirs, err := r.lsTree(rev, false, dirs)
		if err != nil {
			return "", err
		}
		for _, p := range append(listed, inDirs...) {
			entries[read{rev: rev, path: p}] = true
		}
		for dir, patterns := range globs {
			inDir, err := r.lsTree(rev, false, []string{dir})
			if err != nil {
				return "", err
			}
			for _, p := range inDir {
				for _, pattern := range patterns {
					if ok, _ := path.Match(pattern, path.Base(p)); ok {
						entries[read{rev: rev, path: p}] = true
					}
				}
			}
		}
	}
	sorted := make([]read, 0, len(entries))
	for entry := range entries {
		sorted = append(sorted, entry)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].rev != sorted[j].rev {
			return sorted[i].rev < sorted[j].rev
		}
		return sorted[i].path < sorted[j].path
	})
	var input strings.Builder
	for _, entry := range sorted {
		input.WriteString(r.commit(entry.rev) + ":" + entry.path + "\n")
	}
	cmd := exec.Command("git", "-C", r.root, "cat-file", "--batch")
	cmd.Stdin = strings.NewReader(input.String())
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git cat-file --batch: %w", err)
	}
	reader := bufio.NewReader(strings.NewReader(string(out)))
	digest := sha256.New()
	for _, entry := range sorted {
		header, err := reader.ReadString('\n')
		if err != nil {
			return "", fmt.Errorf("git cat-file --batch: %s: %w", entry.path, err)
		}
		fields := strings.Fields(header)
		if len(fields) == 2 && fields[1] == "missing" {
			fmt.Fprintf(digest, "%s\t%s\tabsent\n", entry.rev, entry.path)
			continue
		}
		if len(fields) != 3 {
			return "", fmt.Errorf("git cat-file --batch: %s: unexpected header %q", entry.path, header)
		}
		size, err := strconv.Atoi(fields[2])
		if err != nil {
			return "", fmt.Errorf("git cat-file --batch: %s: %w", entry.path, err)
		}
		body := make([]byte, size+1)
		if _, err := io.ReadFull(reader, body); err != nil {
			return "", fmt.Errorf("git cat-file --batch: %s: %w", entry.path, err)
		}
		sum := sha256.Sum256(body[:size])
		fmt.Fprintf(digest, "%s\t%s\t%s\t%s\n", entry.rev, entry.path, fields[1], hex.EncodeToString(sum[:]))
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}
