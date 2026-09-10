package gqlgenguard

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
)

// Entry is one file in a tree snapshot. Directories are not recorded: an empty
// directory is not an output and its presence or absence is not drift.
type Entry struct {
	// Path is slash-separated and relative to the snapshot root.
	Path string
	// Digest is the hex sha256 of a regular file's bytes, or "symlink:<target>"
	// for a symbolic link. The prefix makes a link and a file that happens to
	// contain the target text distinguishable.
	Digest string
	// Mode carries only the permission bits and the type bits.
	Mode fs.FileMode
	// Size is the byte length of a regular file, and 0 for a symlink.
	Size int64
}

// Snapshot is a content-addressed view of a tree, keyed by slash-separated
// relative path.
type Snapshot map[string]Entry

// skipVCS is the one directory never copied or snapshotted. It is not part of
// the module for the generator's purposes and copying it would multiply the
// cost of every run for nothing.
func skipVCS(rel string) bool {
	return rel == ".git" || strings.HasPrefix(rel, ".git/")
}

// TakeSnapshot digests every regular file and symlink under root.
//
// Every read goes through the root handle, so a symlink pointing outside the
// tree cannot be followed out of it: the operating system refuses the
// traversal and the error names the path.
func TakeSnapshot(root *os.Root, skip func(rel string) bool) (Snapshot, error) {
	snap := Snapshot{}
	err := fs.WalkDir(root.FS(), ".", func(rel string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if skip != nil && skip(rel) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := root.Lstat(rel)
		if err != nil {
			return fmt.Errorf("stat %q: %w", rel, err)
		}
		switch {
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := root.Readlink(rel)
			if err != nil {
				return fmt.Errorf("read symlink %q: %w", rel, err)
			}
			snap[rel] = Entry{Path: rel, Digest: "symlink:" + target, Mode: info.Mode()}
		case info.Mode().IsRegular():
			digest, size, err := digestFile(root, rel)
			if err != nil {
				return err
			}
			snap[rel] = Entry{Path: rel, Digest: digest, Mode: info.Mode().Perm(), Size: size}
		default:
			return fmt.Errorf("refusing tree containing %q: not a regular file, directory or symlink (%s)", rel, info.Mode())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return snap, nil
}

func digestFile(root *os.Root, rel string) (string, int64, error) {
	f, err := root.Open(rel)
	if err != nil {
		return "", 0, fmt.Errorf("open %q: %w", rel, err)
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("read %q: %w", rel, err)
	}
	return hex.EncodeToString(h.Sum(nil)), n, nil
}

// CopyTree copies every file, directory and symlink from src to dst.
//
// Both ends are *os.Root handles, so neither a symlink nor a crafted name can
// read or write outside the two trees. A symlink is REPRODUCED, target text and
// all, including one whose target is absolute or leaves the tree: os.Root
// accepts creating such a link and refuses every read THROUGH it, so the copy
// is a faithful reproduction and the confinement is the operating system's
// rather than a rule about which targets are allowed. Refusing them instead
// would make the guard unusable in any checkout that happens to contain a
// virtual environment or a toolchain cache.
func CopyTree(src, dst *os.Root, skip func(rel string) bool) error {
	return fs.WalkDir(src.FS(), ".", func(rel string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if skip != nil && skip(rel) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		info, err := src.Lstat(rel)
		if err != nil {
			return fmt.Errorf("stat %q: %w", rel, err)
		}
		switch {
		case info.IsDir():
			if err := dst.MkdirAll(rel, info.Mode().Perm()|0o700); err != nil {
				return fmt.Errorf("create directory %q: %w", rel, err)
			}
			return nil
		case info.Mode()&fs.ModeSymlink != 0:
			target, err := src.Readlink(rel)
			if err != nil {
				return fmt.Errorf("read symlink %q: %w", rel, err)
			}
			if err := dst.MkdirAll(path.Dir(rel), 0o755); err != nil {
				return fmt.Errorf("create directory for %q: %w", rel, err)
			}
			if err := dst.Symlink(target, rel); err != nil {
				return fmt.Errorf("create symlink %q -> %q: %w", rel, target, err)
			}
			return nil
		case info.Mode().IsRegular():
			if err := dst.MkdirAll(path.Dir(rel), 0o755); err != nil {
				return fmt.Errorf("create directory for %q: %w", rel, err)
			}
			return copyFile(src, dst, rel, info.Mode().Perm())
		default:
			return fmt.Errorf("refusing to copy %q: not a regular file, directory or symlink (%s)", rel, info.Mode())
		}
	})
}

func copyFile(src, dst *os.Root, rel string, perm fs.FileMode) error {
	in, err := src.Open(rel)
	if err != nil {
		return fmt.Errorf("open %q: %w", rel, err)
	}
	defer in.Close()
	out, err := dst.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return fmt.Errorf("create %q: %w", rel, err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return fmt.Errorf("write %q: %w", rel, err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close %q: %w", rel, err)
	}
	return nil
}

// ReadThroughRoot reads a file inside root. It exists so every read in this
// package goes through a root handle rather than a bare os.ReadFile with a
// joined path, which is what makes "../" and absolute paths impossible here.
func ReadThroughRoot(root *os.Root, rel string) ([]byte, error) {
	f, err := root.Open(rel)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(f)
}

// tmpCounter makes the temporary name of an atomic write unique within one
// process without depending on a random source.
var tmpCounter uint64

// AtomicWrite replaces rel with data, through root, so that a reader either
// sees the whole previous file or the whole new one.
//
// The temporary lands in the destination's own directory, so the rename is
// within one filesystem and cannot fail with a cross-device error. Both the
// temporary path and the destination go through the root handle: a rel that
// escapes is refused by the operating system.
func AtomicWrite(root *os.Root, rel string, data []byte, perm fs.FileMode) (err error) {
	dir := path.Dir(rel)
	tmpCounter++
	tmp := path.Join(dir, fmt.Sprintf(".gqlgen-guard-%d-%d.tmp", os.Getpid(), tmpCounter))

	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return fmt.Errorf("create temporary for %q: %w", rel, err)
	}
	defer func() {
		if err != nil {
			_ = root.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("write temporary for %q: %w", rel, err)
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync temporary for %q: %w", rel, err)
	}
	if err = f.Close(); err != nil {
		return fmt.Errorf("close temporary for %q: %w", rel, err)
	}
	if err = root.Rename(tmp, rel); err != nil {
		return fmt.Errorf("replace %q: %w", rel, err)
	}
	return nil
}

// errNotExist reports whether err is a "no such file" from a root handle.
func errNotExist(err error) bool { return errors.Is(err, fs.ErrNotExist) }
