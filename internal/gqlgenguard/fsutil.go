package gqlgenguard

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
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
	return TakeSnapshotContext(context.Background(), root, skip)
}

// TakeSnapshotContext is TakeSnapshot that stops, returning the context's
// error, as soon as ctx is done -- so a signal that arrives while a large tree
// is being digested ends the run instead of waiting for the walk.
func TakeSnapshotContext(ctx context.Context, root *os.Root, skip func(rel string) bool) (Snapshot, error) {
	snap := Snapshot{}
	err := fs.WalkDir(root.FS(), ".", func(rel string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
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

// DroppedLink is a symbolic link in the source tree that CopyTree did not
// reproduce, and why.
type DroppedLink struct {
	// Path is slash-separated and relative to the source root.
	Path string
	// Reason is one of the dropped* constants.
	Reason string
	// Detail is the resolution error behind droppedUnresolved, kept so the
	// report says WHY a link did not resolve (escaped, dangling, looping), and
	// empty for droppedAbsolute.
	Detail string
}

// The reasons a link is dropped from the copy. They are the whole vocabulary:
// a link is reproduced, or it is dropped for exactly one of these.
const (
	droppedAbsolute   = "absolute target"
	droppedUnresolved = "does not resolve inside the module"
)

// CopyTree copies every file and directory from src to dst, and every symbolic
// link whose target is relative AND resolves, through src, to something that
// exists inside src. Every other link is NOT reproduced: its name is kept as an
// inert link to itself, and it is returned.
//
// Both ends are *os.Root handles, so neither a link nor a crafted name can make
// THIS function read or write outside the two trees. But the copy is handed to
// the generator, an ordinary child process the roots do not confine: a link
// reproduced in the copy is a link the generator follows. Round 1 of review
// proved it -- a schema symlinked from outside the module was read by the real
// CLI and landed in the generated output. So the copy carries only links that
// stay inside the module:
//
//   - an absolute target is dropped even when it names a path inside the
//     module, because in the copy it would name the REAL tree, not the copy;
//   - a relative target is kept only when src.Stat -- which follows the link
//     through the root and refuses any step outside it -- succeeds. A link that
//     escapes, even to come back in, dangles, or loops is dropped.
//
// A dropped link is not simply omitted. Its NAME stays in the copy as a link
// to itself -- an inert self-loop that resolves nowhere -- so any read, write
// or traversal of that name by the generator or the go command fails loudly
// with "too many levels of symbolic links" instead of finding nothing and
// carrying on. A schema that is a link out of the module therefore refuses the
// generation rather than silently vanishing from it.
//
// Dropping rather than refusing keeps the guard usable in a checkout that
// carries a virtual environment or a toolchain cache, both full of absolute
// links the generator never needed. Every dropped link is returned so the
// caller can report it: an input the generator needed and could not see must
// be explicable from the output.
//
// The walk checks ctx before every entry: the guard catches SIGINT/SIGTERM and
// turns them into a cancelled context, and a copy that never looked at it made
// those signals caught-and-ignored for as long as the copy ran.
func CopyTree(ctx context.Context, src, dst *os.Root, skip func(rel string) bool) ([]DroppedLink, error) {
	var dropped []DroppedLink
	err := fs.WalkDir(src.FS(), ".", func(rel string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
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
			if reason, detail := linkDropReason(src, rel, target); reason != "" {
				dropped = append(dropped, DroppedLink{Path: rel, Reason: reason, Detail: detail})
				// The inert stand-in: a relative link to its own name.
				if err := dst.Symlink(path.Base(rel), rel); err != nil {
					return fmt.Errorf("create inert stand-in for dropped link %q: %w", rel, err)
				}
				return nil
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
	if err != nil {
		return nil, err
	}
	return dropped, nil
}

// linkDropReason returns "" when the link at rel may be reproduced, and the
// reason it may not otherwise, with the resolution error as detail.
func linkDropReason(src *os.Root, rel, target string) (reason, detail string) {
	if filepath.IsAbs(target) || path.IsAbs(target) {
		return droppedAbsolute, ""
	}
	// Stat follows the whole chain through the root. It fails for a step
	// outside the root ("path escapes from parent"), a missing target, and a
	// loop alike -- and each of those is a link the copy must not carry, so
	// they share one reason rather than being told apart by error text.
	if _, err := src.Stat(rel); err != nil {
		return droppedUnresolved, err.Error()
	}
	return "", ""
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
