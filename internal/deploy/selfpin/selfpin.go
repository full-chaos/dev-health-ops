// Package selfpin checks that the running dho is the pinned binary at the
// pinned path. It proves the file, not the host: the agent uid can edit both
// the binary and the pin (pm threat model: no OS boundary; a correctness
// check, not a sandbox).
package selfpin

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// PinPath is the pin file on the prod node: one line
	// `<sha256 64 hex> <commit 40 hex> <absolute install path>`.
	PinPath = "/home/ubuntu/.config/dev-health/dho-deploy.pin"
	// SelfExe names the running image on Linux.
	SelfExe = "/proc/self/exe"

	maxPinBytes = 4096
)

var pinLine = regexp.MustCompile(`^([0-9a-f]{64}) ([0-9a-f]{40}) (/[A-Za-z0-9._/-]{1,255})\n?$`)

// Result is what Check found. SHA256 is "-" when the running image could not
// be read.
type Result struct {
	SHA256  string
	ExePath string
	Commit  string
	OK      bool
	Reason  string
}

// Check compares the running image (selfExe, read through the link) with the
// pin file: same sha256, same commit as the build (commit), and the image's
// path is the pinned install path with no symlink in any component.
func Check(pinPath, selfExe, commit string) Result {
	r := Result{SHA256: "-", Commit: commit}
	if sum, err := fileSHA256(selfExe); err == nil {
		r.SHA256 = sum
	} else {
		return r.fail("the running binary is unreadable")
	}
	exe, err := os.Readlink(selfExe)
	if err != nil {
		return r.fail("the running binary path is unreadable")
	}
	r.ExePath = exe
	pin, err := readPin(pinPath)
	if err != nil {
		return r.fail(err.Error())
	}
	m := pinLine.FindStringSubmatch(pin)
	if m == nil {
		return r.fail("the pin file is not one line `<sha256> <commit> <path>`")
	}
	wantSum, wantCommit, wantPath := m[1], m[2], m[3]
	if filepath.Clean(wantPath) != wantPath {
		return r.fail("the pinned path is not clean")
	}
	if err := noSymlinkIn(wantPath); err != nil {
		return r.fail(err.Error())
	}
	switch {
	case exe != wantPath:
		return r.fail("the running binary is not at the pinned path")
	case r.SHA256 != wantSum:
		return r.fail("sha256 of the running binary is not the pinned sha256")
	case commit != wantCommit:
		return r.fail("the build commit is not the pinned commit")
	}
	r.OK = true
	return r
}

func (r Result) fail(reason string) Result {
	r.OK, r.Reason = false, reason
	return r
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func readPin(path string) (string, error) {
	fi, err := os.Lstat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "", errors.New("no pin file")
	case err != nil:
		return "", errors.New("the pin file is unreadable")
	case !fi.Mode().IsRegular():
		return "", errors.New("the pin file is not a regular file (a link or a special file)")
	case fi.Size() > maxPinBytes:
		return "", errors.New("the pin file is too large")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", errors.New("the pin file is unreadable")
	}
	return string(b), nil
}

func noSymlinkIn(path string) error {
	cur := "/"
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if err != nil {
			return fmt.Errorf("the pinned path has a missing component")
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("the pinned path has a symlink component")
		}
	}
	return nil
}
