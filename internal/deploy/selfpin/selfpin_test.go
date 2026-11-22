package selfpin

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const commit = "23bea3be509e238eecbf9ab85200a6af24ac3a4b"

type layout struct {
	dir, bin, exe, pin, sum string
}

// newLayout makes a "binary", a link standing in for /proc/self/exe and a pin
// file naming the binary.
func newLayout(t *testing.T) layout {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	l := layout{dir: dir, bin: filepath.Join(dir, "bin", "dho"), exe: filepath.Join(dir, "self-exe"), pin: filepath.Join(dir, "dho-deploy.pin")}
	if err := os.MkdirAll(filepath.Dir(l.bin), 0o700); err != nil {
		t.Fatal(err)
	}
	body := []byte("not really an ELF, the bytes only")
	if err := os.WriteFile(l.bin, body, 0o500); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	l.sum = hex.EncodeToString(sum[:])
	if err := os.Symlink(l.bin, l.exe); err != nil {
		t.Fatal(err)
	}
	l.writePin(t, l.sum+" "+commit+" "+l.bin+"\n")
	return l
}

func (l layout) writePin(t *testing.T, text string) {
	t.Helper()
	_ = os.Remove(l.pin)
	if err := os.WriteFile(l.pin, []byte(text), 0o400); err != nil {
		t.Fatal(err)
	}
}

func TestThePinnedBinaryPasses(t *testing.T) {
	l := newLayout(t)
	r := Check(l.pin, l.exe, commit)
	if !r.OK || r.SHA256 != l.sum || r.ExePath != l.bin {
		t.Fatalf("%+v", r)
	}
}

func TestEveryMismatchIsRefused(t *testing.T) {
	for name, c := range map[string]struct {
		edit func(t *testing.T, l layout) (exe, commit string)
		want string
	}{
		"one byte of the binary changed": {func(t *testing.T, l layout) (string, string) {
			if err := os.Chmod(l.bin, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(l.bin, []byte("not really an ELF, the bytes onlY"), 0o500); err != nil {
				t.Fatal(err)
			}
			return l.exe, commit
		}, "not the pinned sha256"},
		"another build commit": {func(t *testing.T, l layout) (string, string) {
			return l.exe, strings.Repeat("a", 40)
		}, "not the pinned commit"},
		"unknown build commit": {func(t *testing.T, l layout) (string, string) { return l.exe, "unknown" }, "not the pinned commit"},
		"no pin file": {func(t *testing.T, l layout) (string, string) {
			if err := os.Remove(l.pin); err != nil {
				t.Fatal(err)
			}
			return l.exe, commit
		}, "no pin file"},
		"pin file is a link": {func(t *testing.T, l layout) (string, string) {
			real := l.pin + ".real"
			if err := os.Rename(l.pin, real); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(real, l.pin); err != nil {
				t.Fatal(err)
			}
			return l.exe, commit
		}, "not a regular file"},
		"two pin lines": {func(t *testing.T, l layout) (string, string) {
			l.writePin(t, l.sum+" "+commit+" "+l.bin+"\n"+l.sum+" "+commit+" "+l.bin+"\n")
			return l.exe, commit
		}, "not one line"},
		"pin with a trailing word": {func(t *testing.T, l layout) (string, string) {
			l.writePin(t, l.sum+" "+commit+" "+l.bin+" x\n")
			return l.exe, commit
		}, "not one line"},
		"binary at another path": {func(t *testing.T, l layout) (string, string) {
			other := filepath.Join(l.dir, "dho-copy")
			b, _ := os.ReadFile(l.bin)
			if err := os.WriteFile(other, b, 0o500); err != nil {
				t.Fatal(err)
			}
			exe := filepath.Join(l.dir, "self-exe-2")
			if err := os.Symlink(other, exe); err != nil {
				t.Fatal(err)
			}
			return exe, commit
		}, "not at the pinned path"},
		"symlinked dir in the pinned path": {func(t *testing.T, l layout) (string, string) {
			linkDir := filepath.Join(l.dir, "linkbin")
			if err := os.Symlink(filepath.Join(l.dir, "bin"), linkDir); err != nil {
				t.Fatal(err)
			}
			pinned := filepath.Join(linkDir, "dho")
			l.writePin(t, l.sum+" "+commit+" "+pinned+"\n")
			exe := filepath.Join(l.dir, "self-exe-3")
			if err := os.Symlink(pinned, exe); err != nil {
				t.Fatal(err)
			}
			return exe, commit
		}, "symlink component"},
		"unclean pinned path": {func(t *testing.T, l layout) (string, string) {
			l.writePin(t, l.sum+" "+commit+" "+filepath.Join(l.dir, "bin")+"/../bin/dho\n")
			return l.exe, commit
		}, "not clean"},
		"self exe unreadable": {func(t *testing.T, l layout) (string, string) {
			return filepath.Join(l.dir, "no-such-exe"), commit
		}, "unreadable"},
	} {
		t.Run(name, func(t *testing.T) {
			l := newLayout(t)
			exe, c2 := c.edit(t, l)
			r := Check(l.pin, exe, c2)
			if r.OK || !strings.Contains(r.Reason, c.want) {
				t.Fatalf("want a refusal holding %q, got %+v", c.want, r)
			}
		})
	}
}
