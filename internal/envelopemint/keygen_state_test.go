package envelopemint

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// newPair generates a valid pair under a fresh dir and returns dir and paths.
func newPair(t *testing.T) (string, KeyPaths) {
	t.Helper()
	dir := t.TempDir()
	paths, err := GenerateKeyFiles(dir, "kid-a")
	if err != nil {
		t.Fatalf("GenerateKeyFiles: %v", err)
	}
	return dir, paths
}

// mustRefuse asserts Ensure fails, names `want` in the message, and leaves the
// bytes of both canonical files alone.
func mustRefuse(t *testing.T, dir, want string) {
	t.Helper()
	paths := PathsIn(dir)
	before := map[string][]byte{}
	for _, p := range []string{paths.Private, paths.JWKS} {
		if b, err := os.ReadFile(p); err == nil {
			before[p] = b
		}
	}
	changed, err := EnsureKeyFiles(dir, "kid-a")
	if err == nil {
		t.Fatalf("Ensure accepted the state; want an error mentioning %q", want)
	}
	if changed {
		t.Errorf("Ensure reported a change while refusing")
	}
	if !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not mention %q", err, want)
	}
	for p, b := range before {
		if got, err := os.ReadFile(p); err != nil || string(got) != string(b) {
			t.Errorf("%s changed while refusing", p)
		}
	}
}

func TestEnsureRepairsAPrivateOnlyHalfState(t *testing.T) {
	_, paths := newPair(t)
	dir := filepath.Dir(filepath.Dir(paths.Private))
	privBefore, jwksBefore := readAll(t, paths)
	if err := os.Remove(paths.JWKS); err != nil {
		t.Fatal(err)
	}
	changed, err := EnsureKeyFiles(dir, "kid-a")
	if err != nil || !changed {
		t.Fatalf("Ensure on a private-only state = %v, %v; want repaired", changed, err)
	}
	privAfter, jwksAfter := readAll(t, paths)
	if string(privAfter) != string(privBefore) {
		t.Error("repair replaced the private key")
	}
	if string(jwksAfter) != string(jwksBefore) {
		t.Error("repaired JWKS differs from the one derived from the same key")
	}
	if info, _ := os.Stat(paths.JWKS); info.Mode().Perm() != 0o444 {
		t.Errorf("repaired JWKS mode = %o, want 444", info.Mode().Perm())
	}
}

func TestEnsureRefusesAPublicOnlyState(t *testing.T) {
	dir, paths := newPair(t)
	if err := os.Chmod(filepath.Dir(paths.Private), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(paths.Private); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "cannot leave that")
	if _, err := os.Stat(paths.Private); !errors.Is(err, os.ErrNotExist) {
		t.Error("Ensure created a private key beside a public-only state")
	}
}

func TestEnsureRefusesAMismatchedPair(t *testing.T) {
	dir, _ := newPair(t)
	other := t.TempDir()
	otherPaths, err := GenerateKeyFiles(other, "kid-a")
	if err != nil {
		t.Fatal(err)
	}
	otherJWKS, _ := os.ReadFile(otherPaths.JWKS)
	paths := PathsIn(dir)
	if err := os.Remove(paths.JWKS); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.JWKS, otherJWKS, 0o444); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "does not match")
}

func TestEnsureRefusesAPrivateKeyWithGroupOrOtherBits(t *testing.T) {
	for _, mode := range []os.FileMode{0o440, 0o404, 0o600 | 0o040, 0o644} {
		dir, paths := newPair(t)
		if err := os.Chmod(paths.Private, mode); err != nil {
			t.Fatal(err)
		}
		mustRefuse(t, dir, "private key")
		if info, _ := os.Stat(paths.Private); info.Mode().Perm() != mode {
			t.Errorf("Ensure changed the private key mode from %o to %o", mode, info.Mode().Perm())
		}
	}
}

func TestEnsureRefusesAPrivateDirectoryWithGroupOrOtherBits(t *testing.T) {
	for _, mode := range []os.FileMode{0o750, 0o705, 0o755} {
		dir, paths := newPair(t)
		if err := os.Chmod(filepath.Dir(paths.Private), mode); err != nil {
			t.Fatal(err)
		}
		mustRefuse(t, dir, "private directory")
		if info, _ := os.Stat(filepath.Dir(paths.Private)); info.Mode().Perm() != mode {
			t.Errorf("Ensure changed the private directory mode")
		}
	}
}

func TestEnsureRefusesAJWKSTheReaderCannotRead(t *testing.T) {
	dir, paths := newPair(t)
	if err := os.Chmod(paths.JWKS, 0o400); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "JWKS")

	dir, paths = newPair(t)
	if err := os.Chmod(filepath.Dir(paths.JWKS), 0o700); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "public directory")
}

func TestEnsureRefusesSymlinks(t *testing.T) {
	// Both canonical files are symlinks to a valid, matching pair elsewhere.
	dir := t.TempDir()
	elsewhere := t.TempDir()
	real, err := GenerateKeyFiles(elsewhere, "kid-a")
	if err != nil {
		t.Fatal(err)
	}
	paths := PathsIn(dir)
	for _, d := range []string{filepath.Dir(paths.Private), filepath.Dir(paths.JWKS)} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(filepath.Dir(paths.JWKS), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real.Private, paths.Private); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real.JWKS, paths.JWKS); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "symlink")

	// Only the private key is a symlink.
	dir, p := newPair(t)
	moved := filepath.Join(t.TempDir(), "k.pem")
	b, _ := os.ReadFile(p.Private)
	if err := os.Chmod(filepath.Dir(p.Private), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p.Private); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(moved, b, 0o400); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(moved, p.Private); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "symlink")

	// Only the JWKS is a symlink.
	dir, p = newPair(t)
	jb, _ := os.ReadFile(p.JWKS)
	movedJ := filepath.Join(t.TempDir(), "j.json")
	if err := os.WriteFile(movedJ, jb, 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p.JWKS); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(movedJ, p.JWKS); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "symlink")
}

func TestEnsureRefusesASymlinkedKeySubdirectory(t *testing.T) {
	dir, p := newPair(t)
	realPrivDir := filepath.Join(t.TempDir(), "priv")
	if err := os.Rename(filepath.Dir(p.Private), realPrivDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(realPrivDir, filepath.Dir(p.Private)); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "symlink")
}

var errKill = errors.New("simulated kill")

// killAt makes the write fail at one point without cleanup, as SIGKILL there
// would, runs Ensure (which fails), restores the hook, then runs Ensure again
// as the next `up` would.
func killAt(t *testing.T, point string) (string, KeyPaths) {
	t.Helper()
	dir := t.TempDir()
	crashHook = func(p string) error {
		if p == point {
			return errKill
		}
		return nil
	}
	_, err := EnsureKeyFiles(dir, "kid-a")
	crashHook = func(string) error { return nil }
	if !errors.Is(err, errKill) {
		t.Fatalf("kill at %s: Ensure error = %v, want the simulated kill (is the point reached?)", point, err)
	}
	return dir, PathsIn(dir)
}

func TestKillAtEachWritePointLeavesAStateTheNextRunRecovers(t *testing.T) {
	for _, point := range []string{
		"private:temp-written", "private:committed", "jwks:temp-written", "jwks:committed",
	} {
		t.Run(point, func(t *testing.T) {
			dir, paths := killAt(t, point)
			var privBefore []byte
			if b, err := os.ReadFile(paths.Private); err == nil {
				privBefore = b
			}
			if _, err := os.Stat(paths.JWKS); err == nil && point != "jwks:committed" {
				t.Fatal("the JWKS exists before its commit point")
			}
			if _, err := EnsureKeyFiles(dir, "kid-a"); err != nil {
				t.Fatalf("next run after a kill at %s failed: %v", point, err)
			}
			priv, jwks := readAll(t, paths)
			if privBefore != nil && string(priv) != string(privBefore) {
				t.Error("recovery replaced a private key that was already committed")
			}
			if len(jwks) == 0 {
				t.Error("no JWKS after recovery")
			}
			if changed, err := EnsureKeyFiles(dir, "kid-a"); err != nil || changed {
				t.Errorf("a third run = %v, %v; want a no-op", changed, err)
			}
			for _, d := range []string{filepath.Dir(paths.Private), filepath.Dir(paths.JWKS)} {
				entries, _ := os.ReadDir(d)
				for _, e := range entries {
					if strings.Contains(e.Name(), tempMarker) {
						t.Errorf("stale temp file left in %s: %s", d, e.Name())
					}
				}
			}
		})
	}
}

func TestACommittedPrivateKeyIsNeverVisibleHalfWritten(t *testing.T) {
	// At the private temp-written point the final path must not exist yet.
	_, paths := killAt(t, "private:temp-written")
	if _, err := os.Lstat(paths.Private); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("private key exists at its final path before its commit: %v", err)
	}
}

func TestEnsureRefusesAPathThatIsNotARegularFile(t *testing.T) {
	dir, p := newPair(t)
	if err := os.Remove(p.JWKS); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p.JWKS, 0o755); err != nil {
		t.Fatal(err)
	}
	mustRefuse(t, dir, "not a regular file")
}

func TestACommitNeverOverwritesAFileThatAppearedAfterTheCheck(t *testing.T) {
	dir := t.TempDir()
	paths := PathsIn(dir)
	crashHook = func(p string) error {
		if p == "private:temp-written" {
			return os.WriteFile(paths.Private, []byte("planted"), 0o400)
		}
		return nil
	}
	defer func() { crashHook = func(string) error { return nil } }()
	_, err := GenerateKeyFiles(dir, "kid-a")
	if !errors.Is(err, ErrKeyFilesExist) {
		t.Fatalf("error = %v, want ErrKeyFilesExist", err)
	}
	if b, _ := os.ReadFile(paths.Private); string(b) != "planted" {
		t.Fatal("the commit overwrote a file that appeared after the existence check")
	}
}
