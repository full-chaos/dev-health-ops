package envelopemint

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func resetSeams(t *testing.T) {
	t.Helper()
	hook, sync := crashHook, syncDir
	t.Cleanup(func() { crashHook, syncDir = hook, sync })
}

// A symlinked key subdirectory on EMPTY state must be refused: no key lands
// outside the key directory and nothing in the link target is touched.
func TestFirstUseRefusesASymlinkedKeySubdirectory(t *testing.T) {
	for _, sub := range []string{PrivateSubdir, PublicSubdir} {
		t.Run(sub, func(t *testing.T) {
			root := t.TempDir()
			outside := t.TempDir()
			marker := filepath.Join(outside, ".operator.tmp-keep")
			if err := os.WriteFile(marker, []byte("keep"), 0o600); err != nil {
				t.Fatal(err)
			}
			past := time.Now().Add(-2 * staleTempAge) // old enough to be swept if the link were followed
			if err := os.Chtimes(marker, past, past); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(root, sub)); err != nil {
				t.Fatal(err)
			}
			if _, err := EnsureKeyFiles(root, "kid-a"); err == nil {
				t.Fatal("Ensure followed a symlinked key subdirectory on empty state")
			}
			if _, err := GenerateKeyFiles(root, "kid-a"); err == nil {
				t.Fatal("Generate followed a symlinked key subdirectory on empty state")
			}
			entries, _ := os.ReadDir(outside)
			if len(entries) != 1 || entries[0].Name() != ".operator.tmp-keep" {
				t.Fatalf("link target was touched: %v", entries)
			}
			if b, _ := os.ReadFile(marker); string(b) != "keep" {
				t.Fatal("marker file changed")
			}
		})
	}
}

// A failing directory fsync must surface as an error from the write.
func TestADirectoryFsyncFailureIsReturned(t *testing.T) {
	resetSeams(t)
	boom := errors.New("injected fsync failure")
	syncDir = func(string) error { return boom }
	if _, err := GenerateKeyFiles(t.TempDir(), "kid-a"); !errors.Is(err, boom) {
		t.Fatalf("Generate error = %v, want the fsync failure", err)
	}
	if _, err := EnsureKeyFiles(t.TempDir(), "kid-a"); !errors.Is(err, boom) {
		t.Fatalf("Ensure error = %v, want the fsync failure", err)
	}
}

// pairIsConsistent asserts the end state: a complete pair that passes the
// accept predicate, i.e. the JWKS holds the public key of the private key.
func pairIsConsistent(t *testing.T, dir string) {
	t.Helper()
	if err := checkKeySet(PathsIn(dir), "kid-a"); err != nil {
		t.Fatalf("pair is not consistent: %v", err)
	}
}

// Two first-use runs forced into the interleaving where B completes while A is
// between its checks and its private-key commit. A must converge on B's pair,
// not fail, and never write a key of its own beside B's JWKS.
func TestConcurrentFirstUseConvergesOnOnePair(t *testing.T) {
	for _, point := range []string{"private:temp-written", "jwks:temp-written"} {
		t.Run(point, func(t *testing.T) {
			resetSeams(t)
			dir := t.TempDir()
			inner := false
			var bErr error
			crashHook = func(p string) error {
				if inner || p != point {
					return nil
				}
				inner = true
				_, bErr = EnsureKeyFiles(dir, "kid-a")
				return nil
			}
			if _, err := EnsureKeyFiles(dir, "kid-a"); err != nil {
				t.Fatalf("run A: %v", err)
			}
			if bErr != nil {
				t.Fatalf("run B: %v", bErr)
			}
			pairIsConsistent(t, dir)
		})
	}
}

func TestParallelFirstUseRunsAllSucceedWithOnePair(t *testing.T) {
	for i := 0; i < 40; i++ {
		dir := t.TempDir()
		var wg sync.WaitGroup
		errs := make([]error, 4)
		for j := range errs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, errs[j] = EnsureKeyFiles(dir, "kid-a")
			}()
		}
		wg.Wait()
		for j, err := range errs {
			if err != nil {
				t.Fatalf("round %d run %d: %v", i, j, err)
			}
		}
		pairIsConsistent(t, dir)
	}
}
