package envelopemint

import "testing"

// The two files are read at two moments. A concurrent first-use run can commit
// BOTH files between those reads. The cut point is forced through the write
// hook: the outer run reads its first file, then an inner run completes the
// whole pair, then the outer run reads its second file. The outer run must end
// on the inner run's pair, not refuse a "public without private" state that a
// concurrent commit explains.
func TestAConcurrentCommitBetweenTheTwoStateReadsIsNotRefused(t *testing.T) {
	resetSeams(t)
	dir := t.TempDir()
	inner := false
	var innerErr error
	crashHook = func(p string) error {
		if inner || p != "ensure:between-state-reads" {
			return nil
		}
		inner = true
		_, innerErr = EnsureKeyFiles(dir, "kid-a")
		return nil
	}
	if _, err := EnsureKeyFiles(dir, "kid-a"); err != nil {
		t.Fatalf("outer run: %v", err)
	}
	if !inner {
		t.Fatal("the cut point was not reached: the seam is not in the state read")
	}
	if innerErr != nil {
		t.Fatalf("inner run: %v", innerErr)
	}
	pairIsConsistent(t, dir)
}
