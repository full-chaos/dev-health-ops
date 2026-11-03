//go:build integration

package synccli

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// tmpfsMagic is the statfs type of a tmpfs file system.
const tmpfsMagic = 0x01021994

// The batch fixtures of the files/blame writer place a file whose name is not valid UTF-8 without reading the file
// system's directory order: on tmpfs (newest entry first, as /tmp on the hosted ubuntu-26.04 images) a new directory is
// never the last entry, so a fixture that waits for one never builds. The file lands in zz000, the directory that sorts
// last by name among the root directories.
func TestBlameBatchFixtureBuildsInAnyDirectoryOrder(t *testing.T) {
	var fs syscall.Statfs_t
	if err := syscall.Statfs("/dev/shm", &fs); err != nil || int64(fs.Type) != tmpfsMagic {
		t.Fatalf("/dev/shm is not tmpfs (statfs %v, type %#x): the newest-first directory order cannot be planted", err, fs.Type)
	}
	tmp, err := os.MkdirTemp("/dev/shm", "synccli-walk-order-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmp) })

	var scenario *localScenario
	for _, s := range blameBatchScenarios() {
		if s.allOrNothing {
			scenario = &s
			break
		}
	}
	if scenario == nil {
		t.Fatal("no batch scenario with a name that is not UTF-8")
	}
	f := newFixtureIn(t, tmp, localFixtureName(*scenario))
	scenario.build(f)

	if _, err := os.Stat(filepath.Join(f.dir, "zz000", "bad\xffname.txt")); err != nil {
		t.Fatalf("the name that is not UTF-8 is not at zz000/: %v", err)
	}
	entries, err := os.ReadDir(f.dir)
	if err != nil {
		t.Fatal(err)
	}
	last := ""
	for _, entry := range entries {
		if entry.IsDir() {
			last = entry.Name()
		}
	}
	if last != "zz000" {
		t.Fatalf("the last root directory by name is %q, want zz000: a fixture directory sorts after it", last)
	}
}
