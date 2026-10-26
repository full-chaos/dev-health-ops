package dialclass

import (
	"go/build"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const modulePath = "github.com/full-chaos/dev-health-ops"

// dialclass is the leaf that storage/postgres and platform/selfprobe both
// import. A module package it reaches under internal/storage or
// platform/selfprobe would bring back the import cycle the leaf exists to
// break (storage/postgres integration tests import selfprobe).
func TestDialclassReachesNoStorageOrSelfprobePackage(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	pkg, err := build.ImportDir(filepath.Join(root, "internal", "platform", "dialclass"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(pkg.GoFiles) == 0 {
		t.Fatal("no non-test Go files found: the guard measured nothing")
	}
	forbidden := func(path string) bool {
		return strings.HasPrefix(path, modulePath+"/internal/storage/") ||
			path == modulePath+"/internal/storage" ||
			path == modulePath+"/internal/platform/selfprobe"
	}
	seen := map[string]bool{}
	var walk func(path, via string)
	walk = func(path, via string) {
		if seen[path] {
			return
		}
		seen[path] = true
		if forbidden(path) {
			t.Errorf("%s reaches %s", via, path)
			return
		}
		imported, err := build.ImportDir(filepath.Join(root, strings.TrimPrefix(path, modulePath)), 0)
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			return
		}
		for _, next := range imported.Imports {
			if strings.HasPrefix(next, modulePath+"/") {
				walk(next, path)
			}
		}
	}
	for _, path := range pkg.Imports {
		if strings.HasPrefix(path, modulePath+"/") {
			walk(path, "dialclass")
		}
	}
}
