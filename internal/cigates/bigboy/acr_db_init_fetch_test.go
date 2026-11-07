package bigboy_test

import (
	"os/exec"
	"path/filepath"
	"testing"
)

func TestACRDBInitFetchCells(t *testing.T) {
	script := filepath.Join(moduleRootNoT(), "ci", "bigboy", "test-acr-db-init-fetch.sh")
	cmd := exec.Command("bash", script)
	cmd.Env = append(cmd.Environ(), "TMPDIR="+t.TempDir())
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("acr-db-init-fetch.sh cells failed: %v\n%s", err, out)
	}
}
