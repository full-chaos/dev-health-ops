package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/ingressplanes"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

const smallContract = `{"schema_version": 1, "regex_mode": true, "rules": [
  {"path": "/", "path_type": "Prefix", "plane": "go-api"},
  {"path": "/graphql$", "path_type": "ImplementationSpecific", "plane": "query-api"}
], "public_host_rules": []}`

func write(t *testing.T, path, text string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(text), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestWriteThenCheck: the command writes what the library renders, -check
// accepts that file, and -check refuses a file that was changed afterwards.
func TestWriteThenCheck(t *testing.T) {
	directory := t.TempDir()
	contractPath, outPath := filepath.Join(directory, "planes"), filepath.Join(directory, "router.conf")
	write(t, contractPath, smallContract)

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-contract", contractPath, "-out", outPath}, &stdout, &stderr); code != exitOK {
		t.Fatalf("write: exit %d, stderr %s", code, stderr.String())
	}
	written, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatal(err)
	}
	contract, err := ingressplanes.Parse([]byte(smallContract))
	if err != nil {
		t.Fatal(err)
	}
	want, err := ingressplanes.Render(contract, ingressplanes.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(written, want) {
		t.Fatalf("the command wrote %d bytes that are not the library's render (%d bytes)", len(written), len(want))
	}
	if !strings.Contains(stdout.String(), "2 rules") {
		t.Errorf("the write must say how many rules it wrote, got %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"-contract", contractPath, "-out", outPath, "-check"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("-check on a fresh file: exit %d, stderr %s", code, stderr.String())
	}

	// A hand edit of the file, and a contract change with no generate, are both stale.
	write(t, outPath, strings.Replace(string(written), "query-api:8090", "go-api:8000", 1))
	stderr.Reset()
	if code := run([]string{"-contract", contractPath, "-out", outPath, "-check"}, &stdout, &stderr); code != exitStale || !strings.Contains(stderr.String(), "is not what") {
		t.Fatalf("-check on an edited file: exit %d, stderr %q", code, stderr.String())
	}
	if after, err := os.ReadFile(outPath); err != nil || bytes.Equal(after, written) {
		t.Fatalf("-check must not write the file back (err %v)", err)
	}
	write(t, outPath, string(written))
	write(t, contractPath, strings.Replace(smallContract, "/graphql$", "/query$", 1))
	stderr.Reset()
	if code := run([]string{"-contract", contractPath, "-out", outPath, "-check"}, &stdout, &stderr); code != exitStale {
		t.Fatalf("-check after a contract change: exit %d, stderr %q", code, stderr.String())
	}
}

// TestRefusals: a contract that does not load writes nothing; a stray argument
// and an unknown flag are usage errors.
func TestRefusals(t *testing.T) {
	directory := t.TempDir()
	contractPath, outPath := filepath.Join(directory, "planes"), filepath.Join(directory, "router.conf")
	write(t, contractPath, strings.Replace(smallContract, `"regex_mode": true`, `"regex_mode": false`, 1))
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-contract", contractPath, "-out", outPath}, &stdout, &stderr); code != exitStale || !strings.Contains(stderr.String(), "regex_mode is false") {
		t.Fatalf("an invalid contract: exit %d, stderr %q", code, stderr.String())
	}
	if _, err := os.Stat(outPath); !os.IsNotExist(err) {
		t.Fatalf("an invalid contract must write no file (stat: %v)", err)
	}
	if code := run([]string{"-contract", filepath.Join(directory, "absent"), "-out", outPath}, &stdout, &stderr); code != exitStale {
		t.Fatalf("a contract that is not there: exit %d", code)
	}
	write(t, contractPath, smallContract)
	if code := run([]string{"-contract", contractPath, "-out", outPath, "-check"}, &stdout, &stderr); code != exitStale {
		t.Fatalf("-check with no file on disk: exit %d", code)
	}
	if code := run([]string{"extra"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("a stray argument: exit %d", code)
	}
	if code := run([]string{"-no-such-flag"}, &stdout, &stderr); code != exitUsage {
		t.Fatalf("an unknown flag: exit %d", code)
	}
}

// TestDefaultPathsAreTheCheckedInFiles: with no flag, run from the repository
// root, the command reads the checked-in contract and its -check passes on the
// checked-in file. This is the command the README gives.
func TestDefaultPathsAreTheCheckedInFiles(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(root)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"-check"}, &stdout, &stderr); code != exitOK {
		t.Fatalf("-check from the repository root: exit %d, stderr %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), ingressplanes.RouterConfigPath+" is fresh") {
		t.Errorf("got %q", stdout.String())
	}
}
