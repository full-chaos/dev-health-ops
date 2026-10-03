package envelopekeys

import (
	"bytes"
	"context"
	"encoding/pem"
	"os"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

func runCmd(args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := Command().Run(context.Background(), cli.Env{Args: args, Stdout: &stdout, Stderr: &stderr})
	return code, stdout.String(), stderr.String()
}

func TestFirstRunWritesAndSecondRunRefuses(t *testing.T) {
	dir := t.TempDir()
	code, out, errOut := runCmd("-dir", dir)
	if code != cli.ExitOK {
		t.Fatalf("first run exit = %d, stderr: %s", code, errOut)
	}
	paths := envelopemint.PathsIn(dir)
	privBytes, err := os.ReadFile(paths.Private)
	if err != nil {
		t.Fatal(err)
	}
	jwksBytes, err := os.ReadFile(paths.JWKS)
	if err != nil {
		t.Fatal(err)
	}
	// No key material on stdout or stderr: no PEM block, no key bytes.
	block, _ := pem.Decode(privBytes)
	for _, s := range []string{out, errOut} {
		if strings.Contains(s, "PRIVATE KEY") || strings.Contains(s, string(block.Bytes[len(block.Bytes)-8:])) {
			t.Fatalf("output carries key material: %q", s)
		}
	}

	code, _, errOut = runCmd("-dir", dir)
	if code != cli.ExitRefused {
		t.Fatalf("second run exit = %d, want %d (refused); stderr: %s", code, cli.ExitRefused, errOut)
	}
	if !strings.Contains(errOut, "already exist") {
		t.Errorf("refusal message unclear: %q", errOut)
	}
	privAfter, _ := os.ReadFile(paths.Private)
	jwksAfter, _ := os.ReadFile(paths.JWKS)
	if !bytes.Equal(privBytes, privAfter) || !bytes.Equal(jwksBytes, jwksAfter) {
		t.Fatal("refused run changed a file")
	}
}

func TestSkipExistingExitsZeroOnACompleteSet(t *testing.T) {
	dir := t.TempDir()
	if code, _, e := runCmd("-dir", dir, "-skip-existing"); code != cli.ExitOK {
		t.Fatalf("first run exit = %d: %s", code, e)
	}
	before, _ := os.ReadFile(envelopemint.PathsIn(dir).Private)
	if code, _, e := runCmd("-dir", dir, "-skip-existing"); code != cli.ExitOK {
		t.Fatalf("second run exit = %d: %s", code, e)
	}
	after, _ := os.ReadFile(envelopemint.PathsIn(dir).Private)
	if !bytes.Equal(before, after) {
		t.Fatal("-skip-existing replaced the key")
	}
}

func TestMissingDirIsAUsageError(t *testing.T) {
	if code, _, _ := runCmd(); code != cli.ExitUsage {
		t.Fatalf("exit = %d, want %d", code, cli.ExitUsage)
	}
}

func TestKeyIDComesFromTheEnvironmentWhenSet(t *testing.T) {
	var out bytes.Buffer
	lookup := func(name string) (string, bool) {
		if name == envelopemint.KeyIDEnvVar {
			return "kid-from-env", true
		}
		return "", false
	}
	dir := t.TempDir()
	code, err := run([]string{"-dir", dir}, &out, &bytes.Buffer{}, lookup)
	if err != nil || code != cli.ExitOK {
		t.Fatalf("run = %d, %v", code, err)
	}
	jwks, _ := os.ReadFile(envelopemint.PathsIn(dir).JWKS)
	if !strings.Contains(string(jwks), `"kid":"kid-from-env"`) {
		t.Fatalf("JWKS lacks the env key id: %s", jwks)
	}
}
