package usersbackfill

import (
	"bytes"
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func TestCommandTreeIsValid(t *testing.T) {
	if err := cli.Validate([]cli.Command{Command()}); err != nil {
		t.Fatal(err)
	}
}

func runVerb(args ...string) (int, string) {
	var stderr bytes.Buffer
	verb := Command().Children[0]
	code := verb.Run(context.Background(), cli.Env{
		Args: args, Lookup: func(string) (string, bool) { return "", false }, Stdout: &bytes.Buffer{}, Stderr: &stderr,
	})
	return code, stderr.String()
}

func TestVerbRefusesBadArguments(t *testing.T) {
	for _, args := range [][]string{{"extra"}, {"-batch", "0"}, {"-batch", "20000"}, {"-nope"}} {
		if code, _ := runVerb(args...); code != cli.ExitUsage {
			t.Fatalf("args %v: exit %d, want %d", args, code, cli.ExitUsage)
		}
	}
}

func TestVerbWithoutADatabaseFailsWithoutRunning(t *testing.T) {
	code, stderr := runVerb("-dry-run")
	if code != cli.ExitFailure {
		t.Fatalf("exit %d", code)
	}
	if stderr == "" {
		t.Fatal("no reason printed")
	}
}

func TestVerbIgnoresTheSchemaOwnerDSN(t *testing.T) {
	var stderr bytes.Buffer
	lookup := func(k string) (string, bool) {
		if k == "MIGRATION_DATABASE_URI" || k == "POSTGRES_URI" {
			return "postgres://owner@127.0.0.1:1/db", true
		}
		return "", false
	}
	code := Command().Children[0].Run(context.Background(), cli.Env{Args: []string{"-dry-run"}, Lookup: lookup, Stdout: &bytes.Buffer{}, Stderr: &stderr})
	if code != cli.ExitFailure || !bytes.Contains(stderr.Bytes(), []byte("AUTH_DATABASE_URI")) {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
}
