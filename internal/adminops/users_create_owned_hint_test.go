package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func TestAdminUsersCreateWhileUsersOwnedNamesTheAuthVerb(t *testing.T) {
	values := map[string]string{"AUTH_USERS_OWNED": "true", "API_DATABASE_URI": "postgres://user@[::1/db"}
	var stdout, stderr bytes.Buffer
	code := runUsersCreate(context.Background(), cli.Env{
		Args:   []string{"--email", "a@example.com", "--password-stdin"},
		Lookup: func(k string) (string, bool) { v, ok := values[k]; return v, ok },
		Stdin:  strings.NewReader("Zq7-correct-horse-battery\n"), Stdout: &stdout, Stderr: &stderr,
	})
	if code != cli.ExitRefused || stdout.Len() != 0 || !strings.Contains(stderr.String(), "dho auth users-create") || !strings.Contains(stderr.String(), "authentication service") {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, stdout.String(), stderr.String())
	}
	values["AUTH_USERS_OWNED"] = "false"
	stderr.Reset()
	runUsersCreate(context.Background(), cli.Env{
		Args:   []string{"--email", "a@example.com", "--password-stdin"},
		Lookup: func(k string) (string, bool) { v, ok := values[k]; return v, ok },
		Stdin:  strings.NewReader("Zq7-correct-horse-battery\n"), Stdout: &stdout, Stderr: &stderr,
	})
	if strings.Contains(stderr.String(), "dho auth users-create") {
		t.Fatalf("the hint shows while users are not owned: %q", stderr.String())
	}
}
