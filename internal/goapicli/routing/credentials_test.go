package routing

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	pgstorage "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// connectPostgres serves every verb of this command (status, enable, disable,
// repoint, carry). A login and password that only PGUSER and PGPASSWORD supply are
// in pgx's failure text; the error it returns carries neither (CHAOS-6665).
func TestConnectPostgresRedactsCredentialsFromTheEnvironment(t *testing.T) {
	refusing := fakepg.StartRefusing(t)
	_, err := connectPostgres(context.Background(), refusing.URI, 3*time.Second)
	refusing.RequireConnected(t)
	if err == nil {
		t.Fatal("the refusing server let the pool open")
	}
	if leaks := refusing.Leaks(err.Error()); len(leaks) > 0 {
		t.Errorf("the error carries %v: %v", leaks, err)
	}
}

// The verbs print what connectPostgres returns: `status` in its report (it never
// fails), `disable` as its refusal. (`enable`, `repoint` and `carry` reach the same
// helper behind flag, credential and registry preflights this test does not build.)
func TestVerbsPrintNoCredentialsFromTheEnvironment(t *testing.T) {
	t.Setenv(bearerEnvVar, "")
	catalog := filepath.Join("..", "..", "..", "src", "dev_health_ops", "api", "graphql", "go_api_operations.json")
	for name, argv := range map[string][]string{
		"status":  {"status", "-timeout", "3s", "-catalog", catalog},
		"disable": {"disable", "-mode", "python", "-timeout", "3s", "-catalog", catalog},
	} {
		refusing := fakepg.StartRefusing(t)
		argv = append(argv, "-postgres-uri", refusing.URI)
		savedOut, savedErr := stdout, stderr
		var out, errOut bytes.Buffer
		stdout, stderr = &out, &errOut
		err := run(argv)
		stdout, stderr = savedOut, savedErr
		refusing.RequireConnected(t)
		text := out.String() + errOut.String()
		if err != nil {
			text += err.Error()
		}
		if leaks := refusing.Leaks(text); len(leaks) > 0 {
			t.Errorf("%s: the output carries %v:\n%s", name, leaks, text)
		}
	}
}

// Every origin of a database error a routing verb can print, through every verb
// that reaches it: the connect (a refused login), the ping (a server that accepts the
// login and fails the driver's ping with an echo of the login and password), a
// later statement (the ping succeeds, the next statement echoes: `status`'s census,
// `disable`'s read and its write transaction), a second statement behind a first that
// succeeds (`status`'s classification) and an error while a result's rows are read.
// The credentials reach the driver only through PGUSER and PGPASSWORD.
func TestEveryDatabaseErrorOriginIsRedactedThroughEveryVerb(t *testing.T) {
	t.Setenv(bearerEnvVar, "")
	catalog := filepath.Join("..", "..", "..", "src", "dev_health_ops", "api", "graphql", "go_api_operations.json")
	origins := map[string]struct {
		start  func(*testing.T) fakepg.Refusing
		marker string
	}{
		"connect": {fakepg.StartRefusing, "authentication failed"},
		"ping":    {fakepg.StartEchoingOnPing, "server echo"},
		"query":   {fakepg.StartEchoing, "server echo"},
		// the first statement (status's census) succeeds, the second fails: the
		// classification field of the report.
		"second statement": {fakepg.StartEchoingAfterOneStatement, "server echo"},
		// the first statement's result starts and the error arrives while its rows are read.
		"reading rows": {fakepg.StartEchoingWhileReadingRows, "server echo"},
	}
	verbs := map[string][]string{
		"status":  {"status", "-timeout", "3s", "-catalog", catalog},
		"disable": {"disable", "-mode", "python", "-timeout", "3s", "-catalog", catalog},
	}
	for origin, spec := range origins {
		for verb, base := range verbs {
			server := spec.start(t)
			argv := append(append([]string(nil), base...), "-postgres-uri", server.URI)
			savedOut, savedErr := stdout, stderr
			var out, errOut bytes.Buffer
			stdout, stderr = &out, &errOut
			err := run(argv)
			stdout, stderr = savedOut, savedErr
			server.RequireConnected(t)
			text := out.String() + errOut.String()
			if err != nil {
				text += err.Error()
			}
			if !strings.Contains(text, spec.marker) {
				t.Errorf("%s/%s: the server's error never reached the output (the test measures nothing):\n%s", origin, verb, text)
			}
			if leaks := server.Leaks(text); len(leaks) > 0 {
				t.Errorf("%s/%s: the output carries %v:\n%s", origin, verb, leaks, text)
			}
		}
	}
}

// The command's own error print applies the boundary to whatever a verb returns.
func TestTopLevelErrorPrintRedactsCredentials(t *testing.T) {
	credentialBoundary = pgstorage.Boundary("postgres://someone:hunter2secret@127.0.0.1:1/db")
	t.Cleanup(func() { credentialBoundary = secrets.Boundary{} })
	if got := redactCredentials("failed for someone with hunter2secret"); strings.Contains(got, "hunter2secret") || strings.Contains(got, "someone") {
		t.Errorf("redactCredentials left a credential: %q", got)
	}
	var printed bytes.Buffer
	saved := stderr
	stderr = &printed
	printError(errors.New("a raw driver error: hunter2secret for someone"))
	stderr = saved
	if strings.Contains(printed.String(), "hunter2secret") || strings.Contains(printed.String(), "someone") || !strings.Contains(printed.String(), "go-api-routing: ") {
		t.Errorf("printError left a credential or lost its prefix: %q", printed.String())
	}
	err := refuse("the driver said %v", errors.New("password hunter2secret for someone"))
	if strings.Contains(err.Error(), "hunter2secret") || !errors.Is(err, errRefused) {
		t.Errorf("refuse: %q (errors.Is refused: %v)", err.Error(), errors.Is(err, errRefused))
	}
}
