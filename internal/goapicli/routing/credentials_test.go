package routing

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

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
		"status":  {"status", "-timeout", "3s"},
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
