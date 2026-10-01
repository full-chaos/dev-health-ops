// Package chlogin gives a verb test a real ClickHouse and two logins that the
// server refuses in the two places a verb prints a database error: at connect
// (authentication failure, which names the login) and at the first statement
// (a login without grants, whose access-denied text names the login too).
// The login and passwords are test constants, not credentials.
package chlogin

import (
	"bytes"
	"context"
	"log/slog"
	"net/url"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	chstorage "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// The planted values: a verb's output must hold none of them.
const (
	Login       = "planted_login_c6644"
	Password    = "planted-pw-c6644"
	BadPassword = "planted-badpw-c6644"
)

// Server is a ClickHouse with the planted login created without grants.
type Server struct {
	Instance *containers.Instance
	// WrongPassword is a DSN whose login exists and whose password is wrong:
	// the connect is refused with an authentication failure naming the login.
	WrongPassword string
	// NoGrants is a DSN whose login and password are right and which holds no
	// grants: the connect succeeds and every statement is refused, naming the
	// login.
	NoGrants string
	// Admin is the container's own DSN, for setting a test up.
	Admin string
}

// Start starts one ClickHouse and creates the planted login on it.
func Start(ctx context.Context, t *testing.T) Server {
	t.Helper()
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	// The tables exist, so a login without grants is refused by the server's
	// access check and not by a missing table.
	chschema.Apply(ctx, t, instance)
	conn, err := chstorage.Open(ctx, chstorage.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open the admin connection: %v", err)
	}
	defer conn.Close()
	if err := conn.Exec(ctx, "CREATE USER IF NOT EXISTS "+Login+" IDENTIFIED BY '"+Password+"'"); err != nil {
		t.Fatalf("create the planted login: %v", err)
	}
	parsed, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	with := func(password string) string {
		copyOf := *parsed
		copyOf.User = url.UserPassword(Login, password)
		return copyOf.String()
	}
	return Server{Instance: instance, WrongPassword: with(BadPassword), NoGrants: with(Password), Admin: instance.URI}
}

// Leaked names every planted value that appears in any of the texts.
func Leaked(texts ...string) []string {
	var out []string
	for _, value := range []string{Login, Password, BadPassword} {
		for _, text := range texts {
			if strings.Contains(text, value) {
				out = append(out, value)
				break
			}
		}
	}
	return out
}

// Run runs the verb at path under root with the given arguments and environment
// and returns the exit code and everything the verb printed: stdout, stderr and
// whatever it logged through the process logger.
func Run(t *testing.T, root cli.Command, path []string, args []string, env map[string]string) (code int, stdout, stderr, logs string) {
	t.Helper()
	command := root
	for _, name := range path {
		var next *cli.Command
		for index := range command.Children {
			if command.Children[index].Name == name {
				next = &command.Children[index]
			}
		}
		if next == nil {
			t.Fatalf("no command %q under %q", name, command.Name)
		}
		command = *next
	}
	if command.Run == nil {
		t.Fatalf("%v is not a verb", path)
	}
	var out, errOut, logBuffer bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuffer, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(previous)
	code = command.Run(context.Background(), cli.Env{
		Args: args, Stdout: &out, Stderr: &errOut,
		Lookup: func(key string) (string, bool) { value, ok := env[key]; return value, ok },
	})
	return code, out.String(), errOut.String(), logBuffer.String()
}
