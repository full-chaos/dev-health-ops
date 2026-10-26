package adminops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/apiservice/admin"
	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func TestReportAnswersARefusedExitForTheUsersOwnedSwitch(t *testing.T) {
	var stdout, stderr bytes.Buffer
	env := cli.Env{Stdout: &stdout, Stderr: &stderr}
	code := report(env, func(err error) error { return err }, fmt.Errorf("create user: %w", admin.ErrUsersOwned))
	if code != cli.ExitRefused {
		t.Fatalf("exit %d, want ExitRefused (%d)", code, cli.ExitRefused)
	}
	if !strings.Contains(stderr.String(), "authentication service") || stdout.Len() != 0 {
		t.Fatalf("stderr %q stdout %q, want the moved-operation text on stderr only", stderr.String(), stdout.String())
	}
	var refusal *admin.OperatorError
	if errors.As(admin.ErrUsersOwned, &refusal) {
		t.Fatal("ErrUsersOwned must not be an OperatorError (that prints on stdout and exits 1)")
	}
}

func TestOperatorForReadsTheSwitchFromTheEnvironment(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    bool
		wantErr bool
	}{{"", false, false}, {"true", true, false}, {"false", false, false}, {"maybe", false, true}} {
		on, err := usersOwnedFromEnv(cli.Env{Lookup: func(key string) (string, bool) {
			if key == "AUTH_USERS_OWNED" && tc.value != "" {
				return tc.value, true
			}
			return "", false
		}})
		if (err != nil) != tc.wantErr || on != tc.want {
			t.Errorf("%q: on=%v err=%v", tc.value, on, err)
		}
	}
}

// usersWriteVerbs are the verbs that write public.users, with arguments that
// reach the operator. With the switch on each answers the refusal before it
// resolves a DSN or builds a pool, so a bad DSN cannot replace the refusal.
var usersWriteVerbs = []struct {
	name  string
	run   func(context.Context, cli.Env) int
	args  []string
	stdin string
}{
	{"users create", runUsersCreate, []string{"--email", "a@example.test", "--password-stdin"}, "long-enough-line\n"},
	{"users update profile", runUsersUpdate, []string{"--email", "a@example.test", "--full-name", "New Name"}, ""},
	{"users update password", runUsersUpdate, []string{"--email", "a@example.test", "--password-stdin"}, "long-enough-line\n"},
	{"users delete", runUsersDelete, []string{"--email", "a@example.test", "--yes"}, ""},
	{"users delete force", runUsersDelete, []string{"--email", "a@example.test", "--yes", "--force"}, ""},
}

func badDSNLookup(owned string) func(string) (string, bool) {
	values := map[string]string{"API_DATABASE_URI": "postgres://user@[::1/db", "AUTH_USERS_OWNED": owned}
	return func(key string) (string, bool) { value, ok := values[key]; return value, ok }
}

func TestUsersWriteVerbsRefuseBeforeDialingABadDSN(t *testing.T) {
	for _, verb := range usersWriteVerbs {
		t.Run(verb.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := verb.run(context.Background(), cli.Env{Args: verb.args, Lookup: badDSNLookup("true"), Stdin: strings.NewReader(verb.stdin), Stdout: &stdout, Stderr: &stderr})
			if code != cli.ExitRefused || !strings.Contains(stderr.String(), "authentication service") {
				t.Fatalf("exit %d stderr %q stdout %q, want ExitRefused with the moved-operation text", code, stderr.String(), stdout.String())
			}
		})
	}
}

// With the switch off, and for an update that writes no users column, the
// refusal does not apply: a bad DSN still answers postgres_unavailable.
func TestUsersVerbsStillDialWhenNoUsersWriteIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		run    func(context.Context, cli.Env) int
		args   []string
		stdin  string
		owned  string
		wantIn string
	}{
		{"create, switch off", runUsersCreate, []string{"--email", "a@example.test", "--password-stdin"}, "long-enough-line\n", "false", "postgres_unavailable"},
		{"list, switch on", runUsersList, []string{"--limit", "5"}, "", "true", "postgres_unavailable"},
		{"update membership only, switch on", runUsersUpdate, []string{"--email", "a@example.test", "--org", "x", "--role", "member"}, "", "true", "postgres_unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := tc.run(context.Background(), cli.Env{Args: tc.args, Lookup: badDSNLookup(tc.owned), Stdin: strings.NewReader(tc.stdin), Stdout: &stdout, Stderr: &stderr})
			if code == cli.ExitRefused || !strings.Contains(stderr.String(), tc.wantIn) {
				t.Fatalf("exit %d stderr %q, want a %s failure", code, stderr.String(), tc.wantIn)
			}
		})
	}
}

// A verb that calls an operator method which writes users must be in
// usersWriteVerbs, so a new write verb cannot skip the pre-dial refusal.
func TestEveryUsersWriteVerbIsInTheRefusalTable(t *testing.T) {
	covered := map[string]bool{"runUsersCreate": true, "runUsersUpdate": true, "runUsersDelete": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range parsed.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
					switch sel.Sel.Name {
					case "CreateUser", "UpdateUser", "DeleteUser":
						if !covered[fn.Name.Name] {
							t.Errorf("%s in %s calls Operator.%s but is not in the users-write refusal table", fn.Name.Name, file, sel.Sel.Name)
						}
					}
				}
				return true
			})
		}
	}
	if len(usersWriteVerbs) != 5 {
		t.Fatalf("usersWriteVerbs holds %d rows, want create, update profile, update password, delete, delete force", len(usersWriteVerbs))
	}
}
