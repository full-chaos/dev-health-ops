package adminops

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func TestTheAdminGroupHoldsEveryVerb(t *testing.T) {
	if err := cli.Validate([]cli.Command{Command()}); err != nil {
		t.Fatal(err)
	}
	command := Command()
	var paths []string
	for _, group := range command.Children {
		for _, child := range group.Children {
			paths = append(paths, group.Name+" "+child.Name)
		}
	}
	want := "users create,users list,users update,orgs create,orgs list,orgs delete,llm-settings get,llm-settings set,llm-settings delete,features seed"
	if command.Name != "admin" || strings.Join(paths, ",") != want {
		t.Fatalf("the verbs are %v, want %s", paths, want)
	}
}

// The users and orgs verbs refuse a bad command line before they connect.
func TestUsersVerbsRefuseBadArgumentsBeforeConnecting(t *testing.T) {
	noDatabase := func(string) (string, bool) { return "", false }
	cases := []struct {
		name string
		run  func(context.Context, cli.Env) int
		args []string
	}{
		{"create needs --email", runUsersCreate, []string{"--password", "password1"}},
		{"create needs --password", runUsersCreate, []string{"--email", "a@example.com"}},
		{"create refuses a positional", runUsersCreate, []string{"--email", "a@example.com", "--password", "password1", "x"}},
		{"orgs create needs --name", runOrgsCreate, nil},
		{"update refuses an unknown role", runUsersUpdate, []string{"--email", "a@example.com", "--org", "x", "--role", "root"}},
		{"list refuses a positional", runUsersList, []string{"x"}},
		{"orgs list refuses a non-number limit", runOrgsList, []string{"--limit", "many"}},
		{"orgs delete needs --org-id", runOrgsDelete, nil},
		{"llm get needs an organization", runLLMGet, nil},
		{"llm get refuses an empty --org", runLLMGet, []string{"--org", ""}},
		{"llm set needs --provider", runLLMSet, []string{"--org", "x"}},
		{"llm set needs an organization", runLLMSet, []string{"--provider", "openai"}},
		{"llm delete needs an organization", runLLMDelete, nil},
		{"orgs delete refuses a positional", runOrgsDelete, []string{"--org-id", "x", "y"}},
	}
	for _, c := range cases {
		var stdout, stderr bytes.Buffer
		code := c.run(context.Background(), cli.Env{Args: c.args, Stdout: &stdout, Stderr: &stderr, Lookup: noDatabase})
		if code != cli.ExitUsage {
			t.Errorf("%s: exit %d, want %d (stderr %q)", c.name, code, cli.ExitUsage, stderr.String())
		}
		if stdout.Len() != 0 {
			t.Errorf("%s: stdout = %q", c.name, stdout.String())
		}
	}
	// --no-active and --active are one tri-state flag, the last one wins.
	var active optBool
	flags := newFlags(cli.Env{Stderr: &bytes.Buffer{}}, "t")
	flags.Var(boolFlag{target: &active, on: true}, "active", "")
	flags.Var(boolFlag{target: &active, on: false}, "no-active", "")
	if err := flags.Parse([]string{"--active", "--no-active"}); err != nil || active.ptr() == nil || *active.ptr() {
		t.Fatalf("--active --no-active gave %v (%v), want false", active.ptr(), err)
	}
	if fresh := (&optBool{}); fresh.ptr() != nil {
		t.Fatal("an unset tri-state flag must be nil")
	}
}
