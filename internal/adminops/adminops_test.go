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
	want := "users create,users list,users update,orgs create,orgs list,orgs delete,llm-settings get,llm-settings set,llm-settings delete,licenses keygen,licenses create,bundles create,bundles list,bundles assign-plan,bundles assign-org,billing seed,billing list,billing pull-stripe,billing sync-stripe,features seed"
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
		{"create refuses --password with --password-stdin", runUsersCreate, []string{"--email", "a@example.com", "--password", "password1", "--password-stdin"}},
		{"create --password-stdin needs a password line", runUsersCreate, []string{"--email", "a@example.com", "--password-stdin"}},
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
		{"licenses keygen refuses a positional", runLicensesKeygen, []string{"x"}},
		{"licenses create needs --org-id and --tier", runLicensesCreate, nil},
		{"licenses create refuses an unknown tier", runLicensesCreate, []string{"--org-id", "o", "--tier", "bogus"}},
		{"licenses create refuses a non-integer duration", runLicensesCreate, []string{"--org-id", "o", "--tier", "team", "--duration-days", "many"}},
		{"bundles create needs its flags", runBundlesCreate, []string{"--key", "k"}},
		{"bundles list refuses a positional", runBundlesList, []string{"x"}},
		{"billing seed refuses a positional", runBillingSeed, []string{"x"}},
		{"billing list refuses a flag", runBillingList, []string{"--all"}},
		{"billing pull-stripe refuses a positional", runBillingPullStripe, []string{"x"}},
		{"billing sync-stripe refuses a flag", runBillingSyncStripe, []string{"--dry-run"}},
		{"bundles assign-plan needs its flags", runBundlesAssignPlan, []string{"--bundle-key", "b"}},
		{"bundles assign-org needs its flags", runBundlesAssignOrg, []string{"--org-id", "o"}},
		{"bundles assign-org refuses a non-integer expiry", runBundlesAssignOrg, []string{"--org-id", "o", "--feature-key", "f", "--expires-days", "soon"}},
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

// --password-stdin takes the first line of standard input as the password and
// never an empty one; a good line reaches the database step (no database here,
// so a failure, not a usage refusal).
func TestUsersCreatePasswordStdin(t *testing.T) {
	for in, want := range map[string]string{"pw12345678\n": "pw12345678", "pw12345678\r\nrest\n": "pw12345678", "pw12345678": "pw12345678", " sp ace \n": " sp ace ", "pw12345678\r": "pw12345678\r", "pw12345678\r\r\n": "pw12345678\r"} {
		if got, err := readPasswordLine(strings.NewReader(in)); err != nil || got != want {
			t.Errorf("readPasswordLine(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, in := range []string{"", "\n", "\r\n"} {
		if got, err := readPasswordLine(strings.NewReader(in)); err == nil {
			t.Errorf("readPasswordLine(%q) = %q, nil; want an error", in, got)
		}
	}
	if _, err := readPasswordLine(nil); err == nil {
		t.Error("a nil stdin must be an error")
	}
	noDatabase := func(string) (string, bool) { return "", false }
	var stdout, stderr bytes.Buffer
	code := runUsersCreate(context.Background(), cli.Env{
		Args: []string{"--email", "a@example.com", "--password-stdin"}, Stdin: strings.NewReader("pw12345678\n"),
		Stdout: &stdout, Stderr: &stderr, Lookup: noDatabase,
	})
	if code != cli.ExitFailure {
		t.Errorf("a good stdin password gave exit %d, want %d (stderr %q)", code, cli.ExitFailure, stderr.String())
	}
	// Both given, with a good stdin line: still a usage refusal, not a silent pick.
	stderr.Reset()
	code = runUsersCreate(context.Background(), cli.Env{
		Args: []string{"--email", "a@example.com", "--password", "password1", "--password-stdin"}, Stdin: strings.NewReader("pw12345678\n"),
		Stdout: &stdout, Stderr: &stderr, Lookup: noDatabase,
	})
	if code != cli.ExitUsage || !strings.Contains(stderr.String(), "mutually exclusive") {
		t.Errorf("--password with --password-stdin gave exit %d (stderr %q), want a mutually-exclusive refusal", code, stderr.String())
	}
	if strings.Contains(stderr.String()+stdout.String(), "pw12345678") {
		t.Error("the password was echoed")
	}
}

// An argv secret still works, with a WARN that names the flag and never the
// value; the stdin forms of update and llm-settings set follow the same rules.
func TestSecretFlagsWarnAndStdinForms(t *testing.T) {
	const secret = "s3cr3t-value-9f2"
	noDatabase := func(string) (string, bool) { return "", false }
	cases := []struct {
		name     string
		run      func(context.Context, cli.Env) int
		args     []string
		stdin    string
		wantCode int
		wantWarn string
		wantErr  string
	}{
		{"create argv warns", runUsersCreate, []string{"--email", "a@example.com", "--password", secret}, "", cli.ExitFailure, "WARN: --password ", ""},
		{"update argv warns", runUsersUpdate, []string{"--email", "a@example.com", "--password", secret}, "", cli.ExitFailure, "WARN: --password ", ""},
		{"llm set argv warns", runLLMSet, []string{"--org", "o", "--provider", "p", "--api-key", secret}, "", cli.ExitFailure, "WARN: --api-key ", ""},
		{"update stdin reaches the database", runUsersUpdate, []string{"--email", "a@example.com", "--password-stdin"}, secret + "\n", cli.ExitFailure, "", ""},
		{"update both forms refused", runUsersUpdate, []string{"--email", "a@example.com", "--password", secret, "--password-stdin"}, secret + "\n", cli.ExitUsage, "", "mutually exclusive"},
		{"update empty stdin refused", runUsersUpdate, []string{"--email", "a@example.com", "--password-stdin"}, "\n", cli.ExitUsage, "", "first line of standard input"},
		{"llm set stdin reaches the database", runLLMSet, []string{"--org", "o", "--provider", "p", "--api-key-stdin"}, secret + "\n", cli.ExitFailure, "", ""},
		{"llm set both forms refused", runLLMSet, []string{"--org", "o", "--provider", "p", "--api-key", secret, "--api-key-stdin"}, secret + "\n", cli.ExitUsage, "", "mutually exclusive"},
		{"llm set empty stdin refused", runLLMSet, []string{"--org", "o", "--provider", "p", "--api-key-stdin"}, "", cli.ExitUsage, "", "first line of standard input"},
	}
	for _, c := range cases {
		var stdout, stderr bytes.Buffer
		code := c.run(context.Background(), cli.Env{Args: c.args, Stdin: strings.NewReader(c.stdin), Stdout: &stdout, Stderr: &stderr, Lookup: noDatabase})
		if code != c.wantCode {
			t.Errorf("%s: exit %d, want %d (stderr %q)", c.name, code, c.wantCode, stderr.String())
		}
		if c.wantWarn != "" && !strings.Contains(stderr.String(), c.wantWarn) {
			t.Errorf("%s: stderr %q lacks %q", c.name, stderr.String(), c.wantWarn)
		}
		if c.wantWarn == "" && strings.Contains(stderr.String(), "WARN: --") {
			t.Errorf("%s: unexpected argv WARN %q", c.name, stderr.String())
		}
		if c.wantErr != "" && !strings.Contains(stderr.String(), c.wantErr) {
			t.Errorf("%s: stderr %q lacks %q", c.name, stderr.String(), c.wantErr)
		}
		if strings.Contains(stdout.String()+stderr.String(), secret) {
			t.Errorf("%s: the secret reached the output", c.name)
		}
	}
}
