package adminops

import (
	"bytes"
	"errors"
	"fmt"
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
