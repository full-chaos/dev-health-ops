package adminops

import (
	"strings"
	"testing"
)

const noEchoDSN = "postgres://canary_user:canary_pass@canary.invalid/x"

var noEchoCanaries = []string{"canary_user", "canary_pass", "canary.invalid", "postgres://", "canary_scope"}

func TestServiceCredentialArgErrorsKeepTypedInputOut(t *testing.T) {
	const id = "00000000-0000-4000-8000-0000000000aa"
	for name, tc := range map[string]struct {
		verb string
		args []string
	}{
		"list dsn positional":       {"list", []string{noEchoDSN}},
		"list dsn as option":        {"list", []string{"--" + noEchoDSN}},
		"list unknown option value": {"list", []string{"--canary_pass=canary_scope"}},
		"list short unknown":        {"list", []string{"-x=canary_pass"}},
		"create dsn positional":     {"create", []string{"--scope", "x", noEchoDSN}},
		"create bad service":        {"create", []string{"--service", "canary_pass", "--scope", "x"}},
		"create ambiguous":          {"create", []string{"--s=canary_pass"}},
		"create ignored explicit":   {"create", []string{"--help=canary_pass"}},
		"rotate bad overlap":        {"rotate", []string{id, "--scope", "x", "--overlap-seconds", "canary_pass"}},
		"rotate second positional":  {"rotate", []string{id, noEchoDSN, "--scope", "x"}},
		"revoke unknown":            {"revoke", []string{id, "--canary_pass=canary_scope"}},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runCredentialVerb(t, tc.verb, tc.args...)
			if code != 2 {
				t.Fatalf("exit %d: %q", code, stderr)
			}
			for _, c := range noEchoCanaries {
				if strings.Contains(stdout+stderr, c) {
					t.Errorf("output holds %q: %q %q", c, stdout, stderr)
				}
			}
		})
	}
}

func TestServiceCredentialUnrecognizedStatesTheCount(t *testing.T) {
	_, _, stderr := runCredentialVerb(t, "list", noEchoDSN, "--canary_pass=canary_scope")
	if !strings.Contains(stderr, "unrecognized arguments (2)") {
		t.Fatalf("no count in %q", stderr)
	}
}
