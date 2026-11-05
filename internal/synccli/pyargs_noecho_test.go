package synccli

import (
	"strings"
	"testing"
)

const noEchoDSN = "postgres://canary_user:canary_pass@canary.invalid/x"

var noEchoCanaries = []string{"canary_user", "canary_pass", "canary.invalid", "postgres://", "canary_scope"}

func TestSyncArgErrorsKeepTypedInputOut(t *testing.T) {
	for name, args := range map[string][]string{
		"dsn positional":       {"--provider", "local", noEchoDSN},
		"dsn as option":        {"--provider", "local", "--" + noEchoDSN},
		"unknown option value": {"--provider", "local", "--canary_pass=canary_scope"},
		"bad provider":         {"--provider", "canary_pass"},
		"bad int":              {"--provider", "local", "--batch-size", "canary_pass"},
		"bad float":            {"--provider", "local", "--rate-limit-delay", "canary_pass"},
		"bad date":             {"--provider", "local", "--since", "canary_pass"},
		"ambiguous":            {"--provider", "local", "--s=canary_pass"},
		"ignored explicit":     {"--help=canary_pass"},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runVerb(t, "deployments", neverRuns(t), args, chEnv)
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

func TestSyncUnrecognizedStatesTheCount(t *testing.T) {
	_, _, stderr := runVerb(t, "deployments", neverRuns(t), []string{"--provider", "local", noEchoDSN, "--canary_pass=canary_scope"}, chEnv)
	if !strings.Contains(stderr, "unrecognized arguments (2)") {
		t.Fatalf("no count in %q", stderr)
	}
}
