package cli

import (
	"strings"
	"testing"
)

func TestRootFlagErrorsKeepTypedInputOut(t *testing.T) {
	const dsn = "postgres://canary_user:canary_pass@canary.invalid/x"
	canaries := []string{"canary_user", "canary_pass", "canary.invalid", "postgres://", "canary_scope"}
	for name, args := range map[string][]string{
		"dsn as option":        {"--" + dsn, "fixtures"},
		"unknown option value": {"--canary_pass=canary_scope", "fixtures"},
		"short unknown":        {"-z=canary_pass", "fixtures"},
		"ambiguous":            {"--l=canary_pass", "fixtures"},
		"ignored explicit":     {"--help=canary_pass", "fixtures"},
		"bad concurrency":      {"--llm-concurrency", "canary_pass", "fixtures"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseRootFlags(args)
			if err == nil {
				t.Fatal("no error")
			}
			for _, c := range canaries {
				if strings.Contains(err.Msg, c) {
					t.Errorf("error holds %q: %q", c, err.Msg)
				}
			}
		})
	}
	_, err := parseRootFlags([]string{"--bogus_canary_pass", "fixtures"})
	if err == nil || err.Msg != "unrecognized arguments (1)" {
		t.Fatalf("got %v", err)
	}
}
