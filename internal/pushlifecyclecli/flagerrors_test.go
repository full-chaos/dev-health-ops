package pushlifecyclecli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

var canaryBytes = []string{
	"canaryuser", "canarypass", "canaryhost", "canarydb", "postgres://", "canary:scope",
	"11111111-2222-3333-4444-555555555555", "99999999999999999999",
}

func TestFlagErrorsNeverEchoTypedInput(t *testing.T) {
	const dsn = "postgres://canaryuser:canarypass@canaryhost.invalid/canarydb"
	cases := []struct {
		verb, name string
		args       []string
		wantFlag   string
	}{
		{"backfill-created", "int bad value", []string{"-batch=" + dsn}, "-batch"},
		{"backfill-created", "int out of range", []string{"-batch=99999999999999999999"}, "-batch"},
		{"backfill-created", "bool bad value", []string{"-apply=11111111-2222-3333-4444-555555555555"}, "-apply"},
		{"backfill-created", "unknown flag", []string{"-" + dsn}, ""},
		{"backfill-created", "unknown flag with value", []string{"--" + dsn + "=canary:scope"}, ""},
		{"reconcile", "int bad value", []string{"-batch=" + dsn}, "-batch"},
		{"reconcile", "int out of range", []string{"-batch=99999999999999999999"}, "-batch"},
		{"reconcile", "duration bad value", []string{"-confirm-delay=canary:scope"}, "-confirm-delay"},
		{"reconcile", "bool bad value", []string{"-list=11111111-2222-3333-4444-555555555555"}, "-list"},
		{"reconcile", "unknown flag", []string{"-" + dsn}, ""},
		{"reconcile", "unknown flag with value", []string{"--" + dsn + "=canary:scope"}, ""},
	}
	children := map[string]cli.Command{}
	for _, c := range Command().Children {
		children[c.Name] = c
	}
	if len(children) != 2 {
		t.Fatalf("verbs changed: %d; add cells for the new verb", len(children))
	}
	for _, tc := range cases {
		t.Run(tc.verb+"/"+tc.name, func(t *testing.T) {
			var so, se bytes.Buffer
			env := cli.Env{Args: tc.args, Stdout: &so, Stderr: &se, Lookup: func(string) (string, bool) { return "", false }}
			code := children[tc.verb].Run(context.Background(), env)
			if code != cli.ExitUsage {
				t.Fatalf("exit %d, want %d", code, cli.ExitUsage)
			}
			for _, c := range canaryBytes {
				if strings.Contains(se.String(), c) || strings.Contains(so.String(), c) {
					t.Fatalf("output holds %q: stderr=%q stdout=%q", c, se.String(), so.String())
				}
			}
			if tc.wantFlag != "" && !strings.Contains(se.String(), tc.wantFlag) {
				t.Fatalf("stderr does not name %s: %q", tc.wantFlag, se.String())
			}
		})
	}
}
