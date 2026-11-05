package main

import (
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

var flagCanaries = []string{"canary_user", "canary_pass", "canary.invalid", "postgres://", "canary_scope"}

// pyargparseParityLeaves parse with internal/pyargparse, which repeats an unrecognized argument as Python's
// argparse does (the oracle corpus freezes that text); they are not flag.FlagSet surfaces and are not part of
// CHAOS-13053.
var pyargparseParityLeaves = map[string]bool{"service-credentials list": true}

// TestEveryDhoLeafKeepsTypedFlagInputOutOfItsOutput walks the real tree: an unknown flag whose name and
// value are a DSN and a scope, typed after each command, must leave no byte of it on stdout or stderr.
func TestEveryDhoLeafKeepsTypedFlagInputOutOfItsOutput(t *testing.T) {
	var all []leaf
	leaves(commands(), nil, &all)
	forms := map[string][]string{
		"unknown flag":            {"-postgres://canary_user:canary_pass@canary.invalid/x"},
		"unknown flag with value": {"--canary_pass=canary_scope"},
	}
	for _, l := range all {
		if l.command.IgnoresArguments || pyargparseParityLeaves[l.name()] {
			continue
		}
		for form, after := range forms {
			t.Run(l.name()+"/"+form, func(t *testing.T) {
				out, code := helpWith(t, commands(), l.path, nil, after, false)
				for _, c := range flagCanaries {
					if strings.Contains(out, c) {
						t.Fatalf("output holds %q (exit %d): %q", c, code, out)
					}
				}
				if code == cli.ExitOK {
					t.Fatalf("exit 0 for an unknown flag: %q", out)
				}
			})
		}
	}
}

// TestBadFlagValueNeverEchoedPerVerbFamily types a canary as the value of a typed (bool, int, duration)
// flag of one verb per family; the error must name the flag and hold no byte of the value.
func TestBadFlagValueNeverEchoedPerVerbFamily(t *testing.T) {
	const dsn = "postgres://canary_user:canary_pass@canary.invalid/x"
	cases := []struct {
		family string
		path   string
		flag   string
	}{
		{"adminops", "admin orgs delete", "dry-run"},
		{"adminops-users", "admin users create", "superuser"},
		{"rivermigrate", "migrate owner-move", "plan"},
		{"chmigrate", "migrate clickhouse status", "check"},
		{"pgmigrate", "migrate postgres preflight", "strict"},
		{"pgmigrate-history", "migrate postgres history", "verbose"},
		{"platform-shell", "api", "version"},
		{"fixturescli", "fixtures generate", "days"},
		{"maintenancecli", "maintenance scrub-error-text", "apply"},
		{"metricscli", "metrics validate-flags", "lookback"},
		{"backfillrun", "backfill run", "backfill"},
		{"pushcli", "push status", "poll"},
		{"synccli", "sync teams", "structure"},
		{"usersbackfill", "auth users-backfill", "batch"},
		{"mintcli-edgetoken", "mint edge-token", "ttl"},
		{"mintcli-envelopekeys", "mint envelope-keys", "skip-existing"},
	}
	var all []leaf
	leaves(commands(), nil, &all)
	byPath := map[string]leaf{}
	for _, l := range all {
		byPath[l.name()] = l
	}
	for _, tc := range cases {
		l, found := byPath[tc.path]
		if !found {
			t.Fatalf("%s: no command %q in the tree", tc.family, tc.path)
		}
		for form, value := range map[string]string{"dsn": dsn, "scope": "canary_scope", "int-range": "99999999999999999999"} {
			t.Run(tc.family+"/"+form, func(t *testing.T) {
				out, code := helpWith(t, commands(), l.path, nil, []string{"-" + tc.flag + "=" + value}, false)
				for _, c := range append([]string{"99999999999999999999"}, flagCanaries...) {
					if strings.Contains(out, c) {
						t.Fatalf("output holds %q (exit %d): %q", c, code, out)
					}
				}
				if code != cli.ExitUsage {
					t.Fatalf("exit %d, want %d: %q", code, cli.ExitUsage, out)
				}
				if !strings.Contains(out, "-"+tc.flag) {
					t.Fatalf("output does not name -%s: %q", tc.flag, out)
				}
			})
		}
	}
}
