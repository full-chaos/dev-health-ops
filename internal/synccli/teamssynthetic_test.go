package synccli

import (
	"errors"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// TestSyntheticProviderIsAccepted proves --provider synthetic no longer hits
// the "must be jira, github, gitlab, linear or synthetic" usage refusal
// (CHAOS-7037) -- it dispatches to runSyntheticTeams, which then fails on
// the deliberately-erroring store, never touching real ClickHouse.
func TestSyntheticProviderIsAccepted(t *testing.T) {
	rec := &recorded{}
	openErr := errors.New("dial tcp: connection refused")
	d := stubDeps(rec, failingClient{}, openErr)
	code, _, stderr := run(t, map[string]string{"CLICKHOUSE_URI": dsnValue}, d, "--provider", "synthetic", "--org", "o")
	if code != cli.ExitFailure {
		t.Fatalf("exit %d, want %d (clickhouse_unavailable); stderr=%s", code, cli.ExitFailure, stderr)
	}
	if rec.opened != 1 {
		t.Fatalf("openStore called %d times, want 1", rec.opened)
	}
	if !strings.Contains(stderr, "clickhouse_unavailable") {
		t.Fatalf("stderr does not name the failure: %s", stderr)
	}
}

// TestSyntheticProviderStillRequiresOrg proves --org's validation runs
// before the provider dispatch (synccli.go:180-184 is unconditional on
// provider), so the synthetic branch never opens a store for a missing org.
func TestSyntheticProviderStillRequiresOrg(t *testing.T) {
	rec := &recorded{}
	d := stubDeps(rec, failingClient{}, nil)
	code, _, stderr := run(t, map[string]string{"CLICKHOUSE_URI": dsnValue}, d, "--provider", "synthetic")
	if code != cli.ExitUsage {
		t.Fatalf("exit %d, want %d; stderr=%s", code, cli.ExitUsage, stderr)
	}
	if rec.opened != 0 {
		t.Fatalf("opened the store despite the missing --org")
	}
}

// TestSyntheticProviderRequiresClickHouseURI proves the synthetic branch
// takes the SAME "CLICKHOUSE_URI is not set" configuration refusal every
// other provider takes, rather than skipping straight to generation.
func TestSyntheticProviderRequiresClickHouseURI(t *testing.T) {
	rec := &recorded{}
	d := stubDeps(rec, failingClient{}, nil)
	code, _, stderr := run(t, map[string]string{}, d, "--provider", "synthetic", "--org", "o")
	if code != cli.ExitRefused {
		t.Fatalf("exit %d, want %d; stderr=%s", code, cli.ExitRefused, stderr)
	}
	if rec.opened != 0 {
		t.Fatalf("opened the store without a configured CLICKHOUSE_URI")
	}
	if !strings.Contains(stderr, "CLICKHOUSE_URI") {
		t.Fatalf("stderr does not name CLICKHOUSE_URI: %s", stderr)
	}
}

// TestWrongProviderMentionsSynthetic proves the usage refusal's own text
// was updated, not just the validation logic.
func TestWrongProviderMentionsSynthetic(t *testing.T) {
	rec := &recorded{}
	d := stubDeps(rec, failingClient{}, nil)
	code, _, stderr := run(t, map[string]string{}, d, "--provider", "bitbucket", "--org", "o")
	if code != cli.ExitUsage {
		t.Fatalf("exit %d, want %d", code, cli.ExitUsage)
	}
	if !strings.Contains(stderr, "synthetic") {
		t.Fatalf("usage refusal does not mention synthetic: %s", stderr)
	}
}
