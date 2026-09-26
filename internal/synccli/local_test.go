package synccli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

// CHAOS-6775: `dho sync git|prs --provider local` runs localgit.

type localHarness struct {
	plans    []Plan
	opened   int
	firstOrg []string
	org      string
	found    bool
	err      error
}

func (h *localHarness) executor() Executor {
	return InlineExecutor(InlineDeps{
		OpenStore: func(context.Context, string) (driver.Conn, error) { h.opened++; return fakeStore{}, nil },
		FirstOrg: func(_ context.Context, dbURL string) (string, bool) {
			h.firstOrg = append(h.firstOrg, dbURL)
			return h.org, h.found
		},
		Local: func(_ context.Context, plan Plan, _ driver.Conn, _ cli.Env, _ time.Time) error {
			h.plans = append(h.plans, plan)
			return h.err
		},
	})
}

func TestLocalRunsWithTheGivenOrganization(t *testing.T) {
	for _, target := range []string{"git", "prs"} {
		h := &localHarness{}
		code, _, stderr := runVerb(t, target, h.executor(), []string{"--provider", "local", "--repo-path", "/repo", "--org", "o", "--since", "2026-01-02"}, dbEnv)
		if code != cli.ExitOK {
			t.Fatalf("%s: exit %d: %s", target, code, stderr)
		}
		if len(h.plans) != 1 || h.plans[0].Org == nil || *h.plans[0].Org != "o" || h.plans[0].RepoPath != "/repo" || h.plans[0].Since == nil {
			t.Fatalf("%s: plans = %+v", target, h.plans)
		}
		if h.plans[0].SyncGit != (target == "git") || h.plans[0].SyncPrs != (target == "prs") {
			t.Errorf("%s: flags = git %v prs %v", target, h.plans[0].SyncGit, h.plans[0].SyncPrs)
		}
		if len(h.firstOrg) != 0 {
			t.Errorf("--org was given, but the first organization was looked up")
		}
	}
}

func TestLocalResolvesTheFirstOrganizationLikeMain(t *testing.T) {
	h := &localHarness{org: "org-first", found: true}
	code, _, stderr := runVerb(t, "git", h.executor(), []string{"--provider", "local", "--repo-path", "/repo"}, dbEnv)
	if code != cli.ExitOK || len(h.plans) != 1 || h.plans[0].Org == nil || *h.plans[0].Org != "org-first" {
		t.Fatalf("exit %d plans %+v: %s", code, h.plans, stderr)
	}
	if len(h.firstOrg) != 1 || h.firstOrg[0] != "postgresql://db/x" {
		t.Fatalf("first-organization lookups = %v", h.firstOrg)
	}
}

func TestLocalWithNoOrganizationRunsWithNone(t *testing.T) {
	// Python goes on with org_id None: no org_id column is written.
	h := &localHarness{}
	code, _, stderr := runVerb(t, "git", h.executor(), []string{"--provider", "local", "--repo-path", "/repo"}, chEnv)
	if code != cli.ExitOK || len(h.plans) != 1 || h.plans[0].Org != nil {
		t.Fatalf("exit %d plans %+v: %s", code, h.plans, stderr)
	}
}

func TestLocalFailureIsTheVerbsFailure(t *testing.T) {
	h := &localHarness{org: "o", found: true, err: errors.New("boom")}
	code, _, stderr := runVerb(t, "git", h.executor(), []string{"--provider", "local", "--org", "o"}, dbEnv)
	if code != cli.ExitFailure || !strings.Contains(stderr, "boom") {
		t.Fatalf("exit %d: %s", code, stderr)
	}
}

func TestLocalBlameIsStillRefusedAndNamesItsTicket(t *testing.T) {
	h := &localHarness{}
	code, _, stderr := runVerb(t, "blame", h.executor(), []string{"--provider", "local", "--org", "o"}, dbEnv)
	if code != cli.ExitRefused || !strings.Contains(stderr, ticketLocalBlame) || h.opened != 0 || len(h.plans) != 0 {
		t.Fatalf("exit %d opened %d plans %d: %s", code, h.opened, len(h.plans), stderr)
	}
}

// A folder with no .git is Python's logged error and a clean exit.
func TestSyncLocalWithNoRepositoryWritesNothingAndSucceeds(t *testing.T) {
	var stderr bytes.Buffer
	plan := Plan{RepoPath: t.TempDir(), SyncGit: true}
	if err := syncLocal(context.Background(), plan, fakeStore{}, cli.Env{Stderr: &stderr}, time.Now()); err != nil {
		t.Fatalf("err = %v", err)
	}
	if !strings.Contains(stderr.String(), "no git repository") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}
