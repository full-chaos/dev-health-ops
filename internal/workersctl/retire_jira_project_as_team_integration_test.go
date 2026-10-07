//go:build integration

package workersctl

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providersync"
	clickhousestore "github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// TestRetireJiraProjectAsTeamVerbDryRunThenRetireThenZero runs `providersync
// retire-jira-project-as-team --org-stdin` against a real migrated
// ClickHouse: a dry run counts and writes nothing, a real run retires the
// class (the same function the Jira team catalog run calls), a second real
// run finds nothing. No output or log line holds the organization id.
func TestRetireJiraProjectAsTeamVerbDryRunThenRetireThenZero(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhousestore.Open(ctx, clickhousestore.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatalf("open ClickHouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	orgID := uuid.NewString()
	old := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	exec := func(query string, args ...any) {
		t.Helper()
		if err := conn.Exec(ctx, query, args...); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	// One project-as-team (with manual members and a sync policy), its
	// ownership and lead, and an Atlassian team that stays.
	exec(`INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('OPS', ?, 'Ops', [], ['person@example.test'], ['OPS'], [], 1, ?, ?, 'jira', 'OPS')`, uuid.New(), old, orgID)
	exec(`INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('0b1f3b0a-platform', ?, 'Platform', [], [], ['PLAT'], [], 1, ?, ?, 'jira', 'ari:cloud:identity::team/0b1f3b0a-platform')`, uuid.New(), old, orgID)
	exec(`INSERT INTO team_sync_policies (org_id, team_id, sync_policy, updated_at) VALUES (?, 'OPS', 1, ?)`, orgID, old)
	exec(`INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', 'OPS', '10001', 'OPS', 'native', 1, 100, 10, ?, ?)`, orgID, old, old)
	exec(`INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', '0b1f3b0a-platform', '10002', 'PLAT', 'native', 1, 100, 10, ?, ?)`, orgID, old, old)
	exec(`INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, 'jira', 'OPS', 'jira:lead-1', [], 'native', 1, 100, 10, ?, ?)`, orgID, old, old)

	count := func(query string) uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, query, orgID).Scan(&n); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return n
	}
	state := func() [4]uint64 {
		return [4]uint64{
			count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'OPS' AND is_active = 1`),
			count(`SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = 'OPS' AND valid_to IS NULL`),
			count(`SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'OPS' AND valid_to IS NULL`),
			count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id = '0b1f3b0a-platform' AND is_active = 1`) +
				count(`SELECT count() FROM team_project_ownership FINAL WHERE org_id = ? AND team_id = '0b1f3b0a-platform' AND valid_to IS NULL`),
		}
	}

	run := func(extra ...string) providersync.JiraProjectAsTeamRetireOutcome {
		t.Helper()
		auditor := &recordingAuditor{}
		runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, auditor)
		runtime.lookup = func(key string) (string, bool) {
			if key == "CLICKHOUSE_URI" {
				return instance.URI, true
			}
			return "", false
		}
		runtime.stdin = strings.NewReader(orgID + "\n")
		var stdout, stderr bytes.Buffer
		args := append([]string{"providersync", "retire-jira-project-as-team", "--org-stdin"}, extra...)
		if code := dispatch(ctx, runtime, args, &stdout, &stderr); code != 0 {
			t.Fatalf("%v: code=%d stderr=%q", extra, code, stderr.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), orgID) {
			t.Fatalf("%v: output holds the org id: stdout=%q stderr=%q", extra, stdout.String(), stderr.String())
		}
		dryRun := len(extra) > 0 && extra[0] == "--dry-run"
		if wantEvents := map[bool]int{true: 0, false: 1}[dryRun]; len(auditor.events) != wantEvents {
			t.Fatalf("%v: %d audit rows, want %d", extra, len(auditor.events), wantEvents)
		}
		if !dryRun && auditor.events[0].ResourceID != orgID {
			t.Fatalf("audit row resource = %q, want the organization from stdin", auditor.events[0].ResourceID)
		}
		var answer map[string]providersync.JiraProjectAsTeamRetireOutcome
		if err := json.Unmarshal(stdout.Bytes(), &answer); err != nil {
			t.Fatalf("decode %q: %v", stdout.String(), err)
		}
		outcome, ok := answer["retire_jira_project_as_team"]
		if !ok {
			t.Fatalf("stdout %q has no retire_jira_project_as_team", stdout.String())
		}
		return outcome
	}

	before := state()
	if before != [4]uint64{1, 1, 1, 2} {
		t.Fatalf("seeded state = %v", before)
	}
	dry := run("--dry-run")
	want := providersync.JiraProjectAsTeamRetireOutcome{DryRun: true, Teams: 1, TeamsWithManualMembers: 1, TeamsWithSyncPolicy: 1, OwnershipRows: 1, MembershipRows: 1}
	if dry != want {
		t.Fatalf("dry run = %+v, want %+v", dry, want)
	}
	if after := state(); after != before {
		t.Fatalf("a dry run wrote: state %v -> %v", before, after)
	}

	retired := run(auditFlags()...)
	want = providersync.JiraProjectAsTeamRetireOutcome{Teams: 1, TeamsWithManualMembers: 1, TeamsWithSyncPolicy: 1, OwnershipRows: 1, MembershipRows: 1,
		TeamsRetired: 1, OwnershipClosed: 1, MembershipClosed: 1}
	if retired != want {
		t.Fatalf("real run = %+v, want %+v", retired, want)
	}
	if after := state(); after != [4]uint64{0, 0, 0, 2} {
		t.Fatalf("after the real run state = %v, want the project-as-team retired and the Atlassian team kept", after)
	}

	if second := run(auditFlags()...); second != (providersync.JiraProjectAsTeamRetireOutcome{}) {
		t.Fatalf("second run = %+v, want all zero", second)
	}
}
