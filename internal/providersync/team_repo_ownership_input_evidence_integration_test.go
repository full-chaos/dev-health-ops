//go:build integration

package providersync

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// CHAOS-9349 against the real migration chain: the recorded input evidence of
// a derived claim holds its close while a recorded input row is missing, lets
// it close when every recorded row is in the run (a changed row), and records
// the rows of the chosen donor chain only.

func (org *retractionGuardOrg) exec(query string, args ...any) {
	org.t.Helper()
	if err := org.conn.Exec(org.ctx, query, args...); err != nil {
		org.t.Fatalf("%s: %v", query, err)
	}
}

// liveEvidence reads the live recorded keys of one claim, "input key" each.
func (org *retractionGuardOrg) liveEvidence(repo, team string) []string {
	org.t.Helper()
	rows, err := org.conn.Query(org.ctx, `
SELECT input, input_key
FROM (
    SELECT input, input_key, argMax(is_deleted, (seen_at, is_deleted)) AS deleted
    FROM team_repo_ownership_evidence
    WHERE org_id = ? AND repo_full_name = ? AND team_id = ?
    GROUP BY input, input_key
)
WHERE deleted = 0
ORDER BY input, input_key`, org.orgID, repo, team)
	if err != nil {
		org.t.Fatalf("read evidence: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var input, key string
		if err := rows.Scan(&input, &key); err != nil {
			org.t.Fatalf("scan evidence: %v", err)
		}
		out = append(out, input+" "+key)
	}
	if err := rows.Err(); err != nil {
		org.t.Fatalf("evidence rows.Err: %v", err)
	}
	return out
}

func (org *retractionGuardOrg) linearTeam(id, key string) {
	org.t.Helper()
	org.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES (?, ?, ?, [], [], [], 1, ?, ?, ?, 'linear', ?)`,
		id, uuid.New(), id, org.at, org.at, org.orgID, key)
}

func (org *retractionGuardOrg) mustDerive(wantWritten, wantRetracted, wantHeld int) TeamRepoOwnershipDerivationStats {
	org.t.Helper()
	written, retracted, inputsReady, stats := org.derive()
	if !inputsReady || written != wantWritten || retracted != wantRetracted || stats.RetractionsHeld != wantHeld {
		org.t.Fatalf("Derive: inputsReady=%v written=%d retracted=%d held=%d, want true %d %d %d",
			inputsReady, written, retracted, stats.RetractionsHeld, wantWritten, wantRetracted, wantHeld)
	}
	return stats
}

// donorOrg: a GitHub pull request of acme/repo-r reaches team-d only through a
// dependency edge to the Jira issue jira:D-1 of project jira-proj-d. jira-proj-z
// keeps an open Jira link of another team, so the kind guard proves the Jira
// kind whatever row of team-d is missing.
func donorOrg(t *testing.T, name string, org func(*retractionGuardOrg)) *retractionGuardOrg {
	t.Helper()
	ctx, conn := newWorkItemEffectsConn(t)
	o := newRetractionGuardOrg(t, ctx, conn, name, "acme/repo-r")
	seedTeamProjectOwnership(t, ctx, conn, o.orgID, "jira", "jira-proj-d", "team-d", true, o.at)
	seedTeamProjectOwnership(t, ctx, conn, o.orgID, "jira", "jira-proj-z", "team-z", true, o.at)
	seedWorkItemOfType(t, ctx, conn, o.orgID, "ghpr:acme/repo-r#1", "github", "pr", o.repos["acme/repo-r"], "", o.at)
	seedWorkItem(t, ctx, conn, o.orgID, "jira:D-1", "jira", uuid.Nil, "jira-proj-d", o.at)
	seedWorkItemDependency(t, ctx, conn, o.orgID, "ghpr:acme/repo-r#1", "jira:D-1", "relates_to", o.at)
	if org != nil {
		org(o)
	}
	o.mustDerive(1, 0, 0)
	o.assertOpenOwner("acme/repo-r", "team-d")
	return o
}

func TestDerivedRepoOwnershipMissingDonorInputHoldsTheClose(t *testing.T) {
	for _, tc := range []struct {
		name    string
		remove  func(*retractionGuardOrg)
		restore func(*retractionGuardOrg)
	}{
		{"missing donor item",
			func(o *retractionGuardOrg) { o.deleteRows("work_items", "jira:D-1") },
			func(o *retractionGuardOrg) {
				seedWorkItem(o.t, o.ctx, o.conn, o.orgID, "jira:D-1", "jira", uuid.Nil, "jira-proj-d", o.at)
			}},
		{"missing inheritable edge",
			func(o *retractionGuardOrg) {
				o.exec(`ALTER TABLE work_item_dependencies DELETE WHERE org_id = ? AND target_work_item_id = 'jira:D-1' SETTINGS mutations_sync = 2`, o.orgID)
			},
			func(o *retractionGuardOrg) {
				seedWorkItemDependency(o.t, o.ctx, o.conn, o.orgID, "ghpr:acme/repo-r#1", "jira:D-1", "relates_to", o.at)
			}},
		{"deleted project link key",
			func(o *retractionGuardOrg) {
				o.exec(`ALTER TABLE team_project_ownership DELETE WHERE org_id = ? AND project_id = 'jira-proj-d' SETTINGS mutations_sync = 2`, o.orgID)
			},
			func(o *retractionGuardOrg) {
				seedTeamProjectOwnership(o.t, o.ctx, o.conn, o.orgID, "jira", "jira-proj-d", "team-d", true, o.at)
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			org := donorOrg(t, "chaos-9349-donor-"+strings.ReplaceAll(tc.name, " ", "-"), nil)
			before := org.liveEvidence("acme/repo-r", "team-d")
			if len(before) != 4 {
				t.Fatalf("recorded evidence = %v, want the pull request, the edge, the donor and the link", before)
			}

			tc.remove(org)
			org.mustDerive(0, 0, 1)
			org.assertOpenOwner("acme/repo-r", "team-d")

			// Heal: the row is back, nothing is held, nothing is written again.
			tc.restore(org)
			org.mustDerive(0, 0, 0)
			org.assertOpenOwner("acme/repo-r", "team-d")
			if after := org.liveEvidence("acme/repo-r", "team-d"); strings.Join(after, "\n") != strings.Join(before, "\n") {
				t.Fatalf("evidence after heal = %v, want %v", after, before)
			}
		})
	}
}

// The Linear arm: the team comes from the donor's native_team_key and the
// Linear team row. A second Linear team and item keep the linear_team_key kind
// proven, so only the recorded team row can hold the close.
func TestDerivedRepoOwnershipMissingLinearTeamRowHoldsTheClose(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-linear-team", "acme/repo-r")
	org.linearTeam("linear:ENG", "ENG")
	org.linearTeam("linear:OPS", "OPS")
	seedWorkItemOfType(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "github", "pr", org.repos["acme/repo-r"], "", org.at)
	seedWorkItemWithNativeTeamKey(t, ctx, conn, org.orgID, "linear:ENG-1", "linear", uuid.Nil, "", "ENG", org.at)
	seedWorkItemWithNativeTeamKey(t, ctx, conn, org.orgID, "linear:OPS-1", "linear", uuid.Nil, "", "OPS", org.at)
	seedWorkItemDependency(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "linear:ENG-1", "relates_to", org.at)
	org.mustDerive(1, 0, 0)
	org.assertOpenOwner("acme/repo-r", "linear:ENG")

	org.exec(`ALTER TABLE teams DELETE WHERE org_id = ? AND id = 'linear:ENG' SETTINGS mutations_sync = 2`, org.orgID)
	org.mustDerive(0, 0, 1)
	org.assertOpenOwner("acme/repo-r", "linear:ENG")
}

// D1: the refused donor records no key, and a donor whose type turns into a
// pull request (the row is in the run, changed) closes the claim.
func TestDerivedRepoOwnershipRefusedDonorRecordsNoKeyAndAChangedDonorRetracts(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-refused-donor", "acme/repo-r")
	seedTeamProjectOwnership(t, ctx, conn, org.orgID, "github", "gh-proj-a", "team-a", true, org.at)
	seedTeamProjectOwnership(t, ctx, conn, org.orgID, "github", "gh-proj-b", "team-b", true, org.at)
	seedWorkItemOfType(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "github", "pr", org.repos["acme/repo-r"], "", org.at)
	seedWorkItemOfType(t, ctx, conn, org.orgID, "gh:acme/repo-r#7", "github", "issue", uuid.Nil, "gh-proj-a", org.at)
	// The pull request donor has the smaller id; it is refused, so it never shadows the issue donor.
	seedWorkItemOfType(t, ctx, conn, org.orgID, "gh:acme/repo-a#2", "github", "pr", uuid.Nil, "gh-proj-b", org.at)
	seedWorkItemDependency(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "gh:acme/repo-r#7", "relates_to", org.at)
	seedWorkItemDependency(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "gh:acme/repo-a#2", "relates_to", org.at)
	stats := org.mustDerive(1, 0, 0)
	if stats.DonorsRefused["pr_kind"] != 1 {
		t.Fatalf("donors refused = %v, want pr_kind 1", stats.DonorsRefused)
	}
	org.assertOpenOwner("acme/repo-r", "team-a")
	for _, key := range org.liveEvidence("acme/repo-r", "team-a") {
		if strings.Contains(key, "gh:acme/repo-a#2") || strings.Contains(key, "gh-proj-b") {
			t.Fatalf("the refused donor recorded the key %s", key)
		}
	}

	seedWorkItemOfType(t, ctx, conn, org.orgID, "gh:acme/repo-r#7", "github", "pr", uuid.Nil, "gh-proj-a", org.at.Add(time.Hour))
	org.mustDerive(0, 1, 0)
	org.assertNoOpenOwner("acme/repo-r")
	if live := org.liveEvidence("acme/repo-r", "team-a"); len(live) != 0 {
		t.Fatalf("evidence of the retracted claim = %v, want none", live)
	}
}

// D2: an empty-type Linear donor whose team key is known gives no claim and records no key.
func TestDerivedRepoOwnershipEmptyTypeDonorGivesNoClaimAndNoKey(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-empty-type", "acme/repo-r")
	org.linearTeam("linear:ENG", "ENG")
	seedWorkItemOfType(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "github", "pr", org.repos["acme/repo-r"], "", org.at)
	org.exec(`INSERT INTO work_items (repo_id, work_item_id, provider, type, project_id, native_team_key, org_id, last_synced) VALUES (?, 'linear:ENG-1', 'linear', '', '', 'ENG', ?, ?)`,
		uuid.Nil, org.orgID, org.at)
	seedWorkItemDependency(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "linear:ENG-1", "relates_to", org.at)
	stats := org.mustDerive(0, 0, 0)
	if stats.DonorsRefused["empty_type"] != 1 {
		t.Fatalf("donors refused = %v, want empty_type 1", stats.DonorsRefused)
	}
	org.assertNoOpenOwner("acme/repo-r")
	if live := org.liveEvidence("acme/repo-r", "linear:ENG"); len(live) != 0 {
		t.Fatalf("evidence = %v, want none", live)
	}
}

// extkeyOrg: a pull request names ENG-7 in its text; the Linear issue
// linear:ENG-7 gives linear:ENG. linear:OPS-1 keeps the linear_team_key kind proven.
func extkeyOrg(t *testing.T, name string) *retractionGuardOrg {
	t.Helper()
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, name, "acme/repo-r")
	org.linearTeam("linear:ENG", "ENG")
	org.linearTeam("linear:OPS", "OPS")
	seedWorkItemOfType(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "github", "pr", org.repos["acme/repo-r"], "", org.at)
	seedWorkItemWithNativeTeamKey(t, ctx, conn, org.orgID, "linear:ENG-7", "linear", uuid.Nil, "", "ENG", org.at)
	seedWorkItemWithNativeTeamKey(t, ctx, conn, org.orgID, "linear:OPS-1", "linear", uuid.Nil, "", "OPS", org.at)
	seedWorkItemDependency(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "extkey:ENG-7", "external_issue_key", org.at)
	org.mustDerive(1, 0, 0)
	org.assertOpenOwner("acme/repo-r", "linear:ENG")
	return org
}

// D3: the recorded edge key is the stored raw target; a missing donor row
// holds, and a restored one holds nothing and writes nothing.
func TestDerivedRepoOwnershipExtkeyChainRecordsTheRawTarget(t *testing.T) {
	org := extkeyOrg(t, "chaos-9349-extkey")
	live := org.liveEvidence("acme/repo-r", "linear:ENG")
	raw, donor := false, false
	for _, key := range live {
		raw = raw || (strings.HasPrefix(key, "dependency ") && strings.Contains(key, `"extkey:ENG-7"`))
		donor = donor || (strings.HasPrefix(key, "work_item ") && strings.Contains(key, `"linear:ENG-7"`))
	}
	if !raw || !donor {
		t.Fatalf("evidence = %v, want the raw extkey edge and the donor row", live)
	}

	org.deleteRows("work_items", "linear:ENG-7")
	org.mustDerive(0, 0, 1)
	org.assertOpenOwner("acme/repo-r", "linear:ENG")

	seedWorkItemWithNativeTeamKey(t, org.ctx, org.conn, org.orgID, "linear:ENG-7", "linear", uuid.Nil, "", "ENG", org.at)
	org.mustDerive(0, 0, 0)
	org.assertOpenOwner("acme/repo-r", "linear:ENG")
}

// D4: a second item takes the key ENG-7, so the extkey names no donor
// (unknown). Every recorded row is still in the run: the claim closes.
func TestDerivedRepoOwnershipAmbiguousExtkeyRetracts(t *testing.T) {
	org := extkeyOrg(t, "chaos-9349-extkey-ambiguous")
	seedWorkItem(t, org.ctx, org.conn, org.orgID, "jira:ENG-7", "jira", uuid.Nil, "jira-proj-q", org.at)
	stats := org.mustDerive(0, 1, 0)
	if stats.DonorsRefused["unknown"] != 1 {
		t.Fatalf("donors refused = %v, want unknown 1", stats.DonorsRefused)
	}
	org.assertNoOpenOwner("acme/repo-r")
}

// A missing row that flips the main owner holds the role close, and the
// repository keeps all its rows.
func TestDerivedRepoOwnershipMissingRowDoesNotFlipTheMainOwner(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-flip", "acme/repo-r")
	seedTeamProjectOwnership(t, ctx, conn, org.orgID, "jira", "jira-proj-a", "team-a", true, org.at)
	seedTeamProjectOwnership(t, ctx, conn, org.orgID, "jira", "jira-proj-b", "team-b", true, org.at)
	var teamA []string
	for i := 0; i < 3; i++ {
		teamA = append(teamA, org.link("jira", "jira-proj-a", "team-a", "acme/repo-r"))
	}
	for i := 0; i < 2; i++ {
		org.link("jira", "jira-proj-b", "team-b", "acme/repo-r")
	}
	org.mustDerive(2, 0, 0)
	before := org.teamRows("acme/repo-r", "team-a")

	org.deleteRows("work_items", teamA[0])
	org.deleteRows("work_items", teamA[1])
	_, _, _, stats := org.derive()
	if stats.RoleChanges != 0 || stats.RetractionsHeld == 0 {
		t.Fatalf("role changes=%d held=%d, want 0 and > 0", stats.RoleChanges, stats.RetractionsHeld)
	}
	after := org.teamRows("acme/repo-r", "team-a")
	if len(after) != len(before) || after[0].isPrimary != 1 || after[0].validTo != nil {
		t.Fatalf("team-a rows %+v, want the open main row %+v", after, before)
	}
}

// The native-first rule removed the fallback link a claim was recorded on: a
// native link of the same pull request is in the run, so the removal is
// witnessed and the claim closes. A rewrite of the same key at another tier
// holds nothing.
func TestDerivedRepoOwnershipOutrankedFallbackLinkIsAWitnessedRemoval(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-fallback", "acme/repo-r")
	repoID := org.repos["acme/repo-r"]
	seedTeamProjectOwnership(t, ctx, conn, org.orgID, "jira", "jira-proj-h", "team-h", true, org.at)
	seedWorkItem(t, ctx, conn, org.orgID, "jira:H-1", "jira", uuid.Nil, "jira-proj-h", org.at)
	seedWorkGraphIssuePRWithProvenance(t, ctx, conn, org.orgID, repoID, "jira:H-1", 5, "heuristic", org.at)
	org.mustDerive(1, 0, 0)

	seedWorkGraphIssuePRWithProvenance(t, ctx, conn, org.orgID, repoID, "jira:H-1", 5, "explicit_text", org.at.Add(time.Minute))
	org.mustDerive(0, 0, 0)
	org.assertOpenOwner("acme/repo-r", "team-h")

	seedWorkItem(t, ctx, conn, org.orgID, "jira:N-1", "jira", uuid.Nil, "", org.at)
	seedWorkGraphIssuePRWithProvenance(t, ctx, conn, org.orgID, repoID, "jira:N-1", 5, "native", org.at.Add(2*time.Minute))
	org.exec(`ALTER TABLE work_graph_issue_pr DELETE WHERE org_id = ? AND work_item_id = 'jira:H-1' SETTINGS mutations_sync = 2`, org.orgID)
	org.mustDerive(0, 1, 0)
	org.assertNoOpenOwner("acme/repo-r")
}

// A claim with no recorded key (written before the store) closes on the kind
// guard alone and is counted.
func TestDerivedRepoOwnershipClaimWithoutEvidenceFollowsTheKindGuard(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-bootstrap", "acme/repo-1", "acme/repo-2")
	org.own("jira", "jira-proj-a", "team-a", "acme/repo-1")
	org.own("jira", "jira-proj-a", "team-a", "acme/repo-2")
	org.mustDerive(2, 0, 0)
	org.exec(`ALTER TABLE team_repo_ownership_evidence DELETE WHERE org_id = ? SETTINGS mutations_sync = 2`, org.orgID)

	org.deleteRows("work_items", "jira:G-2")
	stats := org.mustDerive(0, 1, 0)
	if stats.RetractedWithoutEvidence != 1 {
		t.Fatalf("retracted without evidence = %d, want 1", stats.RetractedWithoutEvidence)
	}
	org.assertOpenOwner("acme/repo-1", "team-a")
	org.assertNoOpenOwner("acme/repo-2")
}

// Ruling 11: a carried team id keeps its claim's evidence, so a missing input
// row still holds the close of the carried claim.
func TestDerivedRepoOwnershipEvidenceIsCarriedWithTheTeamID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-carry", "acme/repo-1", "acme/repo-2")
	org.exec(`INSERT INTO teams (id, team_uuid, name, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key) VALUES ('CORE', ?, 'core', [], [], [], 1, ?, ?, ?, 'github', 'CORE')`,
		uuid.New(), org.at, org.at, org.orgID)
	org.own("jira", "jira-proj-a", "CORE", "acme/repo-1")
	org.own("jira", "jira-proj-a", "CORE", "acme/repo-2")
	org.mustDerive(2, 0, 0)

	outcome, err := CarryTeamIDs(org.ctx, org.conn, org.orgID, org.at.Add(time.Hour), false)
	if err != nil || outcome.RepoOwnership != 2 || outcome.RepoOwnershipEvidence == 0 {
		t.Fatalf("carry = %+v, %v", outcome, err)
	}
	carried := org.openOwners("acme/repo-2")
	if len(carried) != 1 || carried[0] == "CORE" {
		t.Fatalf("open owners of acme/repo-2 after the carry = %v, want the carried id", carried)
	}
	if len(org.liveEvidence("acme/repo-2", carried[0])) == 0 {
		t.Fatalf("no evidence under the carried id %s", carried[0])
	}

	// The carry does not move the share column, so the run writes acme/repo-1's claim again with its share.
	org.deleteRows("work_items", "jira:G-2")
	org.mustDerive(1, 0, 1)
	org.assertOpenOwner("acme/repo-1", carried[0])
	org.assertOpenOwner("acme/repo-2", carried[0])
}

// D-cell (CHAOS-14169 on CHAOS-9349), against the store: one sync writes a
// relates_to and a blocks edge of the same pull request and donor at one
// last_synced. The walk keeps the inheritable edge, the claim is derived, and
// the recorded dependency key is the relates_to edge, not the blocks edge.
func TestDerivedRepoOwnershipRecordsTheEdgeTheTieRuleKeeps(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	org := newRetractionGuardOrg(t, ctx, conn, "chaos-9349-tie", "acme/repo-r")
	seedTeamProjectOwnership(t, ctx, conn, org.orgID, "jira", "jira-proj-d", "team-d", true, org.at)
	seedWorkItemOfType(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "github", "pr", org.repos["acme/repo-r"], "", org.at)
	seedWorkItem(t, ctx, conn, org.orgID, "jira:D-1", "jira", uuid.Nil, "jira-proj-d", org.at)
	seedWorkItemDependency(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "jira:D-1", "blocks", org.at)
	seedWorkItemDependency(t, ctx, conn, org.orgID, "ghpr:acme/repo-r#1", "jira:D-1", "relates_to", org.at)
	org.mustDerive(1, 0, 0)
	org.assertOpenOwner("acme/repo-r", "team-d")
	var dependency []string
	for _, key := range org.liveEvidence("acme/repo-r", "team-d") {
		if strings.HasPrefix(key, "dependency ") {
			dependency = append(dependency, key)
		}
	}
	if want := `dependency ["ghpr:acme/repo-r#1","jira:D-1","relates_to"]`; len(dependency) != 1 || dependency[0] != want {
		t.Fatalf("recorded dependency keys = %v, want [%s]", dependency, want)
	}
}
