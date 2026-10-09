//go:build integration

package providersync

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

type carryFixture struct {
	t     *testing.T
	ctx   context.Context
	conn  driver.Conn
	orgID string
}

var (
	carryOld   = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	carryFirst = time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	carryAt    = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
)

const carryAtlassianID = "aaaaaaaa-0000-4000-8000-000000000001"
const carryAtlassianARI = "ari:cloud:identity::team/" + carryAtlassianID

func (f carryFixture) exec(query string, args ...any) {
	f.t.Helper()
	if err := f.conn.Exec(f.ctx, query, args...); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
}

func (f carryFixture) team(provider, id string, native, parent *string, active uint8, updated time.Time, manual []string, sourceID *uuid.UUID) {
	f.t.Helper()
	if manual == nil {
		manual = []string{}
	}
	var source any
	if sourceID != nil {
		source = *sourceID
	}
	f.exec(`INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, last_synced, org_id, provider, native_team_key, parent_team_id, source_id) VALUES (?, ?, ?, ['m1'], ?, [], [], ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, uuid.New(), "team "+id, manual, active, updated, updated, f.orgID, provider, native, parent, source)
}

func (f carryFixture) membership(provider, teamID, memberID string, from time.Time, to *time.Time) {
	f.t.Helper()
	f.exec(`INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, ?, ?, ?, [], 'native', 1, 100, 10, ?, ?, ?)`,
		f.orgID, provider, teamID, memberID, from, to, from)
}

func (f carryFixture) projectLink(provider, teamID, projectID, source string) {
	f.t.Helper()
	f.exec(`INSERT INTO team_project_ownership (org_id, provider, team_id, project_id, project_key, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, ?, NULL, ?, 1, 100, 10, ?, ?)`,
		f.orgID, provider, teamID, projectID, source, carryOld, carryOld)
}

func (f carryFixture) repoLink(provider, teamID, repo string) {
	f.t.Helper()
	f.exec(`INSERT INTO team_repo_ownership (org_id, provider, team_id, repo_id, repo_full_name, match_type, source, is_primary, specificity, priority, valid_from, updated_at) VALUES (?, ?, ?, NULL, ?, 'exact', 'inferred', 1, 100, 10, ?, ?)`,
		f.orgID, provider, teamID, repo, carryOld, carryOld)
}

func (f carryFixture) observation(provider, native, teamID string) {
	f.t.Helper()
	f.exec(`INSERT INTO team_provider_observations (org_id, provider, native_team_key, team_id, name, members_json, project_keys_json, repo_patterns_json, is_active, discovered_at, updated_at) VALUES (?, ?, ?, ?, 'obs', '[]', '[]', '[]', 1, ?, ?)`,
		f.orgID, provider, native, teamID, carryOld, carryOld)
}

func (f carryFixture) drift(teamID, field, status string) string {
	f.t.Helper()
	changeID := changeIDForTeamField(f.orgID, teamID, field, `"a"`, `"b"`)
	f.exec(`INSERT INTO team_drift_changes (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, updated_at) VALUES (?, ?, 'team', ?, 'linear', ?, 'field_changed', ?, '"a"', '"b"', ?, ?, ?, ?)`,
		f.orgID, changeID, teamID, teamID, field, status, carryFirst, carryOld, carryOld)
	return changeID
}

func (f carryFixture) count(query string, args ...any) uint64 {
	f.t.Helper()
	return countRows(f.t, f.ctx, f.conn, query, append([]any{f.orgID}, args...)...)
}

func (f carryFixture) str(query string, args ...any) string {
	f.t.Helper()
	var value string
	if err := f.conn.QueryRow(f.ctx, query, append([]any{f.orgID}, args...)...).Scan(&value); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
	return value
}

func carryPtr(value string) *string { return &value }

// seedEveryClass writes one organization's store with every row class the
// carry meets: provider teams with bare ids (Linear key, Atlassian uuid,
// pushed custom id), keyed teams, an admin team that a Linear observation
// names and one that no observation names, a Jira project-as-team row, an
// inactive bare team, an admin edit of a Linear team, and every row that
// names a team.
func seedEveryClass(f carryFixture) {
	source := uuid.New()
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.team("linear", "SUB", carryPtr("SUB"), carryPtr("ENG"), 1, carryOld, nil, nil)
	f.team("jira", carryAtlassianID, carryPtr(carryAtlassianARI), nil, 1, carryOld, nil, nil)
	f.team("custom", "platform", nil, nil, 1, carryOld, nil, &source)
	f.team("github", "gh:web", carryPtr("web"), nil, 1, carryOld, nil, nil)
	f.team("linear", "linear:OPS", carryPtr("OPS"), nil, 1, carryOld, nil, nil)
	f.team("", "DATA", nil, nil, 1, carryOld, []string{"m9"}, nil)
	f.team("", "chosen", nil, nil, 1, carryOld, nil, nil)
	f.team("jira", "PROJ", carryPtr("PROJ"), nil, 1, carryOld, nil, nil)
	f.team("linear", "OLD", carryPtr("OLD"), nil, 0, carryOld, nil, nil)
	f.team("linear", "QA", carryPtr("QA"), nil, 1, carryOld, nil, nil)
	f.team("", "QA", nil, nil, 1, carryOld.Add(time.Hour), []string{"admin-edit"}, nil)
	f.team("custom", "ENG", nil, nil, 0, carryOld.Add(-time.Hour), nil, nil)

	closed := carryFirst.AddDate(0, 1, 0)
	f.membership("linear", "ENG", "m1", carryFirst, &closed)
	f.membership("linear", "ENG", "m1", carryOld, nil)
	f.membership("linear", "ENG", "m2", carryOld, nil)
	f.membership("jira", carryAtlassianID, "m3", carryOld, nil)
	f.membership("github", "gh:web", "m4", carryOld, nil)
	f.membership("custom", "ENG", "m7", carryOld, nil)
	f.membership("", "DATA", "m9", carryOld, nil)
	f.projectLink("linear", "ENG", "proj-1", "native")
	f.projectLink("jira", carryAtlassianID, "proj-2", "jira_legacy")
	f.projectLink("jira", "PROJ", "PROJ", "native")
	f.repoLink("github", "ENG", "acme/api")
	f.observation("linear", "DATA", "DATA")
	f.observation("jira", carryAtlassianARI, carryAtlassianID)
	f.exec(`INSERT INTO team_sync_policies (org_id, team_id, sync_policy, managed_fields, updated_at) VALUES (?, 'ENG', 1, ['name'], ?)`, f.orgID, carryOld)
	f.exec(`INSERT INTO identities (org_id, canonical_id, identity_uuid, team_ids, updated_at) VALUES (?, 'person-1', ?, ['ENG', 'gh:web'], ?)`, f.orgID, uuid.New(), carryOld)
	f.exec(`INSERT INTO manual_attribution_fallbacks (org_id, provider, scope_type, scope_id, team_id, team_name, reason, valid_from, updated_at) VALUES (?, 'linear', 'project', 'proj-9', 'ENG', 'Eng', 'manual', ?, ?)`, f.orgID, carryOld, carryOld)
}

const carryActiveBareTeams = `SELECT arrayStringConcat(arraySort(groupArray(id)), ',') FROM teams FINAL WHERE org_id = ? AND is_active = 1 AND NOT (startsWith(id, 'gh:') OR startsWith(id, 'gl:') OR startsWith(id, 'linear:') OR startsWith(id, 'jira:') OR startsWith(id, 'custom:'))`

func TestCarryTeamIDsMovesEveryBareProviderTeamID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	other := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	seedEveryClass(f)
	other.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	other.membership("linear", "ENG", "m1", carryOld, nil)
	ghBefore := f.str(`SELECT toString(max(updated_at)) FROM teams WHERE org_id = ? AND id = 'gh:web'`)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil {
		t.Fatalf("carry: %v", err)
	}
	if outcome.Teams != 7 || outcome.AdminTeams != 2 || outcome.AdminTeamsToCustom != 1 || outcome.AdminTeamsNotCarried != 0 || outcome.AmbiguousTeams != 0 ||
		outcome.Memberships != 4 || outcome.ProjectOwnership != 2 || outcome.RepoOwnership != 1 || outcome.Observations != 2 ||
		outcome.SyncPolicies != 1 || outcome.DriftChanges != 0 || outcome.Identities != 1 || outcome.Fallbacks != 1 || outcome.RowsWritten == 0 {
		t.Fatalf("outcome = %+v", outcome)
	}

	// Bare census: only the Jira project-as-team row (retired by its own
	// step) stays active with a bare id; the admin's own team is custom:chosen.
	if got := f.str(carryActiveBareTeams); got != "PROJ" {
		t.Fatalf("active bare team ids = %q, want PROJ", got)
	}
	if got := f.str(`SELECT concat(toString(is_active), '/', provider) FROM teams FINAL WHERE org_id = ? AND id = 'custom:chosen'`); got != "1/" {
		t.Errorf("custom:chosen = %q, want an active admin team", got)
	}
	for _, table := range []string{"team_memberships", "team_project_ownership", "team_repo_ownership"} {
		if got := f.count(`SELECT count() FROM `+table+` FINAL WHERE org_id = ? AND valid_to IS NULL AND team_id IN ('ENG', 'SUB', 'DATA', 'QA', 'platform', ?) AND NOT (provider = 'custom' AND team_id = 'ENG')`, carryAtlassianID); got != 0 {
			t.Errorf("%s: %d open rows still name a carried bare id", table, got)
		}
	}
	if got := f.count(`SELECT count() FROM team_provider_observations FINAL WHERE org_id = ? AND team_id IN ('DATA', ?)`, carryAtlassianID); got != 0 {
		t.Errorf("observations still holding a bare id: %d", got)
	}

	// New rows: the provider's id, its native key, the team_uuid its writer
	// computes, the parent mapped; old rows inactive.
	for _, want := range []struct{ id, provider, native string }{
		{"linear:ENG", "linear", "ENG"}, {"linear:SUB", "linear", "SUB"}, {"jira:" + carryAtlassianID, "jira", carryAtlassianARI},
		{"custom:platform", "", "platform"}, {"linear:DATA", "", ""}, {"linear:QA", "linear", "QA"},
	} {
		got := f.str(`SELECT concat(provider, '|', ifNull(native_team_key, ''), '|', toString(is_active)) FROM teams FINAL WHERE org_id = ? AND id = ?`, want.id)
		if got != want.provider+"|"+want.native+"|1" {
			t.Errorf("team %s = %q, want %s|%s|1", want.id, got, want.provider, want.native)
		}
	}
	if got := f.str(`SELECT toString(team_uuid) FROM teams FINAL WHERE org_id = ? AND id = 'linear:ENG'`); got != uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:linear:ENG")).String() {
		t.Errorf("linear:ENG team_uuid = %s", got)
	}
	if got := f.str(`SELECT toString(team_uuid) FROM teams FINAL WHERE org_id = ? AND id = 'linear:DATA'`); got != uuid.NewSHA1(uuid.MustParse("6ba7b811-9dad-11d1-80b4-00c04fd430c8"), []byte("team:"+f.orgID+":linear:DATA")).String() {
		t.Errorf("admin linear:DATA team_uuid = %s, want the admin rule", got)
	}
	if got := f.str(`SELECT ifNull(parent_team_id, '') FROM teams FINAL WHERE org_id = ? AND id = 'linear:SUB'`); got != "linear:ENG" {
		t.Errorf("linear:SUB parent = %q, want linear:ENG", got)
	}
	if got := f.str(`SELECT arrayStringConcat(manual_members, ',') FROM teams FINAL WHERE org_id = ? AND id = 'linear:QA'`); got != "admin-edit" {
		t.Errorf("linear:QA manual_members = %q, want the admin edit", got)
	}
	if got := f.count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'custom:platform' AND source_id IS NOT NULL`); got != 1 {
		t.Errorf("custom:platform lost its source_id")
	}
	if got := f.count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id IN ('ENG', 'SUB', 'DATA', 'QA', 'platform', 'chosen', ?) AND is_active = 1`, carryAtlassianID); got != 0 {
		t.Errorf("%d old team rows still active", got)
	}
	if got := f.str(`SELECT toString(max(updated_at)) FROM teams WHERE org_id = ? AND id = 'gh:web'`); got != ghBefore {
		t.Errorf("keyed gh:web was written again: %s -> %s", ghBefore, got)
	}
	if got := f.count(`SELECT count() FROM teams WHERE org_id = ? AND id = 'linear:OPS'`); got != 1 {
		t.Errorf("keyed linear:OPS has %d rows, want 1", got)
	}
	if got := f.count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id IN ('OLD', 'PROJ') AND id IN (SELECT id FROM teams WHERE org_id = ? AND updated_at > ?)`, f.orgID, carryOld.Add(2*time.Hour)); got != 0 {
		t.Errorf("an inactive or project-as-team id was written: %d", got)
	}

	// Links: first-seen valid_from (closed rows included), old row closed.
	if got := f.str(`SELECT toString(valid_from) FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'linear:ENG' AND member_id = 'm1' AND valid_to IS NULL`); !strings.HasPrefix(got, "2026-06-01 00:00:00") {
		t.Errorf("linear:ENG m1 valid_from = %s, want the first-seen 2026-06-01", got)
	}
	if got := f.str(`SELECT toString(valid_from) FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'linear:ENG' AND member_id = 'm2' AND valid_to IS NULL`); !strings.HasPrefix(got, "2026-09-01 00:00:00") {
		t.Errorf("linear:ENG m2 valid_from = %s, want 2026-09-01", got)
	}
	if got := f.str(`SELECT toString(valid_to) FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'ENG' AND member_id = 'm2'`); !strings.HasPrefix(got, "2026-10-08 12:00:00") {
		t.Errorf("old ENG m2 valid_to = %s, want the carry time", got)
	}
	for _, want := range []struct{ table, provider, teamID, natural, value string }{
		{"team_memberships", "jira", "jira:" + carryAtlassianID, "member_id", "m3"},
		{"team_memberships", "", "linear:DATA", "member_id", "m9"},
		{"team_project_ownership", "linear", "linear:ENG", "project_id", "proj-1"},
		{"team_project_ownership", "jira", "jira:" + carryAtlassianID, "project_id", "proj-2"},
		{"team_project_ownership", "jira", "PROJ", "project_id", "PROJ"},
		{"team_repo_ownership", "github", "linear:ENG", "repo_full_name", "acme/api"},
		{"team_memberships", "github", "gh:web", "member_id", "m4"},
		{"team_memberships", "custom", "ENG", "member_id", "m7"},
	} {
		if got := f.count(`SELECT count() FROM `+want.table+` FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND `+want.natural+` = ? AND valid_to IS NULL`, want.provider, want.teamID, want.value); got != 1 {
			t.Errorf("%s open %s/%s/%s = %d, want 1", want.table, want.provider, want.teamID, want.value, got)
		}
	}

	// Rows that name the team without a provider.
	if got := f.count(`SELECT count() FROM team_sync_policies FINAL WHERE org_id = ? AND team_id = 'linear:ENG' AND sync_policy = 1`); got != 1 {
		t.Errorf("sync policy not on linear:ENG")
	}
	if got := f.str(`SELECT arrayStringConcat(team_ids, ',') FROM identities FINAL WHERE org_id = ? AND canonical_id = 'person-1'`); got != "linear:ENG,gh:web" {
		t.Errorf("identity team_ids = %q", got)
	}
	if got := f.str(`SELECT team_id FROM manual_attribution_fallbacks FINAL WHERE org_id = ? AND scope_id = 'proj-9'`); got != "linear:ENG" {
		t.Errorf("fallback team_id = %q", got)
	}
	if got := f.str(`SELECT team_id FROM team_provider_observations FINAL WHERE org_id = ? AND provider = 'jira'`); got != "jira:"+carryAtlassianID {
		t.Errorf("jira observation team_id = %q", got)
	}

	// The other organization is not touched.
	if got := other.count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'ENG' AND is_active = 1`); got != 1 {
		t.Errorf("the other organization's ENG was changed")
	}
	if got := other.count(`SELECT count() FROM teams WHERE org_id = ?`); got != 1 {
		t.Errorf("the other organization has %d team rows, want 1", got)
	}

	// A second run finds nothing and writes nothing.
	again, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), false)
	if err != nil || again.Found() || again.RowsWritten != 0 || again != (TeamIDCarryOutcome{}) {
		t.Fatalf("second run = %+v, %v; want zero", again, err)
	}
}

func TestCarryTeamIDsMovesDriftChangesWithTheirDecision(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	pending := f.drift("ENG", "name", "pending")
	f.drift("ENG", "description", "dismissed")

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.DriftChanges != 2 {
		t.Fatalf("carry = %+v, %v", outcome, err)
	}
	for _, want := range []struct{ field, status string }{{"name", "pending"}, {"description", "dismissed"}} {
		changeID := changeIDForTeamField(f.orgID, "linear:ENG", want.field, `"a"`, `"b"`)
		got := f.str(`SELECT concat(entity_id, '|', status, '|', toString(first_seen_at)) FROM team_drift_changes FINAL WHERE org_id = ? AND change_id = ?`, changeID)
		if !strings.HasPrefix(got, "linear:ENG|"+want.status+"|2026-06-01") {
			t.Errorf("%s change under linear:ENG = %q", want.field, got)
		}
	}
	if got := f.str(`SELECT status FROM team_drift_changes FINAL WHERE org_id = ? AND change_id = ?`, pending); got != "superseded" {
		t.Errorf("old pending change status = %q, want superseded", got)
	}
}

// A prefixed writer that ran first keeps its rows: the keyed team row and
// its open link are not written again, the bare ones are retired.
func TestCarryTeamIDsKeepsRowsAKeyedWriterWrote(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "CORE", carryPtr("CORE"), nil, 1, carryOld, nil, nil)
	f.membership("linear", "CORE", "m5", carryFirst, nil)
	later := carryOld.Add(24 * time.Hour)
	f.team("linear", "linear:CORE", carryPtr("CORE"), nil, 1, later, nil, nil)
	f.membership("linear", "linear:CORE", "m5", later, nil)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.TeamsAlreadyKeyed != 1 || outcome.LinkRowsAlreadyKeyed != 1 || outcome.Memberships != 0 {
		t.Fatalf("carry = %+v, %v", outcome, err)
	}
	if got := f.count(`SELECT count() FROM teams WHERE org_id = ? AND id = 'linear:CORE'`); got != 1 {
		t.Errorf("linear:CORE rows = %d, want the writer's one row", got)
	}
	if got := f.count(`SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'linear:CORE' AND valid_to IS NULL`); got != 1 {
		t.Errorf("open linear:CORE memberships = %d, want 1", got)
	}
	if got := f.count(`SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'CORE' AND valid_to IS NULL`); got != 0 {
		t.Errorf("bare CORE membership still open")
	}
	if got := f.str(carryActiveBareTeams); got != "" {
		t.Errorf("active bare ids = %q", got)
	}
}

// Two providers' teams with one bare id each move to their own id; the
// rows that name the id without a provider stay and are counted.
func TestCarryTeamIDsSplitsAnIDTwoProvidersHold(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.team("custom", "ENG", nil, nil, 1, carryOld.Add(time.Minute), nil, nil)
	f.membership("linear", "ENG", "m1", carryOld, nil)
	f.membership("custom", "ENG", "m2", carryOld, nil)
	f.exec(`INSERT INTO team_sync_policies (org_id, team_id, sync_policy, managed_fields, updated_at) VALUES (?, 'ENG', 1, [], ?)`, f.orgID, carryOld)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.Teams != 1 || outcome.AmbiguousTeams != 1 || outcome.SyncPolicies != 0 {
		t.Fatalf("carry = %+v, %v", outcome, err)
	}
	for _, want := range []struct{ provider, teamID, member string }{{"linear", "linear:ENG", "m1"}, {"custom", "custom:ENG", "m2"}} {
		if got := f.count(`SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND provider = ? AND team_id = ? AND member_id = ? AND valid_to IS NULL`, want.provider, want.teamID, want.member); got != 1 {
			t.Errorf("open %s membership = %d", want.teamID, got)
		}
		if got := f.count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id = ? AND is_active = 1`, want.teamID); got != 1 {
			t.Errorf("team %s not active", want.teamID)
		}
	}
	if got := f.str(carryActiveBareTeams); got != "" {
		t.Errorf("active bare ids = %q", got)
	}
	again, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), false)
	if err != nil || again.Found() || again.RowsWritten != 0 {
		t.Errorf("second run = %+v, %v; want nothing", again, err)
	}
}

func carryTableCounts(f carryFixture) []uint64 {
	f.t.Helper()
	var counts []uint64
	for _, table := range []string{"teams", "team_memberships", "team_project_ownership", "team_repo_ownership", "team_provider_observations",
		"team_sync_policies", "team_drift_changes", "identities", "manual_attribution_fallbacks"} {
		counts = append(counts, f.count(`SELECT count() FROM `+table+` WHERE org_id = ?`))
	}
	return counts
}

func TestCarryTeamIDsDryRunWritesNothing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	seedEveryClass(f)
	before := carryTableCounts(f)
	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, true)
	if err != nil || !outcome.DryRun || outcome.Teams != 7 || outcome.Memberships != 4 || outcome.RowsWritten != 0 {
		t.Fatalf("dry run = %+v, %v", outcome, err)
	}
	if after := carryTableCounts(f); !reflect.DeepEqual(before, after) {
		t.Fatalf("dry run wrote rows: %v -> %v", before, after)
	}
}

// failingCarryConn fails the first read whose text holds failOn.
type failingCarryConn struct {
	driver.Conn
	failOn string
	writes int
}

var errCarryReadFailed = errors.New("planted read failure")

func (c *failingCarryConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if strings.Contains(query, c.failOn) {
		return nil, errCarryReadFailed
	}
	return c.Conn.Query(ctx, query, args...)
}

func (c *failingCarryConn) PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.writes++
	return c.Conn.PrepareBatch(ctx, query, opts...)
}

// A failed read fails the carry before its first write, whichever read it is.
func TestCarryTeamIDsFailedReadWritesNothing(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	seedEveryClass(f)
	f.drift("ENG", "name", "pending")
	before := carryTableCounts(f)
	for _, failOn := range []string{"argMax(is_active", "LIMIT 1 BY provider, id", "SELECT org_id, provider, native_team_key, team_id, name",
		"SELECT DISTINCT id FROM teams", "FROM team_memberships FINAL", "min(valid_from) FROM team_repo_ownership", "FROM team_sync_policies FINAL",
		"FROM team_drift_changes FINAL", "FROM identities FINAL", "FROM manual_attribution_fallbacks FINAL"} {
		failing := &failingCarryConn{Conn: conn, failOn: failOn}
		if _, err := CarryTeamIDs(ctx, failing, f.orgID, carryAt, false); !errors.Is(err, errCarryReadFailed) {
			t.Errorf("fail on %q: err = %v, want the read failure", failOn, err)
		}
		if failing.writes != 0 {
			t.Errorf("fail on %q: %d writes started", failOn, failing.writes)
		}
	}
	if after := carryTableCounts(f); !reflect.DeepEqual(before, after) {
		t.Fatalf("a failed carry wrote rows: %v -> %v", before, after)
	}
}

// The SQL bare-id condition agrees with teamid.HasKey and teamid.Malformed
// on every id shape.
func TestTheBareIDConditionMatchesHasKey(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, id := range []string{"ENG", "linear:ENG", "linear:", "gh:x", "gh:", "gl:a/b", "jira:" + carryAtlassianID, carryAtlassianID,
		"custom:x", "custom:", "pagerduty:p", "ms-teams:t", "atlassian:x", "atlassian:", " gh: ", "a:b", " linear:ENG ", "linear: ", "", "  ", "LINEAR:ENG"} {
		var bare uint8
		if err := conn.QueryRow(ctx, `SELECT `+teamIDCarryBare("{id:String}"), clickhouse.Named("id", id)).Scan(&bare); err != nil {
			t.Fatalf("%q: %v", id, err)
		}
		want := strings.TrimSpace(id) != "" && !teamid.HasKey(id) && !teamid.Malformed(id)
		if (bare == 1) != want {
			t.Errorf("bare(%q) = %d, want %v", id, bare, want)
		}
	}
}

// An observation that holds a bare id is moved even when no team row of
// that id is left bare.
func TestCarryTeamIDsMovesAnObservationWithoutABareTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "linear:ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.observation("linear", "ENG", "ENG")

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.Teams != 0 || outcome.Observations != 1 {
		t.Fatalf("carry = %+v, %v; want the observation only", outcome, err)
	}
	if got := f.str(`SELECT team_id FROM team_provider_observations FINAL WHERE org_id = ? AND provider = 'linear' AND native_team_key = 'ENG'`); got != "linear:ENG" {
		t.Errorf("observation team_id = %q, want linear:ENG", got)
	}
}

// Only a pending change of the old id is superseded: a decided one keeps
// its decision.
func TestCarryTeamIDsKeepsTheDecisionOfAnOldDecidedChange(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	dismissed := f.drift("ENG", "description", "dismissed")

	if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	if got := f.str(`SELECT status FROM team_drift_changes FINAL WHERE org_id = ? AND change_id = ?`, dismissed); got != "dismissed" {
		t.Errorf("old dismissed change status = %q, want dismissed", got)
	}
}

// An identity that names both the bare and the prefixed id names the
// prefixed id once.
func TestCarryTeamIDsNamesATeamOnceInAnIdentity(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.exec(`INSERT INTO identities (org_id, canonical_id, identity_uuid, display_name, email, provider_identities, team_ids, is_active, updated_at) VALUES (?, 'person-1', generateUUIDv4(), 'P', NULL, '{}', ['ENG', 'linear:ENG'], 1, ?)`, f.orgID, carryOld)

	if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	if got := f.str(`SELECT toString(team_ids) FROM identities FINAL WHERE org_id = ? AND canonical_id = 'person-1'`); got != "['linear:ENG']" {
		t.Errorf("team_ids = %s, want ['linear:ENG']", got)
	}
}

// An observation's parent moves with the parent team.
func TestCarryTeamIDsMovesTheParentOfAnObservation(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.team("linear", "SUB", carryPtr("SUB"), carryPtr("ENG"), 1, carryOld, nil, nil)
	f.exec(`INSERT INTO team_provider_observations (org_id, provider, native_team_key, team_id, name, members_json, project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at) VALUES (?, 'linear', 'SUB', 'SUB', 'obs', '[]', '[]', '[]', 1, 'ENG', ?, ?)`,
		f.orgID, carryOld, carryOld)

	if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	if got := f.str(`SELECT concat(team_id, '|', ifNull(parent_team_id, '')) FROM team_provider_observations FINAL WHERE org_id = ? AND native_team_key = 'SUB'`); got != "linear:SUB|linear:ENG" {
		t.Errorf("observation = %q, want linear:SUB|linear:ENG", got)
	}
}

// An open link whose validity starts after the carry is closed at its own
// start, never before it.
func TestCarryTeamIDsClosesAFutureLinkAtItsStart(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	future := carryAt.Add(48 * time.Hour)
	f.membership("linear", "ENG", "m1", future, nil)

	if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	if got := f.str(`SELECT toString(valid_to) FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'ENG' AND member_id = 'm1'`); got != "2026-10-10 12:00:00.000" {
		t.Errorf("old link valid_to = %s, want its own valid_from 2026-10-10 12:00:00.000", got)
	}
}

// An admin edit of a Jira project-as-team row is that row: the carry does
// not make it a Jira team (RetireJiraProjectAsTeamRows owns it).
func TestCarryTeamIDsLeavesAnAdminEditOfAProjectAsTeamRow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("jira", "PROJ", carryPtr("PROJ"), nil, 1, carryOld, nil, nil)
	f.team("", "PROJ", nil, nil, 1, carryOld.Add(time.Hour), []string{"admin-edit"}, nil)
	f.observation("jira", "PROJ", "PROJ")

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.Teams != 0 || outcome.AdminTeams != 0 || outcome.AdminTeamsNotCarried != 1 {
		t.Fatalf("carry = %+v, %v; want the admin edit not carried", outcome, err)
	}
	if got := f.count(`SELECT count() FROM teams FINAL WHERE org_id = ? AND id = 'jira:PROJ'`); got != 0 {
		t.Errorf("jira:PROJ team rows = %d, want 0", got)
	}
	later, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), true)
	if err != nil || later.Found() || later.RowsWritten != 0 {
		t.Errorf("later dry run = %+v, %v; want nothing left", later, err)
	}
}

// A pending identity membership change of a moved team is superseded (the
// review observes only the prefixed id, so it would stay pending and its
// approval would write to the inactive bare team); a decided one stays.
func TestCarryTeamIDsSupersedesAPendingIdentityChangeOfAMovedTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.team("linear", "linear:KEPT", carryPtr("KEPT"), nil, 1, carryOld, nil, nil)
	// A Jira project-as-team row of the same id does not move.
	f.team("jira", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	identity := func(changeID, provider, teamID, status string) {
		f.exec(`INSERT INTO team_drift_changes (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, updated_at) VALUES (?, ?, 'identity', ?, ?, ?, 'membership_changed', 'team_memberships', '{}', ?, ?, ?, ?, ?)`,
			f.orgID, changeID, teamID, provider, teamID, `{"provider":"`+provider+`","team_id":"`+teamID+`","member_id":"m1"}`, status, carryFirst, carryOld, carryOld)
	}
	identity("c-pending", "linear", "ENG", "pending")
	identity("c-dismissed", "linear", "ENG", "dismissed")
	identity("c-other", "linear", "linear:KEPT", "pending")
	identity("c-jira", "jira", "ENG", "pending")

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.IdentityDriftChanges != 1 {
		t.Fatalf("carry = %+v, %v; want one identity change superseded", outcome, err)
	}
	got := f.str(`SELECT arrayStringConcat(arraySort(groupArray(concat(change_id, '=', status))), ',') FROM team_drift_changes FINAL WHERE org_id = ? AND entity_type = 'identity'`)
	if got != "c-dismissed=dismissed,c-jira=pending,c-other=pending,c-pending=superseded" {
		t.Errorf("identity changes = %s", got)
	}
}

// A moved link is closed for a reader that compares valid_to with
// ClickHouse now() (second precision) as soon as the carry returns.
func TestCarryTeamIDsClosesALinkForAReaderOfNow(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.exec(`INSERT INTO team_memberships (org_id, provider, team_id, member_id, identity_facets, source, is_primary, specificity, priority, valid_from, valid_to, updated_at) VALUES (?, 'linear', 'ENG', 'm1', [], 'manual', 1, 100, 10, ?, NULL, ?)`, f.orgID, carryOld, carryOld)
	// Start well inside a second, so the read below runs in the carry's second.
	for ns := time.Now().Nanosecond(); ns < 100_000_000 || ns > 500_000_000; ns = time.Now().Nanosecond() {
		time.Sleep(5 * time.Millisecond)
	}
	if err := CarryTeamIDsBeforeWrite(ctx, conn, f.orgID, "test"); err != nil {
		t.Fatal(err)
	}
	if got := f.str(`SELECT arrayStringConcat(arraySort(groupArray(team_id)), ',') FROM team_memberships FINAL WHERE org_id = ? AND member_id = 'm1' AND source = 'manual' AND (valid_to IS NULL OR valid_to > now())`); got != "linear:ENG" {
		t.Errorf("active manual memberships of m1 = %q, want only linear:ENG", got)
	}
}

// A team id that is only a provider prefix is not a team of any provider:
// the carry leaves it, counts it, and still moves the other bare ids.
func TestCarryTeamIDsSkipsAPrefixOnlyID(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	for _, malformed := range []string{"gh:", "linear:", " jira: ", "atlassian:"} {
		t.Run(strings.TrimSpace(malformed), func(t *testing.T) {
			f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
			f.team("linear", malformed, carryPtr(malformed), nil, 1, carryOld, nil, nil)
			f.observation("linear", malformed, malformed)
			f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)

			outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
			if err != nil || outcome.Teams != 1 || outcome.MalformedTeamIDs != 2 {
				t.Fatalf("carry = %+v, %v; want ENG moved and the team and the observation of %q counted as malformed", outcome, err, malformed)
			}
			if got := f.str(`SELECT arrayStringConcat(groupArray(id), ',') FROM (SELECT id FROM teams FINAL WHERE org_id = ? AND is_active = 1 ORDER BY id)`); got != malformed+",linear:ENG" {
				t.Errorf("active = %q, want %q", got, malformed+",linear:ENG")
			}
			if got := f.str(`SELECT arrayStringConcat(groupArray(team_id), ',') FROM team_provider_observations FINAL WHERE org_id = ?`); got != malformed {
				t.Errorf("observations = %q, want %q", got, malformed)
			}
			again, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), false)
			if err != nil || again.Teams != 0 || again.Observations != 0 || again.RowsWritten != 0 || again.MalformedTeamIDs != 2 {
				t.Errorf("second carry = %+v, %v; want no write and the malformed ids counted again", again, err)
			}
		})
	}
}

// An admin's own team (no single provider's observation names its id) moves
// to custom:<id> with every row that names it; an admin team that two
// providers' observations name does too. A second run writes nothing.
func TestCarryTeamIDsMovesAnAdminsOwnTeamToCustom(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "chosen", nil, nil, 1, carryOld, []string{"m1"}, nil)
	f.team("", "TWO", nil, nil, 1, carryOld, nil, nil)
	f.observation("linear", "TWO", "TWO")
	f.observation("gitlab", "TWO", "TWO")
	f.membership("", "chosen", "m1", carryOld, nil)
	f.exec(`INSERT INTO identities (org_id, canonical_id, identity_uuid, team_ids, updated_at) VALUES (?, 'person-1', ?, ['chosen'], ?)`, f.orgID, uuid.New(), carryOld)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.Teams != 2 || outcome.AdminTeams != 2 || outcome.AdminTeamsToCustom != 2 || outcome.AdminTeamsNotCarried != 0 || outcome.Memberships != 1 || outcome.Identities != 1 {
		t.Fatalf("carry = %+v, %v; want both admin teams moved to custom", outcome, err)
	}
	if got := f.str(`SELECT arrayStringConcat(groupArray(concat(id, '/', arrayStringConcat(manual_members, ';'))), ',') FROM (SELECT id, manual_members FROM teams FINAL WHERE org_id = ? AND is_active = 1 ORDER BY id)`); got != "custom:TWO/,custom:chosen/m1" {
		t.Errorf("active = %q, want custom:TWO and custom:chosen with m1", got)
	}
	if got := f.str(`SELECT arrayStringConcat(team_ids, ',') FROM identities FINAL WHERE org_id = ? AND canonical_id = 'person-1'`); got != "custom:chosen" {
		t.Errorf("identity team_ids = %q, want custom:chosen", got)
	}
	if got := f.count(`SELECT count() FROM team_memberships FINAL WHERE org_id = ? AND team_id = 'custom:chosen' AND valid_to IS NULL`); got != 1 {
		t.Errorf("open custom:chosen memberships = %d, want 1", got)
	}
	again, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), false)
	if err != nil || again.Found() || again.RowsWritten != 0 {
		t.Errorf("second carry = %+v, %v; want nothing", again, err)
	}
}

// An admin row counts only while it is the team's current row: an older
// admin edit of a team whose newer row is inactive is not carried, so the
// inactive team does not come back as custom:<id>.
func TestCarryTeamIDsLeavesAnAdminRowOlderThanItsInactiveTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "ENG", nil, nil, 1, carryOld, []string{"admin-edit"}, nil)
	f.team("linear", "ENG", carryPtr("ENG"), nil, 0, carryOld.Add(time.Hour), nil, nil)

	// The count read finds nothing: no plan read follows it.
	counting := &queryCountingConn{TeamIDCarryConn: conn}
	outcome, err := CarryTeamIDs(ctx, counting, f.orgID, carryAt, false)
	if err != nil || outcome.Found() || outcome.RowsWritten != 0 || counting.queries != 1 {
		t.Fatalf("carry = %+v, %v, %d reads; want the count read only", outcome, err, counting.queries)
	}
	// With another bare team to carry, the plan still leaves the admin row.
	f.team("linear", "OPS", carryPtr("OPS"), nil, 1, carryOld, nil, nil)
	outcome, err = CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.Teams != 1 || outcome.AdminTeams != 0 {
		t.Fatalf("carry = %+v, %v; want OPS only", outcome, err)
	}
	if got := f.str(`SELECT arrayStringConcat(groupArray(id), ',') FROM (SELECT id FROM teams FINAL WHERE org_id = ? AND is_active = 1 ORDER BY id)`); got != "linear:OPS" {
		t.Errorf("active = %q, want linear:OPS", got)
	}
}

type queryCountingConn struct {
	TeamIDCarryConn
	queries int
}

func (c *queryCountingConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.queries++
	return c.TeamIDCarryConn.Query(ctx, query, args...)
}

// A parent id that two providers' teams hold resolves inside the child's
// provider, for the team row and for the observation.
func TestCarryTeamIDsResolvesAnAmbiguousParentInTheChildsProvider(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "PARENT", carryPtr("PARENT"), nil, 1, carryOld, nil, nil)
	f.team("gitlab", "PARENT", carryPtr("PARENT"), nil, 1, carryOld.Add(time.Minute), nil, nil)
	f.team("linear", "CHILD", carryPtr("CHILD"), carryPtr("PARENT"), 1, carryOld, nil, nil)
	f.exec(`INSERT INTO team_provider_observations (org_id, provider, native_team_key, team_id, name, members_json, project_keys_json, repo_patterns_json, is_active, parent_team_id, discovered_at, updated_at) VALUES (?, 'linear', 'CHILD', 'CHILD', 'obs', '[]', '[]', '[]', 1, 'PARENT', ?, ?)`, f.orgID, carryOld, carryOld)

	if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	if got := f.str(`SELECT ifNull(parent_team_id, '') FROM teams FINAL WHERE org_id = ? AND id = 'linear:CHILD' AND is_active = 1`); got != "linear:PARENT" {
		t.Errorf("child parent = %q, want linear:PARENT", got)
	}
	if got := f.str(`SELECT ifNull(parent_team_id, '') FROM team_provider_observations FINAL WHERE org_id = ? AND provider = 'linear' AND native_team_key = 'CHILD'`); got != "linear:PARENT" {
		t.Errorf("observation parent = %q, want linear:PARENT", got)
	}
}

// An admin edit of an id that two providers' teams hold is kept as the
// admin's own team, custom:<id>, with its members.
func TestCarryTeamIDsKeepsAnAmbiguousAdminEditAsTheAdminsTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.team("gitlab", "ENG", carryPtr("ENG"), nil, 1, carryOld.Add(time.Minute), nil, nil)
	f.team("", "ENG", nil, nil, 1, carryOld.Add(2*time.Minute), []string{"admin@example.com"}, nil)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.AdminTeamsToCustom != 1 {
		t.Fatalf("carry = %+v, %v; want the admin edit moved to custom", outcome, err)
	}
	if got := f.str(`SELECT arrayStringConcat(groupArray(concat(id, '|', arrayStringConcat(manual_members, ','))), ';') FROM (SELECT id, manual_members FROM teams FINAL WHERE org_id = ? AND is_active = 1 ORDER BY id)`); got != "custom:ENG|admin@example.com;gl:ENG|;linear:ENG|" {
		t.Errorf("active = %q, want custom:ENG with the admin's member, gl:ENG, linear:ENG", got)
	}
	again, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), false)
	if err != nil || again.Found() || again.RowsWritten != 0 {
		t.Errorf("second carry = %+v, %v; want nothing", again, err)
	}
}

// An admin's own team "eng" and a pushed custom-system team "eng" are one
// custom team, custom:eng: the carry moves the admin team to that id, where
// the pushed row is kept (a keyed row is not written again), takes the
// admin's manual members, and the bare admin row goes inactive. This holds
// for a pushed row stored with no provider or with provider custom, and for
// an admin row older or newer than it.
func TestCarryTeamIDsMovesAnAdminTeamOntoThePushedCustomTeamOfItsID(t *testing.T) {
	for _, c := range []struct {
		name, pushedProvider string
		adminNewer           bool
	}{{"stored no provider", "", false}, {"stored custom, admin newer", "custom", true}, {"stored custom, admin older", "custom", false}} {
		t.Run(c.name, func(t *testing.T) {
			ctx, conn := newWorkItemEffectsConn(t)
			f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
			adminAt, pushedAt := carryOld, carryOld.Add(time.Minute)
			if c.adminNewer {
				adminAt, pushedAt = pushedAt, adminAt
			}
			f.team("", "eng", nil, nil, 1, adminAt, []string{"admin@example.com"}, nil)
			f.team(c.pushedProvider, "custom:eng", carryPtr("eng"), nil, 1, pushedAt, []string{"pushed@example.com"}, nil)

			outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
			if err != nil || outcome.AdminTeamsToCustom != 1 || outcome.TeamsAlreadyKeyed != 1 || outcome.ManualMembersFolded != 1 {
				t.Fatalf("carry = %+v, %v; want the admin team folded into the existing custom:eng", outcome, err)
			}
			want := "custom:eng|" + c.pushedProvider + "|team custom:eng|pushed@example.com,admin@example.com|1;eng||team eng|admin@example.com|0"
			if got := f.str(`SELECT arrayStringConcat(groupArray(concat(id, '|', provider, '|', name, '|', arrayStringConcat(manual_members, ','), '|', toString(is_active))), ';') FROM (SELECT * FROM teams FINAL WHERE org_id = ? ORDER BY id)`); got != want {
				t.Errorf("teams = %q, want %q", got, want)
			}
			again, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt.Add(time.Hour), false)
			if err != nil || again.Found() || again.RowsWritten != 0 {
				t.Errorf("second carry = %+v, %v; want nothing", again, err)
			}
		})
	}
}

// The fold reads and writes the kept row of the carried organization only:
// another organization's row of the same id keeps its members and gets no
// new version.
func TestCarryTeamIDsFoldStaysInItsOrganization(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	other := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "custom:eng", carryPtr("eng"), nil, 1, carryOld, nil, nil)
	f.team("", "eng", nil, nil, 1, carryOld.Add(time.Hour), []string{"a@example.com"}, nil)
	other.team("", "custom:eng", carryPtr("eng"), nil, 1, carryOld, []string{"b@example.com"}, nil)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.ManualMembersFolded != 1 {
		t.Fatalf("carry = %+v, %v; want one fold", outcome, err)
	}
	const q = `SELECT concat(toString((SELECT count() FROM teams WHERE org_id = ? AND id = 'custom:eng')), '|', arrayStringConcat(manual_members, ',')) FROM teams FINAL WHERE org_id = ? AND id = 'custom:eng'`
	if got := other.str(q, other.orgID); got != "1|b@example.com" {
		t.Errorf("other organization custom:eng = %q, want 1|b@example.com", got)
	}
	if got := f.str(q, f.orgID); got != "2|a@example.com" {
		t.Errorf("own organization custom:eng = %q, want 2|a@example.com", got)
	}
}

// A kept row that already holds every manual member of the moved row is not
// written again.
func TestCarryTeamIDsDoesNotRewriteAKeptRowThatHoldsTheMembers(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "eng", nil, nil, 1, carryOld, []string{"admin@example.com"}, nil)
	f.team("", "custom:eng", carryPtr("eng"), nil, 1, carryOld, []string{"admin@example.com"}, nil)
	before := f.str(`SELECT toString(max(updated_at)) FROM teams WHERE org_id = ? AND id = 'custom:eng'`)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.TeamsAlreadyKeyed != 1 || outcome.ManualMembersFolded != 0 {
		t.Fatalf("carry = %+v, %v; want nothing folded", outcome, err)
	}
	if got := f.str(`SELECT concat(toString(count()), '|', toString(max(updated_at))) FROM teams WHERE org_id = ? AND id = 'custom:eng'`); got != "1|"+before {
		t.Errorf("custom:eng rows|newest = %q, want one row at %s", got, before)
	}
	if got := f.str(`SELECT arrayStringConcat(manual_members, ',') FROM teams FINAL WHERE org_id = ? AND id = 'custom:eng'`); got != "admin@example.com" {
		t.Errorf("custom:eng manual members = %q, want admin@example.com once", got)
	}
}

// An admin edit of a provider team's bare id, carried after that team was
// already written under its prefixed id, is that team: the kept row takes
// the edit's manual members and keeps its own origin.
func TestCarryTeamIDsFoldsAnAdminEditIntoTheKeptProviderTeam(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("", "ENG", nil, nil, 1, carryOld.Add(2*time.Minute), []string{"admin@example.com"}, nil)
	f.observation("linear", "ENG", "ENG")
	f.team("linear", "linear:ENG", carryPtr("ENG"), nil, 1, carryOld.Add(time.Minute), nil, nil)

	outcome, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false)
	if err != nil || outcome.ManualMembersFolded != 1 {
		t.Fatalf("carry = %+v, %v; want the admin edit's member folded", outcome, err)
	}
	if got := f.str(`SELECT arrayStringConcat(groupArray(concat(id, '|', provider, '|', ifNull(native_team_key, ''), '|', arrayStringConcat(manual_members, ','), '|', toString(is_active))), ';') FROM (SELECT * FROM teams FINAL WHERE org_id = ? ORDER BY id)`); got != "ENG|||admin@example.com|0;linear:ENG|linear|ENG|admin@example.com|1" {
		t.Errorf("teams = %q, want linear:ENG with the admin's member", got)
	}
}

// A parent id that is not one team (a Linear team and a Jira project-as-team
// row) resolves only inside the child's provider: a GitLab child keeps it.
func TestCarryTeamIDsKeepsAParentThatIsNotOneTeamOutsideItsProvider(t *testing.T) {
	ctx, conn := newWorkItemEffectsConn(t)
	f := carryFixture{t: t, ctx: ctx, conn: conn, orgID: uuid.NewString()}
	f.team("linear", "ENG", carryPtr("ENG"), nil, 1, carryOld, nil, nil)
	f.team("jira", "ENG", carryPtr("ENG"), nil, 1, carryOld.Add(-time.Minute), nil, nil)
	f.team("gitlab", "SUB", carryPtr("SUB"), carryPtr("ENG"), 1, carryOld, nil, nil)
	f.team("linear", "LSUB", carryPtr("LSUB"), carryPtr("ENG"), 1, carryOld, nil, nil)

	if _, err := CarryTeamIDs(ctx, conn, f.orgID, carryAt, false); err != nil {
		t.Fatal(err)
	}
	for id, want := range map[string]string{"gl:SUB": "ENG", "linear:LSUB": "linear:ENG"} {
		if got := f.str(`SELECT ifNull(parent_team_id, '') FROM teams FINAL WHERE org_id = ? AND id = ?`, id); got != want {
			t.Errorf("%s parent = %q, want %q", id, got, want)
		}
	}
}
