//go:build integration

package teamsidentity

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

func writeSeamStore(t *testing.T) (Store, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	chschema.Apply(ctx, t, instance)
	conn, err := clickhouse.Open(ctx, clickhouse.DefaultConfig(instance.URI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return Store{Conn: conn}, ctx
}

// writeSeamSeed writes one active team of a provider with a bare id, as a
// store held it before ids carried a prefix.
func writeSeamSeed(t *testing.T, s Store, ctx context.Context, provider, id string) {
	t.Helper()
	writeSeamSeedNative(t, s, ctx, provider, id, id)
}

func writeSeamSeedNative(t *testing.T, s Store, ctx context.Context, provider, id, native string) {
	t.Helper()
	if err := s.Conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES (?, generateUUIDv4(), 'Eng', [], [], [], [], 1, '2026-09-01 00:00:00', 'org-1', ?, ?)`, id, provider, native); err != nil {
		t.Fatal(err)
	}
}

func writeSeamActive(t *testing.T, s Store, ctx context.Context) string {
	t.Helper()
	rows, err := s.Conn.Query(ctx, `SELECT id FROM teams FINAL WHERE org_id = 'org-1' AND is_active = 1 ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return strings.Join(ids, ",")
}

func writeSeamCall(t *testing.T, h handlers, handler http.HandlerFunc, method, path, teamID string, payload map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	if teamID == "" {
		return callWithBody(t, h, handler, method, path, "org-1", payload)
	}
	return callWithBody(t, h, func(w http.ResponseWriter, r *http.Request) {
		r.SetPathValue("team_id", teamID)
		handler(w, r)
	}, method, path, "org-1", payload)
}

// An identity assignment that names a carried bare team id lands on the
// prefixed team; the bare team stays inactive.
func TestAnIdentityAssignOfACarriedBareTeamIDWritesTheKeyedTeam(t *testing.T) {
	const atlassianTeam = "aaaaaaaa-0000-4000-8000-000000000001"
	for _, c := range []struct{ provider, bare, native, keyed string }{
		{"linear", "ENG", "ENG", "linear:ENG"},
		{"jira", atlassianTeam, "ari:cloud:identity::team/" + atlassianTeam, "jira:" + atlassianTeam},
		{"github", "ENG", "ENG", "gh:ENG"},
		{"gitlab", "ENG", "ENG", "gl:ENG"},
	} {
		t.Run(c.provider, func(t *testing.T) {
			s, ctx := writeSeamStore(t)
			writeSeamSeedNative(t, s, ctx, c.provider, c.bare, c.native)
			if _, err := providersync.CarryTeamIDs(ctx, s.Conn, "org-1", time.Now().UTC(), false); err != nil {
				t.Fatal(err)
			}
			keyed := c.keyed
			if got := writeSeamActive(t, s, ctx); got != keyed {
				t.Fatalf("after the carry: active = %q, want %q", got, keyed)
			}
			h := newTestHandlers(s)
			rec := writeSeamCall(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "",
				map[string]any{"canonical_id": "m1", "email": "m1@example.com", "team_ids": []string{c.bare}})
			if rec.Code != http.StatusOK {
				t.Fatalf("identity assign = %d %s", rec.Code, rec.Body.String())
			}
			if got := writeSeamActive(t, s, ctx); got != keyed {
				t.Errorf("after the assign: active = %q, want %q", got, keyed)
			}
			identity, err := s.GetIdentity(ctx, "org-1", "m1")
			if err != nil || identity == nil {
				t.Fatalf("identity = %v, %v", identity, err)
			}
			if strings.Join(identity.TeamIDs, ",") != keyed {
				t.Errorf("identity team_ids = %v, want [%s]", identity.TeamIDs, keyed)
			}
			team, err := s.GetTeam(ctx, "org-1", keyed)
			if err != nil || team == nil || !strings.Contains(strings.Join(team.ManualMembers, ","), "m1@example.com") {
				t.Errorf("keyed team = %+v, %v; want m1@example.com in its manual members", team, err)
			}
		})
	}
}

// An admin create of the prefixed id of a team the store still holds bare
// carries the bare team first: one active team.
func TestAnAdminTeamCreateCarriesTheBareTeamFirst(t *testing.T) {
	s, ctx := writeSeamStore(t)
	writeSeamSeed(t, s, ctx, "linear", "ENG")
	h := newTestHandlers(s)
	rec := writeSeamCall(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", "",
		map[string]any{"team_id": "linear:ENG", "name": "Eng"})
	if rec.Code != http.StatusOK {
		t.Fatalf("create = %d %s", rec.Code, rec.Body.String())
	}
	if got := writeSeamActive(t, s, ctx); got != "linear:ENG" {
		t.Errorf("active = %q, want linear:ENG", got)
	}
}

// An admin create or update that names a bare id of a carried team writes
// the prefixed team.
func TestAnAdminTeamWriteOfABareIDWritesTheKeyedTeam(t *testing.T) {
	for name, call := range map[string]func(h handlers) *httptest.ResponseRecorder{
		"create": func(h handlers) *httptest.ResponseRecorder {
			return writeSeamCall(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", "",
				map[string]any{"team_id": "ENG", "name": "Renamed"})
		},
		"update": func(h handlers) *httptest.ResponseRecorder {
			return writeSeamCall(t, h, h.updateTeam, http.MethodPatch, "/api/v1/admin/teams/ENG", "ENG",
				map[string]any{"name": "Renamed"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, ctx := writeSeamStore(t)
			writeSeamSeed(t, s, ctx, "linear", "ENG")
			h := newTestHandlers(s)
			rec := call(h)
			if rec.Code != http.StatusOK {
				t.Fatalf("%s = %d %s", name, rec.Code, rec.Body.String())
			}
			if got := writeSeamActive(t, s, ctx); got != "linear:ENG" {
				t.Errorf("active = %q, want linear:ENG", got)
			}
			team, err := s.GetTeam(ctx, "org-1", "linear:ENG")
			if err != nil || team == nil || team.Name != "Renamed" {
				t.Errorf("linear:ENG = %+v, %v; want name Renamed", team, err)
			}
		})
	}
}

// A bare id that two providers' prefixed teams hold is refused before any
// write; a prefix-only id is refused.
func TestAnAdminTeamWriteRefusesAnAmbiguousOrMalformedID(t *testing.T) {
	s, ctx := writeSeamStore(t)
	writeSeamSeed(t, s, ctx, "linear", "ENG")
	writeSeamSeed(t, s, ctx, "github", "gh:ENG")
	if _, err := providersync.CarryTeamIDs(ctx, s.Conn, "org-1", time.Now().UTC(), false); err != nil {
		t.Fatal(err)
	}
	before := writeSeamActive(t, s, ctx)
	if before != "gh:ENG,linear:ENG" {
		t.Fatalf("seed: active = %q", before)
	}
	h := newTestHandlers(s)
	for id, want := range map[string]int{"ENG": http.StatusConflict, "gh:": http.StatusUnprocessableEntity, " linear: ": http.StatusUnprocessableEntity} {
		rec := writeSeamCall(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", "",
			map[string]any{"team_id": id, "name": "X"})
		if rec.Code != want {
			t.Errorf("create %q = %d %s, want %d", id, rec.Code, rec.Body.String(), want)
		}
		rec = writeSeamCall(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "",
			map[string]any{"canonical_id": "m1", "team_ids": []string{id}})
		if rec.Code != want {
			t.Errorf("identity %q = %d %s, want %d", id, rec.Code, rec.Body.String(), want)
		}
	}
	if got := writeSeamActive(t, s, ctx); got != before {
		t.Errorf("active = %q after refused writes, want %q", got, before)
	}
	if identity, err := s.GetIdentity(ctx, "org-1", "m1"); err != nil || identity != nil {
		t.Errorf("identity written on a refused write: %+v, %v", identity, err)
	}
}

// A bare id that no prefixed team holds is refused before any write.
func TestAnAdminTeamWriteRefusesABareIDNoProviderTeamHolds(t *testing.T) {
	s, ctx := writeSeamStore(t)
	h := newTestHandlers(s)
	rec := writeSeamCall(t, h, h.createOrUpdateTeam, http.MethodPost, "/api/v1/admin/teams", "",
		map[string]any{"team_id": "eng", "name": "Eng"})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("create = %d %s, want 422", rec.Code, rec.Body.String())
	}
	if got := writeSeamActive(t, s, ctx); got != "" {
		t.Errorf("active = %q after a refused create, want none", got)
	}
}

// The member confirmations and a drift decision name a team by the path:
// a bare path id of a carried team lands on the prefixed team.
func TestTheMemberAndDecisionWritersKeyTheirPathTeamID(t *testing.T) {
	s, ctx := writeSeamStore(t)
	writeSeamSeed(t, s, ctx, "linear", "ENG")
	h := newTestHandlers(s)
	rec := writeSeamCall(t, h, h.confirmMembers, http.MethodPost, "/api/v1/admin/teams/ENG/confirm-members", "ENG",
		map[string]any{"team_id": "ENG", "links": []map[string]any{{"provider": "github", "provider_identity": "octo", "canonical_id": "m1", "action": "create"}}})
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm members = %d %s", rec.Code, rec.Body.String())
	}
	rec = writeSeamCall(t, h, h.confirmInferredMembers, http.MethodPost, "/api/v1/admin/teams/ENG/confirm-inferred-members", "ENG",
		map[string]any{"team_id": "ENG", "members": []map[string]any{{"account_id": "acc-1", "action": "add", "display_name": "Ann"}}})
	if rec.Code != http.StatusOK {
		t.Fatalf("confirm inferred = %d %s", rec.Code, rec.Body.String())
	}
	rec = writeSeamCall(t, h, h.dismissChanges, http.MethodPost, "/api/v1/admin/teams/ENG/dismiss-changes", "ENG",
		map[string]any{"dismiss_all": true})
	if rec.Code != http.StatusOK {
		t.Fatalf("dismiss = %d %s", rec.Code, rec.Body.String())
	}
	if got := writeSeamActive(t, s, ctx); got != "linear:ENG" {
		t.Errorf("active = %q, want linear:ENG", got)
	}
	for _, canonical := range []string{"m1", "jira:acc-1"} {
		identity, err := s.GetIdentity(ctx, "org-1", canonical)
		if err != nil || identity == nil || strings.Join(identity.TeamIDs, ",") != "linear:ENG" {
			t.Errorf("identity %s = %+v, %v; want team_ids [linear:ENG]", canonical, identity, err)
		}
	}
	team, err := s.GetTeam(ctx, "org-1", "linear:ENG")
	if err != nil || team == nil || !strings.Contains(strings.Join(team.ManualMembers, ","), "m1") {
		t.Errorf("linear:ENG = %+v, %v; want the confirmed members", team, err)
	}
}

// An import of a provider team id that is only a prefix is refused before
// anything is written.
func TestTheAdminImportRefusesAPrefixOnlyTeamID(t *testing.T) {
	s, ctx := writeSeamStore(t)
	rec := postImportBody(t, s, `{"teams":[{"provider_type":"github","provider_team_id":"gh:","name":"X"}]}`)
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("import = %d %s, want 422", rec.Code, rec.Body.String())
	}
	var observations uint64
	if err := s.Conn.QueryRow(ctx, `SELECT count() FROM team_provider_observations WHERE org_id = 'org-1'`).Scan(&observations); err != nil {
		t.Fatal(err)
	}
	if got := writeSeamActive(t, s, ctx); got != "" || observations != 0 {
		t.Errorf("active = %q, observations = %d after a refused import, want none", got, observations)
	}
}

// The store refuses to write a bare or malformed team id, and an open
// membership or fallback of one; it still closes a stored one.
func TestTheStoreRefusesABareTeamIDWrite(t *testing.T) {
	s, ctx := writeSeamStore(t)
	for _, id := range []string{"ENG", "gh:", "linear:gh:", ""} {
		if _, err := s.CreateOrUpdateTeam(ctx, "org-1", TeamWrite{TeamID: id, Name: "X"}); !errors.Is(err, teamid.ErrBareTeamID) {
			t.Errorf("CreateOrUpdateTeam(%q) error = %v, want %v", id, err, teamid.ErrBareTeamID)
		}
	}
	now := time.Now().UTC()
	membership := func(teamID string, validTo any) *pyjson.Object {
		row := pyjson.NewObject()
		for _, kv := range [][2]any{{"provider", "github"}, {"team_id", teamID}, {"member_id", "m1"}, {"source", "manual"},
			{"valid_from", now}, {"valid_to", validTo}, {"updated_at", now}, {"scope_type", "member"}, {"scope_id", "m1"}} {
			row.Set(kv[0].(string), kv[1])
		}
		return row
	}
	if err := s.insertTeamMembership(ctx, "org-1", membership("ENG", nil)); !errors.Is(err, teamid.ErrBareTeamID) {
		t.Errorf("open bare membership: error = %v", err)
	}
	if err := s.insertManualFallback(ctx, "org-1", membership("ENG", nil), now); !errors.Is(err, teamid.ErrBareTeamID) {
		t.Errorf("open bare fallback: error = %v", err)
	}
	if err := s.insertTeamMembership(ctx, "org-1", membership("ENG", now)); err != nil {
		t.Errorf("closing a stored bare membership: error = %v", err)
	}
	if err := s.insertManualFallback(ctx, "org-1", membership("ENG", now), now); err != nil {
		t.Errorf("closing a stored bare fallback: error = %v", err)
	}
	if got := writeSeamActive(t, s, ctx); got != "" {
		t.Errorf("active = %q, want none", got)
	}
}

// An identity that leaves a team it names by a stored bare id is written;
// the bare team is not written again, so an inactive one stays inactive.
func TestAnIdentityLeavingAStoredBareTeamSkipsIt(t *testing.T) {
	s, ctx := writeSeamStore(t)
	writeSeamSeed(t, s, ctx, "linear", "linear:ENG")
	if err := s.Conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('ENG', generateUUIDv4(), 'Eng', [], ['m1@example.com'], [], [], 0, '2026-09-02 00:00:00', 'org-1', 'linear', 'ENG')`); err != nil {
		t.Fatal(err)
	}
	if err := s.Conn.Exec(ctx, `INSERT INTO identities (org_id, canonical_id, identity_uuid, provider_identities, team_ids, is_active, updated_at) VALUES ('org-1', 'm1', generateUUIDv4(), '{}', ['ENG'], 1, '2026-09-01 00:00:00')`); err != nil {
		t.Fatal(err)
	}
	h := newTestHandlers(s)
	rec := writeSeamCall(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "",
		map[string]any{"canonical_id": "m1", "email": "m1@example.com", "team_ids": []string{"linear:ENG"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("identity = %d %s", rec.Code, rec.Body.String())
	}
	if got := writeSeamActive(t, s, ctx); got != "linear:ENG" {
		t.Errorf("active = %q, want linear:ENG", got)
	}
}

// A bare id resolves only to an ACTIVE prefixed team: an inactive one is
// not written active again. A request that names a prefixed and a bare id
// keys both.
func TestTheWriteSeamResolvesOnlyToAnActiveTeamAndKeysAMixedRequest(t *testing.T) {
	s, ctx := writeSeamStore(t)
	writeSeamSeed(t, s, ctx, "linear", "linear:ENG")
	writeSeamSeed(t, s, ctx, "", "custom:ops")
	if err := s.Conn.Exec(ctx, `INSERT INTO teams (id, team_uuid, name, members, manual_members, project_keys, repo_patterns, is_active, updated_at, org_id, provider, native_team_key) VALUES ('linear:OLD', generateUUIDv4(), 'Old', [], [], [], [], 0, '2026-09-01 00:00:00', 'org-1', 'linear', 'OLD')`); err != nil {
		t.Fatal(err)
	}
	h := newTestHandlers(s)
	rec := writeSeamCall(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "",
		map[string]any{"canonical_id": "m1", "team_ids": []string{"OLD"}})
	if rec.Code != http.StatusUnprocessableEntity {
		t.Errorf("identity naming an inactive team's bare id = %d %s, want 422", rec.Code, rec.Body.String())
	}
	rec = writeSeamCall(t, h, h.createOrUpdateIdentity, http.MethodPost, "/api/v1/admin/identities", "",
		map[string]any{"canonical_id": "m1", "team_ids": []string{"custom:ops", "ENG"}})
	if rec.Code != http.StatusOK {
		t.Fatalf("mixed identity = %d %s", rec.Code, rec.Body.String())
	}
	identity, err := s.GetIdentity(ctx, "org-1", "m1")
	if err != nil || identity == nil || strings.Join(identity.TeamIDs, ",") != "custom:ops,linear:ENG" {
		t.Errorf("identity = %+v, %v; want team_ids [custom:ops linear:ENG]", identity, err)
	}
	if got := writeSeamActive(t, s, ctx); got != "custom:ops,linear:ENG" {
		t.Errorf("active = %q, want custom:ops,linear:ENG", got)
	}
}

// A drift decision named by a carried bare id decides the prefixed team's
// pending change.
func TestADriftDecisionByABareIDDecidesTheKeyedTeamsChange(t *testing.T) {
	s, ctx := writeSeamStore(t)
	writeSeamSeed(t, s, ctx, "linear", "linear:ENG")
	if err := s.Conn.Exec(ctx, `INSERT INTO team_drift_changes (org_id, change_id, entity_type, entity_id, provider, native_team_key, change_type, field, old_value_json, new_value_json, status, first_seen_at, last_seen_at, updated_at) VALUES ('org-1', 'chg-1', 'team', 'linear:ENG', 'linear', 'ENG', 'field_changed', 'name', '"a"', '"b"', 'pending', '2026-09-01 00:00:00', '2026-09-01 00:00:00', '2026-09-01 00:00:00')`); err != nil {
		t.Fatal(err)
	}
	h := newTestHandlers(s)
	rec := writeSeamCall(t, h, h.dismissChanges, http.MethodPost, "/api/v1/admin/teams/ENG/dismiss-changes", "ENG",
		map[string]any{"dismiss_all": true})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"dismissed":1`) {
		t.Errorf("dismiss by bare id = %d %s, want the one pending change dismissed", rec.Code, rec.Body.String())
	}
}
