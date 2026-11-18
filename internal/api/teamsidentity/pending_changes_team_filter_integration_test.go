//go:build integration

package teamsidentity

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func seedPendingChange(t *testing.T, s Store, ctx context.Context, orgID, changeID, entityID, provider string) {
	t.Helper()
	now := time.Now().UTC()
	field := "name"
	if err := s.insertChanges(ctx, []teamDriftChangeRow{{
		OrgID: orgID, ChangeID: changeID, EntityType: entityTypeTeam, EntityID: entityID, Provider: provider,
		ChangeType: changeTypeField, Field: &field, OldValueJSON: `"a"`, NewValueJSON: `"b"`,
		Status: statusPending, FirstSeenAt: now, LastSeenAt: now, UpdatedAt: now,
	}}); err != nil {
		t.Fatal(err)
	}
}

func pendingCall(t *testing.T, h handlers, orgID, rawQuery string) (int, []string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/admin/teams/pending-changes?"+rawQuery, nil)
	req = req.WithContext(policy.WithUser(req.Context(), &policy.User{OrgID: orgID, Role: "admin"}))
	rec := httptest.NewRecorder()
	h.pendingChanges(rec, req)
	if rec.Code != http.StatusOK {
		return rec.Code, nil
	}
	body := decodeBody(t, rec)
	var ids []string
	for _, c := range body["changes"].([]any) {
		ids = append(ids, c.(map[string]any)["change_id"].(string))
	}
	sort.Strings(ids)
	if int(body["total"].(float64)) != len(ids) {
		t.Fatalf("total %v != %d changes", body["total"], len(ids))
	}
	return rec.Code, ids
}

// CHAOS-6426: team_id narrows the list to that team inside the caller's org.
func TestPendingChangesHonoursTeamID(t *testing.T) {
	s, ctx := writeSeamStore(t)
	for _, c := range []struct{ provider, native, keyed string }{
		{"jira", "ENG", "jira:ENG"}, {"linear", "LIN", "linear:LIN"}, {"github", "GH", "gh:GH"}, {"gitlab", "GL", "gl:GL"},
	} {
		writeSeamSeedNative(t, s, ctx, c.provider, c.keyed, c.native)
		seedPendingChange(t, s, ctx, "org-1", "c-"+c.keyed, c.keyed, c.provider)
	}
	seedPendingChange(t, s, ctx, "org-2", "c-other-org", "jira:ENG", "jira")
	seedPendingChange(t, s, ctx, "org-2", "c-other-org-lin", "linear:LIN", "linear")
	h := newTestHandlers(s)

	all := "c-gh:GH,c-gl:GL,c-jira:ENG,c-linear:LIN"
	for _, c := range []struct{ name, query, want string }{
		{"absent", "", all},
		{"empty is no filter", "team_id=", all},
		{"keyed jira", "team_id=" + url.QueryEscape("jira:ENG"), "c-jira:ENG"},
		{"keyed linear", "team_id=" + url.QueryEscape("linear:LIN"), "c-linear:LIN"},
		{"keyed github", "team_id=" + url.QueryEscape("gh:GH"), "c-gh:GH"},
		{"keyed gitlab", "team_id=" + url.QueryEscape("gl:GL"), "c-gl:GL"},
		{"bare id resolves to the keyed team", "team_id=ENG", "c-jira:ENG"},
		{"bare linear", "team_id=LIN", "c-linear:LIN"},
		{"unknown team", "team_id=nope", ""},
		{"repeated values: the last wins", "team_id=" + url.QueryEscape("jira:ENG") + "&team_id=" + url.QueryEscape("linear:LIN"), "c-linear:LIN"},
		{"repeated values: an empty last is no filter", "team_id=" + url.QueryEscape("jira:ENG") + "&team_id=", all},
		{"repeated values: a named last narrows after an empty first", "team_id=&team_id=" + url.QueryEscape("gh:GH"), "c-gh:GH"},
	} {
		t.Run(c.name, func(t *testing.T) {
			code, ids := pendingCall(t, h, "org-1", c.query)
			if code != http.StatusOK || strings.Join(ids, ",") != c.want {
				t.Fatalf("got %d %v, want 200 [%s]", code, ids, c.want)
			}
		})
	}

	t.Run("other org sees only its own", func(t *testing.T) {
		_, ids := pendingCall(t, h, "org-2", "team_id="+url.QueryEscape("jira:ENG"))
		if strings.Join(ids, ",") != "c-other-org" {
			t.Fatalf("org-2 got %v", ids)
		}
		_, ids = pendingCall(t, h, "org-2", "")
		if strings.Join(ids, ",") != "c-other-org,c-other-org-lin" {
			t.Fatalf("org-2 unfiltered got %v", ids)
		}
		_, ids = pendingCall(t, h, "org-1", "team_id="+url.QueryEscape("jira:ENG"))
		if strings.Join(ids, ",") != "c-jira:ENG" {
			t.Fatalf("org-1 got %v", ids)
		}
	})

	t.Run("bare id held by two providers is a conflict", func(t *testing.T) {
		writeSeamSeedNative(t, s, ctx, "linear", "linear:ENG", "ENG")
		seedPendingChange(t, s, ctx, "org-1", "c-linear:ENG", "linear:ENG", "linear")
		if code, _ := pendingCall(t, h, "org-1", "team_id=ENG"); code != http.StatusConflict {
			t.Fatalf("ambiguous bare id = %d, want 409", code)
		}
	})
}
