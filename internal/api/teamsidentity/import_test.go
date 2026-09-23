package teamsidentity

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

// postImportBody drives POST /teams/import's handler with a decoded body
// (bypassing only the auth guard, exactly as the other handler-level tests
// in this package do). Validation failures answer before the store is ever
// touched, so a zero Store is enough for every 422 case here.
func postImportBody(t *testing.T, store Store, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/teams/import", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.SetPathValue("team_id", "import")
	decoded, outcome, failure, err := pybody.Read(req)
	if err != nil || outcome != pybody.Ready {
		t.Fatalf("pybody.Read: outcome=%v failure=%v err=%v", outcome, failure, err)
	}
	ctx := context.WithValue(req.Context(), bodyKey{}, decoded)
	ctx = policy.WithUser(ctx, &policy.User{OrgID: "org-1"})
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()
	handlers{store: store, logger: slog.Default()}.postTeamWildcard(rec, req)
	return rec
}

// TestImportTeamsValidationMatchesPydantic pins TeamImportRequest's 422
// bodies against a live pydantic run (each `want` below is what
// TeamImportRequest.model_validate reports for the same payload, with
// FastAPI's "body" loc prefix): associations is `dict[str, Any] =
// Field(default_factory=dict)`, so an explicit null (or any non-dict) is a
// dict_type error, never silently replaced by {} -- CHAOS-6311 r1 finding
// 2 imported such a team without its associations and answered 200.
func TestImportTeamsValidationMatchesPydantic(t *testing.T) {
	const team = `"provider_type":"github","provider_team_id":"eng","name":"Eng"`
	tests := []struct {
		name  string
		patch string
		want  string
	}{
		{"associations null", `,"associations":null`,
			`{"detail":[{"type":"dict_type","loc":["body","teams",0,"associations"],"msg":"Input should be a valid dictionary","input":null}]}`},
		{"associations list", `,"associations":[1]`,
			`{"detail":[{"type":"dict_type","loc":["body","teams",0,"associations"],"msg":"Input should be a valid dictionary","input":[1]}]}`},
		{"description number", `,"description":5`,
			`{"detail":[{"type":"string_type","loc":["body","teams",0,"description"],"msg":"Input should be a valid string","input":5}]}`},
		{"member_count unparseable string", `,"member_count":"x"`,
			`{"detail":[{"type":"int_parsing","loc":["body","teams",0,"member_count"],"msg":"Input should be a valid integer, unable to parse string as an integer","input":"x"}]}`},
		{"member_count fractional float", `,"member_count":5.5`,
			`{"detail":[{"type":"int_from_float","loc":["body","teams",0,"member_count"],"msg":"Input should be a valid integer, got a number with a fractional part","input":5.5}]}`},
		{"member_count list", `,"member_count":[]`,
			`{"detail":[{"type":"int_type","loc":["body","teams",0,"member_count"],"msg":"Input should be a valid integer","input":[]}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rec := postImportBody(t, Store{}, `{"teams":[{`+team+test.patch+`}]}`)
			if rec.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422\nbody: %s", rec.Code, rec.Body.String())
			}
			if rec.Body.String() != test.want {
				t.Errorf("body = %s\nwant   %s", rec.Body.String(), test.want)
			}
		})
	}
}

// TestParseDiscoveredTeamAcceptsWhatPydanticAccepts: lax-mode coercions and
// the associations default must NOT be rejected.
func TestParseDiscoveredTeamAcceptsWhatPydanticAccepts(t *testing.T) {
	for _, memberCount := range []string{`"5"`, `" 7 "`, `5.0`, `true`, `"5.0"`} {
		body := `{"teams":[{"provider_type":"jira","provider_team_id":"ENG","name":"Eng","member_count":` + memberCount + `}]}`
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
		decoded, _, _, err := pybody.Read(req)
		if err != nil {
			t.Fatal(err)
		}
		var problems pybody.Errors
		object, _ := problems.Object(decoded)
		teams, _, parseProblems := parseTeamImportRequest(object)
		if len(parseProblems) != 0 || len(teams) != 1 {
			t.Errorf("member_count=%s: problems=%v teams=%d, want accepted", memberCount, parseProblems, len(teams))
			continue
		}
		if teams[0].MemberCount == nil {
			t.Errorf("member_count=%s: not captured", memberCount)
		}
		if teams[0].Associations == nil {
			t.Errorf("member_count=%s: absent associations must default to {}, got nil", memberCount)
		}
	}
}

// TestImportedAssociationsReachTheObservationRow: a JSON request body
// decodes arrays as []pyjson.Value, and Python passes
// `associations.get("project_keys"/"repo_patterns", [])` straight through --
// CHAOS-6311 r1's venue run found Go writing EMPTY lists for every imported
// team because only hand-built []string values were recognized (every
// earlier unit test built its associations that way, so none could see it).
func TestImportedAssociationsReachTheObservationRow(t *testing.T) {
	body := `{"teams":[{"provider_type":"github","provider_team_id":"platform","name":"P","associations":{"repo_patterns":["acme/api","acme/web"],"project_keys":["PK"],"provider_org":"acme"}}]}`
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
	decoded, _, _, err := pybody.Read(req)
	if err != nil {
		t.Fatal(err)
	}
	var problems pybody.Errors
	object, _ := problems.Object(decoded)
	teams, _, parseProblems := parseTeamImportRequest(object)
	if len(parseProblems) != 0 || len(teams) != 1 {
		t.Fatalf("parse: problems=%v teams=%d", parseProblems, len(teams))
	}
	row := observedRowFromDiscovered("org-1", teams[0], time.Now(), time.Now())
	if got := strings.Join(row.RepoPatterns, ","); got != "acme/api,acme/web" {
		t.Errorf("RepoPatterns = %q, want acme/api,acme/web", got)
	}
	if got := strings.Join(row.ProjectKeys, ","); got != "PK" {
		t.Errorf("ProjectKeys = %q, want PK", got)
	}
}

func TestImportMissingTeamsReportsTheWholeBodyAsInput(t *testing.T) {
	rec := postImportBody(t, Store{}, `{}`)
	want := `{"detail":[{"type":"missing","loc":["body","teams"],"msg":"Field required","input":{}}]}`
	if rec.Code != http.StatusUnprocessableEntity || rec.Body.String() != want {
		t.Errorf("got %d %s, want 422 %s", rec.Code, rec.Body.String(), want)
	}
}

func TestPostToAnotherTeamIDIsMethodNotAllowed(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/api/v1/admin/teams/eng", strings.NewReader(`{}`))
	req.SetPathValue("team_id", "eng")
	rec := httptest.NewRecorder()
	handlers{store: Store{}, logger: slog.Default()}.postTeamWildcard(rec, req)
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "DELETE" ||
		rec.Body.String() != `{"detail":"Method Not Allowed"}` {
		t.Errorf("got %d allow=%q body=%s", rec.Code, rec.Header().Get("Allow"), rec.Body.String())
	}
}
