package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// issuerUnsetGolden is what main (def2abd1, before query-api routed tokens by
// row) answered for each request below, recorded there by running the same
// requests through its own REST, /query and /graphql paths. With
// AUTH_ACCESS_TOKEN_ISSUER unset every answer must stay byte-identical.
const issuerUnsetGolden = `{"graphql POST edge inactive":{"body":"{\"detail\":\"Authentication required\"}","status":401},
"graphql POST edge member":{"body":"","status":200},
"graphql POST edge orgless":{"body":"{\"detail\":\"Authentication required\"}","status":401},
"graphql POST edge orgless superuser":{"body":"","status":200},
"graphql POST edge stale token_version":{"body":"{\"detail\":\"Authentication required\"}","status":401},
"graphql POST edge users store error":{"body":"{\"detail\":\"Internal Server Error\"}","status":500},
"graphql POST edge users store unavailable":{"body":"{\"detail\":\"Internal Server Error\"}","status":500},
"graphql POST envelope":{"body":"{\"detail\":\"Authentication required\"}","status":401},
"graphql POST garbage":{"body":"{\"detail\":\"Authentication required\"}","status":401},
"graphql POST no bearer":{"body":"{\"detail\":\"Authentication required\"}","status":401},
"graphql POST user access token":{"body":"{\"detail\":\"Authentication required\"}","status":401},
"query POST edge inactive":{"body":"unauthorized","status":401},
"query POST edge member":{"body":"","status":200},
"query POST edge orgless":{"body":"unauthorized","status":401},
"query POST edge orgless superuser":{"body":"","status":200},
"query POST edge stale token_version":{"body":"unauthorized","status":401},
"query POST edge users store error":{"body":"unauthorized","status":401},
"query POST edge users store unavailable":{"body":"unauthorized","status":401},
"query POST envelope":{"body":"","status":200},
"query POST garbage":{"body":"unauthorized","status":401},
"query POST no bearer":{"body":"unauthorized","status":401},
"query POST user access token":{"body":"unauthorized","status":401},
"rest GET edge member on GET":{"body":"{\"org\":\"22222222-2222-2222-2222-222222222222\"}","status":200},
"rest GET edge sub not a string":{"body":"{\"detail\":\"Internal Server Error\"}","status":500},
"rest POST edge inactive":{"body":"{\"detail\":{\"message\":\"Invalid or expired token\"}}","status":401},
"rest POST edge member":{"body":"{\"org\":\"22222222-2222-2222-2222-222222222222\"}","status":200},
"rest POST edge orgless":{"body":"{\"detail\":\"Organization context required\"}","status":403},
"rest POST edge orgless superuser":{"body":"{\"org\":\"\"}","status":200},
"rest POST edge stale token_version":{"body":"{\"detail\":{\"message\":\"Invalid or expired token\"}}","status":401},
"rest POST edge users store error":{"body":"{\"detail\":\"Internal Server Error\"}","status":500},
"rest POST edge users store unavailable":{"body":"{\"detail\":{\"message\":\"Database temporarily unavailable\"}}","status":503},
"rest POST envelope":{"body":"{\"org\":\"22222222-2222-2222-2222-222222222222\"}","status":200},
"rest POST garbage":{"body":"{\"detail\":{\"message\":\"Invalid or expired token\"}}","status":401},
"rest POST no bearer":{"body":"{\"detail\":{\"message\":\"Not authenticated\"}}","status":401},
"rest POST user access token":{"body":"{\"detail\":{\"message\":\"Invalid or expired token\"}}","status":401},
"rest PUT edge member on PUT":{"body":"{\"detail\":\"Method Not Allowed\"}","status":405}}`

// unsetCase is one request of the issuer-unset golden.
type unsetCase struct {
	name   string
	store  func(*fakeEdgeStore)
	path   string // "rest", "query" or "graphql"
	method string
	bearer func(t *testing.T, envelope string) string
}

func unsetCases(m *accessMinter) []unsetCase {
	edge := func(c ecEdgeClaims) func(*testing.T, string) string {
		return func(t *testing.T, _ string) string { return ecMintEdgeToken(t, c) }
	}
	member := ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5}
	stale := member
	stale.tokenVersion = 4
	orgless := ecEdgeClaims{sub: ecUser.String(), role: "admin", tokenVersion: 5}
	fixed := func(token string) func(*testing.T, string) string {
		return func(*testing.T, string) string { return token }
	}
	env := func(_ *testing.T, envelope string) string { return envelope }
	user := fixed(m.mint(ecUser, nil, nil))
	inactive := func(s *fakeEdgeStore) { s.states[ecUser] = policy.UserState{TokenVersion: 5} }
	super := func(s *fakeEdgeStore) {
		s.states[ecUser] = policy.UserState{IsActive: true, IsSuperuser: true, TokenVersion: 5}
	}
	down := func(s *fakeEdgeStore) { s.errUserState = policy.ErrUnavailable }
	broken := func(s *fakeEdgeStore) { s.errUserState = errors.New("syntax error") }
	var cases []unsetCase
	for _, path := range []string{"rest", "query", "graphql"} {
		method := http.MethodPost
		cases = append(cases,
			unsetCase{"no bearer", nil, path, method, fixed("")},
			unsetCase{"edge member", nil, path, method, edge(member)},
			unsetCase{"edge stale token_version", nil, path, method, edge(stale)},
			unsetCase{"edge inactive", inactive, path, method, edge(member)},
			unsetCase{"edge orgless", nil, path, method, edge(orgless)},
			unsetCase{"edge orgless superuser", super, path, method, edge(orgless)},
			unsetCase{"edge users store unavailable", down, path, method, edge(member)},
			unsetCase{"edge users store error", broken, path, method, edge(member)},
			unsetCase{"envelope", nil, path, method, env},
			unsetCase{"user access token", nil, path, method, user},
			unsetCase{"garbage", nil, path, method, fixed("not-a-token")},
		)
	}
	cases = append(cases,
		unsetCase{"edge member on GET", nil, "rest", http.MethodGet, edge(member)},
		unsetCase{"edge member on PUT", nil, "rest", http.MethodPut, edge(member)},
		unsetCase{"edge sub not a string", nil, "rest", http.MethodGet, func(t *testing.T, _ string) string {
			return ecMintEdgeTokenRaw(t, map[string]any{"sub": 7, "org_id": ecOrg.String(), "tv": 5, "type": "access"})
		}},
	)
	return cases
}

func TestQueryAPIIssuerUnsetAnswersAreMainsAnswers(t *testing.T) {
	quietStdLog(t)
	m := newAccessMinter(t)
	got := map[string]answer{}
	for _, tc := range unsetCases(m) {
		store := newAccessStore()
		if tc.store != nil {
			tc.store(store.fakeEdgeStore)
		}
		key := tc.path + " " + tc.method + " " + tc.name
		switch tc.path {
		case "rest":
			f := newRESTFixture(t, store, nil, true)
			got[key] = f.do(tc.method, tc.bearer(t, f.envelope))
		default:
			stack := newAccessStack(t, store, nil, true)
			path := "/query"
			if tc.path == "graphql" {
				path = graphQLEdgePath
			}
			a := stack.send("query", tc.method, path, "", tc.bearer(t, stack.envelope))
			got[key] = answer{a.Status, strings.TrimSpace(a.Body)}
		}
	}
	keys := make([]string, 0, len(got))
	for key := range got {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var want map[string]answer
	if err := json.Unmarshal([]byte(issuerUnsetGolden), &want); err != nil {
		t.Fatalf("golden: %v", err)
	}
	if len(want) != len(got) || len(got) < 30 {
		t.Fatalf("%d answers, golden has %d", len(got), len(want))
	}
	for _, key := range keys {
		if got[key] != want[key] {
			t.Errorf("%s: got %+v, main answered %+v", key, got[key], want[key])
		}
	}
}
