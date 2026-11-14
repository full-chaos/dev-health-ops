package teamsidentity

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

type surrogateStore struct {
	mu    sync.Mutex
	state map[uuid.UUID]policy.UserState
	roles map[[2]uuid.UUID]string
}

func (s *surrogateStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	st, ok := s.state[id]
	return st, ok, nil
}
func (s *surrogateStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	role, ok := s.roles[[2]uuid.UUID{user, org}]
	return role, ok, nil
}
func (s *surrogateStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, ok, err := s.Membership(ctx, user, org)
	return role, ok, true, err
}
func (s *surrogateStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}
func (*surrogateStore) ReadsPlatformRoles() bool { return false }

// The admin team and identity routes read their body through decodeFirst; a
// lone surrogate in any string position is a 422 there, before the store (this
// handler has no ClickHouse connection: a request that is not refused fails
// at the store).
func TestTeamsIdentityBodiesRefuseALoneSurrogateAtTheBoundary(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	user, org := uuid.New(), uuid.New()
	store := &surrogateStore{state: map[uuid.UUID]policy.UserState{user: {IsActive: true}}, roles: map[[2]uuid.UUID]string{{user, org}: "admin"}}
	tokens := edtokens.New(t)
	guard := policy.NewGuard(tokens.AuthenticatorOver(t, store, logger), logger)
	bearer := tokens.MustMint(t, edtokens.Claims{UserID: user, OrgID: org.String()})
	h := handlers{logger: logger}
	routes := map[string]http.Handler{
		"team":     h.decodeFirst(guard, http.HandlerFunc(h.createOrUpdateTeam)),
		"identity": h.decodeFirst(guard, http.HandlerFunc(h.createOrUpdateIdentity)),
	}
	serve := func(route, body string) (code int, text string) {
		defer func() {
			if recover() != nil {
				code, text = -1, "handler reached the store and panicked"
			}
		}()
		req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		routes[route].ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	cases := []struct{ name, route, body, loc string }{
		{"team description (str)", "team", `{"team_id":"t","name":"n","description":"a\ud800b"}`, `["body","description"]`},
		{"team repo_patterns item", "team", `{"team_id":"t","name":"n","repo_patterns":["x\udc00y"]}`, `["body","repo_patterns",0]`},
		{"identity display_name (str)", "identity", `{"canonical_id":"i","display_name":"a\ud800b"}`, `["body","display_name"]`},
		{"identity provider_identities value", "identity", `{"canonical_id":"i","provider_identities":{"github":["v\ud800"]}}`, `["body","provider_identities","github",0]`},
		{"identity provider_identities key", "identity", `{"canonical_id":"i","provider_identities":{"k\udc00":["v"]}}`, `["body","provider_identities","[key]"]`},
	}
	for _, c := range cases {
		code, text := serve(c.route, c.body)
		lower := strings.ToLower(text)
		if code != http.StatusUnprocessableEntity || !strings.Contains(text, `"string_unicode"`) || !strings.Contains(strings.ReplaceAll(text, " ", ""), c.loc) {
			t.Errorf("%s: %d %s, want 422 string_unicode at %s", c.name, code, text, c.loc)
		}
		if !utf8.ValidString(text) || strings.Contains(lower, "ud800") || strings.Contains(lower, "udc00") || strings.Contains(lower, `"input"`) {
			t.Errorf("%s: the refusal echoes the input: %s", c.name, text)
		}
	}
}
