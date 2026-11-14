package integrationsadmin

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
)

type boundaryStore struct {
	state map[uuid.UUID]policy.UserState
	roles map[[2]uuid.UUID]string
}

func (s *boundaryStore) UserState(_ context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	st, ok := s.state[id]
	return st, ok, nil
}
func (s *boundaryStore) Membership(_ context.Context, user, org uuid.UUID) (string, bool, error) {
	role, ok := s.roles[[2]uuid.UUID{user, org}]
	return role, ok, nil
}
func (s *boundaryStore) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	role, ok, err := s.Membership(ctx, user, org)
	return role, ok, true, err
}
func (s *boundaryStore) ActiveImpersonation(context.Context, uuid.UUID) (*policy.Impersonation, error) {
	return nil, nil
}
func (*boundaryStore) ReadsPlatformRoles() bool { return false }

// The integration write routes read their body through Guard.BodyFirst; a lone
// surrogate in any string position is a 422 there, before the database (this rig
// has no pool: a request that is not refused fails at the pool).
func TestIntegrationBodiesRefuseALoneSurrogateAtTheBoundary(t *testing.T) {
	logger := slog.New(slog.DiscardHandler)
	user, org := uuid.New(), uuid.New()
	store := &boundaryStore{state: map[uuid.UUID]policy.UserState{user: {IsActive: true}}, roles: map[[2]uuid.UUID]string{{user, org}: "admin"}}
	tokens := edtokens.New(t)
	guard := policy.NewGuard(tokens.AuthenticatorOver(t, store, logger), logger)
	bearer := tokens.MustMint(t, edtokens.Claims{UserID: user, OrgID: org.String()})
	mux := http.NewServeMux()
	for _, route := range Routes(Deps{Guard: guard, Logger: logger}) {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	serve := func(method, path, body string) (code int, text string) {
		defer func() {
			if recover() != nil {
				code, text = -1, "handler reached the database and panicked"
			}
		}()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+bearer)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}
	id := uuid.NewString()
	cases := []struct{ name, method, path, body, loc string }{
		{"create schedule_cron (str)", http.MethodPost, prefix, `{"name":"n","provider":"github","schedule_cron":"\ud800"}`, `["body","schedule_cron"]`},
		{"create timezone (str)", http.MethodPost, prefix, `{"name":"n","provider":"github","timezone":"a\udc00"}`, `["body","timezone"]`},
		{"create credential_id (str)", http.MethodPost, prefix, `{"name":"n","provider":"github","credential_id":"\ud800"}`, `["body","credential_id"]`},
		{"create config value", http.MethodPost, prefix, `{"name":"n","provider":"github","config":{"x":"\ud800"}}`, `["body","config","x"]`},
		{"create config key", http.MethodPost, prefix, `{"name":"n","provider":"github","config":{"\ud800":"x"}}`, `["body","config","[key]"]`},
		{"patch datasets dataset_key", http.MethodPatch, prefix + "/" + id + "/datasets", `{"datasets":[{"dataset_key":"\ud800","is_enabled":true}]}`, `["body","datasets",0,"dataset_key"]`},
	}
	for _, c := range cases {
		code, text := serve(c.method, c.path, c.body)
		lower := strings.ToLower(text)
		if code != http.StatusUnprocessableEntity || !strings.Contains(text, `"string_unicode"`) || !strings.Contains(strings.ReplaceAll(text, " ", ""), c.loc) {
			t.Errorf("%s: %d %s, want 422 string_unicode at %s", c.name, code, text, c.loc)
		}
		if !utf8.ValidString(text) || strings.Contains(lower, "ud800") || strings.Contains(lower, "udc00") || strings.Contains(lower, `"input"`) {
			t.Errorf("%s: the refusal echoes the input: %s", c.name, text)
		}
	}
}
