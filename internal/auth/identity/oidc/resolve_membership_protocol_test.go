package oidc

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
)

const oauthMembershipRefusalBody = `{"detail":"This account is not a member of the provider's organization."}`

// TestNotMemberRefusalBodyByProtocolAndPath pins the exact 403 body of every non-member refusal path:
// OIDC answers go-api's structured detail, OAuth keeps its earlier plain detail.
func TestNotMemberRefusalBodyByProtocolAndPath(t *testing.T) {
	user := Principal{ID: uuid.New(), Active: true}
	paths := map[string]struct {
		ids  *resolveIdentities
		jit  MembershipJIT
		role string
	}{
		"identity lookup outside the organization": {&resolveIdentities{err: ErrNotOrgMember}, &resolveJIT{}, ""},
		"existing account, no JIT":                 {&resolveIdentities{principal: user}, nil, ""},
		"existing account, join not a member":      {&resolveIdentities{principal: user}, &resolveJIT{err: ErrNotOrgMember}, ""},
		"existing account, ungrantable role":       {&resolveIdentities{principal: user}, &resolveJIT{}, "owner"},
		"new account, no JIT":                      {&resolveIdentities{err: ErrIdentityNotFound}, nil, ""},
		"new account, provision not a member":      {&resolveIdentities{err: ErrIdentityNotFound}, &resolveJIT{err: ErrNotOrgMember}, ""},
		"new account, ungrantable role":            {&resolveIdentities{err: ErrIdentityNotFound}, &resolveJIT{}, "owner"},
	}
	protocols := map[string]string{"oidc": goAPIMembershipRefusalBody, "oauth_gitlab": oauthMembershipRefusalBody}
	for pathName, tc := range paths {
		for protocol, want := range protocols {
			t.Run(protocol+"/"+pathName, func(t *testing.T) {
				svc := &Service{cfg: Config{Identities: tc.ids, Memberships: resolveMembers{}, Sessions: &resolveSessions{}, MembershipJIT: tc.jit, Logger: slog.New(slog.DiscardHandler)}}
				row := Provider{ID: uuid.New(), OrgID: uuid.New(), Protocol: protocol, AutoProvision: true,
					AllowedDomains: []string{"example.com"}, DefaultRole: tc.role}
				v := validated{profile: profile{Email: "p@example.com", EmailVerified: true}}
				_, failure := svc.resolve(context.Background(), row, v, nil)
				rec := httptest.NewRecorder()
				svc.writeError(rec, failure)
				if rec.Code != 403 || strings.TrimSpace(rec.Body.String()) != want {
					t.Fatalf("answer = %d %q, want 403 %s", rec.Code, rec.Body, want)
				}
			})
		}
	}
}
