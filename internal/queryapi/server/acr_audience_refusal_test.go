package server

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// acrQueryReads counts every read the user-token path makes after the
// verifier: the session, the users row and the membership.
type acrQueryReads struct {
	*accessStore
	reads int
}

func (s *acrQueryReads) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	s.reads++
	return s.accessStore.SessionState(ctx, id)
}

func (s *acrQueryReads) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	s.reads++
	return s.accessStore.UserState(ctx, id)
}

func (s *acrQueryReads) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	s.reads++
	return s.accessStore.Membership(ctx, user, org)
}

func (s *acrQueryReads) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	s.reads++
	return s.accessStore.OrgMembership(ctx, user, org)
}

// acrRefusalReason is the refused-counter reason of an ACR-audience token at a
// platform verifier: since dev-health-go v0.36.0 authverify knows the audiences,
// so the token fails the verifier's audience pin (ReasonAudience, counted
// claims), not its claims contract.
const acrRefusalReason = accessverify.ReasonClaims

// TestAnACRAudienceTokenIsRefusedAtQueryAPIBeforeAnyRead is G-43 condition 1
// at query-api (ACP-ADR-08 amendment): an access token with aud acr-api or
// acr-mcp is each route's 401 on the real listeners and reads nothing. The
// positive control is the same claims with query-api's audience and scope,
// served after reads, and the ACR token itself is a valid access-token.v1
// document, so the refusal comes from the audience.
func TestAnACRAudienceTokenIsRefusedAtQueryAPIBeforeAnyRead(t *testing.T) {
	quietStdLog(t)
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	m := newAccessMinter(t)
	type target struct{ listener, method, path, refusal string }
	targets := []target{
		{"query", http.MethodPost, "/query", queryRefusal},
		{"query", http.MethodPost, "/graphql", graphQLRefusal},
		{"internal", http.MethodPost, "/query/run-operation", queryRefusal},
	}
	for _, acr := range []struct{ aud, scope string }{
		{contracts.ACRAPIAudience, "acr:api"},
		{contracts.ACRMCPAudience, "acr:mcp"},
	} {
		t.Run(acr.aud, func(t *testing.T) {
			store := &acrQueryReads{accessStore: newAccessStore()}
			stack := newAccessStackOn(t, queryAPIUserStore{queryAPIStoreBase: store}, testAccess(m.keys, nil, accessTestNow), true)

			for _, target := range targets {
				store.reads = 0
				if got := stack.send(target.listener, target.method, target.path, "", m.mint(ecUser, nil, nil)); got.Status != http.StatusOK || store.reads == 0 {
					t.Fatalf("positive control %s %s: %d %s with %d reads; want served after reads", target.method, target.path, got.Status, got.Body, store.reads)
				}
			}

			token := m.mint(ecUser, map[string]any{"aud": acr.aud, "scope": acr.scope}, nil)
			if _, err := contracts.ParseAccessToken(root, acrCompactDocument(t, token)); err != nil {
				t.Fatalf("the %s token is not a valid access-token.v1 document: %v", acr.aud, err)
			}
			store.reads = 0
			for _, target := range targets {
				got := stack.send(target.listener, target.method, target.path, "", token)
				if got.Status != http.StatusUnauthorized || got.Body != target.refusal || store.reads != 0 {
					t.Fatalf("%s %s with aud %s: %d %q with %d reads; want the route's 401 and no read", target.method, target.path, acr.aud, got.Status, got.Body, store.reads)
				}
			}
			reason := acrRefusalReason
			refused := 0
			for series := range stack.reader.counts(t) {
				if strings.HasPrefix(series, accessverify.RefusedMetric+"|") && strings.Contains(series, ",reason="+reason+",") {
					refused++
				}
			}
			if refused != len(targets) {
				t.Fatalf("refused series with reason %s: %d, want one per route (%d): %v", reason, refused, len(targets), stack.reader.counts(t))
			}
		})
	}
}

// acrCompactDocument is the {"header", "claims"} document of a compact JWS,
// the input of contracts.ParseAccessToken.
func acrCompactDocument(t *testing.T, compact string) []byte {
	t.Helper()
	parts := strings.Split(compact, ".")
	if len(parts) != 3 {
		t.Fatalf("%d parts", len(parts))
	}
	header, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatal(err)
	}
	claims, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	return []byte(`{"header":` + string(header) + `,"claims":` + string(claims) + `}`)
}
