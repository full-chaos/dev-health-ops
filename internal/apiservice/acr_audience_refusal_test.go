package apiservice

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

// acrReadCounter counts every read the EdDSA user path makes after the
// verifier, so a refusal is shown to come before the session, user and
// membership reads.
type acrReadCounter struct {
	*probeStore
	reads int
}

func (c *acrReadCounter) SessionState(ctx context.Context, id uuid.UUID) (policy.SessionRow, bool, error) {
	c.reads++
	return c.probeStore.SessionState(ctx, id)
}

func (c *acrReadCounter) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	c.reads++
	return c.probeStore.AccessUser(ctx, id)
}

func (c *acrReadCounter) UserState(ctx context.Context, id uuid.UUID) (policy.UserState, bool, error) {
	c.reads++
	return c.probeStore.UserState(ctx, id)
}

func (c *acrReadCounter) Membership(ctx context.Context, user, org uuid.UUID) (string, bool, error) {
	c.reads++
	return c.probeStore.Membership(ctx, user, org)
}

func (c *acrReadCounter) OrgMembership(ctx context.Context, user, org uuid.UUID) (string, bool, bool, error) {
	c.reads++
	return c.probeStore.OrgMembership(ctx, user, org)
}

// acrRefusalReason is the refused-counter reason of an ACR-audience token at a
// platform verifier: since dev-health-go v0.36.0 authverify knows the audiences,
// so the token fails the verifier's audience pin (ReasonAudience, counted
// claims), not its claims contract.
const acrRefusalReason = accessverify.ReasonClaims

// acrAudiences are the two ACR audiences with their scopes (CHAOS-12248).
var acrAudiences = []struct{ aud, scope string }{
	{contracts.ACRAPIAudience, "acr:api"},
	{contracts.ACRMCPAudience, "acr:mcp"},
}

// TestAnACRAudienceTokenIsRefusedAtGoAPIBeforeAnyRead is G-43 condition 1 at
// go-api (ACP-ADR-08 amendment): an access token with aud acr-api or acr-mcp
// is the generic 401 at the api listener and reads nothing. The positive
// control is the same claims with go-api's audience and scope, which are
// served and read, and the ACR token itself is a valid access-token.v1
// document, so the refusal comes from the audience.
func TestAnACRAudienceTokenIsRefusedAtGoAPIBeforeAnyRead(t *testing.T) {
	root, err := contracts.RepoRoot(".")
	if err != nil {
		t.Fatal(err)
	}
	minter := newEdDSAMinter(t)
	for _, acr := range acrAudiences {
		t.Run(acr.aud, func(t *testing.T) {
			store := &acrReadCounter{probeStore: newProbeStore()}
			f := newRoutedFixture(t, store, userRows(t), probeIssuer, minter.keys, nil, probeNow)

			control := probeDo(f.handler, probeRequest{"control", http.MethodGet, probeAuthenticated, bearer(minter.mint(probeUser, nil))})
			if control.Status != http.StatusOK || store.reads == 0 {
				t.Fatalf("positive control: %d %s with %d reads; want served after reads", control.Status, control.Body, store.reads)
			}

			token := minter.mint(probeUser, map[string]any{"aud": acr.aud, "scope": acr.scope})
			if _, err := contracts.ParseAccessToken(root, compactDocument(t, token)); err != nil {
				t.Fatalf("the %s token is not a valid access-token.v1 document: %v", acr.aud, err)
			}
			store.reads = 0
			for _, path := range []string{probeAuthenticated, probeAdmin} {
				got := probeDo(f.handler, probeRequest{acr.aud, http.MethodGet, path, bearer(token)})
				if got.Status != http.StatusUnauthorized || got.Body != genericRefusal || store.reads != 0 {
					t.Fatalf("GET %s with aud %s: %d %s with %d reads; want the generic 401 and no read", path, acr.aud, got.Status, got.Body, store.reads)
				}
			}
			counters := f.counters(t)
			for _, path := range []string{probeAuthenticated, probeAdmin} {
				series := accessverify.RefusedMetric + "|profile=GET " + path + ",reason=" + acrRefusalReason + ",service=" + goAPIAccessService
				if counters[series] == 0 {
					t.Errorf("no %s", series)
				}
			}
		})
	}
}

// compactDocument is the {"header", "claims"} document of a compact JWS, the
// input of contracts.ParseAccessToken.
func compactDocument(t *testing.T, compact string) []byte {
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
