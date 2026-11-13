//go:build integration

package decisionstate

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authorization"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// The read listing's cap recount (CHAOS-13618, acp ruling option 3) on a real auth database: grants and repository
// identity rows written to auth.resource_grants and auth.repository_identities, read by this package's reader through
// the mounted listing route and through /v1/authorize:repositories' rule.

const capKeyID = "cap-recount-key"

type capPlane struct {
	issuer, subject string
	private         ed25519.PrivateKey
	set             token.KeySet
}

func (p *capPlane) KeySet(context.Context) (token.KeySet, error) { return p.set, nil }

func (p *capPlane) bearer() string {
	now := time.Now()
	claims, _ := json.Marshal(contracts.WorkloadTokenClaims{Iss: p.issuer, Aud: "auth-service", Sub: p.subject, PrincipalType: "workload",
		Jti: uuid.NewString(), Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300})
	header, _ := json.Marshal(map[string]string{"alg": token.Algorithm, "kid": capKeyID, "typ": contracts.WorkloadTokenTyp})
	enc := base64.RawURLEncoding
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	return "Bearer " + input + "." + enc.EncodeToString(ed25519.Sign(p.private, []byte(input)))
}

// capListing mounts the read listing route over the real reader and returns a caller of it.
func capListing(t *testing.T, e *env) func(principal, org uuid.UUID) (int, string) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: capKeyID, State: token.StateActive, Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	plane := &capPlane{issuer: "https://auth.cap.example.test", subject: session.WirePrincipalID(uuid.New()), private: private, set: set}
	workload, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{IssuerURL: plane.issuer, Keys: plane, Subject: plane.subject})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := json.Marshal(map[string]any{"rows": []map[string]any{{"service": authapi.AccessService, "method": "POST",
		"route": authapi.PatternReadableRepositories, "accepted_credential_classes": []string{"workload_token"}}}})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := authapi.ReadableRepositoriesRoutes(authapi.ReadableRepositoriesDeps{State: e.reader, Evaluator: authorization.NewEvaluator(),
		Workload: workload, ProfileRows: rows})
	if err != nil || len(routes) != 1 {
		t.Fatalf("listing route: %d routes, %v", len(routes), err)
	}
	return func(principal, org uuid.UUID) (int, string) {
		t.Helper()
		request := httptest.NewRequest(http.MethodPost, authapi.PatternReadableRepositories,
			strings.NewReader(`{"principal_id":"`+principal.String()+`","organization_id":"`+org.String()+`"}`))
		request.Header.Set("Authorization", plane.bearer())
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		routes[0].Handler.ServeHTTP(response, request)
		return response.Code, strings.TrimSpace(response.Body.String())
	}
}

// capReader is a viewer of a new organization with n path-only viewer grants acme/repo-000.. and an identity row for
// each of the first identities keys. It returns the principal, the organization and the n keys, sorted.
func (e *env) capReader(t *testing.T, ctx context.Context, n, identities int) (uuid.UUID, uuid.UUID, []string) {
	t.Helper()
	user, org := e.user(t, ctx, true, false), e.org(t, ctx)
	e.exec(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at) VALUES ($1, $2, $3, 'viewer', now())`, uuid.New(), user, org)
	e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id)
SELECT $1, $2, 'viewer', 'repository', 'acme/repo-' || lpad(i::text, 3, '0') FROM generate_series(0, $3 - 1) AS i`, user, org, n)
	if identities > 0 {
		e.armed(t, ctx, `INSERT INTO auth.repository_identities (organization_id, provider, provider_repo_id, current_path)
SELECT $1, 'github', (1000 + i)::text, 'acme/repo-' || lpad(i::text, 3, '0') FROM generate_series(0, $2 - 1) AS i`, org, identities)
	}
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("acme/repo-%03d", i)
	}
	return user, org, keys
}

func repositoriesBody(keys []string) string {
	raw, _ := json.Marshal(keys)
	return `{"schema_version":"readable_repositories.v1","scope":"repositories","repositories":` + string(raw) + `}`
}

const capDenyBody = `{"schema_version":"readable_repositories.v1","scope":"denied","reason_code":"repository_allowlist_too_large"}`

func TestTheReadListingRecountsAnOverCapListingOnTheIdentityRows(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	list := capListing(t, e)

	t.Run("(a) 257 grants, 256 with an identity row: 200 with exactly those 256", func(t *testing.T) {
		user, org, keys := e.capReader(t, ctx, 257, 256)
		if code, body := list(user, org); code != http.StatusOK || body != repositoriesBody(keys[:256]) {
			t.Fatalf("%d %s, want 200 with the 256 keys that have an identity row", code, body)
		}
	})
	t.Run("(b) 258 grants, 257 with an identity row: the same deny", func(t *testing.T) {
		user, org, _ := e.capReader(t, ctx, 258, 257)
		if code, body := list(user, org); code != http.StatusOK || body != capDenyBody {
			t.Fatalf("%d %s, want 200 %s", code, body, capDenyBody)
		}
	})
	t.Run("(c) 256 grants, 3 with an identity row: every key, as main", func(t *testing.T) {
		user, org, keys := e.capReader(t, ctx, 256, 3)
		if code, body := list(user, org); code != http.StatusOK || body != repositoriesBody(keys) {
			t.Fatalf("%d %s, want 200 with all 256 keys", code, body)
		}
	})
	t.Run("(d) 256 grants and an id-bearing grant with no live path: the 256, as main", func(t *testing.T) {
		user, org, keys := e.capReader(t, ctx, 256, 256)
		e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, resource_repo_id)
VALUES ($1, $2, 'viewer', 'repository', 'acme/gone', 'github:404')`, user, org)
		if code, body := list(user, org); code != http.StatusOK || body != repositoriesBody(keys) {
			t.Fatalf("%d %s, want 200 with the 256 path keys only", code, body)
		}
	})
}

// C-2: the snapshot reads the identity rows for a pair with more than 256 grant rows, and the state version covers the
// same rows: a path move of one identity row is seen by both. A pair with 256 or fewer path-only grants reads neither.
func TestTheSnapshotAndTheStateVersionReadIdentitiesForAnOverCapPair(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, tc := range []struct {
		grants int
		read   bool
	}{{257, true}, {256, false}} {
		t.Run(fmt.Sprintf("%d path-only grants", tc.grants), func(t *testing.T) {
			user, org, keys := e.capReader(t, ctx, tc.grants, tc.grants)
			snap, ok := e.snapshot(t, ctx, user, org.String()).(authorization.RepositoryIdentities)
			if !ok {
				t.Fatal("the snapshot reads no repository identities")
			}
			if _, found := snap.RepositoryID(org.String(), keys[0]); found != tc.read {
				t.Fatalf("snapshot resolved %s: %v, want %v", keys[0], found, tc.read)
			}
			before := e.stateVersion(t, ctx, e.reader, user, org.String())
			e.armed(t, ctx, `UPDATE auth.repository_identities SET current_path = 'acme/moved' WHERE organization_id = $1 AND provider_repo_id = '1000'`, org)
			if moved := e.stateVersion(t, ctx, e.reader, user, org.String()) != before; moved != tc.read {
				t.Fatalf("state version moved on an identity write: %v, want %v", moved, tc.read)
			}
		})
	}
}

// C-4 residual: /v1/authorize:repositories' rule (Evaluator.ReadableRepositories) and the filtered acr.credential.issue
// keep the cap deny for the reader the read listing now serves.
func TestTheOtherScopeUsersKeepTheCapDenyOnTheRealSnapshot(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	user, org, _ := e.capReader(t, ctx, 257, 256)
	snap := e.snapshot(t, ctx, user, org.String())
	ev := authorization.NewEvaluator()
	listing := ev.ReadableRepositories(authorization.Request{Action: "acr.context.read", ResourceKind: "repository", ResourceKey: "*", OrganizationID: org.String()}, snap)
	issue := ev.Evaluate(authorization.Request{Action: "acr.credential.issue", ResourceKind: "organization", OrganizationID: org.String()}, snap)
	for name, d := range map[string]authorization.Decision{"authorize:repositories": listing, "acr.credential.issue": issue} {
		if d.Allowed || d.Reason != authorization.ReasonRepositoryAllowlistTooLarge {
			t.Errorf("%s: %+v, want the cap deny", name, d)
		}
	}
}

// C-3b census: the Go condition the snapshot loads identities on (needsIdentities) and the SQL condition the state
// version reads them on (identitiesCondition) agree on the same rows.
func TestTheGoAndSQLIdentityConditionsAgree(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name          string
		grants, rowID int
		want          bool
	}{
		{"no grant", 0, 0, false},
		{"256 path-only grants", 256, 0, false},
		{"257 path-only grants", 257, 0, true},
		{"3 path-only grants and 1 id-bearing grant", 3, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			user, org, _ := e.capReader(t, ctx, tc.grants, 0)
			if tc.rowID > 0 {
				e.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id, resource_repo_id)
VALUES ($1, $2, 'viewer', 'repository', 'acme/with-id', 'github:77')`, user, org)
			}
			grants, err := e.snapshot(t, ctx, user, org.String()).Grants(org.String())
			if err != nil {
				t.Fatal(err)
			}
			var inSQL bool
			if err := e.admin.QueryRow(ctx, `SELECT `+identitiesCondition(`"auth"`), user, org).Scan(&inSQL); err != nil {
				t.Fatal(err)
			}
			if inGo := needsIdentities(grants); inGo != tc.want || inSQL != tc.want {
				t.Fatalf("Go %v, SQL %v, want both %v", inGo, inSQL, tc.want)
			}
		})
	}
}
