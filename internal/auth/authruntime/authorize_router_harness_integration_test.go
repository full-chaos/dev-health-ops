//go:build integration

package authruntime

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

const azIssuer = "https://auth.example.test"

// azRouterEnv is the real stack with the authorize routes on: an EdDSA signing
// key, the issuer, one caller workload, and limits high enough for a corpus.
type azRouterEnv struct {
	*routesEnv
	signer *token.Signer
	caller string
}

// newAuthorizeRouterEnv builds the handler configure builds over a live
// PostgreSQL and Valkey; extra adds or replaces settings (AUTH_REVISION_FEED).
func newAuthorizeRouterEnv(t *testing.T, ctx context.Context, extra map[string]string) *azRouterEnv {
	t.Helper()
	useKeyStates(t, staticKeyStates(nil))
	dir := t.TempDir()
	writeSigningKey(t, dir, "az.pem")
	caller := session.WirePrincipalID(uuid.New())
	settings := map[string]string{
		authconfig.EnvSigningKeyFile:          filepath.Join(dir, "az.pem"),
		authconfig.EnvSigningKeyID:            "az",
		authconfig.EnvAccessTokenClass:        authconfig.AccessClassEdDSA,
		authconfig.EnvAccessTokenAudience:     contracts.PlatformAPIAudience,
		authconfig.EnvTokenIssuerURL:          azIssuer,
		authconfig.EnvAuthorizeCallerSubjects: caller,
		authconfig.EnvRateLimitPerSecond:      "100000",
		authconfig.EnvRateLimitBurst:          "100000",
	}
	for name, value := range extra {
		settings[name] = value
	}
	e := newRoutesEnvWith(t, ctx, settings)
	return &azRouterEnv{routesEnv: e, signer: token.NewFileSigner(token.FileConfig{KeyPath: filepath.Join(dir, "az.pem"), KeyID: "az"}),
		caller: caller}
}

func (a *azRouterEnv) sign(t *testing.T, typ string, claims any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	compact, _, err := a.signer.Sign(context.Background(), typ, payload)
	if err != nil {
		t.Fatal(err)
	}
	return compact
}

// callerToken is the policy-enforcement point's workload token.
func (a *azRouterEnv) callerToken(t *testing.T) string {
	t.Helper()
	now := time.Now().Unix()
	return a.sign(t, contracts.WorkloadTokenTyp, contracts.WorkloadTokenClaims{Iss: azIssuer, Aud: "auth-service", Sub: a.caller,
		PrincipalType: "workload", Jti: uuid.NewString(), Iat: now, Nbf: now, Exp: now + 300})
}

// reference is the end user's access-token.v1 for the platform API, naming
// the session sid: the principal_reference of a request.
func (a *azRouterEnv) reference(t *testing.T, principal, sid uuid.UUID) string {
	t.Helper()
	scope, ok := contracts.AccessTokenScope(contracts.PlatformAPIAudience)
	if !ok {
		t.Fatal("the platform API audience has no access token scope")
	}
	now := time.Now().Unix()
	return a.sign(t, "at+jwt", contracts.AccessTokenClaims{Iss: azIssuer, Aud: contracts.PlatformAPIAudience,
		Sub: session.WirePrincipalID(principal), PrincipalType: "human", Sid: session.WireSessionID(sid), Jti: uuid.NewString(),
		Iat: now, Nbf: now, Exp: now + 600, Scope: scope})
}

func (a *azRouterEnv) authorize(t *testing.T, body []byte) answer {
	t.Helper()
	return a.do(t, http.MethodPost, authapi.PatternAuthorize, string(body), a.callerToken(t))
}

func (a *azRouterEnv) authorizeBatch(t *testing.T, body []byte) answer {
	t.Helper()
	return a.do(t, http.MethodPost, authapi.PatternAuthorizeBatch, string(body), a.callerToken(t))
}

// armed writes a guarded security table the way a test seed may: in an admin
// transaction with the triggers off (session_replication_role replica), as the
// decisionstate fixtures do.
func (a *azRouterEnv) armed(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	tx, err := a.admin.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL session_replication_role = replica`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit %s: %v", sql, err)
	}
}

func (a *azRouterEnv) exec(t *testing.T, ctx context.Context, sql string, args ...any) {
	t.Helper()
	if _, err := a.admin.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

// azUser seeds go-api's users row (active) and projects its principal.
func (a *azRouterEnv) azUser(t *testing.T, ctx context.Context, superuser bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	a.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, $3, 0, now(), now())`, id, "az-"+id.String()[:8]+"@example.test", superuser)
	if err := a.identity.Project(ctx, id); err != nil {
		t.Fatalf("project the principal: %v", err)
	}
	return id
}

// azOrg seeds go-api's organization row; projected, it is also auth's.
func (a *azRouterEnv) azOrg(t *testing.T, ctx context.Context, projected bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	a.exec(t, ctx, `INSERT INTO public.organizations (id, slug, name) VALUES ($1, $2, 'az-org')`, id, "az-"+id.String()[:8])
	if projected {
		if err := a.identity.ProjectOrganization(ctx, id); err != nil {
			t.Fatalf("project the organization: %v", err)
		}
	}
	return id
}

// azSession starts a session of principal and returns its principal reference.
func (a *azRouterEnv) azSession(t *testing.T, ctx context.Context, principal uuid.UUID) string {
	t.Helper()
	issued, err := a.sessions.Create(ctx, principal, nil, []string{"pwd"})
	if err != nil {
		t.Fatalf("start a session: %v", err)
	}
	return a.reference(t, principal, issued.Session.ID)
}
