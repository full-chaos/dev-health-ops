// Package authplane serves auth-service's compatibility routes (go-api's refresh and logout, the session issue route)
// in process, as auth-service wires them (authruntime compatRoutes), over the auth schema of a test database, and
// returns go-api's real session issue client for it, presenting a go-api workload token. A venue rig gives go-api this
// plane as AUTH_PROXY_SESSIONS=true gives it auth-service in production: every token pair comes from the issue route,
// every refresh is forwarded to it.
package authplane

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/edtokens"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// IssuerURL is the iss of the access tokens the plane issues (AUTH_TOKEN_ISSUER_URL).
const IssuerURL = "https://auth.venue.example.test"

const (
	keyID         = "venue-auth-1"
	workloadIss   = "https://auth.workload.venue.example.test"
	workloadKeyID = "venue-issue-workload"
)

// Key is the plane's Ed25519 access token key, in a mode 0600 PEM file under the test's temp dir. Tokens mints
// under the same key, issuer and key id, so a go-api that verifies Tokens' tokens verifies the plane's too.
type Key struct {
	Tokens *edtokens.Issuer
	path   string
}

// NewKey is a fresh Key.
func NewKey(t testing.TB) Key {
	t.Helper()
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "access.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	tokens, err := edtokens.FromKey(private, keyID, IssuerURL)
	if err != nil {
		t.Fatal(err)
	}
	return Key{Tokens: tokens, path: path}
}

// Plane is a running plane: its base URL and go-api's issue client for it.
type Plane struct {
	URL    *url.URL
	Issuer *sessionissue.Client
}

// Schema is the auth schema the plane serves over: the api's default (AUTH_DATABASE_SCHEMA unset).
const Schema = "auth"

// Start serves the plane over the database at adminURI, whose auth schema (Schema) is already applied, signing access tokens
// with key. jwtKey is auth-service's JWT_SECRET_KEY. It first projects every public.users row into the auth schema with
// auth-service's projector (the users-backfill state of a deployed auth-service), so a principal written later for a
// seeded session (venueoracle.WriteAuthSessions) is already a whole user.
func Start(t testing.TB, ctx context.Context, adminURI string, key Key, jwtKey string) Plane {
	t.Helper()
	pool, err := pgxpool.New(ctx, adminURI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier(Schema)
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sessions, err := session.New(session.Config{Pool: pool, Schema: schema, Logger: logger,
		SuccessorKey: contracts.NewSecretString(random(t))})
	if err != nil {
		t.Fatal(err)
	}
	projector, err := identity.New(identity.Config{Pool: pool, Schema: schema, Logger: logger})
	if err != nil {
		t.Fatal(err)
	}
	projectUsers(t, ctx, pool, projector)
	issuer, err := authapi.NewEdDSAIssuer(jwtKey, "dev-health-ops", edtokens.Audience,
		token.NewFileSigner(token.FileConfig{KeyPath: key.path, KeyID: keyID}), IssuerURL)
	if err != nil {
		t.Fatal(err)
	}
	limits, err := authapi.NewLimits(httpapi.NewMemoryCounters(nil), random(t))
	if err != nil {
		t.Fatal(err)
	}
	workload := newWorkloadKey(t)
	issueWorkload, err := authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{IssuerURL: workloadIss, Keys: workload,
		Subject: workload.subject})
	if err != nil {
		t.Fatal(err)
	}
	routes, err := authapi.CompatRoutes(authapi.CompatDeps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: pool}, Issuer: issuer, Logger: logger,
		Projector: projector, IssueWorkload: issueWorkload,
		AppendAudit: func(ctx context.Context, event audit.AuditEvent) error { return audit.Append(ctx, pool, schema, event) },
		Limits:      limits,
	})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	// The audit rows of the issue route name the workload principal as their actor.
	if _, err := pool.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', 'go-api-venue')`,
		workload.principal.String()); err != nil {
		t.Fatal(err)
	}
	base, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := sessionissue.NewWorkload(base, workload.client(t), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return Plane{URL: base, Issuer: client}
}

// projectUsers projects every public.users row with projector.
func projectUsers(t testing.TB, ctx context.Context, pool *pgxpool.Pool, projector *identity.Store) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT id FROM public.users ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	ids, err := pgx.CollectRows(rows, pgx.RowTo[uuid.UUID])
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		if err := projector.Project(ctx, id); err != nil {
			t.Fatalf("authplane: project user %s: %v", id, err)
		}
	}
}

func random(t testing.TB) string {
	t.Helper()
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// workloadKey is go-api's workload principal and its Ed25519 key: the issue route's key set and subject, and the
// signer of the workload tokens go-api's client exchanges for.
type workloadKey struct {
	principal uuid.UUID
	subject   string
	private   ed25519.PrivateKey
	set       token.KeySet
}

func newWorkloadKey(t testing.TB) *workloadKey {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: workloadKeyID, State: token.StateActive, Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		t.Fatal(err)
	}
	principal := uuid.New()
	return &workloadKey{principal: principal, subject: session.WirePrincipalID(principal), private: private, set: set}
}

// KeySet is the published key set (an authapi.KeySetSource).
func (k *workloadKey) KeySet(context.Context) (token.KeySet, error) { return k.set, nil }

// mint is a fresh workload token for auth-service, valid 300 s.
func (k *workloadKey) mint() (string, error) {
	now := time.Now()
	claims, err := json.Marshal(contracts.WorkloadTokenClaims{
		Iss: workloadIss, Aud: "auth-service", Sub: k.subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
	})
	if err != nil {
		return "", err
	}
	header, err := json.Marshal(map[string]string{"alg": token.Algorithm, "kid": workloadKeyID, "typ": contracts.WorkloadTokenTyp})
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	return input + "." + enc.EncodeToString(ed25519.Sign(k.private, []byte(input))), nil
}

// client is go-api's workload token client over a bootstrap file; its exchange answers a fresh mint every time.
func (k *workloadKey) client(t testing.TB) *workloadtoken.Client {
	t.Helper()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		minted, err := k.mint()
		if err != nil {
			http.Error(w, "mint", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": minted, "issued_token_type": workloadtoken.TokenTypeJWT,
			"token_type": "Bearer", "expires_in": 300})
	}))
	t.Cleanup(exchange.Close)
	path := filepath.Join(t.TempDir(), "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+random(t)), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: workloadIss,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
