//go:build integration

package lockorder

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

func randomText(n int) (string, error) {
	raw := make([]byte, n)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// startAuthPlane serves auth-service's compatibility routes (go-api's refresh,
// the session issue route) as auth-service wires them (authruntime
// compatRoutes), over the auth stores on the runtime role.
func (e *env) startAuthPlane(ctx context.Context, schema authschema.ValidatedIdentifier, logger *slog.Logger, jwtKey string) error {
	plane, err := newIssuePlane("https://auth.lockorder.example.test")
	if err != nil {
		return err
	}
	e.issuePlane = plane
	server, err := e.authPlaneOn(e.runtime, schema, logger, jwtKey)
	if err != nil {
		return err
	}
	e.authServer = server
	// The same routes on a one-connection pool (AUTH_DATABASE_MAX_CONNS=1 is a
	// valid setting): a refresh must never need a second connection while its
	// exchange transaction holds the only one.
	cfg := e.runtime.Config().Copy()
	cfg.MaxConns = 1
	if e.runtimeOne, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
		return fmt.Errorf("one-connection pool: %w", err)
	}
	closers = append(closers, e.runtimeOne.Close)
	e.authServerOne, err = e.authPlaneOn(e.runtimeOne, schema, logger, jwtKey)
	return err
}

func (e *env) authPlaneOn(pool *pgxpool.Pool, schema authschema.ValidatedIdentifier, logger *slog.Logger, jwtKey string) (*httptest.Server, error) {
	successor, err := randomText(32)
	if err != nil {
		return nil, err
	}
	sessions, err := session.New(session.Config{Pool: pool, Schema: schema, Logger: logger, UsersOwned: true,
		SuccessorKey: contracts.NewSecretString(successor)})
	if err != nil {
		return nil, fmt.Errorf("session store: %w", err)
	}
	keyDir, err := os.MkdirTemp("", "issuer-key-")
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() { _ = os.RemoveAll(keyDir) })
	_, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKCS8PrivateKey(private)
	if err != nil {
		return nil, err
	}
	keyPath := filepath.Join(keyDir, "k1.pem")
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), 0o600); err != nil {
		return nil, err
	}
	issuer, err := authapi.NewEdDSAIssuer("dev-health-api", token.NewFileSigner(token.FileConfig{KeyPath: keyPath, KeyID: "k1"}), "https://auth.example.test")
	if err != nil {
		return nil, fmt.Errorf("issuer: %w", err)
	}
	limitKey, err := randomText(32)
	if err != nil {
		return nil, err
	}
	limits, err := authapi.NewLimits(httpapi.NewMemoryCounters(nil), limitKey)
	if err != nil {
		return nil, fmt.Errorf("limits: %w", err)
	}
	runtime := pool
	workload, err := e.issuePlane.workload()
	if err != nil {
		return nil, fmt.Errorf("session issue workload: %w", err)
	}
	routes, err := authapi.CompatRoutes(authapi.CompatDeps{
		Sessions: sessions, Users: authapi.PGUsers{Pool: runtime}, Issuer: issuer, Logger: logger,
		Projector: e.identity, IssueWorkload: workload,
		AppendAudit: func(ctx context.Context, event audit.AuditEvent) error {
			return audit.Append(ctx, runtime, schema, event)
		},
		Limits: limits,
	})
	if err != nil {
		return nil, fmt.Errorf("compat routes: %w", err)
	}
	v1, err := authapi.Routes(authapi.Deps{Sessions: sessions, Users: authapi.PGUsers{Pool: runtime}, Issuer: issuer, Logger: logger})
	if err != nil {
		return nil, fmt.Errorf("v1 routes: %w", err)
	}
	routes = append(routes, v1...)
	mux := http.NewServeMux()
	for _, route := range routes {
		mux.Handle(route.Method+" "+route.Pattern, route.Handler)
	}
	server := httptest.NewServer(mux)
	closers = append(closers, server.Close)
	return server, nil
}

// seedIssueClient stores go-api's workload principal and returns go-api's
// issue client for authServer, which presents the go-api workload token.
func (e *env) seedIssueClient(ctx context.Context) (*sessionissue.Client, error) {
	if err := e.issuePlane.seed(ctx, e.admin); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "lockorder-issue-")
	if err != nil {
		return nil, err
	}
	closers = append(closers, func() { _ = os.RemoveAll(dir) })
	workload, stop, err := e.issuePlane.client(dir)
	if err != nil {
		return nil, err
	}
	closers = append(closers, stop)
	base, err := url.Parse(e.authServer.URL)
	if err != nil {
		return nil, err
	}
	one, err := url.Parse(e.authServerOne.URL)
	if err != nil {
		return nil, err
	}
	if e.issueOne, err = sessionissue.NewWorkload(one, workload, 5*time.Second); err != nil {
		return nil, err
	}
	return sessionissue.NewWorkload(base, workload, 5*time.Second)
}

// issuePlane is go-api's workload principal and one in-memory Ed25519 key: auth-service's key set and subject for
// the session issue route, and go-api's workload token client whose exchange answers a token that key signed.
// Kept local (no shared test API); it folds into internal/testsupport/workloadclient once CHAOS-12782 lands.
type issuePlane struct {
	issuer    string
	principal uuid.UUID
	subject   string
	private   ed25519.PrivateKey
	set       token.KeySet
}

const issuePlaneKeyID = "issue-workload-test"

func newIssuePlane(issuer string) (*issuePlane, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: issuePlaneKeyID, State: token.StateActive, Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		return nil, err
	}
	principal := uuid.New()
	return &issuePlane{issuer: issuer, principal: principal, subject: session.WirePrincipalID(principal), private: private, set: set}, nil
}

// KeySet is the published key set (an authapi.KeySetSource).
func (p *issuePlane) KeySet(context.Context) (token.KeySet, error) { return p.set, nil }

// workload is the session issue route's workload path over the plane's key and subject.
func (p *issuePlane) workload() (*authapi.SessionIssueWorkload, error) {
	return authapi.NewSessionIssueWorkload(authapi.SessionIssueWorkloadConfig{IssuerURL: p.issuer, Keys: p, Subject: p.subject})
}

// mint is a fresh go-api workload token for auth-service, valid 300 s.
func (p *issuePlane) mint() (string, error) {
	now := time.Now()
	claims, err := json.Marshal(contracts.WorkloadTokenClaims{
		Iss: p.issuer, Aud: "auth-service", Sub: p.subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
	})
	if err != nil {
		return "", err
	}
	header, err := json.Marshal(map[string]string{"alg": token.Algorithm, "kid": issuePlaneKeyID, "typ": contracts.WorkloadTokenTyp})
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	return input + "." + enc.EncodeToString(ed25519.Sign(p.private, []byte(input))), nil
}

// seed writes the workload principal to auth.principals: the audit rows name it as their actor.
func (p *issuePlane) seed(ctx context.Context, db *pgxpool.Pool) error {
	_, err := db.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', 'go-api-test')`, p.principal.String())
	return err
}

// client is go-api's workload token client over a bootstrap file in dir; its exchange (a local server, stopped by
// stop) answers a fresh mint on every exchange.
func (p *issuePlane) client(dir string) (client *workloadtoken.Client, stop func(), err error) {
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		minted, err := p.mint()
		if err != nil {
			http.Error(w, "mint", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": minted, "issued_token_type": workloadtoken.TokenTypeJWT, "token_type": "Bearer", "expires_in": 300})
	}))
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		exchange.Close()
		return nil, nil, err
	}
	path := filepath.Join(dir, "bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+base64.RawURLEncoding.EncodeToString(random)), 0o600); err != nil {
		exchange.Close()
		return nil, nil, err
	}
	client, err = workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: p.issuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		exchange.Close()
		return nil, nil, err
	}
	return client, exchange.Close, nil
}
