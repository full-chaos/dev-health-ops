// Package issueworkload is the go-api workload path of auth-service's session issue routes, for tests on both sides:
// one in-memory Ed25519 key and one workload principal give auth-service its key set and subject, and give go-api a
// workloadtoken.Client whose exchange answers a token that key signed.
package issueworkload

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// KeyID is the kid of the one key.
const KeyID = "issue-workload-test"

// Plane is one key and one go-api workload principal.
type Plane struct {
	// Issuer is the iss every token carries (auth-service's AUTH_TOKEN_ISSUER_URL).
	Issuer string
	// Principal is the workload principal; Subject is its session.v1 wire form (AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT).
	Principal uuid.UUID
	Subject   string
	private   ed25519.PrivateKey
	set       token.KeySet
}

// New makes a Plane for issuer, an https URL.
func New(issuer string) (*Plane, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	set, err := token.NewKeySet([]token.Key{{KeyID: KeyID, State: token.StateActive, Algorithm: token.Algorithm, PublicKey: public}})
	if err != nil {
		return nil, err
	}
	principal := uuid.New()
	return &Plane{Issuer: issuer, Principal: principal, Subject: session.WirePrincipalID(principal), private: private, set: set}, nil
}

// KeySet is the published key set (an authapi.KeySetSource).
func (p *Plane) KeySet(context.Context) (token.KeySet, error) { return p.set, nil }

// Token is a fresh go-api workload token for auth-service: audience auth-service, the Plane's subject, valid 300 s.
func (p *Plane) Token() (string, error) {
	now := time.Now()
	claims, err := json.Marshal(contracts.WorkloadTokenClaims{
		Iss: p.Issuer, Aud: "auth-service", Sub: p.Subject, PrincipalType: "workload", Jti: uuid.NewString(),
		Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
	})
	if err != nil {
		return "", err
	}
	header, err := json.Marshal(map[string]string{"alg": token.Algorithm, "kid": KeyID, "typ": contracts.WorkloadTokenTyp})
	if err != nil {
		return "", err
	}
	enc := base64.RawURLEncoding
	input := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	return input + "." + enc.EncodeToString(ed25519.Sign(p.private, []byte(input))), nil
}

// Executor runs one statement (a pgx pool or transaction).
type Executor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Seed writes the workload principal to auth.principals: audit rows name it as their actor.
func (p *Plane) Seed(ctx context.Context, db Executor) error {
	if _, err := db.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', 'go-api-test')`, p.Principal.String()); err != nil {
		return fmt.Errorf("issueworkload: seed the workload principal: %w", err)
	}
	return nil
}

// Client is go-api's workload token client over a bootstrap credential file in dir, exchanging at a local server
// that answers a fresh Token on every exchange. stop closes that server.
func (p *Plane) Client(dir string) (client *workloadtoken.Client, stop func(), err error) {
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		minted, err := p.Token()
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
	client, err = workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: p.Issuer,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		exchange.Close()
		return nil, nil, err
	}
	return client, exchange.Close, nil
}
