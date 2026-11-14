package authplane

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-go/pep"
	"github.com/full-chaos/dev-health-go/workloadtoken"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/authruntime"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// goAPIClientService is the client_service go-api's decision point client names.
const goAPIClientService = "dev-health-ops-api"

// Authorize is a running decision point: auth-service's authorize routes, built by authruntime's production builder
// (AuthorizeRoutesWithoutFeed) over the auth schema of a test database, with the plane's access token key as the
// signer. Client is go-api's decision point client for it, presenting a workload token the routes' caller gate admits.
type Authorize struct {
	Client *pep.Client
	server *httptest.Server
	mu     sync.Mutex
	seen   []string
}

// AuthorizeOptions are the decision point's switches.
type AuthorizeOptions struct {
	// PlatformRolesOwned and UsersOwned are auth-service's AUTH_PLATFORM_ROLES_OWNED and AUTH_USERS_OWNED (the decision
	// input's users read, authapi.PGUsers, branches on both).
	PlatformRolesOwned, UsersOwned bool
	// Wrap, when set, wraps the decision state the routes read (a planted defect).
	Wrap func(authapi.DecisionState) authapi.DecisionState
}

// StartAuthorize serves the decision point over the database at adminURI, whose auth schema (Schema) is already
// applied. Tokens minted by key.Tokens are the principal references it resolves.
func StartAuthorize(t testing.TB, ctx context.Context, adminURI string, key Key, opts AuthorizeOptions) *Authorize {
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
	signer := token.NewFileSigner(token.FileConfig{KeyPath: key.path, KeyID: keyID})
	caller := uuid.New()
	subject := session.WirePrincipalID(caller)
	// The audit rows of a high-risk decision name the caller workload.
	if _, err := pool.Exec(ctx, `INSERT INTO auth.principals (id, kind, display_name) VALUES ($1, 'workload', 'go-api-authorize')`,
		caller.String()); err != nil {
		t.Fatal(err)
	}
	cfg := authconfig.Config{TokenIssuerURL: IssuerURL, AuthorizeCallerSubjects: []string{subject},
		PlatformRolesOwned: opts.PlatformRolesOwned, UsersOwned: opts.UsersOwned}
	routes, err := authruntime.AuthorizeRoutesWithoutFeed(cfg, slog.New(slog.DiscardHandler), pool, schema, signer, opts.Wrap)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) == 0 {
		t.Fatal("authplane: the production builder mounted no authorize route")
	}
	// Served as auth-service serves its routes (the authorize handler fixture's server options).
	served, err := httpapi.NewServer(httpapi.ServerOptions{Address: "127.0.0.1:0", Logger: slog.New(slog.DiscardHandler), Routes: routes,
		RequestTimeout: 5 * time.Second, MaxBodyBytes: 1 << 20, RateLimit: 1000, RateLimitBurst: 1000})
	if err != nil {
		t.Fatal(err)
	}
	a := &Authorize{}
	a.server = httptest.NewServer(a.record(served.Handler()))
	t.Cleanup(a.server.Close)
	workload := authorizeWorkloadClient(t, signer, subject)
	httpClient, err := workload.HTTPClient(&http.Transport{}, a.server.URL, authprovision.WorkloadAudience)
	if err != nil {
		t.Fatal(err)
	}
	if a.Client, err = pep.New(pep.Config{BaseURL: a.server.URL, AllowPlainHTTP: true, HTTPClient: httpClient,
		ClientService: goAPIClientService, Timeout: 2 * time.Second}); err != nil {
		t.Fatal(err)
	}
	return a
}

// Stop closes the decision point: every later call fails, as an outage does.
func (a *Authorize) Stop() { a.server.Close() }

// Take returns the decisions made since the last Take, one line each: "<action> <resource type>[/<org>]
// allowed=<bool> reason=<code>", and forgets them.
func (a *Authorize) Take() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := slices.Clone(a.seen)
	a.seen = nil
	return out
}

type wireQuestion struct {
	Action   string `json:"action"`
	Resource struct {
		Type string `json:"type"`
		Org  string `json:"organization_id"`
	} `json:"resource"`
}

type wireAnswer struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason_code"`
}

// record keeps each decision of a single or batch call: the question from the request, the verdict from the answer.
func (a *Authorize) record(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(raw))
		rec := httptest.NewRecorder()
		next.ServeHTTP(rec, r)
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(rec.Body.Bytes())

		var questions []wireQuestion
		var answers []wireAnswer
		if r.URL.Path == pep.PathAuthorizeBatch {
			var in struct{ Requests []wireQuestion }
			var out struct {
				Decisions []struct {
					Index    int        `json:"index"`
					Decision wireAnswer `json:"decision"`
				}
			}
			_ = json.Unmarshal(raw, &in)
			_ = json.Unmarshal(rec.Body.Bytes(), &out)
			questions = in.Requests
			answers = make([]wireAnswer, len(questions))
			for _, d := range out.Decisions {
				if d.Index >= 0 && d.Index < len(answers) {
					answers[d.Index] = d.Decision
				}
			}
			if len(out.Decisions) != len(questions) {
				answers = nil
			}
		} else {
			var in wireQuestion
			var out wireAnswer
			_ = json.Unmarshal(raw, &in)
			_ = json.Unmarshal(rec.Body.Bytes(), &out)
			questions, answers = []wireQuestion{in}, []wireAnswer{out}
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, q := range questions {
			line := q.Action + " " + q.Resource.Type
			if q.Resource.Org != "" {
				line += "/" + q.Resource.Org
			}
			if rec.Code != http.StatusOK || i >= len(answers) {
				line += fmt.Sprintf(" status=%d", rec.Code)
			} else {
				line += fmt.Sprintf(" allowed=%v reason=%s", answers[i].Allowed, answers[i].Reason)
			}
			a.seen = append(a.seen, line)
		}
	})
}

// authorizeWorkloadClient is go-api's workload token client whose exchange answers a workload token for subject,
// signed by the plane's access key under IssuerURL: the token the authorize caller gate verifies.
func authorizeWorkloadClient(t testing.TB, signer *token.Signer, subject string) *workloadtoken.Client {
	t.Helper()
	exchange := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		claims, err := json.Marshal(contracts.WorkloadTokenClaims{
			Iss: IssuerURL, Aud: authprovision.WorkloadAudience, Sub: subject, PrincipalType: "workload", Jti: uuid.NewString(),
			Iat: now.Unix(), Nbf: now.Unix(), Exp: now.Unix() + 300,
		})
		if err != nil {
			http.Error(w, "claims", http.StatusInternalServerError)
			return
		}
		minted, _, err := signer.Sign(r.Context(), contracts.WorkloadTokenTyp, claims)
		if err != nil {
			http.Error(w, "sign", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": minted, "issued_token_type": workloadtoken.TokenTypeJWT,
			"token_type": "Bearer", "expires_in": 300})
	}))
	t.Cleanup(exchange.Close)
	path := filepath.Join(t.TempDir(), "authorize-bootstrap")
	if err := os.WriteFile(path, []byte(workloadtoken.BootstrapPrefix+random(t)), 0o600); err != nil {
		t.Fatal(err)
	}
	client, err := workloadtoken.New(workloadtoken.Config{ExchangeURL: exchange.URL, AllowPlainHTTP: true, Issuer: IssuerURL,
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: &http.Transport{}})
	if err != nil {
		t.Fatal(err)
	}
	return client
}
