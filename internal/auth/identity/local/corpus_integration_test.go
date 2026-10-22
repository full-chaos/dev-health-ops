//go:build integration

package local

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/sessionscenario"
)

// loginHandler is the test-only POST /api/v1/auth/login: it maps the typed
// Outcome to go-api's answer for the credential-decision rows only. Token
// bodies, organization selection and the 422 validation rows belong to the
// route layer (C1/D1).
func loginHandler(svc *Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Email    string `json:"email"`
			Password string `json:"password"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad body", http.StatusUnprocessableEntity)
			return
		}
		out, err := svc.Login(r.Context(), Credentials{Email: in.Email, Password: in.Password})
		if err != nil {
			http.Error(w, "error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("content-type", "application/json")
		switch out.Kind {
		case KindLocked:
			w.WriteHeader(http.StatusTooManyRequests)
			fmt.Fprintf(w, `{"detail":{"message":"Too many failed login attempts. Please try again later.","retry_after_seconds":%d}}`, int64(out.RetryAfter/time.Second))
		case KindInvalidCredentials, KindDisabled, KindPasswordUnavailable:
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, `{"detail":{"message":"Invalid credentials"}}`)
		case KindEmailUnverified:
			normalized, _ := identity.NormalizeEmail(in.Email)
			email, _ := json.Marshal(normalized)
			fmt.Fprintf(w, `{"status":"email_verification_required","email":%s,"message":"Please verify your email address before logging in"}`, email)
		default:
			fmt.Fprint(w, `{"access_token":"<not compared>"}`)
		}
	})
}

// TestCorpusLoginDecisionRows replays the credential-decision login rows of
// the A3 corpus (sessionscenario, the frozen-Python answers) against the local
// login: status and body for every refusal, status alone where the answer
// carries tokens.
func TestCorpusLoginDecisionRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	admin, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	pgschema.Apply(ctx, t, admin)
	role, err := containers.RoleName("local_corpus_runtime", instance)
	if err != nil {
		t.Fatal(err)
	}
	const rolePassword = "local-corpus-test-only-not-a-credential"
	if _, err := admin.Exec(ctx, fmt.Sprintf(`CREATE ROLE %q LOGIN PASSWORD '%s'`, role, rolePassword)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })
	if _, err := authschema.Apply(ctx, admin, authschema.Options{Schema: "auth", RuntimeRole: role, UsersOwned: true}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	cfg := admin.Config().Copy()
	cfg.ConnConfig.User, cfg.ConnConfig.Password = role, rolePassword
	runtime, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(sessionscenario.Password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	sessionscenario.Seed(t, ctx, admin, string(hash))

	sessions, err := session.New(session.Config{Pool: runtime, Schema: schema,
		SuccessorKey: contracts.NewSecretString(testKey)})
	if err != nil {
		t.Fatal(err)
	}
	ids, err := identity.New(identity.Config{Pool: admin, Schema: schema})
	if err != nil {
		t.Fatal(err)
	}
	svc, err := New(Config{
		Users: IdentityUsers{Store: ids}, Sessions: sessions, Attempts: NewPGAttempts(runtime, schema),
		LockoutKey: contracts.NewSecretString(testKey),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(loginHandler(svc))
	t.Cleanup(server.Close)

	golden := sessionscenario.LoadGolden(t)
	answers := map[string]sessionscenario.RecordedAnswer{}
	for _, a := range golden.Answers {
		answers[a.Name] = a
	}

	type row struct{ name, email, password string }
	pw := sessionscenario.Password
	rows := []row{
		{"login: alice", "alice@example.com", pw},
		{"login: bob superuser", "bob@example.com", pw},
		{"login: carol unverified", "carol@example.com", pw},
		{"login: dave inactive", "dave@example.com", pw},
		{"login: erin no password", "erin@example.com", pw},
		{"login: frank github no password", "frank@example.com", pw},
		{"login: heidi null provider unverified", "heidi@example.com", pw},
		{"login: unknown user", "nobody@example.com", pw},
		{"login: alice wrong password", "alice@example.com", "wrong-password"},
		{"login: password over 72 bytes", "alice@example.com", strings.Repeat("p", 100)},
		{"login: judy", "judy@example.com", pw},
		{"login: karl unverified, no membership, an existing org", "karl@example.com", pw},
	}
	for i := 1; i <= 5; i++ {
		rows = append(rows, row{fmt.Sprintf("lockout: wrong password %d", i), "grace@example.com", "wrong"})
	}
	rows = append(rows,
		row{"lockout: wrong password 6", "grace@example.com", "wrong"},
		row{"lockout: right password while locked", "GRACE@example.com", pw})
	for i := 1; i <= 5; i++ {
		rows = append(rows, row{fmt.Sprintf("lockout: unknown user wrong %d", i), "mallory@example.com", "wrong"})
	}

	for _, r := range rows {
		want, ok := answers[r.name]
		if !ok {
			t.Fatalf("the corpus has no row %q", r.name)
		}
		body, _ := json.Marshal(map[string]string{"email": r.email, "password": r.password})
		resp, err := http.Post(server.URL, "application/json", strings.NewReader(string(body)))
		if err != nil {
			t.Fatalf("%s: %v", r.name, err)
		}
		var got strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := resp.Body.Read(buf)
			got.Write(buf[:n])
			if err != nil {
				break
			}
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want.Status {
			t.Errorf("%s: status %d, corpus %d", r.name, resp.StatusCode, want.Status)
			continue
		}
		if want.Status != http.StatusOK || strings.Contains(want.Body, "email_verification_required") {
			if g := sessionscenario.Normalize(got.String()); g != want.Body {
				t.Errorf("%s: body %s, corpus %s", r.name, g, want.Body)
			}
		}
	}
}
