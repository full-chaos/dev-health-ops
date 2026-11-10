package session

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
)

// socialHolder is the one public.users row of socialHolderServer.
type socialHolder struct {
	id                           uuid.UUID
	email, authProvider, subject string
	superuser                    bool
}

// socialHolderServer is a PostgreSQL wire server over one users row and no memberships. It answers the statements
// of a social sign-in by the literal values the simple protocol puts in the text, and records every statement.
type socialHolderServer struct {
	user       socialHolder
	mu         sync.Mutex
	statements []string
}

func (s *socialHolderServer) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.statements...)
}

func newSocialHolderServer(t *testing.T, user socialHolder) (*socialHolderServer, *pgxpool.Pool) {
	t.Helper()
	server := &socialHolderServer{user: user}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		connections.Wait()
	})
	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer connection.Close()
				server.serve(connection)
			}()
		}
	}()
	config, err := pgxpool.ParseConfig("postgres://holder@" + listener.Addr().String() + "/holder?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return server, pool
}

func socialHolderColumn(name string, oid uint32) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: -1, TypeModifier: -1}
}

func (s *socialHolderServer) serve(connection net.Conn) {
	backend := pgproto3.NewBackend(connection, connection)
	message, err := backend.ReceiveStartupMessage()
	if _, ok := message.(*pgproto3.StartupMessage); err != nil || !ok {
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: 1, SecretKey: []byte{0, 0, 0, 1}})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	status := byte('I')
	u := s.user
	for {
		message, err := backend.Receive()
		if err != nil {
			return
		}
		query, ok := message.(*pgproto3.Query)
		if !ok {
			return
		}
		sql := strings.ToLower(strings.Join(strings.Fields(query.String), " "))
		s.mu.Lock()
		s.statements = append(s.statements, sql)
		s.mu.Unlock()
		userRowMatch := false
		switch {
		case sql == "begin":
			status = 'T'
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
		case sql == "commit":
			status = 'I'
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("COMMIT")})
		case sql == "rollback":
			status = 'I'
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("ROLLBACK")})
		case strings.HasPrefix(sql, "select id, email, username, full_name, password_hash,"):
			switch {
			case strings.Contains(sql, "where lower(email) ="):
				userRowMatch = strings.Contains(sql, "'"+strings.ToLower(u.email)+"'")
			case strings.Contains(sql, "where u.auth_provider_id ="):
				userRowMatch = u.subject != "" && strings.Contains(sql, "auth_provider_id = '"+u.subject+"'") &&
					strings.Contains(sql, "auth_provider = '"+u.authProvider+"'")
			}
			fields := []pgproto3.FieldDescription{socialHolderColumn("id", 2950), socialHolderColumn("email", 25),
				socialHolderColumn("username", 25), socialHolderColumn("full_name", 25), socialHolderColumn("password_hash", 25),
				socialHolderColumn("auth_provider", 25), socialHolderColumn("auth_provider_id", 25), socialHolderColumn("is_active", 16),
				socialHolderColumn("is_verified", 16), socialHolderColumn("is_superuser", 16), socialHolderColumn("token_version", 20)}
			backend.Send(&pgproto3.RowDescription{Fields: fields})
			n := 0
			if userRowMatch {
				superuser := "f"
				if u.superuser {
					superuser = "t"
				}
				var subject []byte
				if u.subject != "" {
					subject = []byte(u.subject)
				}
				backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(u.id.String()), []byte(u.email), nil, nil, nil,
					[]byte(u.authProvider), subject, []byte("t"), []byte("t"), []byte(superuser), []byte("0")}})
				n = 1
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(n))})
		case strings.HasPrefix(sql, "select org_id, role, joined_at, created_at from memberships"):
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{socialHolderColumn("org_id", 2950),
				socialHolderColumn("role", 25), socialHolderColumn("joined_at", 1184), socialHolderColumn("created_at", 1184)}})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 0")})
		case strings.HasPrefix(sql, "insert into"):
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")})
		case strings.HasPrefix(sql, "update users"):
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 1")})
		default:
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "socialHolderServer: unexpected statement"})
		}
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		if backend.Flush() != nil {
			return
		}
	}
}

// socialLoginAgainst posts a GitHub social login for GitHub account 7, whose public (so verified) address is email,
// to a go-api whose database holds user.
func socialLoginAgainst(t *testing.T, user socialHolder, email string) (int, string, *socialHolderServer) {
	t.Helper()
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/user" {
			_, _ = w.Write([]byte(`{"id": 7, "login": "u", "email": "` + email + `"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(provider.Close)
	server, pool := newSocialHolderServer(t, user)
	deps := completeDeps(t)
	deps.Pool = pool
	deps.Getenv = func(string) string { return "set" }
	deps.Now = time.Now
	deps.OAuth = &oauthprovider.Client{HTTP: provider.Client(),
		Endpoints: oauthprovider.Endpoints{GitHubUser: provider.URL + "/user", GitHubEmails: provider.URL + "/emails"}}
	var handler http.Handler
	for _, route := range Routes(deps) {
		if route.Pattern == "/api/v1/auth/social-login" {
			handler = route.Handler
		}
	}
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/social-login",
		strings.NewReader(`{"provider": "github", "provider_access_token": "x"}`))
	r.Header.Set("Content-Type", "application/json")
	handler.ServeHTTP(w, r)
	return w.Code, w.Body.String(), server
}

func socialWrites(statements []string) []string {
	var out []string
	for _, statement := range statements {
		if strings.HasPrefix(statement, "insert") || strings.HasPrefix(statement, "update") || strings.HasPrefix(statement, "delete") {
			out = append(out, statement)
		}
	}
	return out
}

// A social sign-in whose GitHub identity holds no account and whose verified email a platform admin's account holds
// is refused exactly as for any other account: the same 409 status and body, no session and no write. The email
// is never a link to an existing account.
func TestSocialLoginRefusesAnEmailAPlatformAdminHoldsAsItRefusesAnyAccount(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "false")
	answers := map[bool]string{}
	for _, superuser := range []bool{true, false} {
		holder := socialHolder{id: uuid.New(), email: "root.admin@allowed.example", authProvider: "local", superuser: superuser}
		code, body, server := socialLoginAgainst(t, holder, "Root.Admin@allowed.example")
		if code != http.StatusConflict || !strings.Contains(body, socialAccountExists) || strings.Contains(body, "access_token") {
			t.Fatalf("superuser holder=%v: %d %s, want 409 %q and no token; statements %q", superuser, code, body, socialAccountExists, server.seen())
		}
		if writes := socialWrites(server.seen()); len(writes) != 0 {
			t.Fatalf("superuser holder=%v: the refused sign-in wrote %v", superuser, writes)
		}
		answers[superuser] = strconv.Itoa(code) + " " + body
	}
	if answers[true] != answers[false] {
		t.Fatalf("platform admin holder answer %q, other holder answer %q; want one answer", answers[true], answers[false])
	}
	for _, word := range []string{"superuser", "platform"} {
		if strings.Contains(strings.ToLower(answers[true]), word) {
			t.Fatalf("the conflict answer %q names the account class (%q)", answers[true], word)
		}
	}
}

// A platform admin whose account holds the GitHub identity still signs in by it, with no organization and no
// onboarding (product rule 1: a platform admin needs no membership).
func TestSocialLoginOfAPlatformAdminByItsIdentityStillSignsIn(t *testing.T) {
	t.Setenv("AUTH_USERS_OWNED", "false")
	admin := socialHolder{id: uuid.New(), email: "root.admin@allowed.example", authProvider: "github", subject: "7", superuser: true}
	code, body, server := socialLoginAgainst(t, admin, "root.admin@allowed.example")
	if code != http.StatusOK {
		t.Fatalf("%d %s, want 200; statements %q", code, body, server.seen())
	}
	var got struct {
		NeedsOnboarding bool `json:"needs_onboarding"`
		IsNewUser       bool `json:"is_new_user"`
		User            struct {
			ID          string `json:"id"`
			IsSuperuser bool   `json:"is_superuser"`
		} `json:"user"`
	}
	if err := json.Unmarshal([]byte(body), &got); err != nil {
		t.Fatal(err)
	}
	if got.User.ID != admin.id.String() || !got.User.IsSuperuser || got.NeedsOnboarding || got.IsNewUser {
		t.Fatalf("body %s; want the platform admin %v, is_superuser true, no onboarding, not new", body, admin.id)
	}
	for _, write := range socialWrites(server.seen()) {
		if strings.HasPrefix(write, "insert") {
			t.Fatalf("the identity sign-in inserted a row: %s", write)
		}
	}
}
