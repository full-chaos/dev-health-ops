package apiservice

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/edgetoken"
)

// recordingPG is a PostgreSQL wire server that answers BEGIN and refuses every other statement, keeping the text of each.
type recordingPG struct {
	mu   sync.Mutex
	sqls []string
}

func (r *recordingPG) all() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return strings.Join(r.sqls, "\n")
}

func startRecordingPG(t *testing.T) (*recordingPG, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	rec := &recordingPG{}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go rec.serve(conn)
		}
	}()
	return rec, listener.Addr().String()
}

func (r *recordingPG) serve(conn net.Conn) {
	defer conn.Close()
	backend := pgproto3.NewBackend(conn, conn)
	if _, err := backend.ReceiveStartupMessage(); err != nil {
		return
	}
	backend.Send(&pgproto3.AuthenticationOk{})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if backend.Flush() != nil {
		return
	}
	refuse := func(sql string) {
		r.mu.Lock()
		r.sqls = append(r.sqls, sql)
		r.mu.Unlock()
		backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "recording server"})
	}
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			if strings.EqualFold(strings.TrimSpace(m.String), "begin") {
				backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("BEGIN")})
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'T'})
			} else {
				refuse(m.String)
				backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'E'})
			}
		case *pgproto3.Parse:
			refuse(m.Query)
		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'E'})
		case *pgproto3.Terminate:
			return
		}
		if backend.Flush() != nil {
			return
		}
	}
}

// With the platform roles switch on, a route Routes serves reads is_superuser from auth.platform_role_assignments;
// off, it reads the public column. The login route's user read is the statement.
func TestARoutedUsersReadFollowsThePlatformRolesSwitch(t *testing.T) {
	for _, c := range []struct {
		owned bool
		want  string
	}{{true, `"auth_x".platform_role_assignments`}, {false, ""}} {
		rec, addr := startRecordingPG(t)
		pool, err := pgxpool.New(context.Background(), "postgres://u:p@"+addr+"/db?sslmode=disable&connect_timeout=2")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pool.Close)
		verifier, err := edgetoken.New(probeKey, "dev-health-ops", "dev-health-api")
		if err != nil {
			t.Fatal(err)
		}
		signer, err := edgetoken.NewSigner(probeKey, "dev-health-ops", "dev-health-api")
		if err != nil {
			t.Fatal(err)
		}
		auth, err := policy.NewAuthenticator(verifier, newProbeStore(), quietLogger())
		if err != nil {
			t.Fatal(err)
		}
		deps := Deps{Pool: pool, Auth: auth, Guard: policy.NewGuard(auth, quietLogger()), Verifier: verifier, Signer: signer,
			PlatformRolesOwned: c.owned, AuthSchema: "auth_x"}
		server, err := NewServer(apiTestConfig(), quietLogger(), Routes(deps, quietLogger()))
		if err != nil {
			t.Fatal(err)
		}
		request := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", strings.NewReader(`{"email":"a@example.com","password":"pw"}`))
		request.Header.Set("Content-Type", "application/json")
		server.Handler().ServeHTTP(httptest.NewRecorder(), request)

		sqls := rec.all()
		if !strings.Contains(sqls, "FROM users WHERE lower(email)") {
			t.Fatalf("owned %v: the login route sent no users read: %q", c.owned, sqls)
		}
		if got := strings.Contains(sqls, "platform_role_assignments"); got != c.owned {
			t.Errorf("owned %v: users read names platform_role_assignments = %v: %q", c.owned, got, sqls)
		}
		if c.want != "" && !strings.Contains(sqls, c.want) {
			t.Errorf("owned %v: users read does not use schema auth_x: %q", c.owned, sqls)
		}
	}
}
