package sso

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// holderUser is one public.users row of holderServer.
type holderUser struct {
	id                           uuid.UUID
	email, authProvider, subject string
	superuser                    bool
	// role is the user's membership role in the provider's organization; empty means no membership.
	role string
}

// holderServer is a PostgreSQL wire server over a few users rows. It answers the statements of an SSO sign-in by
// the literal values the simple protocol puts in the text, and records every statement.
type holderServer struct {
	users      []holderUser
	orgID      uuid.UUID
	mu         sync.Mutex
	statements []string
}

func (s *holderServer) saw(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, statement := range s.statements {
		if strings.HasPrefix(statement, prefix) {
			return true
		}
	}
	return false
}

func (s *holderServer) writes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []string
	for _, statement := range s.statements {
		if strings.HasPrefix(statement, "insert") || strings.HasPrefix(statement, "update") || strings.HasPrefix(statement, "delete") {
			out = append(out, statement)
		}
	}
	return out
}

func newHolderServer(t *testing.T, orgID uuid.UUID, users ...holderUser) (*holderServer, *pgxpool.Pool) {
	t.Helper()
	server := &holderServer{users: users, orgID: orgID}
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

func holderColumn(name string, oid uint32) pgproto3.FieldDescription {
	return pgproto3.FieldDescription{Name: []byte(name), DataTypeOID: oid, DataTypeSize: -1, TypeModifier: -1}
}

func holderText(value string) []byte {
	if value == "" {
		return nil
	}
	return []byte(value)
}

func (s *holderServer) serve(connection net.Conn) {
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
		case strings.HasPrefix(sql, "select id from users where auth_provider ="), strings.HasPrefix(sql, "select u.id from users u where u.auth_provider ="):
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{holderColumn("id", 2950)}})
			n := 0
			for _, u := range s.users {
				if strings.Contains(sql, "auth_provider = '"+u.authProvider+"'") && strings.Contains(sql, "auth_provider_id = '"+strings.ToLower(u.subject)+"'") {
					backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(u.id.String())}})
					n++
				}
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(n))})
		case strings.HasPrefix(sql, "select exists (select 1 from users where lower(email) = lower("):
			taken := "f"
			for _, u := range s.users {
				if strings.Contains(sql, "'"+strings.ToLower(u.email)+"'") {
					taken = "t"
				}
			}
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{holderColumn("exists", 16)}})
			backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(taken)}})
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
		case strings.HasPrefix(sql, "select id, email, username, full_name,"):
			avatar := strings.Contains(sql, "avatar_url")
			fields := []pgproto3.FieldDescription{holderColumn("id", 2950), holderColumn("email", 25), holderColumn("username", 25), holderColumn("full_name", 25)}
			if avatar {
				fields = append(fields, holderColumn("avatar_url", 25))
			}
			fields = append(fields, holderColumn("is_superuser", 16), holderColumn("token_version", 20))
			backend.Send(&pgproto3.RowDescription{Fields: fields})
			n := 0
			for _, u := range s.users {
				if !strings.Contains(sql, "'"+u.id.String()+"'") {
					continue
				}
				superuser := "f"
				if u.superuser {
					superuser = "t"
				}
				values := [][]byte{[]byte(u.id.String()), []byte(u.email), nil, nil}
				if avatar {
					values = append(values, nil)
				}
				values = append(values, []byte(superuser), []byte("0"))
				backend.Send(&pgproto3.DataRow{Values: values})
				n++
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(n))})
		case strings.HasPrefix(sql, "select org_id, role from memberships where user_id ="):
			backend.Send(&pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{holderColumn("org_id", 2950), holderColumn("role", 25)}})
			n := 0
			for _, u := range s.users {
				if u.role != "" && strings.Contains(sql, "'"+u.id.String()+"'") && strings.Contains(sql, "'"+s.orgID.String()+"'") {
					backend.Send(&pgproto3.DataRow{Values: [][]byte{[]byte(s.orgID.String()), holderText(u.role)}})
					n++
				}
			}
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT " + strconv.Itoa(n))})
		case strings.HasPrefix(sql, "insert into"):
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("INSERT 0 1")})
		case strings.HasPrefix(sql, "update users"):
			backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("UPDATE 1")})
		default:
			backend.Send(&pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: "holderServer: unexpected statement"})
		}
		backend.Send(&pgproto3.ReadyForQuery{TxStatus: status})
		if backend.Flush() != nil {
			return
		}
	}
}

type holderSignIn struct {
	protocol string
	run      func(h handlers, row *providerRow, email, subject string) (*provisionedUser, *provisionedMembership, error)
}

// holderSignIns are the three go-api sign-in paths that look an account up: OIDC and SAML (provisionOrGetUser,
// the ACS and the OIDC callback, JIT included) and enterprise OAuth (oauthProvisionUser).
func holderSignIns() []holderSignIn {
	provider := uuid.MustParse("6f1c2a43-8a35-4b8e-9d5a-0e5b8c3c7a10")
	return []holderSignIn{
		{"oidc", func(h handlers, row *providerRow, email, subject string) (*provisionedUser, *provisionedMembership, error) {
			return h.provisionOrGetUser(context.Background(), row, email, "", provider, subject, nil)
		}},
		{"saml", func(h handlers, row *providerRow, email, subject string) (*provisionedUser, *provisionedMembership, error) {
			return h.provisionOrGetUser(context.Background(), row, email, "", provider, subject, &samlNameIDParts{})
		}},
		{"oauth_github", func(h handlers, row *providerRow, email, subject string) (*provisionedUser, *provisionedMembership, error) {
			return h.oauthProvisionUser(context.Background(), row, "github", oauthClaims{email: email, externalID: subject})
		}},
	}
}

func holderProviderRow(protocol string, orgID uuid.UUID) *providerRow {
	return &providerRow{ID: "6f1c2a43-8a35-4b8e-9d5a-0e5b8c3c7a10", OrgID: orgID.String(), Protocol: protocol, Status: "active", AutoProvision: true}
}

func holderAuthProvider(protocol string) string {
	if protocol == "oauth_github" {
		return "github"
	}
	return protocol
}

// A sign-in whose subject holds no account and whose email a platform admin's account holds is refused as it is for
// any other account: errIdentityConflict, the one generic 409 the OIDC, SAML and OAuth callbacks write
// (identityConflictDetail), and no write. The email is never a link to an existing account, with or without users
// owned by auth-service, and auto_provision on.
func TestSSOSignInRefusesAnEmailAPlatformAdminHoldsAsItRefusesAnyAccount(t *testing.T) {
	for _, owned := range []string{"false", "true"} {
		for _, signIn := range holderSignIns() {
			answers := map[bool]error{}
			for _, superuser := range []bool{true, false} {
				t.Run(signIn.protocol+"/owned="+owned+"/superuser="+map[bool]string{true: "yes", false: "no"}[superuser], func(t *testing.T) {
					t.Setenv("AUTH_USERS_OWNED", owned)
					orgID := uuid.New()
					holder := holderUser{id: uuid.New(), email: "root.admin@allowed.example", authProvider: "local", superuser: superuser, role: "owner"}
					server, pool := newHolderServer(t, orgID, holder)
					h := handlers{Deps: Deps{Pool: pool, Now: time.Now}}
					user, membership, err := signIn.run(h, holderProviderRow(signIn.protocol, orgID), "Root.Admin@allowed.example", "new-subject-8984")
					if !errors.Is(err, errIdentityConflict) || user != nil || membership != nil {
						t.Fatalf("sign-in = %v, %v, %v; want errIdentityConflict and no account: the email of an existing account is never linked; statements %q", user, membership, err, server.statements)
					}
					if !server.saw("select exists (select 1 from users where lower(email) = lower(") {
						t.Fatal("the sign-in never asked whether the email is held: the cell did not reach the check")
					}
					if writes := server.writes(); len(writes) != 0 {
						t.Fatalf("the refused sign-in wrote %v", writes)
					}
					answers[superuser] = err
				})
			}
			if answers[true] != nil && answers[true] != answers[false] {
				t.Errorf("%s owned=%s: platform admin holder answer %v, other holder answer %v; want one answer", signIn.protocol, owned, answers[true], answers[false])
			}
		}
	}
	raw, err := identityConflictDetail().MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	detail := strings.ToLower(string(raw))
	for _, word := range []string{"superuser", "platform"} {
		if strings.Contains(detail, word) {
			t.Fatalf("the conflict answer %s names the account class (%q)", detail, word)
		}
	}
}

// A platform admin whose account holds the provider's identity key still signs in by that key, with the legacy
// sign-in write while users are not owned.
func TestSSOKeySignInOfAPlatformAdminStillSignsIn(t *testing.T) {
	for _, signIn := range holderSignIns() {
		t.Run(signIn.protocol, func(t *testing.T) {
			t.Setenv("AUTH_USERS_OWNED", "false")
			orgID := uuid.New()
			admin := holderUser{id: uuid.New(), email: "root.admin@allowed.example", authProvider: holderAuthProvider(signIn.protocol),
				subject: "admin-subject-8984", superuser: true, role: "member"}
			server, pool := newHolderServer(t, orgID, admin)
			h := handlers{Deps: Deps{Pool: pool, Now: time.Now}}
			user, membership, err := signIn.run(h, holderProviderRow(signIn.protocol, orgID), "root.admin@allowed.example", "admin-subject-8984")
			if err != nil || user == nil || membership == nil {
				t.Fatalf("key sign-in = %v, %v, %v; want the platform admin signed in", user, membership, err)
			}
			if user.id != admin.id || !user.isSuperuser || membership.orgID != orgID || membership.role != "member" {
				t.Fatalf("signed in %+v %+v; want the platform admin %v with its member membership", user, membership, admin.id)
			}
			if !server.saw("update users set") {
				t.Fatal("the key sign-in did not write the sign-in to the users row")
			}
			if server.saw("insert into") {
				t.Fatal("the key sign-in inserted a row")
			}
		})
	}
}
