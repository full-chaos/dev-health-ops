// Package fakepg is a PostgreSQL server that only refuses: it reads the startup
// message, asks for a cleartext password, and answers with an authentication
// failure whose text echoes the login and the password it was given. It is the
// server a redaction test needs: a driver's failure text carries the effective
// login (and, from a server that echoes, the password), whatever configuration
// source supplied them (the URI, PGUSER and PGPASSWORD, a service file).
package fakepg

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type firstStatement int

const (
	firstFails firstStatement = iota
	firstEmpty
	firstErrorsInResult
)

// Server is a running refusing server.
type Server struct {
	Host string
	Port int
	// acceptLogin makes the server accept the login and fail every later
	// statement with an error that echoes the login and password (a server whose
	// authentication works and whose query errors carry them).
	acceptLogin bool
	// failPing also fails the driver's ping (an empty statement) with the echo.
	failPing bool
	// first says what the first real statement gets: firstFails (the default: the
	// echo), firstEmpty (an empty two-column result set; every later statement
	// fails) or firstErrorsInResult (the result set starts, then the echo arrives
	// while the rows are read).
	first firstStatement
	// connections counts the connections that reached the server.
	connections atomic.Int64
}

// Connections is how many connections the server has accepted: a test that
// expects a login attempt asserts it is not zero, so a verb that failed before
// dialing (a malformed URI, a refusal) does not pass for a redaction.
func (s *Server) Connections() int { return int(s.connections.Load()) }

// New listens on a loopback port until the test ends. Every connection is refused
// with SQLSTATE 28P01.
func New(t testing.TB) *Server { return newServer(t, false, false, firstFails) }

func newServer(t testing.TB, acceptLogin, failPing bool, first firstStatement) *Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakepg listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := &Server{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, acceptLogin: acceptLogin, failPing: failPing, first: first}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.connections.Add(1)
			go serve(conn, acceptLogin, failPing, first)
		}
	}()
	return server
}

// Start is New for a test that needs only the address.
func Start(t testing.TB) (host string, port int) {
	t.Helper()
	server := New(t)
	return server.Host, server.Port
}

func serve(conn net.Conn, acceptLogin, failPing bool, first firstStatement) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var params map[string]string
	for {
		var header [8]byte
		if _, err := io.ReadFull(reader, header[:]); err != nil {
			return
		}
		length := int(binary.BigEndian.Uint32(header[:4]))
		code := binary.BigEndian.Uint32(header[4:])
		body := make([]byte, length-8)
		if _, err := io.ReadFull(reader, body); err != nil {
			return
		}
		if code == 80877103 || code == 80877104 { // SSLRequest, GSSENCRequest: not offered
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
			continue
		}
		params = map[string]string{}
		fields := strings.Split(string(body), "\x00")
		for index := 0; index+1 < len(fields); index += 2 {
			if fields[index] == "" {
				break
			}
			params[fields[index]] = fields[index+1]
		}
		break
	}
	// AuthenticationCleartextPassword.
	if _, err := conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 3}); err != nil {
		return
	}
	var kind [1]byte
	var size [4]byte
	if _, err := io.ReadFull(reader, kind[:]); err != nil {
		return
	}
	if _, err := io.ReadFull(reader, size[:]); err != nil {
		return
	}
	payload := make([]byte, int(binary.BigEndian.Uint32(size[:]))-4)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return
	}
	password := strings.TrimRight(string(payload), "\x00")
	if acceptLogin {
		serveAccepted(conn, reader, params["user"], password, failPing, first)
		return
	}
	writeError(conn, "FATAL", "28P01", fmt.Sprintf("password authentication failed for user %q (password %q)", params["user"], password))
}

// writeError sends an ErrorResponse.
func writeError(conn net.Conn, severity, code, message string) {
	var fieldsBody []byte
	for _, field := range [][2]string{{"S", severity}, {"V", severity}, {"C", code}, {"M", message}} {
		fieldsBody = append(fieldsBody, field[0][0])
		fieldsBody = append(fieldsBody, field[1]...)
		fieldsBody = append(fieldsBody, 0)
	}
	fieldsBody = append(fieldsBody, 0)
	out := []byte{'E', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], uint32(len(fieldsBody)+4))
	_, _ = conn.Write(append(out, fieldsBody...))
}

// Refusing is a refusing server together with a login and password that reach
// the driver only through the environment (PGUSER, PGPASSWORD), never through the
// URI the verb resolves.
type Refusing struct {
	*Server
	// URI names the server and a database and carries no credential.
	URI             string
	Login, Password string
}

// StartRefusing starts the server and sets PGUSER, PGPASSWORD and an empty
// PGSERVICEFILE for the test (t.Setenv, so the test cannot run in parallel).
func StartRefusing(t *testing.T) Refusing {
	t.Helper()
	server := New(t)
	r := Refusing{
		Server:   server,
		URI:      fmt.Sprintf("postgres://%s:%d/appdb?sslmode=disable", server.Host, server.Port),
		Login:    "env_login_zq4",
		Password: "env_password_mv7",
	}
	t.Setenv("PGUSER", r.Login)
	t.Setenv("PGPASSWORD", r.Password)
	t.Setenv("PGSERVICEFILE", "")
	return r
}

// Leaks names the credentials found in text.
func (r Refusing) Leaks(text string) []string {
	var found []string
	for _, secret := range []string{r.Login, r.Password} {
		if strings.Contains(text, secret) {
			found = append(found, secret)
		}
	}
	return found
}

// RequireConnected fails the test when no connection reached the server: the verb
// never tried to log in, so its output says nothing about credentials.
func (r Refusing) RequireConnected(t testing.TB) {
	t.Helper()
	if r.Connections() == 0 {
		t.Fatal("no connection reached the refusing server: the verb never tried to log in (the test measures nothing)")
	}
}

// serveAccepted accepts the login (AuthenticationOk, ReadyForQuery), answers the
// driver's ping (an empty query) and fails every other statement with an error that
// echoes the login and password.
func serveAccepted(conn net.Conn, reader *bufio.Reader, user, password string, failPing bool, first firstStatement) {
	if _, err := conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0, 'Z', 0, 0, 0, 5, 'I'}); err != nil {
		return
	}
	echo := fmt.Sprintf("server echo %s %s", user, password)
	failed, emptyStatement := false, false
	firstDone, current := false, ""
	passed := map[string]bool{}
	for {
		var kind [1]byte
		var size [4]byte
		if _, err := io.ReadFull(reader, kind[:]); err != nil {
			return
		}
		if _, err := io.ReadFull(reader, size[:]); err != nil {
			return
		}
		body := make([]byte, int(binary.BigEndian.Uint32(size[:]))-4)
		if _, err := io.ReadFull(reader, body); err != nil {
			return
		}
		switch kind[0] {
		case 'X':
			return
		case 'Q':
			if !failPing && emptyQuery(strings.TrimRight(string(body), "\x00")) {
				if _, err := conn.Write([]byte{'I', 0, 0, 0, 4, 'Z', 0, 0, 0, 5, 'I'}); err != nil {
					return
				}
				continue
			}
			writeError(conn, "ERROR", "XX000", echo)
			if _, err := conn.Write([]byte{'Z', 0, 0, 0, 5, 'I'}); err != nil {
				return
			}
		case 'P':
			// Parse: name\0 query\0 ...; the ping's empty statement is accepted, every
			// other statement fails once and the rest of the batch is skipped to Sync.
			parts := strings.SplitN(string(body), "\x00", 3)
			query := ""
			if len(parts) > 1 {
				query = parts[1]
			}
			emptyStatement = !failPing && emptyQuery(query)
			if !emptyStatement && first != firstFails && !firstDone {
				firstDone = true
				passed[parts[0]] = true
				if _, err := conn.Write([]byte{'1', 0, 0, 0, 4}); err != nil {
					return
				}
				continue
			}
			if emptyStatement {
				if _, err := conn.Write([]byte{'1', 0, 0, 0, 4}); err != nil {
					return
				}
			} else if !failed {
				writeError(conn, "ERROR", "XX000", echo)
				failed = true
			}
		case 'D':
			if len(body) > 0 && passed[strings.TrimRight(string(body[1:]), "\x00")] && body[0] == 'S' {
				if _, err := conn.Write(append([]byte{'t', 0, 0, 0, 6, 0, 0}, twoColumnDescription...)); err != nil {
					return
				}
			} else if len(body) > 0 && body[0] == 'P' && passed[current] {
				if _, err := conn.Write(twoColumnDescription); err != nil {
					return
				}
			} else if emptyStatement && !failed {
				// ParameterDescription (no parameters), then NoData.
				if _, err := conn.Write([]byte{'t', 0, 0, 0, 6, 0, 0, 'n', 0, 0, 0, 4}); err != nil {
					return
				}
			}
		case 'B':
			bind := strings.SplitN(string(body), "\x00", 3)
			current = ""
			if len(bind) > 1 {
				current = bind[1]
			}
			if passed[current] {
				if _, err := conn.Write([]byte{'2', 0, 0, 0, 4}); err != nil {
					return
				}
			} else if emptyStatement && !failed {
				if _, err := conn.Write([]byte{'2', 0, 0, 0, 4}); err != nil {
					return
				}
			}
		case 'E':
			if passed[current] {
				if first == firstErrorsInResult {
					writeError(conn, "ERROR", "XX000", echo)
				} else if _, err := conn.Write(append([]byte{'C', 0, 0, 0, 13}, []byte("SELECT 0\x00")...)); err != nil {
					return
				}
			} else if emptyStatement && !failed {
				if _, err := conn.Write([]byte{'I', 0, 0, 0, 4}); err != nil {
					return
				}
			}
		case 'S':
			failed, emptyStatement = false, false
			if _, err := conn.Write([]byte{'Z', 0, 0, 0, 5, 'I'}); err != nil {
				return
			}
		}
	}
}

// StartEchoing is StartRefusing for a server that accepts the login and then
// fails every statement with an error carrying the login and password.
func StartEchoing(t *testing.T) Refusing { return startEchoing(t, false, firstFails) }

// StartEchoingOnPing is StartEchoing for a server that fails the driver's ping too:
// the error origin is the ping, not a later statement.
func StartEchoingOnPing(t *testing.T) Refusing { return startEchoing(t, true, firstFails) }

// StartEchoingAfterOneStatement answers the first statement with an empty result and
// fails every later one with the echo: the error origin is the SECOND statement
// (`status`'s classification query behind its census).
func StartEchoingAfterOneStatement(t *testing.T) Refusing { return startEchoing(t, false, firstEmpty) }

// StartEchoingWhileReadingRows starts the first statement's result and then fails it
// with the echo: the error origin is reading the rows (rows.Next / rows.Err).
func StartEchoingWhileReadingRows(t *testing.T) Refusing {
	return startEchoing(t, false, firstErrorsInResult)
}

func startEchoing(t *testing.T, failPing bool, first firstStatement) Refusing {
	t.Helper()
	server := newServer(t, true, failPing, first)
	r := Refusing{
		Server:   server,
		URI:      fmt.Sprintf("postgres://%s:%d/appdb?sslmode=disable", server.Host, server.Port),
		Login:    "env_login_zq4",
		Password: "env_password_mv7",
	}
	t.Setenv("PGUSER", r.Login)
	t.Setenv("PGPASSWORD", r.Password)
	t.Setenv("PGSERVICEFILE", "")
	return r
}

// emptyQuery reports a statement that only has comments, whitespace and semicolons:
// the driver's ping ("-- ping" or ";").
func emptyQuery(text string) bool {
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		if strings.Trim(line, "; \t") != "" {
			return false
		}
	}
	return true
}

// twoColumnDescription is a RowDescription of a text and an int8 column.
var twoColumnDescription = func() []byte {
	body := []byte{0, 2}
	for _, column := range []struct {
		name   string
		oid    uint32
		length int16
	}{{"c1", 25, -1}, {"c2", 20, 8}} {
		body = append(body, column.name...)
		body = append(body, 0)
		body = binary.BigEndian.AppendUint32(body, 0)
		body = binary.BigEndian.AppendUint16(body, 0)
		body = binary.BigEndian.AppendUint32(body, column.oid)
		body = binary.BigEndian.AppendUint16(body, uint16(column.length))
		body = binary.BigEndian.AppendUint32(body, 0xffffffff)
		body = binary.BigEndian.AppendUint16(body, 0)
	}
	out := []byte{'T', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(out[1:], uint32(len(body)+4))
	return append(out, body...)
}()

// URIs are the credential-free URIs a raw pgx.Connect verb must redact the resolved
// credentials for: the plain one, and one the pool parser rejects (pool_max_conns=0)
// while pgx.Connect accepts it.
func (r Refusing) URIs() []string { return []string{r.URI, r.URI + "&pool_max_conns=0"} }

// RequireConnectedSince fails the test when no connection arrived after the count
// `before` (r.Connections() taken before the run).
func (r Refusing) RequireConnectedSince(t testing.TB, before int) {
	t.Helper()
	if r.Connections() <= before {
		t.Fatal("no connection reached the refusing server during this run: the verb never tried to log in (the test measures nothing)")
	}
}

// Form is one way of writing the connection string of the refusing server: the
// credentials a driver resolves from it (or from the environment, a service file or a
// password file) are the ones the server echoes, so they are the ones no output may
// carry.
type Form struct {
	Name string
	DSN  string
	// Env is set for the run (t.Setenv); PGUSER and PGPASSWORD are cleared first.
	Env map[string]string
	// Login and Password are what the driver dials with for this form.
	Login, Password string
	// ConnParses and PoolParses say whether pgx.ParseConfig and pgxpool.ParseConfig
	// accept the form (a form the pool parser rejects never reaches the server through
	// a pool verb).
	ConnParses, PoolParses bool
}

// Leaks names the form's credentials found in text.
func (f Form) Leaks(text string) []string {
	var found []string
	for _, secret := range []string{f.Login, f.Password} {
		if secret != "" && strings.Contains(text, secret) {
			found = append(found, secret)
		}
	}
	return found
}

// Grid is the forms a connection string takes: a URI and a keyword string, with and
// without userinfo, with pool parameters valid, invalid and zero, with options and
// sslmode, percent-encoded userinfo, and the credentials coming from the
// environment, a service file or a password file. Each form's environment is applied
// with Apply before the verb runs. (An IPv6 host is not in the grid: the server listens
// on IPv4 only.)
func (r Refusing) Grid(t *testing.T) []Form {
	t.Helper()
	dir := t.TempDir()
	host, port := r.Host, r.Port
	base := fmt.Sprintf("postgres://%s:%d/appdb?sslmode=disable", host, port)
	keyword := fmt.Sprintf("host=%s port=%d dbname=appdb sslmode=disable", host, port)
	service := filepath.Join(dir, "pg_service.conf")
	serviceBody := fmt.Sprintf("[svc]\nhost=%s\nport=%d\nuser=svc_login_k3\npassword=svc_password_w8\ndbname=appdb\nsslmode=disable\n", host, port)
	if err := os.WriteFile(service, []byte(serviceBody), 0o600); err != nil {
		t.Fatal(err)
	}
	passfile := filepath.Join(dir, "pgpass")
	passBody := fmt.Sprintf("%s:%d:appdb:%s:pgpass_password_r5\n", host, port, r.Login)
	if err := os.WriteFile(passfile, []byte(passBody), 0o600); err != nil {
		t.Fatal(err)
	}
	env := func(extra map[string]string) map[string]string {
		out := map[string]string{"PGUSER": r.Login, "PGPASSWORD": r.Password, "PGSERVICEFILE": "", "PGPASSFILE": filepath.Join(dir, "no-pgpass")}
		for key, value := range extra {
			out[key] = value
		}
		return out
	}
	withEnvCreds := func(name, dsn string) Form {
		return Form{Name: name, DSN: dsn, Env: env(nil), Login: r.Login, Password: r.Password}
	}
	forms := []Form{
		withEnvCreds("uri", base),
		withEnvCreds("uri, pool_max_conns=0", base+"&pool_max_conns=0"),
		withEnvCreds("uri, pool_max_conns=4 pool_min_conns=0", base+"&pool_max_conns=4&pool_min_conns=0"),
		withEnvCreds("uri, pool_max_conns=abc", base+"&pool_max_conns=abc"),
		withEnvCreds("uri, pool_max_conns=-1", base+"&pool_max_conns=-1"),
		withEnvCreds("uri, pool_health_check_period=5s", base+"&pool_health_check_period=5s"),
		withEnvCreds("uri, options and application_name", base+"&options=-c%20statement_timeout%3D1000&application_name=grid"),
		withEnvCreds("uri, sslmode=prefer", strings.Replace(base, "sslmode=disable", "sslmode=prefer", 1)),
		withEnvCreds("keyword", keyword),
		withEnvCreds("keyword, pool_max_conns=0", keyword+" pool_max_conns=0"),
		withEnvCreds("keyword, pool_max_conns=2", keyword+" pool_max_conns=2"),
		{Name: "uri, percent-encoded userinfo", DSN: fmt.Sprintf("postgres://us%%40er:p%%40ss%%2Fw@%s:%d/appdb?sslmode=disable", host, port), Env: env(nil), Login: "us@er", Password: "p@ss/w"},
		{Name: "uri, percent-encoded userinfo, pool_max_conns=0", DSN: fmt.Sprintf("postgres://us%%40er:p%%40ss%%2Fw@%s:%d/appdb?sslmode=disable&pool_max_conns=0", host, port), Env: env(nil), Login: "us@er", Password: "p@ss/w"},
		{Name: "keyword, userinfo", DSN: keyword + " user=kw_login password=kw_pass", Env: env(nil), Login: "kw_login", Password: "kw_pass"},
		{Name: "keyword, userinfo, pool_max_conns=0", DSN: keyword + " user=kw_login password=kw_pass pool_max_conns=0", Env: env(nil), Login: "kw_login", Password: "kw_pass"},
		{Name: "uri, service file", DSN: "postgres:///appdb?service=svc", Env: env(map[string]string{"PGSERVICEFILE": service, "PGUSER": "", "PGPASSWORD": ""}), Login: "svc_login_k3", Password: "svc_password_w8"},
		{Name: "uri, service file, pool_max_conns=0", DSN: "postgres:///appdb?service=svc&pool_max_conns=0", Env: env(map[string]string{"PGSERVICEFILE": service, "PGUSER": "", "PGPASSWORD": ""}), Login: "svc_login_k3", Password: "svc_password_w8"},
		{Name: "keyword, service file", DSN: "service=svc dbname=appdb", Env: env(map[string]string{"PGSERVICEFILE": service, "PGUSER": "", "PGPASSWORD": ""}), Login: "svc_login_k3", Password: "svc_password_w8"},
		{Name: "uri, password file", DSN: base, Env: env(map[string]string{"PGPASSFILE": passfile, "PGPASSWORD": ""}), Login: r.Login, Password: "pgpass_password_r5"},
		{Name: "uri, password file, pool_max_conns=0", DSN: base + "&pool_max_conns=0", Env: env(map[string]string{"PGPASSFILE": passfile, "PGPASSWORD": ""}), Login: r.Login, Password: "pgpass_password_r5"},
	}
	for index := range forms {
		forms[index].Apply(t)
		forms[index].ConnParses = parses(func() error { _, err := pgx.ParseConfig(forms[index].DSN); return err })
		forms[index].PoolParses = parses(func() error { _, err := pgxpool.ParseConfig(forms[index].DSN); return err })
	}
	return forms
}

func parses(parse func() error) bool { return parse() == nil }

// Apply sets the form's environment for the rest of the test.
func (f Form) Apply(t *testing.T) {
	t.Helper()
	for key, value := range f.Env {
		t.Setenv(key, value)
	}
}

// RunGrid runs a verb once per form of the grid. run gets the form's connection
// string and returns everything the verb printed. Every run's output must carry none
// of the form's credentials; a run whose opener parses the form must have reached the
// server (pool says whether the opener is a pool, whose parser is stricter): a form
// the opener refuses to parse is still checked for leaks, but cannot dial.
func (r Refusing) RunGrid(t *testing.T, pool bool, run func(t *testing.T, dsn string) string) {
	t.Helper()
	reached := 0
	forms := r.Grid(t)
	for _, form := range forms {
		form.Apply(t)
		before := r.Connections()
		output := run(t, form.DSN)
		dials := (pool && form.PoolParses) || (!pool && form.ConnParses)
		if dials {
			if r.Connections() <= before {
				t.Errorf("%s: no connection reached the refusing server although the opener parses the form (the run measures nothing)", form.Name)
			}
			reached++
		}
		if leaks := form.Leaks(output); len(leaks) > 0 {
			t.Errorf("%s: the output carries %v:\n%s", form.Name, leaks, output)
		}
	}
	if reached < 8 {
		t.Fatalf("only %d of %d forms reached the server: the grid measures too little", reached, len(forms))
	}
}
