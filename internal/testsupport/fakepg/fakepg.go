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
	"strings"
	"sync/atomic"
	"testing"
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
	// connections counts the connections that reached the server.
	connections atomic.Int64
}

// Connections is how many connections the server has accepted: a test that
// expects a login attempt asserts it is not zero, so a verb that failed before
// dialing (a malformed URI, a refusal) does not pass for a redaction.
func (s *Server) Connections() int { return int(s.connections.Load()) }

// New listens on a loopback port until the test ends. Every connection is refused
// with SQLSTATE 28P01.
func New(t testing.TB) *Server { return newServer(t, false, false) }

func newServer(t testing.TB, acceptLogin, failPing bool) *Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("fakepg listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	server := &Server{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, acceptLogin: acceptLogin, failPing: failPing}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			server.connections.Add(1)
			go serve(conn, acceptLogin, failPing)
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

func serve(conn net.Conn, acceptLogin, failPing bool) {
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
		serveAccepted(conn, reader, params["user"], password, failPing)
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
func serveAccepted(conn net.Conn, reader *bufio.Reader, user, password string, failPing bool) {
	if _, err := conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0, 'Z', 0, 0, 0, 5, 'I'}); err != nil {
		return
	}
	echo := fmt.Sprintf("server echo %s %s", user, password)
	failed, emptyStatement := false, false
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
			if emptyStatement {
				if _, err := conn.Write([]byte{'1', 0, 0, 0, 4}); err != nil {
					return
				}
			} else if !failed {
				writeError(conn, "ERROR", "XX000", echo)
				failed = true
			}
		case 'D':
			if emptyStatement && !failed {
				// ParameterDescription (no parameters), then NoData.
				if _, err := conn.Write([]byte{'t', 0, 0, 0, 6, 0, 0, 'n', 0, 0, 0, 4}); err != nil {
					return
				}
			}
		case 'B':
			if emptyStatement && !failed {
				if _, err := conn.Write([]byte{'2', 0, 0, 0, 4}); err != nil {
					return
				}
			}
		case 'E':
			if emptyStatement && !failed {
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
func StartEchoing(t *testing.T) Refusing { return startEchoing(t, false) }

// StartEchoingOnPing is StartEchoing for a server that fails the driver's ping too:
// the error origin is the ping, not a later statement.
func StartEchoingOnPing(t *testing.T) Refusing { return startEchoing(t, true) }

func startEchoing(t *testing.T, failPing bool) Refusing {
	t.Helper()
	server := newServer(t, true, failPing)
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
