package adminops

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakepg"
)

// RED until the shared redactor covers escaped forms (CHAOS-14565 follow-up). A database error can carry the
// credential re-serialized: a URL echoed with its query reordered keeps the password's %-encoding, and a %q echo
// doubles a backslash (and a JSON error line doubles it again). The redactor removes the DSN's own bytes and the decoded login and password only, so the
// escaped password reaches the output. Each cell drives a real verb through the dho tree.

// startReorderingEcho is fakepg's refusal with another echo: it asks for a cleartext password, then answers SQLSTATE
// 28P01 with the connection URL rebuilt from the login and password it received and its query in reverse order.
func startReorderingEcho(t *testing.T, query []string) (string, int, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	port := listener.Addr().(*net.TCPAddr).Port
	var connections atomic.Int64
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer conn.Close()
				reader := bufio.NewReader(conn)
				params := map[string]string{}
				for {
					var header [8]byte
					if _, err := io.ReadFull(reader, header[:]); err != nil {
						return
					}
					body := make([]byte, int(binary.BigEndian.Uint32(header[:4]))-8)
					if _, err := io.ReadFull(reader, body); err != nil {
						return
					}
					if code := binary.BigEndian.Uint32(header[4:]); code == 80877103 || code == 80877104 {
						if _, err := conn.Write([]byte{'N'}); err != nil {
							return
						}
						continue
					}
					fields := strings.Split(string(body), "\x00")
					for i := 0; i+1 < len(fields) && fields[i] != ""; i += 2 {
						params[fields[i]] = fields[i+1]
					}
					break
				}
				if _, err := conn.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 3}); err != nil {
					return
				}
				var head [5]byte
				if _, err := io.ReadFull(reader, head[:]); err != nil {
					return
				}
				payload := make([]byte, int(binary.BigEndian.Uint32(head[1:]))-4)
				if _, err := io.ReadFull(reader, payload); err != nil {
					return
				}
				reversed := make([]string, len(query))
				for i, pair := range query {
					reversed[len(query)-1-i] = pair
				}
				echo := url.URL{Scheme: "postgres", User: url.UserPassword(params["user"], strings.TrimRight(string(payload), "\x00")),
					Host: fmt.Sprintf("127.0.0.1:%d", port), Path: "/" + params["database"], RawQuery: strings.Join(reversed, "&")}
				var fields []byte
				for _, field := range [][2]string{{"S", "FATAL"}, {"V", "FATAL"}, {"C", "28P01"}, {"M", "password authentication failed for " + echo.String()}} {
					fields = append(append(append(fields, field[0][0]), field[1]...), 0)
				}
				fields = append(fields, 0)
				out := []byte{'E', 0, 0, 0, 0}
				binary.BigEndian.PutUint32(out[1:], uint32(len(fields)+4))
				_, _ = conn.Write(append(out, fields...))
			}()
		}
	}()
	return "127.0.0.1", port, &connections
}

// redactEscapedVerb is a verb on each redactor: users list on accountRedactor (API_DATABASE_URI here) and
// sync-configs artifact-excludes show on redactor (MIGRATION_DATABASE_URI).
type redactEscapedVerb struct {
	name, env string
	args      []string
}

func redactEscapedVerbs() []redactEscapedVerb {
	return []redactEscapedVerb{
		{"users list", "API_DATABASE_URI", []string{"users", "list", "--limit", "5"}},
		{"sync-configs show", "MIGRATION_DATABASE_URI", []string{"sync-configs", "artifact-excludes", "show", "--config-id", "c0ffee00-1455-4000-8000-0000000000c1"}},
	}
}

func redactEscapedRun(t *testing.T, name, dsn string, args []string) (int, string) {
	t.Helper()
	values := map[string]string{name: dsn}
	var stdout, stderr bytes.Buffer
	code := cli.Execute(context.Background(), "dho", []cli.Command{Command()}, cli.Env{
		Args: append([]string{"admin"}, args...), Lookup: func(key string) (string, bool) { value, ok := values[key]; return value, ok },
		Stdin: strings.NewReader(""), Stdout: &stdout, Stderr: &stderr,
	})
	return code, stdout.String() + stderr.String()
}

// (i) A URL DSN whose password holds '@' (encoded %40; 10 characters, over the 8-character by-value minimum); the server echoes the URL with its query reordered.
func TestAdminVerbsRedactAPasswordEchoedInAReorderedURL(t *testing.T) {
	const login, password = "adminlogin_k7", "pw@z9q7k4m"
	query := []string{"sslmode=disable", "connect_timeout=5"}
	for _, verb := range redactEscapedVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			host, port, connections := startReorderingEcho(t, query)
			dsn := (&url.URL{Scheme: "postgres", User: url.UserPassword(login, password), Host: fmt.Sprintf("%s:%d", host, port), Path: "/appdb", RawQuery: strings.Join(query, "&")}).String()
			if !strings.Contains(dsn, "pw%40z9q7k4m") {
				t.Fatalf("the DSN %q does not encode the password's @", dsn)
			}
			code, output := redactEscapedRun(t, verb.env, dsn, verb.args)
			if connections.Load() == 0 || code == cli.ExitOK {
				t.Fatalf("exit %d, %d connection(s): want a refused login; output %q", code, connections.Load(), output)
			}
			if !strings.Contains(output, "password authentication failed") {
				t.Fatalf("the failure lost the server's text: %q", output)
			}
			for _, form := range []string{password, "pw%40z9q7k4m"} {
				if strings.Contains(output, form) {
					t.Errorf("output carries the password as %q: %q", form, output)
				}
			}
		})
	}
}

// (ii) A key=value DSN whose password holds a backslash; fakepg echoes it with %q (the backslash doubled).
func TestAdminVerbsRedactAPasswordEchoedWithQuoting(t *testing.T) {
	const login, password = "adminlogin_k7", `pw\z9q7k4m`
	for _, verb := range redactEscapedVerbs() {
		t.Run(verb.name, func(t *testing.T) {
			server := fakepg.New(t)
			dsn := fmt.Sprintf(`host=%s port=%d dbname=appdb user=%s password='pw\\z9q7k4m' sslmode=disable`, server.Host, server.Port, login)
			code, output := redactEscapedRun(t, verb.env, dsn, verb.args)
			if server.Connections() == 0 || code == cli.ExitOK {
				t.Fatalf("exit %d, %d connection(s): want a refused login; output %q", code, server.Connections(), output)
			}
			if !strings.Contains(output, "password authentication failed") {
				t.Fatalf("the failure lost the server's text: %q", output)
			}
			for _, form := range []string{password, `pw\\z9q7k4m`, `pw\\\\z9q7k4m`} {
				if strings.Contains(output, form) {
					t.Errorf("output carries the password as %q: %q", form, output)
				}
			}
		})
	}
}
