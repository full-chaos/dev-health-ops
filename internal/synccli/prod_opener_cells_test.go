package synccli

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
)

// The default store opener (storage/clickhouse.Open) redacts its own errors with
// the ClickHouse DSN before the CLI sees them. A ClickHouse server whose
// exception text holds "svc%3A7kZ2" (the sink login "svc:" spelled escaped, then
// the database password's tail "7kZ2") makes the REAL opener return
// "[REDACTED]7kZ2": the CLI can no longer match the database password "A7kZ2".
// The cells use the real opener against a fake server that answers the native
// handshake with an exception; every value is made up.
func hostileClickHouse(t *testing.T) string {
	t.Helper()
	return hostileClickHouseSaying(t, "refused for svc%3A7kZ2 end", func(addr string) string { return "clickhouse://svc%3A:zz9q@" + addr + "/d" })
}

// hostileClickHouseSaying is the DSN of a fake native ClickHouse server whose
// exception text is message; dsn builds the DSN from the server's address.
func hostileClickHouseSaying(t *testing.T, message string, dsn func(addr string) string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	text := func(value string) []byte { return append(binary.AppendUvarint(nil, uint64(len(value))), value...) }
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = conn.Read(make([]byte, 4096))
				packet := []byte{2}
				packet = binary.LittleEndian.AppendUint32(packet, 516)
				packet = append(packet, text("DB::Exception")...)
				packet = append(packet, text(message)...)
				packet = append(packet, text("")...)
				packet = append(packet, 0)
				_, _ = conn.Write(packet)
			}()
		}
	}()
	return dsn(listener.Addr().String())
}

func TestProdOpenerErrorKeepsTheDatabasePasswordHiddenOnSyncTargets(t *testing.T) {
	sink := hostileClickHouse(t)
	env := map[string]string{"CLICKHOUSE_URI": sink, "POSTGRES_URI": residualDB, "ORG_ID": "org-1"}
	githubArgs := []string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-test"}
	for name, c := range map[string]struct {
		target string
		args   []string
	}{
		"inline": {"prs", githubArgs},
		"local":  {"git", []string{"--provider", "local", "--repo-path", "/repo", "--org", "o", "--since", "2026-01-02"}},
	} {
		t.Run(name, func(t *testing.T) {
			code, stdout, stderr := runVerb(t, c.target, InlineExecutor(defaultInlineDeps()), c.args, env)
			if code == 0 || !strings.Contains(stderr, "refused for") {
				t.Fatalf("exit %d: the failure never reached the printing path (or lost the server's text): %q", code, stderr)
			}
			if strings.Contains(stdout+stderr, residualLeakTail) {
				t.Errorf("the database password's tail printed: %q", stderr)
			}
		})
	}
}

func TestProdOpenerErrorKeepsTheTokenHiddenOnSyncTeams(t *testing.T) {
	env := validEnv()
	env["CLICKHOUSE_URI"] = hostileClickHouse(t)
	env["ATLASSIAN_API_TOKEN"] = "A7kZ2"
	d := defaultDeps()
	d.newClient = stubDeps(&recorded{}, failingClient{}, nil).newClient
	code, _, stderr := run(t, env, d, "--provider", "jira", "--org", "o")
	if code == 0 || !strings.Contains(stderr, "refused for") {
		t.Fatalf("exit %d: the failure never reached the printing path (or lost the server's text): %q", code, stderr)
	}
	if strings.Contains(stderr, residualLeakTail) {
		t.Errorf("the token's tail printed: %q", stderr)
	}
}

// The driver settles the login and password by its own parser, which accepts
// hosts that net/url refuses (an IPv6 literal in a comma list). The CLI union
// parses the sink URI with net/url and finds no component there; the login and
// the password the driver resolved still must not print.
func TestProdOpenerErrorKeepsTheResolvedDriverCredentialsHidden(t *testing.T) {
	sink := hostileClickHouseSaying(t, "refused for svc: with zz9q end", func(addr string) string {
		host, port, _ := net.SplitHostPort(addr)
		return "clickhouse://svc%3A:zz9q@127.0.0.1:1,[::ffff:" + host + "]:" + port + "/d"
	})
	check := func(t *testing.T, code int, printed string) {
		t.Helper()
		if code == 0 || !strings.Contains(printed, "refused for") {
			t.Fatalf("exit %d: the failure never reached the printing path (or lost the server's text): %q", code, printed)
		}
		for _, secret := range []string{"zz9q", "svc:"} {
			if strings.Contains(printed, secret) {
				t.Errorf("%q printed: %q", secret, printed)
			}
		}
	}
	t.Run("sync target", func(t *testing.T) {
		env := map[string]string{"CLICKHOUSE_URI": sink, "POSTGRES_URI": residualDB, "ORG_ID": "org-1"}
		githubArgs := []string{"--provider", "github", "--owner", "acme", "--repo", "api", "--auth", "ghp-test"}
		code, stdout, stderr := runVerb(t, "prs", InlineExecutor(defaultInlineDeps()), githubArgs, env)
		check(t, code, stdout+stderr)
	})
	t.Run("sync teams", func(t *testing.T) {
		env := validEnv()
		env["CLICKHOUSE_URI"] = sink
		d := defaultDeps()
		d.newClient = stubDeps(&recorded{}, failingClient{}, nil).newClient
		code, stdout, stderr := run(t, env, d, "--provider", "jira", "--org", "o")
		check(t, code, stdout+stderr)
	})
}
