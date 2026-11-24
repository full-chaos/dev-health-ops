package synccli

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// A catalog provider's HTTP client can fail with a text that echoes the token the
// run was given (a proxy or gateway that prints the request headers). The print
// union of `dho sync teams --provider github|gitlab|linear` is the ClickHouse DSN,
// the database and the driver-resolved credentials: the token is in none of them.
// Every value is made up.
const catalogCredential = "zz9tok-synthetic-aaaaaaaa"

// catalogConn is a connection whose first query (the team id carry's count) returns
// one row of zeros and every later one no rows, so the run reaches the provider.
type catalogConn struct {
	driver.Conn
	queries *int
}

func (catalogConn) Close() error { return nil }
func (c catalogConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	*c.queries++
	return &catalogRows{first: *c.queries == 1}, nil
}
func (catalogConn) Exec(context.Context, string, ...any) error { return nil }

type catalogRows struct {
	driver.Rows
	first, read bool
}

func (r *catalogRows) Next() bool {
	if r.first && !r.read {
		r.read = true
		return true
	}
	return false
}
func (r *catalogRows) Scan(dest ...any) error {
	for _, target := range dest {
		if value, ok := target.(*uint64); ok {
			*value = 0
		}
	}
	return nil
}
func (r *catalogRows) Close() error { return nil }
func (r *catalogRows) Err() error   { return nil }

// echoingDoer answers every provider request the way a gateway that prints the
// request headers would: as a transport error, or as an HTTP error whose body
// holds the token.
type echoingDoer struct{ body bool }

func (d echoingDoer) Do(*http.Request) (*http.Response, error) {
	if d.body {
		return &http.Response{StatusCode: http.StatusUnauthorized, Status: "401 Unauthorized", Header: http.Header{"Content-Type": {"application/json"}},
			Body: io.NopCloser(strings.NewReader(`{"message":"Bad credentials for ` + catalogCredential + `"}`))}, nil
	}
	return nil, errors.New("gateway refused: Authorization: Bearer " + catalogCredential)
}

func TestCatalogTeamsErrorKeepsTheProviderTokenHidden(t *testing.T) {
	providers := map[string][]string{
		"github": {"--provider", "github", "--owner", "acme", "--org", "o", "--auth", catalogCredential, "--structure"},
		"gitlab": {"--provider", "gitlab", "--owner", "acme", "--org", "o", "--auth", catalogCredential, "--structure"},
		"linear": {"--provider", "linear", "--org", "o", "--auth", catalogCredential, "--structure"},
	}
	for provider, args := range providers {
		for form, doer := range map[string]echoingDoer{"transport error": {}, "error body": {body: true}} {
			t.Run(provider+"/"+form, func(t *testing.T) {
				queries := 0
				env := validEnv()
				env["CLICKHOUSE_URI"] = "clickhouse://zz9-cat-login:zz9-cat-pass@127.0.0.1:9000/d"
				d := defaultDeps()
				d.doer = fakehttp.Client(doer)
				d.openStore = func(context.Context, string) (driver.Conn, error) { return catalogConn{queries: &queries}, nil }
				code, stdout, stderr := run(t, env, d, args...)
				if code == 0 || queries == 0 || !strings.Contains(stderr, `"code":"sync_failed"`) {
					t.Fatalf("exit %d, %d queries: the provider error never reached the printing path: %q", code, queries, stderr[max(0, len(stderr)-500):])
				}
				if strings.Contains(stdout+stderr, catalogCredential) {
					t.Errorf("the provider token printed: %q", stderr)
				}
			})
		}
	}
}
