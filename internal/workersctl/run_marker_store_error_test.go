package workersctl

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

// A process whose CLICKHOUSE_URI does not reach ClickHouse (or has none) fails
// each run marker verb with the stable code and exit code, the setting it used
// and the redacted cause, never the credential: attachRunMarker (the real
// finalize-redrive path) and daily-marker-backfill (also with --dry-run).
func TestRunMarkerStoreFailureNamesTheSettingAndTheCause(t *testing.T) {
	const password = "marker-store-secret-pw"
	envs := map[string]map[string]string{
		"unreachable": {"CLICKHOUSE_URI": "clickhouse://dho:" + password + "@127.0.0.1:1/default"},
		"missing":     {},
	}
	// A lazy pool: the backfill verb needs one to reach the store, and opening
	// the store fails before any Postgres query.
	pool, err := pgxpool.New(context.Background(), "postgres://dho@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	callers := map[string]func(context.Context, *operatorRuntime, io.Writer) int{
		"attachRunMarker": func(ctx context.Context, runtime *operatorRuntime, stderr io.Writer) int {
			closeConn, code := attachRunMarker(ctx, runtime, nil, stderr)
			if closeConn != nil {
				t.Error("attachRunMarker returned a close function on failure")
			}
			return code
		},
		"daily-marker-backfill": func(ctx context.Context, runtime *operatorRuntime, stderr io.Writer) int {
			runtime.pools = &postgresstore.RuntimePools{Domain: pool}
			return dispatchMetricsDailyMarkerBackfill(ctx, runtime, []string{
				"--org", "00000000-0000-4000-8000-000000009462", "--from", "2026-10-12", "--to", "2026-10-12", "--dry-run",
			}, io.Discard, stderr)
		},
	}
	for envName, env := range envs {
		for callerName, call := range callers {
			t.Run(envName+"/"+callerName, func(t *testing.T) {
				runtime := commandRuntimeWithAuditor(t, commandAuthorizer{}, &refusingAuditor{})
				runtime.lookup = func(key string) (string, bool) {
					value, ok := env[key]
					return value, ok
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				var stderr bytes.Buffer
				if code := call(ctx, runtime, &stderr); code != cli.ExitFailure {
					t.Fatalf("exit code %d, want %d; stderr %s", code, cli.ExitFailure, stderr.String())
				}
				if strings.Contains(stderr.String(), password) {
					t.Fatalf("stderr carries the password: %s", stderr.String())
				}
				lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
				var payload struct {
					Error struct {
						Code   string `json:"code"`
						DSNKey string `json:"dsn_key"`
						Detail string `json:"detail"`
					} `json:"error"`
				}
				if err := json.Unmarshal([]byte(lines[len(lines)-1]), &payload); err != nil {
					t.Fatalf("last stderr line is not JSON: %q", lines[len(lines)-1])
				}
				t.Logf("detail: %s", payload.Error.Detail)
				wantDetail := map[string]string{"unreachable": "127.0.0.1:1", "missing": "CLICKHOUSE_URI is required"}[envName]
				if payload.Error.Code != "operator_backend_unavailable" || payload.Error.DSNKey != "CLICKHOUSE_URI" ||
					!strings.Contains(payload.Error.Detail, wantDetail) {
					t.Errorf("error = %+v, want code operator_backend_unavailable, dsn_key CLICKHOUSE_URI and a detail naming %q",
						payload.Error, wantDetail)
				}
			})
		}
	}
}
