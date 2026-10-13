package workersctl

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// A process whose CLICKHOUSE_URI does not reach ClickHouse (or has none) fails
// the run marker verbs with the stable code, the setting it used and the
// redacted cause, never the credential.
func TestRunMarkerStoreFailureNamesTheSettingAndTheCause(t *testing.T) {
	const password = "marker-store-secret-pw"
	cases := map[string]map[string]string{
		"unreachable": {"CLICKHOUSE_URI": "clickhouse://dho:" + password + "@127.0.0.1:1/default"},
		"missing":     {},
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			runtime := &operatorRuntime{lookup: func(key string) (string, bool) {
				value, ok := env[key]
				return value, ok
			}}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			var stderr bytes.Buffer
			closeConn, code := attachRunMarker(ctx, runtime, nil, &stderr)
			if closeConn != nil || code == 0 {
				t.Fatalf("attachRunMarker = (%v, %d), want a failure", closeConn != nil, code)
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
			if payload.Error.Code != "operator_backend_unavailable" || payload.Error.DSNKey != "CLICKHOUSE_URI" || payload.Error.Detail == "" {
				t.Errorf("error = %+v, want code operator_backend_unavailable, dsn_key CLICKHOUSE_URI and a detail", payload.Error)
			}
		})
	}
}
