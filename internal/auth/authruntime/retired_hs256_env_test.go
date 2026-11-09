package authruntime

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// A retired HS256 setting left in auth-service's environment never stops the start (D6465): each one that is set
// logs exactly one WARN naming it, never its value, and an unset one logs none.
func TestAuthServiceWarnsOnceForEachRetiredHS256SettingThatIsSet(t *testing.T) {
	leftover := strings.Repeat("leftover-hs256-", 3)
	keyFile := filepath.Join(t.TempDir(), "hs256.key")
	for _, name := range []string{"AUTH_JWT_SECRET_KEY", "AUTH_JWT_SECRET_KEY_FILE", "AUTH_JWT_ISSUER", "AUTH_LEGACY_JWT_AUDIENCE", "none"} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{
				authconfig.EnvDatabaseURI:    "postgres://auth@127.0.0.1:1/devhealth",
				authconfig.EnvSigningKeyFile: "/nonexistent",
				authconfig.EnvSigningKeyID:   "retired-hs256-env-test",
			}
			want := 0
			switch name {
			case "none":
			case "AUTH_JWT_SECRET_KEY_FILE":
				env[name], want = keyFile, 1
			default:
				env[name], want = leftover, 1
			}
			var logs bytes.Buffer
			// The WARN is written before any dependency starts, so a context that is already done stops the run
			// right after it, whether or not the rest of the configuration loads.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			Execute(ctx, nil, lookupFrom(env), IO{Stdout: &logs, Stderr: &logs})
			if strings.Contains(logs.String(), leftover) || strings.Contains(logs.String(), keyFile) {
				t.Fatal("a log line carries the retired setting's value")
			}
			warns := 0
			scanner := bufio.NewScanner(&logs)
			scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
			for scanner.Scan() {
				var record struct {
					Level string `json:"level"`
					Msg   string `json:"msg"`
					Key   string `json:"key"`
				}
				if json.Unmarshal(scanner.Bytes(), &record) == nil && record.Level == "WARN" &&
					strings.Contains(record.Msg, "unused (HS256 retired)") && record.Key == name {
					warns++
				}
			}
			if warns != want {
				t.Fatalf("%s: %d WARN lines name it, want %d; log:\n%s", name, warns, want, logs.String())
			}
		})
	}
}
