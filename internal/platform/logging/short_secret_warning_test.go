package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

// A credential field below the minimum length cannot be redacted by value; the
// warning must still say WHICH field, through the real log redactor.
func TestShortCredentialFieldWarningNamesTheField(t *testing.T) {
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	var out bytes.Buffer
	restore := InstallDefault(NewJSON(&out, slog.LevelDebug))
	t.Cleanup(restore)

	for _, field := range []string{"client_secret", "private_token", "api_key", "password"} {
		secrets.RegisterCredentialJSON([]byte(`{"` + field + `":"abc"}`))
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("want 4 warnings, got %d: %s", len(lines), out.String())
	}
	for index, field := range []string{"client_secret", "private_token", "api_key", "password"} {
		var record map[string]any
		if err := json.Unmarshal([]byte(lines[index]), &record); err != nil {
			t.Fatal(err)
		}
		if record["field"] != field {
			t.Errorf("warning %d: field = %v, want %q (line: %s)", index, record["field"], field, lines[index])
		}
		if strings.Contains(lines[index], "abc") {
			t.Errorf("warning %d leaks the value: %s", index, lines[index])
		}
	}
}
