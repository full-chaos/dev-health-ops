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

func TestShortEnvSettingWarningKeepsItsNamedDetail(t *testing.T) {
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	var out bytes.Buffer
	restore := InstallDefault(NewJSON(&out, slog.LevelDebug))
	t.Cleanup(restore)

	secrets.Register("CLICKHOUSE_PASSWORD", "abc")

	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &record); err != nil {
		t.Fatal(err)
	}
	if record["setting"] != "CLICKHOUSE_PASSWORD" {
		t.Errorf("setting = %v", record["setting"])
	}
	if detail, _ := record["detail"].(string); !strings.Contains(detail, "CLICKHOUSE_PASSWORD") {
		t.Errorf("detail lost the setting name: %v", record["detail"])
	}
	if _, has := record["field"]; has {
		t.Errorf("an environment setting carries no field attribute: %s", out.String())
	}
}

func shortFieldRecord(t *testing.T, field string) (map[string]any, string) {
	t.Helper()
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	var out bytes.Buffer
	restore := InstallDefault(NewJSON(&out, slog.LevelDebug))
	t.Cleanup(restore)
	secrets.RegisterCredentialJSON([]byte(`{"` + field + `":"abc"}`))
	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &record); err != nil {
		t.Fatalf("%v: %s", err, out.String())
	}
	return record, out.String()
}

func TestShortCredentialFieldNameIsLoggedOnlyAsItsDeclaredSpelling(t *testing.T) {
	for sent, declared := range map[string]string{
		"client_secret":          "client_secret",
		"clientSecret":           "client_secret",
		"API-KEY":                "api_key",
		"webhook_signing_secret": "webhook_signing_secret",
	} {
		record, line := shortFieldRecord(t, sent)
		if record["field"] != declared {
			t.Errorf("%q: field = %v, want the declared %q (%s)", sent, record["field"], declared, line)
		}
		if sent != declared && strings.Contains(line, sent) {
			t.Errorf("%q: the text sent reached the log: %s", sent, line)
		}
	}
}

func TestShortCredentialFieldNameThatIsNotDeclaredIsReportedByLengthOnly(t *testing.T) {
	for _, sent := range []string{
		"password token-12345678",
		"password-12345678",
		"client_secret_" + strings.Repeat("a", 60),
		"passwordABCDEFGH",
		"my-custom-token",
		"token_a1b2c3d4e5",
	} {
		record, line := shortFieldRecord(t, sent)
		if record["field"] != "undeclared_name" || record["field_length"] != float64(len(sent)) {
			t.Errorf("%q: not reported as undeclared with its length: %s", sent, line)
		}
		if strings.Contains(line, sent) || strings.Contains(line, "12345678") || strings.Contains(line, "ABCDEFGH") {
			t.Errorf("%q: caller-supplied text reached the log: %s", sent, line)
		}
	}
}
