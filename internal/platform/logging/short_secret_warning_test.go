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

func TestShortCredentialFieldWithProseInItsNameIsReportedAsUnusual(t *testing.T) {
	secrets.ResetRegistered()
	t.Cleanup(secrets.ResetRegistered)
	var out bytes.Buffer
	restore := InstallDefault(NewJSON(&out, slog.LevelDebug))
	t.Cleanup(restore)

	secrets.RegisterCredentialJSON([]byte(`{"password token-12345678":"abc"}`))

	var record map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &record); err != nil {
		t.Fatal(err)
	}
	if record["field"] != "non_identifier_name" || record["field_length"] != float64(len("password token-12345678")) {
		t.Errorf("unusual field name not reported as such: %s", out.String())
	}
	if strings.Contains(out.String(), "12345678") {
		t.Errorf("the field name leaked: %s", out.String())
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

func TestShortCredentialFieldNameLongerThanSixtyFourIsStillNamed(t *testing.T) {
	field := "client_secret_" + strings.Repeat("a", 60)
	record, line := shortFieldRecord(t, field)
	if record["field"] != field {
		t.Errorf("long identifier-like field not named: %s", line)
	}
}

func TestShortCredentialFieldNameCarryingADigitRunIsNotLoggedVerbatim(t *testing.T) {
	for _, field := range []string{"password-12345678", "token_a1b2c3d4e5", "secret.9f8e7d6c5b"} {
		record, line := shortFieldRecord(t, field)
		if record["field"] != "non_identifier_name" {
			t.Errorf("%q: field = %v, want non_identifier_name (%s)", field, record["field"], line)
		}
		if strings.Contains(line, field) {
			t.Errorf("%q leaked: %s", field, line)
		}
	}
}

func TestShortCredentialFieldNamesWithHyphensAndDigitsInWordsStayNamed(t *testing.T) {
	for _, field := range []string{"client-secret", "x-api-key", "oauth2_token", "api_key_v2"} {
		record, line := shortFieldRecord(t, field)
		if record["field"] != field {
			t.Errorf("%q not named: %s", field, line)
		}
	}
}
