package synccli

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cli"
)

func lookupRecords(t *testing.T, out string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %v: %s", err, line)
		}
		if record["msg"] == credentialLookupFailedEvent {
			records = append(records, record)
		}
	}
	return records
}

func onlyRefusalRecord(t *testing.T, out string) map[string]any {
	t.Helper()
	records := lookupRecords(t, out)
	if len(records) != 1 {
		t.Fatalf("%d refusal records, want 1: %s", len(records), out)
	}
	return records[0]
}

func shapeRecord(t *testing.T, payload string) (map[string]any, string) {
	t.Helper()
	log := captureLookupLog(t)
	_, _, err := githubCredentialFromJSON([]byte(payload))
	logCredentialLookupFailure(context.Background(), "org-1", failureOf(t, err))
	return onlyRefusalRecord(t, log.String()), log.String()
}

func TestShapeRefusalLogsTheIgnoredKeyNamesNeverTheirValues(t *testing.T) {
	record, raw := shapeRecord(t, `{"org":"acme","zz_extra":"distinctive-secret-9727"}`)
	if record["reason"] != "credential_shape_invalid" || record["ignored_fields"] != "org,zz_extra" || record["ignored_field_count"] != float64(2) {
		t.Fatalf("record = %v, want reason credential_shape_invalid, ignored_fields org,zz_extra, ignored_field_count 2", record)
	}
	if strings.Contains(raw, "distinctive-secret-9727") {
		t.Fatalf("log carries a payload value: %s", raw)
	}
}

func TestBlankKeysAreCountedEvenThoughTheirNamesAreNotLogged(t *testing.T) {
	for _, payload := range []string{`{"":"blank-value"}`, `{" ":"space-value"}`} {
		record, raw := shapeRecord(t, payload)
		if record["ignored_fields"] != "" || record["ignored_field_count"] != float64(1) {
			t.Fatalf("payload %s: record = %v, want no names and ignored_field_count 1", payload, record)
		}
		if strings.Contains(raw, "blank-value") || strings.Contains(raw, "space-value") {
			t.Fatalf("payload %s: log carries a value: %s", payload, raw)
		}
	}
}

func TestIgnoredKeyNamesAreBoundedInTheRefusalLine(t *testing.T) {
	var keys, want []string
	for i := 0; i < 25; i++ {
		keys = append(keys, fmt.Sprintf(`"k%02d":"v"`, i))
		if i < maxLoggedFieldNames {
			want = append(want, fmt.Sprintf("k%02d", i))
		}
	}
	record, _ := shapeRecord(t, "{"+strings.Join(keys, ",")+"}")
	wantNames := strings.Join(want, ",") + ",(5 more)"
	if record["ignored_fields"] != wantNames || record["ignored_field_count"] != float64(25) {
		t.Fatalf("ignored_fields = %v, ignored_field_count = %v; want %q and 25", record["ignored_fields"], record["ignored_field_count"], wantNames)
	}
}

func TestRedactedKeyNameIsPinned(t *testing.T) {
	record, _ := shapeRecord(t, `{"token=hostile-name":"value"}`)
	if record["ignored_fields"] != "token=[REDACTED]" {
		t.Fatalf("ignored_fields = %v, want the production redactor's rendering token=[REDACTED]", record["ignored_fields"])
	}
}

func TestNonShapeRefusalsKeepTheirKeySet(t *testing.T) {
	log := captureLookupLog(t)
	_, err := githubCredentialFromDB(context.Background(), "mysql://dho_user:s3cret-pw@127.0.0.1:1/dho", "org-1", cli.Env{})
	if err == nil {
		t.Fatal("want a refusal for a database URL the lookup cannot use")
	}
	record := onlyRefusalRecord(t, log.String())
	if record["reason"] != "database_url_refused" {
		t.Fatalf("reason = %v, want database_url_refused", record["reason"])
	}
	for _, key := range []string{"ignored_fields", "ignored_field_count"} {
		if _, present := record[key]; present {
			t.Fatalf("a non-shape refusal carries %s: %v", key, record)
		}
	}
	if strings.Contains(log.String(), "s3cret-pw") {
		t.Fatalf("log carries the password: %s", log.String())
	}
}
