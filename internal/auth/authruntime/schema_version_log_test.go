package authruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// The roll readback reads this line with kubectl logs: its message and key names are a contract.
func TestSchemaCheckedLinePinsKeysThroughTheProductionRedactor(t *testing.T) {
	var out bytes.Buffer
	logger := logging.NewJSON(&out, slog.LevelInfo)
	logSchemaChecked(context.Background(), logger, "auth")

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("want exactly one line, got %d: %q", len(lines), out.String())
	}
	entry := map[string]any{}
	if err := json.Unmarshal([]byte(lines[0]), &entry); err != nil {
		t.Fatal(err)
	}
	if entry["msg"] != "auth schema checked" || entry["level"] != "INFO" {
		t.Fatalf("msg/level = %v/%v", entry["msg"], entry["level"])
	}
	var keys []string
	for key := range entry {
		if key != "time" && key != "level" && key != "msg" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if want := []string{"embedded_head_version", "schema"}; !reflect.DeepEqual(keys, want) {
		t.Fatalf("keys = %v, want %v (line %s)", keys, want, lines[0])
	}
	head, err := authschema.HeadVersion()
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := entry["embedded_head_version"].(float64); !ok || int(got) != head {
		t.Fatalf("embedded_head_version = %v, want %d", entry["embedded_head_version"], head)
	}
	if entry["schema"] != "auth" {
		t.Fatalf("schema = %v", entry["schema"])
	}
	if strings.Contains(lines[0], "REDACTED") {
		t.Fatalf("the redactor masked a key or value: %s", lines[0])
	}
	for _, key := range []string{"schema", "embedded_head_version"} {
		if !logging.KeyClean(key) {
			t.Errorf("key %q is protected by the log redactor", key)
		}
	}
}
