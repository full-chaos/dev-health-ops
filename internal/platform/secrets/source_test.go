package secrets

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mapLookup(values map[string]string) LookupEnv {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

func TestResolveDirectAndFileSources(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "database-uri")
	if err := os.WriteFile(path, []byte("postgres://file-user:file-secret@db/app\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	fromFile, configured, err := Resolve("POSTGRES_URI", mapLookup(map[string]string{
		"POSTGRES_URI_FILE": path,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !configured || fromFile.Reveal() != "postgres://file-user:file-secret@db/app" {
		t.Fatalf("unexpected file value: configured=%v value=%q", configured, fromFile.Reveal())
	}

	direct, configured, err := Resolve("POSTGRES_URI", mapLookup(map[string]string{
		"POSTGRES_URI": "postgres://direct-user:direct-secret@db/app",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !configured || direct.Reveal() != "postgres://direct-user:direct-secret@db/app" {
		t.Fatalf("unexpected direct value: configured=%v value=%q", configured, direct.Reveal())
	}
}

func TestResolveRejectsAmbiguousSourcesWithoutLeakingValues(t *testing.T) {
	t.Parallel()

	errSecret := "postgres://user:top-secret@db/app"
	_, _, err := Resolve("POSTGRES_URI", mapLookup(map[string]string{
		"POSTGRES_URI":      errSecret,
		"POSTGRES_URI_FILE": "/mounted/database-uri",
	}))
	if err == nil {
		t.Fatal("expected source conflict")
	}
	if !IsSourceConflict(err, "POSTGRES_URI") {
		t.Fatalf("expected classified source conflict, got %v", err)
	}
	if strings.Contains(err.Error(), errSecret) {
		t.Fatalf("error leaked secret: %v", err)
	}
}

func TestValueIsRedactedByFormattingLoggingAndJSON(t *testing.T) {
	t.Parallel()

	raw := "postgres://user:top-secret@db/app"
	value := NewValue(raw)
	formatted := fmt.Sprintf("%s %#v", value, value)
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	logged := value.LogValue()

	combined := formatted + string(encoded) + logged.String()
	if strings.Contains(combined, raw) || strings.Contains(combined, "top-secret") {
		t.Fatalf("secret was exposed: %s", combined)
	}
	if logged.Kind() != slog.KindString || !strings.Contains(combined, redacted) {
		t.Fatalf("expected redaction marker, got %s", combined)
	}
}
