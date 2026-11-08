package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"log/slog"
	"strings"
	"testing"
)

// The process logger (NewJSON, installed by InstallDefault) writes one JSON record per line, so a line break or
// a quote in a log value cannot start a record or add a field. The query-api services log request ids, paths and
// errors through the standard log package, which InstallDefault routes through this handler.
func TestControlCharactersInLogValuesCannotForgeARecordOrField(t *testing.T) {
	forged := "x\r\n" + `{"time":"2026-01-01T00:00:00Z","level":"ERROR","msg":"forged","forged_field":"1"}` + "\nLEVEL=ERROR msg=forged"
	var out bytes.Buffer
	restore := InstallDefault(NewJSON(&out, slog.LevelInfo))
	defer restore()

	log.Printf("query-api: internal request refused: path=%s request_id=%s", forged, forged)
	log.Print(forged)
	slog.Info("attr", slog.String("request_id", forged), slog.Any("err", errors.New(forged)))
	slog.Info("grouped", slog.Group("g", slog.String("request_id", forged)))
	slog.Default().With("request_id", forged).Warn("with")
	slog.Info("key", forged, "v")

	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	if len(lines) != 6 {
		t.Fatalf("got %d lines for 6 records: %q", len(lines), out.String())
	}
	for i, line := range lines {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("line %d is not one JSON record: %v: %q", i, err, line)
		}
		if _, ok := rec["forged_field"]; ok {
			t.Fatalf("line %d: a forged field became a top-level key: %q", i, line)
		}
		if rec["msg"] == "forged" || rec["level"] == "ERROR" {
			t.Fatalf("line %d: a forged record was read as the record: %q", i, line)
		}
	}
}
