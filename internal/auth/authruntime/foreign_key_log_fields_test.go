package authruntime

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// foreignKeyLogFields names the log fields that carry a foreign key error: SQLSTATE 23503 as a whole number, or the
// database text. The time is skipped: its nanoseconds can hold "23503" by chance.
func foreignKeyLogFields(logs string) []string {
	var found []string
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			if foreignKeyText(line) {
				found = append(found, "raw="+line)
			}
			continue
		}
		for field, value := range record {
			if field == slog.TimeKey {
				continue
			}
			if text := fmt.Sprint(value); foreignKeyText(text) {
				found = append(found, field+"="+text)
			}
		}
	}
	return found
}

func foreignKeyText(text string) bool {
	if strings.Contains(text, "foreign key") || strings.Contains(text, "violates") {
		return true
	}
	for rest := text; ; {
		i := strings.Index(rest, "23503")
		if i < 0 {
			return false
		}
		before, after := i == 0 || !isDigit(rest[i-1]), i+5 == len(rest) || !isDigit(rest[i+5])
		if before && after {
			return true
		}
		rest = rest[i+1:]
	}
}

func isDigit(b byte) bool { return b >= '0' && b <= '9' }

func TestForeignKeyLogFieldsSkipTheTimeAndPartNumbers(t *testing.T) {
	for line, want := range map[string]int{
		`{"time":"2026-11-20T13:38:06.123503Z","level":"INFO","msg":"identity: principal projected"}`:                       0,
		`{"time":"2026-11-20T13:38:06.23503Z","level":"INFO","msg":"identity: principal projected"}`:                        0,
		`{"time":"2026-11-20T13:38:06.1Z","level":"INFO","msg":"authorize","duration_ns":235031,"id":"a1235039"}`:           0,
		`{"time":"2026-11-20T13:38:06.1Z","level":"ERROR","msg":"audit insert","code":"23503"}`:                             1,
		`{"time":"2026-11-20T13:38:06.1Z","level":"ERROR","msg":"insert violates foreign key constraint (SQLSTATE 23503)"}`: 1,
		`not json: ERROR 23503`: 1,
	} {
		if got := len(foreignKeyLogFields(line)); got != want {
			t.Errorf("foreignKeyLogFields(%s) = %d fields, want %d", line, got, want)
		}
	}
}
