package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func noEnv(string) (string, bool) { return "", false }

func TestRunRequiresExactlyOneSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), nil, noEnv, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("stderr = %q, want a usage message", stderr.String())
	}

	code = run(context.Background(), []string{"queue-depth", "extra"}, noEnv, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code = %d, want 2 for extra args", code)
	}
}

func TestRunFailsClosedWithoutPostgresURI(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"queue-depth"}, noEnv, &stdout, &stderr)
	if code != 1 {
		t.Fatalf("code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "POSTGRES_URI") {
		t.Fatalf("stderr = %q, want it to name POSTGRES_URI", stderr.String())
	}
}

func TestRunRejectsAnUnknownCheckBeforeTouchingPostgresURI(t *testing.T) {
	// The check name must be validated BEFORE POSTGRES_URI is even looked
	// up: a typo'd healthcheck.test entry in compose must fail fast with a
	// clear "unknown check" message, never a confusing pool-open error (or
	// worse, a silent success) caused by lookup ordering.
	lookup := func(name string) (string, bool) {
		t.Fatalf("lookup(%q) called; an unknown check must refuse before reading any env var", name)
		return "", false
	}
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), []string{"not-a-real-check"}, lookup, &stdout, &stderr)
	if code != 2 {
		t.Fatalf("code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), `unknown check "not-a-real-check"`) {
		t.Fatalf("stderr = %q, want it to name the bad check", stderr.String())
	}
}

// TestEnvIntMatchesProductionRetentionDaysSemantics pins envInt against
// internal/scheduler/fixed/producers.go's retentionDays: absent/empty/
// unparseable/whitespace-only fall back to the default (no intent
// expressed); zero is a legitimate configured horizon, NEVER a fallback
// trigger; negative is a hard configuration error, never a silent
// fallback -- a negative horizon would delete every row older than now.
func TestEnvIntMatchesProductionRetentionDaysSemantics(t *testing.T) {
	cases := []struct {
		name      string
		lookup    func(string) (string, bool)
		fallback  int
		want      int
		wantError bool
	}{
		{"missing", func(string) (string, bool) { return "", false }, 14, 14, false},
		{"empty", func(string) (string, bool) { return "", true }, 14, 14, false},
		{"whitespace-only", func(string) (string, bool) { return "   ", true }, 14, 14, false},
		{"non-numeric", func(string) (string, bool) { return "nope", true }, 14, 14, false},
		{"zero is a valid horizon, not a fallback trigger", func(string) (string, bool) { return "0", true }, 14, 0, false},
		{"negative is a hard error, never a silent fallback", func(string) (string, bool) { return "-3", true }, 14, 0, true},
		{"valid", func(string) (string, bool) { return "30", true }, 14, 30, false},
		{"trims surrounding whitespace", func(string) (string, bool) { return " 30 ", true }, 14, 30, false},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := envInt(testCase.lookup, "X", testCase.fallback)
			if testCase.wantError {
				if err == nil {
					t.Fatalf("envInt() error = nil, want an error for a negative value")
				}
				return
			}
			if err != nil {
				t.Fatalf("envInt() error = %v, want nil", err)
			}
			if got != testCase.want {
				t.Fatalf("envInt() = %d, want %d", got, testCase.want)
			}
		})
	}
}

func TestWriteReceiptEmitsOneJSONLine(t *testing.T) {
	var stdout bytes.Buffer
	code := writeReceipt(&stdout, map[string]any{"check": "queue-depth"})
	if code != 0 {
		t.Fatalf("code = %d, want 0", code)
	}
	if got := strings.TrimSpace(stdout.String()); got != `{"check":"queue-depth"}` {
		t.Fatalf("stdout = %q", got)
	}
}
