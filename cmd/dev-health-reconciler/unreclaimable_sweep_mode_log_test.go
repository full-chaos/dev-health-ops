package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
)

// TestBuildUnreclaimableSweepLogsTheResolvedMode pins the startup line.
//
// Before it existed, the ONLY way to learn which mode a deployment was running
// in was to read the body of a WARN the sweep emits solely when it has
// candidates. So a shadow deployment with nothing stranded, an active one, and
// a deployment nobody had ever configured all looked identical -- and every
// production deployment was in that third state, which is why CHAOS-4005's
// safety net had been observation-only since it shipped without anyone
// noticing.
//
// `source` is asserted alongside `mode` because the two demand different
// responses: `configured` is a decision somebody made and can re-examine,
// `default` is a shape nobody has looked at.
//
// RED CONTROL: on the parent commit this function emits nothing at all, so
// every subtest below fails on an empty buffer.
func TestBuildUnreclaimableSweepLogsTheResolvedMode(t *testing.T) {
	for _, testCase := range []struct {
		name       string
		configured string
		wantMode   string
		wantSource string
	}{
		{
			name:       "off is configured",
			configured: "off",
			wantMode:   "off",
			wantSource: "configured",
		},
		{
			name:       "active is configured",
			configured: "active",
			wantMode:   "active",
			wantSource: "configured",
		},
		{
			// The state every production deployment was in: nothing set it,
			// so the compiled fallback answered and nothing said so.
			name:       "unset falls back and says so",
			configured: "",
			wantMode:   "shadow",
			wantSource: "default",
		},
		{
			// Whitespace is not a configuration. Treating "   " as configured
			// would report an operator decision that nobody made.
			name:       "whitespace is not a configuration",
			configured: "   ",
			wantMode:   "shadow",
			wantSource: "default",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var captured bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&captured, &slog.HandlerOptions{
				Level: slog.LevelDebug,
			})))
			t.Cleanup(func() { slog.SetDefault(previous) })

			// Pools are nil on purpose. The log is emitted BEFORE the sweep is
			// constructed, precisely so an operator learns the mode even when
			// wiring later fails -- the returned error is therefore not the
			// subject here and is deliberately ignored.
			_, _ = buildUnreclaimableSweep(nil, nil, nil, "river", config.Config{
				UnreclaimableSweepMode: testCase.configured,
			})

			var line struct {
				Message string `json:"msg"`
				Mode    string `json:"mode"`
				Source  string `json:"source"`
			}
			found := false
			for _, raw := range bytes.Split(bytes.TrimSpace(captured.Bytes()), []byte("\n")) {
				if len(raw) == 0 {
					continue
				}
				if err := json.Unmarshal(raw, &line); err != nil {
					t.Fatalf("log line %q is not JSON: %v", raw, err)
				}
				if line.Message == "syncreconciler.unreclaimable_sweep_mode_resolved" {
					found = true
					break
				}
			}
			if !found {
				t.Fatalf("no resolved-mode line in %q", captured.String())
			}
			if line.Mode != testCase.wantMode {
				t.Fatalf("mode = %q, want %q", line.Mode, testCase.wantMode)
			}
			if line.Source != testCase.wantSource {
				t.Fatalf("source = %q, want %q -- an unconfigured deployment and a deliberately "+
					"chosen one must not read identically", line.Source, testCase.wantSource)
			}
		})
	}
}

// An unparseable value must still be a hard startup error, not a quiet
// fallback: "active" is an assertion about the deployment and a typo must
// never quietly become one, nor quietly disable the safety net.
func TestBuildUnreclaimableSweepStillRejectsAnUnknownMode(t *testing.T) {
	if _, err := buildUnreclaimableSweep(nil, nil, nil, "river", config.Config{
		UnreclaimableSweepMode: "activ",
	}); err == nil {
		t.Fatal("a typo was accepted; ParseSweepMode's rejection must not have been softened by the logging change")
	}
}
