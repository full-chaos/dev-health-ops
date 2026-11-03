package policy

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/pep"
)

type shadowState struct{ enabled, invalid bool }

func recordState(t *testing.T) *[]shadowState {
	t.Helper()
	var got []shadowState
	previous := recordShadowState
	recordShadowState = func(_ context.Context, enabled, invalid bool) { got = append(got, shadowState{enabled, invalid}) }
	t.Cleanup(func() { recordShadowState = previous })
	return &got
}

func envLookup(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) { v, ok := values[key]; return v, ok }
}

func envDeps(logs *bytes.Buffer) ShadowEnvDeps {
	return ShadowEnvDeps{Rows: shadowTestRows, Logger: slog.New(slog.NewTextHandler(logs, nil)),
		NewDecider: func() (pep.Decider, error) {
			return funcDecider{func(context.Context, pep.Request) pep.Decision { return pep.Decision{} }}, nil
		}}
}

func TestShadowFromEnvOffByDefault(t *testing.T) {
	for _, values := range []map[string]string{nil, {EnvShadowPDP: ""}, {EnvShadowPDP: "false"}, {EnvShadowPDP: " 0 "},
		{EnvShadowPDP: "false", EnvShadowPDPSample: "bad", EnvShadowPDPInFlight: "bad", EnvShadowPDPHighRisk: "bad"}} {
		state := recordState(t)
		var logs bytes.Buffer
		if s := ShadowFromEnv(envLookup(values), envDeps(&logs)); s != nil {
			t.Fatalf("%v: a shadow was built", values)
		}
		if logs.Len() != 0 || len(*state) != 1 || (*state)[0] != (shadowState{false, false}) {
			t.Fatalf("%v: logs %q state %v, want no log and off", values, logs.String(), *state)
		}
	}
}

func TestShadowFromEnvOn(t *testing.T) {
	state := recordState(t)
	var logs bytes.Buffer
	s := ShadowFromEnv(envLookup(map[string]string{EnvShadowPDP: "true", EnvShadowPDPHighRisk: "true", EnvShadowPDPSample: "25", EnvShadowPDPInFlight: "7"}), envDeps(&logs))
	if s == nil || !s.highRisk || s.percent != 25 || cap(s.slots) != 7 {
		t.Fatalf("shadow %+v", s)
	}
	if logs.Len() != 0 || len(*state) != 1 || (*state)[0] != (shadowState{true, false}) {
		t.Fatalf("logs %q state %v, want no log and enabled", logs.String(), *state)
	}
	s = ShadowFromEnv(envLookup(map[string]string{EnvShadowPDP: "true"}), envDeps(&logs))
	if s == nil || s.highRisk || s.percent != 100 || cap(s.slots) != shadowDefaultInFlight {
		t.Fatalf("defaults %+v", s)
	}
}

func TestShadowFromEnvBadInputDisablesAndNeverFailsAStart(t *testing.T) {
	on := func(extra map[string]string) map[string]string {
		values := map[string]string{EnvShadowPDP: "true"}
		for k, v := range extra {
			values[k] = v
		}
		return values
	}
	cases := []struct {
		name     string
		values   map[string]string
		mutate   func(*ShadowEnvDeps)
		variable string
		secret   string
	}{
		{"bad switch", map[string]string{EnvShadowPDP: "secret-switch"}, nil, EnvShadowPDP, "secret-switch"},
		{"bad high risk", on(map[string]string{EnvShadowPDPHighRisk: "secret-risk"}), nil, EnvShadowPDPHighRisk, "secret-risk"},
		{"sample word", on(map[string]string{EnvShadowPDPSample: "secret-sample"}), nil, EnvShadowPDPSample, "secret-sample"},
		{"sample zero", on(map[string]string{EnvShadowPDPSample: "0"}), nil, EnvShadowPDPSample, ""},
		{"sample over 100", on(map[string]string{EnvShadowPDPSample: "101"}), nil, EnvShadowPDPSample, ""},
		{"in flight word", on(map[string]string{EnvShadowPDPInFlight: "secret-n"}), nil, EnvShadowPDPInFlight, "secret-n"},
		{"in flight zero", on(map[string]string{EnvShadowPDPInFlight: "0"}), nil, EnvShadowPDPInFlight, ""},
		{"decider fails", on(nil), func(d *ShadowEnvDeps) {
			d.NewDecider = func() (pep.Decider, error) { return nil, errors.New("open /secret/path: denied") }
		}, "decider", "/secret/path"},
		{"no decider factory", on(nil), func(d *ShadowEnvDeps) { d.NewDecider = nil }, "decider", ""},
		{"no rows", on(nil), func(d *ShadowEnvDeps) { d.Rows = nil }, "rows", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := recordState(t)
			var logs bytes.Buffer
			deps := envDeps(&logs)
			if tc.mutate != nil {
				tc.mutate(&deps)
			}
			if s := ShadowFromEnv(envLookup(tc.values), deps); s != nil {
				t.Fatal("a shadow was built from bad input")
			}
			text := logs.String()
			if !strings.Contains(text, "level=ERROR") || !strings.Contains(text, "variable="+tc.variable) {
				t.Fatalf("log %q lacks an ERROR naming %s", text, tc.variable)
			}
			if tc.secret != "" && strings.Contains(text, tc.secret) {
				t.Fatalf("log holds the bad value: %q", text)
			}
			if len(*state) != 1 || (*state)[0] != (shadowState{false, true}) {
				t.Fatalf("state %v, want disabled and config invalid", *state)
			}
		})
	}
}
