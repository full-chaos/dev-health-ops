package pgmigrate

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

func TestTheAuthLineageSearchRunsOnlyWithoutAConfiguredLineage(t *testing.T) {
	for name, testCase := range map[string]struct {
		observation Observation
		want        bool
	}{
		"foreign objects and no configured lineage":      {Observation{Objects: 3}, true},
		"foreign objects and a valid configured lineage": {Observation{Objects: 3, AuthSchema: "auth", AuthObjects: 12}, false},
		"a version table": {Observation{HasVersionTable: true, Objects: 3}, false},
		"no objects":      {Observation{}, false},
		"a valid configured lineage and no other objects":         {Observation{AuthSchema: "auth", AuthObjects: 12}, false},
		"foreign objects and a configured schema with no lineage": {Observation{Objects: 3, AuthObjects: 0}, true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := authLineageSearchWanted(testCase.observation); got != testCase.want {
				t.Fatalf("authLineageSearchWanted(%+v) = %v, want %v", testCase.observation, got, testCase.want)
			}
		})
	}
}

const authExemptionLine = "not counted as foreign objects"

func logLines(logs *bytes.Buffer) int {
	return strings.Count(logs.String(), authExemptionLine)
}

// The exemption line is logged once, after the walk the decision belongs to
// succeeded: a retried walk does not log it twice and a refusal logs none.
func TestTheAuthExemptionLineIsLoggedOnceAfterTheDecision(t *testing.T) {
	exempt := walked{authSchema: "auth", authObjects: 12}
	retried := exempt
	retried.attempted = "0139"
	for name, testCase := range map[string]struct {
		script   []walked
		errs     []error
		recorded map[string]bool
		want     int
	}{
		"one walk":                 {[]walked{exempt}, []error{nil}, nil, 1},
		"a retried walk":           {[]walked{retried, exempt}, []error{errWalk, nil}, map[string]bool{"0139": true}, 1},
		"a refusal":                {[]walked{exempt}, []error{errors.New("refused")}, nil, 0},
		"no exemption in the walk": {[]walked{{}}, []error{nil}, nil, 0},
	} {
		t.Run(name, func(t *testing.T) {
			var logs bytes.Buffer
			walks := &scriptedWalks{t: t, script: testCase.script, errs: testCase.errs, recorded: testCase.recorded}
			_, _ = walkWithRetry(walks.walk, walks.wasRecorded, slog.New(slog.NewTextHandler(&logs, nil)))
			if got := logLines(&logs); got != testCase.want {
				t.Fatalf("the exemption line was logged %d times, want %d: %s", got, testCase.want, logs.String())
			}
		})
	}
}

func TestAnEmptyAuthSchemaValueIsTheDefaultSchema(t *testing.T) {
	for _, value := range []string{"", "   ", "\t"} {
		t.Setenv(AuthSchemaEnv, value)
		if got := configuredAuthName(); got != defaultAuthSchema {
			t.Errorf("AUTH_DATABASE_SCHEMA=%q: configuredAuthName() = %q, want %q", value, got, defaultAuthSchema)
		}
		if got := configuredAuthSchema(); got != defaultAuthSchema {
			t.Errorf("AUTH_DATABASE_SCHEMA=%q: configuredAuthSchema() = %q, want %q", value, got, defaultAuthSchema)
		}
	}
}
