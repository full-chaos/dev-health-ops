package externalingest

import (
	"errors"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestParseStoredDictLikeStatusPyParseJSON(t *testing.T) {
	for _, test := range []struct {
		name, raw string
		wantNil   bool
		wantErr   bool
		wantKeys  int
	}{
		{"sql null", ``, true, false, 0},
		{"json null", `null`, true, false, 0},
		{"object keeps order", `{"zeta":1,"alpha":{"b":2,"a":1}}`, false, false, 2},
		{"empty object", `{}`, false, false, 0},
		{"array", `[]`, false, true, 0},
		{"array of pairs", `[["a",1]]`, false, true, 0},
		{"number", `5`, false, true, 0},
		{"true", `true`, false, true, 0},
		{"string", `"abc"`, false, true, 0},
		{"empty string", `""`, false, true, 0},
		{"string holding an object", `"{\"a\":1}"`, false, false, 1},
		{"string holding an empty list", `"[]"`, false, false, 0},
		{"string holding an empty string", `"\"\""`, false, false, 0},
		{"string holding null", `"null"`, false, true, 0},
		{"string holding a number", `"5"`, false, true, 0},
		{"string holding garbage", `"{"`, false, true, 0},
	} {
		got, err := parseStoredDict([]byte(test.raw))
		if (err != nil) != test.wantErr || (err != nil && !errors.Is(err, ErrStoredJSONColumn)) {
			t.Errorf("%s: err = %v, want error %v (ErrStoredJSONColumn)", test.name, err, test.wantErr)
			continue
		}
		if test.wantErr {
			continue
		}
		if (got == nil) != test.wantNil || (got != nil && got.Len() != test.wantKeys) {
			t.Errorf("%s: got %v, want nil=%v keys=%d", test.name, got, test.wantNil, test.wantKeys)
		}
	}
	object, _ := parseStoredDict([]byte(`{"zeta":1,"alpha":2}`))
	if keys := object.Keys(); keys[0] != "zeta" || keys[1] != "alpha" {
		t.Errorf("stored key order lost: %v", keys)
	}
}

func scopeOf(t *testing.T, raw string) *pyjson.Object {
	t.Helper()
	object, err := parseStoredDict([]byte(raw))
	if err != nil || object == nil {
		t.Fatalf("scope %s: %v", raw, err)
	}
	return object
}

func TestRecomputeScopeResponseLikeStatusPy(t *testing.T) {
	for _, test := range []struct {
		name, raw, want string
		wantErr         bool
	}{
		{"empty", `{}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
		{"stored order does not matter", `{"cappedRepos":true,"repoIds":["a"],"teamIds":["t"]}`, `{"repoIds":["a"],"teamIds":["t"],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":false,"cappedRepos":true}`, false},
		{"a str is its characters", `{"repoIds":"ab"}`, `{"repoIds":["a","b"],"teamIds":[],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
		{"a dict is its keys", `{"teamIds":{"x":1,"y":2}}`, `{"repoIds":[],"teamIds":["x","y"],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
		{"falsy values are empty", `{"repoIds":0,"teamIds":false}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
		{"a truthy number is not iterable", `{"repoIds":5}`, ``, true},
		{"true is not iterable", `{"teamIds":true}`, ``, true},
		{"an item that is not a str", `{"repoIds":["a",1]}`, ``, true},
		{"capped is Python truthiness", `{"cappedDays":"x","cappedRepos":[0]}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":true,"cappedRepos":true}`, false},
		{"capped falsy", `{"cappedDays":"","cappedRepos":0}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
		{"a naive datetime is UTC", `{"windowStartedAt":"2026-09-01T00:00:00"}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":"2026-09-01T00:00:00Z","windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
		{"an offset is kept", `{"windowEndedAt":"2026-09-08T00:00:00+05:30"}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":null,"windowEndedAt":"2026-09-08T00:00:00+05:30","cappedDays":false,"cappedRepos":false}`, false},
		{"a date only", `{"windowStartedAt":"2026-09-01"}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":"2026-09-01T00:00:00Z","windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
		{"garbage datetime raises", `{"windowStartedAt":"garbage"}`, ``, true},
		{"a number datetime raises", `{"windowEndedAt":5}`, ``, true},
		{"an empty str datetime raises", `{"windowStartedAt":""}`, ``, true},
		{"null datetime is None", `{"windowStartedAt":null}`, `{"repoIds":[],"teamIds":[],"windowStartedAt":null,"windowEndedAt":null,"cappedDays":false,"cappedRepos":false}`, false},
	} {
		got, err := recomputeScopeResponse(scopeOf(t, test.raw))
		if (err != nil) != test.wantErr || (err != nil && !errors.Is(err, ErrStoredJSONColumn)) {
			t.Errorf("%s: err = %v, want error %v", test.name, err, test.wantErr)
			continue
		}
		if test.wantErr {
			continue
		}
		encoded, _ := pyjson.Marshal(got)
		if string(encoded) != test.want {
			t.Errorf("%s:\n got  %s\n want %s", test.name, encoded, test.want)
		}
	}
	if value, err := recomputeScopeResponse(nil); value != nil || err != nil {
		t.Errorf("no scope is None: got (%v, %v)", value, err)
	}
}
