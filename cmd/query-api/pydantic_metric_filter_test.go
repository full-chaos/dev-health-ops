package main

import (
	"reflect"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// TestValidateMetricFilterMatchesLiveCapture pins validateMetricFilter
// against live FastAPI TestClient captures of POST /api/v1/investment/
// explain's real 422 body for each malformed nested field (uv run
// python3, get_current_user dependency-overridden; bodies quoted in this
// PR's TEST-EVIDENCE).
func TestValidateMetricFilterMatchesLiveCapture(t *testing.T) {
	loc := []any{"body", "filters"}
	cases := []struct {
		name    string
		filters string
		want    []pydanticErrorDetail
	}{
		{
			name:    "scope.level bad literal",
			filters: `{"scope": {"level": "bogus"}}`,
			want: []pydanticErrorDetail{{
				Type: "literal_error", Loc: []any{"body", "filters", "scope", "level"},
				Msg: "Input should be 'org', 'team', 'repo', 'service' or 'developer'", Input: "bogus",
				Ctx: map[string]string{"expected": "'org', 'team', 'repo', 'service' or 'developer'"},
			}},
		},
		{
			name:    "scope.ids not a list",
			filters: `{"scope": {"ids": "not-a-list"}}`,
			want: []pydanticErrorDetail{{
				Type: "list_type", Loc: []any{"body", "filters", "scope", "ids"},
				Msg: "Input should be a valid list", Input: "not-a-list",
			}},
		},
		{
			name:    "scope.ids element not string",
			filters: `{"scope": {"ids": ["a", 5]}}`,
			want: []pydanticErrorDetail{{
				Type: "string_type", Loc: []any{"body", "filters", "scope", "ids", 1},
				Msg: "Input should be a valid string", Input: pyjson.IntOf(5),
			}},
		},
		{
			name:    "how.blocked bad type",
			filters: `{"how": {"blocked": "maybe"}}`,
			want: []pydanticErrorDetail{{
				Type: "bool_parsing", Loc: []any{"body", "filters", "how", "blocked"},
				Msg: "Input should be a valid boolean, unable to interpret input", Input: "maybe",
			}},
		},
		{
			name:    "how.blocked coerces string true",
			filters: `{"how": {"blocked": "true"}}`,
			want:    nil,
		},
		{
			name:    "time.start_date wrong type (number)",
			filters: `{"time": {"start_date": 123}}`,
			want: []pydanticErrorDetail{{
				Type: "date_from_datetime_inexact", Loc: []any{"body", "filters", "time", "start_date"},
				Msg: "Datetimes provided to dates should have zero time - e.g. be exact dates", Input: pyjson.IntOf(123),
			}},
		},
		{
			name:    "time.range_days bad type",
			filters: `{"time": {"range_days": "abc"}}`,
			want: []pydanticErrorDetail{{
				Type: "int_parsing", Loc: []any{"body", "filters", "time", "range_days"},
				Msg: "Input should be a valid integer, unable to parse string as an integer", Input: "abc",
			}},
		},
		{
			name:    "what.artifacts bad literal",
			filters: `{"what": {"artifacts": ["pr", "bogus"]}}`,
			want: []pydanticErrorDetail{{
				Type: "literal_error", Loc: []any{"body", "filters", "what", "artifacts", 1},
				Msg: "Input should be 'pr', 'issue', 'commit' or 'pipeline'", Input: "bogus",
				Ctx: map[string]string{"expected": "'pr', 'issue', 'commit' or 'pipeline'"},
			}},
		},
		{
			name:    "time not a dict",
			filters: `{"time": "nope"}`,
			want: []pydanticErrorDetail{{
				Type: "model_attributes_type", Loc: []any{"body", "filters", "time"},
				Msg: "Input should be a valid dictionary or object to extract fields from", Input: "nope",
			}},
		},
		{
			name:    "multiple nested errors aggregate in field order",
			filters: `{"scope": {"level": "bogus"}, "how": {"blocked": "maybe"}}`,
			want: []pydanticErrorDetail{
				{
					Type: "literal_error", Loc: []any{"body", "filters", "scope", "level"},
					Msg: "Input should be 'org', 'team', 'repo', 'service' or 'developer'", Input: "bogus",
					Ctx: map[string]string{"expected": "'org', 'team', 'repo', 'service' or 'developer'"},
				},
				{
					Type: "bool_parsing", Loc: []any{"body", "filters", "how", "blocked"},
					Msg: "Input should be a valid boolean, unable to interpret input", Input: "maybe",
				},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filters, err := pyjson.DecodeString(tc.filters)
			if err != nil {
				t.Fatal(err)
			}
			got := validateMetricFilter(loc, filters)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("validateMetricFilter = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestValidateMetricFilterAbsentIsNotAnError pins that a nil "filters"
// value produces no errors -- MetricFilter's own callers own the
// required-ness check separately.
func TestValidateMetricFilterAbsentIsNotAnError(t *testing.T) {
	if got := validateMetricFilter([]any{"body", "filters"}, nil); got != nil {
		t.Fatalf("validateMetricFilter(nil) = %+v, want nil", got)
	}
}

// TestValidateMetricFilterWrongTopLevelType pins the "filters" value
// itself being a non-object.
func TestValidateMetricFilterWrongTopLevelType(t *testing.T) {
	got := validateMetricFilter([]any{"body", "filters"}, "nope")
	want := []pydanticErrorDetail{{
		Type: "model_attributes_type", Loc: []any{"body", "filters"},
		Msg: "Input should be a valid dictionary or object to extract fields from", Input: "nope",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("validateMetricFilter = %+v, want %+v", got, want)
	}
}
