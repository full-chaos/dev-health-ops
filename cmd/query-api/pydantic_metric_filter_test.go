package main

import (
	"reflect"
	"testing"
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
		filters map[string]any
		want    []pydanticErrorDetail
	}{
		{
			name:    "scope.level bad literal",
			filters: map[string]any{"scope": map[string]any{"level": "bogus"}},
			want: []pydanticErrorDetail{{
				Type: "literal_error", Loc: []any{"body", "filters", "scope", "level"},
				Msg: "Input should be 'org', 'team', 'repo', 'service' or 'developer'", Input: "bogus",
				Ctx: map[string]string{"expected": "'org', 'team', 'repo', 'service' or 'developer'"},
			}},
		},
		{
			name:    "scope.ids not a list",
			filters: map[string]any{"scope": map[string]any{"ids": "not-a-list"}},
			want: []pydanticErrorDetail{{
				Type: "list_type", Loc: []any{"body", "filters", "scope", "ids"},
				Msg: "Input should be a valid list", Input: "not-a-list",
			}},
		},
		{
			name:    "scope.ids element not string",
			filters: map[string]any{"scope": map[string]any{"ids": []any{"a", float64(5)}}},
			want: []pydanticErrorDetail{{
				Type: "string_type", Loc: []any{"body", "filters", "scope", "ids", 1},
				Msg: "Input should be a valid string", Input: float64(5),
			}},
		},
		{
			name:    "how.blocked bad type",
			filters: map[string]any{"how": map[string]any{"blocked": "maybe"}},
			want: []pydanticErrorDetail{{
				Type: "bool_parsing", Loc: []any{"body", "filters", "how", "blocked"},
				Msg: "Input should be a valid boolean, unable to interpret input", Input: "maybe",
			}},
		},
		{
			name:    "how.blocked coerces string true",
			filters: map[string]any{"how": map[string]any{"blocked": "true"}},
			want:    nil,
		},
		{
			name:    "time.start_date wrong type (number)",
			filters: map[string]any{"time": map[string]any{"start_date": float64(123)}},
			want: []pydanticErrorDetail{{
				Type: "date_from_datetime_inexact", Loc: []any{"body", "filters", "time", "start_date"},
				Msg: "Datetimes provided to dates should have zero time - e.g. be exact dates", Input: float64(123),
			}},
		},
		{
			name:    "time.range_days bad type",
			filters: map[string]any{"time": map[string]any{"range_days": "abc"}},
			want: []pydanticErrorDetail{{
				Type: "int_parsing", Loc: []any{"body", "filters", "time", "range_days"},
				Msg: "Input should be a valid integer, unable to parse string as an integer", Input: "abc",
			}},
		},
		{
			name:    "what.artifacts bad literal",
			filters: map[string]any{"what": map[string]any{"artifacts": []any{"pr", "bogus"}}},
			want: []pydanticErrorDetail{{
				Type: "literal_error", Loc: []any{"body", "filters", "what", "artifacts", 1},
				Msg: "Input should be 'pr', 'issue', 'commit' or 'pipeline'", Input: "bogus",
				Ctx: map[string]string{"expected": "'pr', 'issue', 'commit' or 'pipeline'"},
			}},
		},
		{
			name:    "time not a dict",
			filters: map[string]any{"time": "nope"},
			want: []pydanticErrorDetail{{
				Type: "model_attributes_type", Loc: []any{"body", "filters", "time"},
				Msg: "Input should be a valid dictionary or object to extract fields from", Input: "nope",
			}},
		},
		{
			name:    "multiple nested errors aggregate in field order",
			filters: map[string]any{"scope": map[string]any{"level": "bogus"}, "how": map[string]any{"blocked": "maybe"}},
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
			got := validateMetricFilter(loc, map[string]any(tc.filters))
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
