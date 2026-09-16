package main

import (
	"testing"
	"time"
)

// TestParseISODateTimeQueryParamAbsent pins the "" -> present=false, ok=true
// contract parseISODateTimeQueryParam shares with parseISODateQueryParam.
func TestParseISODateTimeQueryParamAbsent(t *testing.T) {
	_, present, ok := parseISODateTimeQueryParam("")
	if present || !ok {
		t.Fatalf("present=%v ok=%v, want false true", present, ok)
	}
}

// TestParseISODateTimeQueryParamAccepts pins every realistic shape
// Pydantic's `datetime` field type accepts -- confirmed live (this route's
// own TEST-EVIDENCE) against pydantic 2.13.4's TypeAdapter(datetime).
func TestParseISODateTimeQueryParamAccepts(t *testing.T) {
	cases := []struct {
		raw  string
		want time.Time
	}{
		{"2024-01-01", time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"2024-01-01T12:00", time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"2024-01-01T12:00:00", time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"2024-01-01T12:00:00Z", time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"2024-01-01T12:00:00+00:00", time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"2024-01-01 12:00:00", time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"2024-01-01t12:00:00", time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
		{"2024-01-01_12:00", time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			got, present, ok := parseISODateTimeQueryParam(tc.raw)
			if !present || !ok {
				t.Fatalf("present=%v ok=%v, want true true", present, ok)
			}
			if !got.Equal(tc.want) {
				t.Fatalf("parseISODateTimeQueryParam(%q) = %v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}

// TestClassifyDateTimeParseErrorReasons pins classifyDateTimeParseError's
// taxonomy against pydantic-core/speedate's own reasons, confirmed live
// (this route's own TEST-EVIDENCE) for every realistic malformed shape:
// the shared date-prefix checks (byte-identical to classifyDateParseError)
// and the datetime-only catch-all for anything past a valid prefix.
func TestClassifyDateTimeParseErrorReasons(t *testing.T) {
	cases := []struct {
		raw  string
		want string
	}{
		{"", "input is too short"},
		{"2024-01", "input is too short"},
		{"not-a-date", "invalid character in year"},
		{"2024x01-01", "invalid date separator, expected `-`"},
		{"2024-13-01", "month value is outside expected range of 1-12"},
		{"2024-01x01", "invalid date separator, expected `-`"},
		{"2024-01-32", "day value is outside expected range"},
		{"2024-02-30", "day value is outside expected range"},
		{"2024-01-01X12:00:00", "unexpected extra characters at the end of the input"},
		{"2024-01-01T", "unexpected extra characters at the end of the input"},
		{"2024-01-01T25:00:00", "unexpected extra characters at the end of the input"},
		{"2024-01-01T12:70:00", "unexpected extra characters at the end of the input"},
		{"2024-01-01Tgarbage", "unexpected extra characters at the end of the input"},
	}
	for _, tc := range cases {
		t.Run(tc.raw, func(t *testing.T) {
			if got := classifyDateTimeParseError(tc.raw); got != tc.want {
				t.Fatalf("classifyDateTimeParseError(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

// TestParseISODateTimeQueryParamRejectsClassifiedInputs pins that every
// classifyDateTimeParseError case above (other than the two "input is too
// short" cases, which are absent/present edge shapes already covered) is
// ALSO one parseISODateTimeQueryParam itself refuses (ok=false) -- the two
// functions must agree on what is malformed.
func TestParseISODateTimeQueryParamRejectsClassifiedInputs(t *testing.T) {
	for _, raw := range []string{
		"not-a-date", "2024x01-01", "2024-13-01", "2024-01x01", "2024-01-32",
		"2024-02-30", "2024-01-01X12:00:00", "2024-01-01T", "2024-01-01T25:00:00",
		"2024-01-01T12:70:00", "2024-01-01Tgarbage",
	} {
		t.Run(raw, func(t *testing.T) {
			_, present, ok := parseISODateTimeQueryParam(raw)
			if !present || ok {
				t.Fatalf("parseISODateTimeQueryParam(%q) = present=%v ok=%v, want true false", raw, present, ok)
			}
		})
	}
}

// TestDateTimeQueryParamErrorShape pins the full pydanticErrorDetail shape
// dateTimeQueryParamError builds, confirmed live against FastAPI's own
// response for a `cursor: datetime | None` query param.
func TestDateTimeQueryParamErrorShape(t *testing.T) {
	got := dateTimeQueryParamError([]any{"query", "cursor"}, "not-a-date")
	if got.Type != "datetime_from_date_parsing" {
		t.Fatalf("Type = %q, want datetime_from_date_parsing", got.Type)
	}
	if got.Msg != "Input should be a valid datetime or date, invalid character in year" {
		t.Fatalf("Msg = %q", got.Msg)
	}
	if got.Input != "not-a-date" {
		t.Fatalf("Input = %v, want not-a-date", got.Input)
	}
	if got.Ctx["error"] != "invalid character in year" {
		t.Fatalf("Ctx = %v", got.Ctx)
	}
	if len(got.Loc) != 2 || got.Loc[0] != "query" || got.Loc[1] != "cursor" {
		t.Fatalf("Loc = %v", got.Loc)
	}
}
