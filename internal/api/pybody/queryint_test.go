package pybody

import (
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func TestQueryIntKeepsAnUnboundedValueExact(t *testing.T) {
	var errs Errors
	raw := "9223372036854775808"
	zero := int64(0)
	value, ok := errs.QueryInt("offset", &raw, 0, &zero, nil)
	if !ok || value.String() != raw || value.IsInt64() {
		t.Fatalf("value %v ok %v: want the exact value past int64", value, ok)
	}
	value, ok = errs.QueryInt("offset", nil, 7, &zero, nil)
	if !ok || value.Int64() != 7 {
		t.Fatalf("absent: %v %v", value, ok)
	}
}

// The expected objects are pydantic 2 output for the same inputs, taken from
// TypeAdapter(datetime).validate_python.
func TestQueryDatetimeErrorsAreFastAPIs(t *testing.T) {
	var errs Errors
	for _, raw := range []string{"2026-09-23", "x", "2026-13-01"} {
		value := raw
		errs.QueryDatetime("from", &value)
	}
	if parsed, ok := errs.QueryDatetime("to", nil); parsed != nil || !ok {
		t.Fatalf("absent: %v %v", parsed, ok)
	}
	got, err := pyjson.Marshal(Detail(errs))
	if err != nil {
		t.Fatal(err)
	}
	want := `{"detail":[` +
		`{"type":"datetime_from_date_parsing","loc":["query","from"],"msg":"Input should be a valid datetime or date, input is too short","input":"x","ctx":{"error":"input is too short"}},` +
		`{"type":"datetime_from_date_parsing","loc":["query","from"],"msg":"Input should be a valid datetime or date, month value is outside expected range of 1-12","input":"2026-13-01","ctx":{"error":"month value is outside expected range of 1-12"}}]}`
	if string(got) != want {
		t.Fatalf("got  %s\nwant %s", got, want)
	}
}
