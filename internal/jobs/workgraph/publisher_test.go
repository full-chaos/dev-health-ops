package workgraph

import "testing"

func TestSameJSONAcceptsPostgresJSONBFormatting(t *testing.T) {
	t.Parallel()
	left := []byte(`{"from_date": "2026-01-01", "to_date": "2026-01-14"}`)
	right := []byte(`{"from_date":"2026-01-01","to_date":"2026-01-14"}`)
	if !sameJSON(left, right) {
		t.Fatal("equivalent JSONB scope was rejected as a mutated duplicate")
	}
	if sameJSON(left, []byte(`{"from_date":"2026-01-02","to_date":"2026-01-14"}`)) {
		t.Fatal("mutated JSON scope was accepted")
	}
}
