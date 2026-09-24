package httpapi

import (
	"net/url"
	"testing"
)

func TestQueryLastReturnsTheLastOfRepeatedValues(t *testing.T) {
	values := url.Values{"x": {"first", "last"}}
	if got := QueryLast(values, "x"); got != "last" {
		t.Fatalf("QueryLast = %q, want %q", got, "last")
	}
	if got := QueryLast(values, "absent"); got != "" {
		t.Fatalf("QueryLast(absent) = %q, want empty", got)
	}
	if got := QueryLast(url.Values{"x": {""}}, "x"); got != "" {
		t.Fatalf("present-and-empty = %q, want empty string (not treated as absent by the caller)", got)
	}
}

func TestQueryLastPtrDistinguishesAbsentFromPresentEmpty(t *testing.T) {
	if ptr := QueryLastPtr(url.Values{}, "x"); ptr != nil {
		t.Fatalf("absent should be nil, got %v", *ptr)
	}
	ptr := QueryLastPtr(url.Values{"x": {"a", ""}}, "x")
	if ptr == nil || *ptr != "" {
		t.Fatalf("present-and-empty should be a pointer to \"\", got %v", ptr)
	}
	ptr = QueryLastPtr(url.Values{"x": {"first", "second", "third"}}, "x")
	if ptr == nil || *ptr != "third" {
		t.Fatalf("QueryLastPtr = %v, want pointer to %q", ptr, "third")
	}
}
