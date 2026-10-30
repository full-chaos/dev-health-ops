package sso

import (
	"regexp"
	"slices"
	"strings"
	"testing"
)

// A provider failure record writes last_error, last_error_at and updated_at, and nothing else: the provider's
// status is never part of it.
func TestTheProviderFailureRecordLeavesTheStatusAsItWas(t *testing.T) {
	m := regexp.MustCompile(`(?is)^\s*UPDATE\s+sso_providers\s+SET\s+(.*?)\s+WHERE\s+id\s*=\s*\$1::uuid\s*$`).FindStringSubmatch(recordProviderFailureSQL)
	if m == nil {
		t.Fatalf("not one UPDATE of sso_providers by id: %q", recordProviderFailureSQL)
	}
	var columns []string
	for _, assignment := range strings.Split(m[1], ",") {
		columns = append(columns, strings.TrimSpace(strings.SplitN(assignment, "=", 2)[0]))
	}
	slices.Sort(columns)
	if want := []string{"last_error", "last_error_at", "updated_at"}; !slices.Equal(columns, want) {
		t.Fatalf("SET columns %v, want %v", columns, want)
	}
}
