package pagereader

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func expectedRow() tokenRow {
	src := uuid.MustParse("11111111-0000-4000-8000-000000000001")
	exp := time.Date(2027, 1, 2, 3, 4, 5, 123456789, time.FixedZone("x", 3600))
	return tokenRow{ID: uuid.MustParse("cccccccc-0000-4000-8000-00000000000c"), OrgID: "aaaaaaaa-0000-4000-8000-00000000000a",
		SourceID: &src, Scopes: []byte(`["schema:read","ingest:write","ingest:write"]`), ExpiresAt: &exp}
}

// The expected state is built with the producer's own mapping (wireIDs, scopeNames, credentialScopes).
func TestExpectedStateMapsAStoredTokenWithTheProducersRules(t *testing.T) {
	got := ExpectedState(expectedRow())
	if got.Unwireable || got.ID != "cccccccc-0000-4000-8000-00000000000c" || got.Organization != "aaaaaaaa-0000-4000-8000-00000000000a" ||
		got.SourceID != "11111111-0000-4000-8000-000000000001" || got.Revoked {
		t.Fatalf("token = %+v", got)
	}
	if !slices.Equal(got.Scopes, []string{"ingest:write", "schema:read"}) {
		t.Fatalf("scopes = %v", got.Scopes)
	}
	if got.ExpiresAt == nil || got.ExpiresAt.Location() != time.UTC || !got.ExpiresAt.Equal(expectedRow().ExpiresAt.UTC()) {
		t.Fatalf("expiry = %v", got.ExpiresAt)
	}
}

func TestExpectedStateOrganizationWideAndRevoked(t *testing.T) {
	row := expectedRow()
	revoked := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	row.SourceID, row.RevokedAt, row.ExpiresAt = nil, &revoked, nil
	got := ExpectedState(row)
	if got.SourceID != "" || !got.Revoked || got.ExpiresAt != nil || got.Unwireable {
		t.Fatalf("token = %+v", got)
	}
}

func TestExpectedStateFlagsRowsTheWireCannotCarry(t *testing.T) {
	for name, mutate := range map[string]func(*tokenRow){
		"org not a uuid":    func(r *tokenRow) { r.OrgID = "not-a-uuid" },
		"org empty":         func(r *tokenRow) { r.OrgID = "" },
		"scopes not a list": func(r *tokenRow) { r.Scopes = []byte(`{"a":1}`) },
		"scopes non-string": func(r *tokenRow) { r.Scopes = []byte(`[1]`) },
		"scopes bad json":   func(r *tokenRow) { r.Scopes = []byte(`[`) },
	} {
		row := expectedRow()
		mutate(&row)
		if got := ExpectedState(row); !got.Unwireable {
			t.Errorf("%s: token = %+v, want Unwireable", name, got)
		}
	}
}

func TestExpectedStateOutputHoldsNoNameOrPrefix(t *testing.T) {
	row := expectedRow()
	row.Name, row.Prefix = "CANARY-NAME", "fcpush_CANARY"
	if out := fmt.Sprintf("%+v", ExpectedState(row)); strings.Contains(out, "CANARY") || out == "" || strings.Contains(out, "{}") {
		t.Fatalf("token text = %q", out)
	}
}
