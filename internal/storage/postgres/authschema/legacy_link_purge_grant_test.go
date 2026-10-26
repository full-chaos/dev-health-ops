package authschema

import (
	"reflect"
	"testing"
)

func TestLegacyLinkPurgeGrantsAreTheReviewedTables(t *testing.T) {
	want := []DeleteGrant{
		{Table: "email_verification_tokens", Select: []string{"id"}},
		{Table: "password_reset_tokens", Select: []string{"id"}},
	}
	if got := LegacyLinkPurgeGrants(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LegacyLinkPurgeGrants = %v, want %v", got, want)
	}
	for _, grant := range LegacyLinkPurgeGrants() {
		if err := ValidateIdentifier(grant.Table); err != nil {
			t.Errorf("table %q: %v", grant.Table, err)
		}
	}
}
