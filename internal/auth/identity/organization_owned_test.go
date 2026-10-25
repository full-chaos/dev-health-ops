package identity

import (
	"strings"
	"testing"
)

func TestProjectOrganizationOwnedSQLNamesNoPublicTable(t *testing.T) {
	if ownedOrgExistsSQL == "" {
		t.Fatal("the owned-organizations branch has no query")
	}
	if strings.Contains(strings.ToLower(ownedOrgExistsSQL), "public.") {
		t.Fatalf("the owned-organizations query reads a public table: %s", ownedOrgExistsSQL)
	}
	if strings.Contains(strings.ToLower(ownedOrgExistsSQL), "insert") || strings.Contains(strings.ToLower(ownedOrgExistsSQL), "update") {
		t.Fatalf("the owned-organizations query writes: %s", ownedOrgExistsSQL)
	}
}

func TestOwnedProjectionOutcomeLabelIsInTheClosedSet(t *testing.T) {
	if ownedOutcome != "owned" {
		t.Fatalf("ownedOutcome = %q, want owned", ownedOutcome)
	}
}
