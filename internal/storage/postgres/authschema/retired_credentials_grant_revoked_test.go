package authschema

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// The grants pass grants nothing on the retired public.internal_service_credentials and revokes what the runtime role
// still holds there (the column grant PublicReadColumns gave before the table was retired), so a database the live
// release migrated loses it. Where the role holds nothing it sends no statement on the table.
func TestTheGrantsPassRevokesTheRetiredServiceCredentialsGrant(t *testing.T) {
	options := Options{Schema: "auth", RuntimeRole: "rt", UsersOwned: true, RequirePublicTables: true}
	table := `"public"."internal_service_credentials"`
	for _, held := range []bool{true, false} {
		statements, err := publicReadStatements(context.Background(), heldQuerier{held: held}, options, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		revoked := 0
		for _, statement := range statements {
			if !strings.Contains(statement, " "+table+" ") && !strings.HasSuffix(statement, " "+table) {
				continue
			}
			if !strings.HasPrefix(statement, "REVOKE ALL ON TABLE "+table+" FROM ") {
				t.Errorf("held=%v: the grants pass sends %q on internal_service_credentials", held, statement)
				continue
			}
			revoked++
		}
		want := 0
		if held {
			want = 1
		}
		if revoked != want {
			t.Errorf("held=%v: %d REVOKE ALL on %s among %d statements, want %d", held, revoked, table, len(statements), want)
		}
	}
}

// retiredServiceCredentialsAckDigest is the -ack-grant-narrowing digest of the one narrowing this removal makes on a
// database the live release migrated: the runtime role loses its six SELECT column privileges on
// public.internal_service_credentials.
const retiredServiceCredentialsAckDigest = "24c190cdacb39062"

func retiredServiceCredentialsLost() []GrantRecord {
	var lost []GrantRecord
	for _, column := range []string{"id", "service_name", "token_hash", "scopes", "expires_at", "revoked_at"} {
		lost = append(lost, GrantRecord{Kind: "column", Object: publicSchema + ".internal_service_credentials", Column: column, Privilege: "SELECT"})
	}
	return lost
}

func TestTheRetiredServiceCredentialsAckDigest(t *testing.T) {
	before := []GrantRecord{}
	for _, g := range retiredServiceCredentialsLost() {
		g.Object = "internal_service_credentials"
		before = append(before, g)
	}
	sorted := narrowedGrants(before, nil)
	for i := range sorted {
		sorted[i].Object = publicSchema + "." + sorted[i].Object
	}
	if got := narrowingDigest(sorted); got != retiredServiceCredentialsAckDigest {
		t.Fatalf("digest of the retired internal_service_credentials narrowing = %s, want %s", got, retiredServiceCredentialsAckDigest)
	}
}
