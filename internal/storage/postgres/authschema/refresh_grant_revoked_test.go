package authschema

import (
	"context"
	"log/slog"
	"strings"
	"testing"
)

// The grants pass revokes the retired legacy-exchange grant on
// public.refresh_tokens and grants nothing on it, so a database migrated
// before the removal loses the old grant.
func TestTheGrantsPassRevokesTheRetiredRefreshTokensGrant(t *testing.T) {
	options := Options{Schema: "auth", RuntimeRole: "rt", UsersOwned: true, RequirePublicTables: true}
	statements, err := publicReadStatements(context.Background(), presentQuerier{}, options, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	table := `"public"."refresh_tokens"`
	var revoked bool
	for _, statement := range statements {
		if strings.HasPrefix(statement, "REVOKE ALL ON TABLE "+table+" FROM ") {
			revoked = true
		}
		if strings.HasPrefix(statement, "GRANT ") && strings.Contains(statement, " ON TABLE "+table+" ") {
			t.Errorf("the grants pass grants on refresh_tokens: %s", statement)
		}
	}
	if !revoked {
		t.Errorf("no REVOKE ALL on %s among %d statements", table, len(statements))
	}
}
