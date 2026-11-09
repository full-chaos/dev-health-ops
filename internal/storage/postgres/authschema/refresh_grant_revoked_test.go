package authschema

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// heldQuerier answers like presentQuerier, except that the runtime role's direct
// privileges on a public table (the aclexplode read) are held.
type heldQuerier struct {
	presentQuerier
	held bool
}

type heldRow struct{ held bool }

func (r heldRow) Scan(dest ...any) error {
	for _, d := range dest {
		if v, ok := d.(*bool); ok {
			*v = r.held
		}
	}
	return nil
}

func (q heldQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, "aclexplode") {
		return heldRow{held: q.held}
	}
	return q.presentQuerier.QueryRow(ctx, sql, args...)
}

// The grants pass grants nothing on the retired public.refresh_tokens and revokes
// what the runtime role still holds there, so a database migrated before the
// removal loses the old grant. Where the role holds nothing it sends no statement
// on the table: the migration login holds no GRANT OPTION there any more.
func TestTheGrantsPassRevokesTheRetiredRefreshTokensGrant(t *testing.T) {
	options := Options{Schema: "auth", RuntimeRole: "rt", UsersOwned: true, RequirePublicTables: true}
	table := `"public"."refresh_tokens"`
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
				t.Errorf("held=%v: the grants pass sends %q on refresh_tokens", held, statement)
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

// retiredRefreshTokensAckDigest is the -ack-grant-narrowing digest of the one
// narrowing this removal makes on a database the live release migrated: the
// runtime role loses its seven column privileges on public.refresh_tokens.
const retiredRefreshTokensAckDigest = "7fe114204925961e"

func retiredRefreshTokensLost() []GrantRecord {
	var lost []GrantRecord
	for _, column := range []string{"token_hash", "user_id", "org_id", "family_id", "expires_at", "revoked_at"} {
		lost = append(lost, GrantRecord{Kind: "column", Object: publicSchema + "." + retiredRefreshTokensTable, Column: column, Privilege: "SELECT"})
	}
	lost = append(lost, GrantRecord{Kind: "column", Object: publicSchema + "." + retiredRefreshTokensTable, Column: "revoked_at", Privilege: "UPDATE"})
	return lost
}

func TestTheRetiredRefreshTokensAckDigest(t *testing.T) {
	lost := retiredRefreshTokensLost()
	before := []GrantRecord{}
	for _, g := range lost {
		g.Object = retiredRefreshTokensTable
		before = append(before, g)
	}
	sorted := narrowedGrants(before, nil)
	for i := range sorted {
		sorted[i].Object = publicSchema + "." + sorted[i].Object
	}
	if got := narrowingDigest(sorted); got != retiredRefreshTokensAckDigest {
		t.Fatalf("digest of the retired refresh_tokens narrowing = %s, want %s", got, retiredRefreshTokensAckDigest)
	}
}
