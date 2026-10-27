//go:build integration

package pgmigrate_test

import (
	"context"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Revision 0160 adds two nullable users columns for the SAML NameID parts. Expand-only: rows that exist before it
// keep NULL in both, so "never captured" is readable.
func TestRevision0160AddsTheNameIDColumnsAndLeavesExistingRowsNull(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	idx := -1
	for i, file := range chain {
		if file.Revision == "0160" {
			idx = i
		}
	}
	if idx < 1 {
		t.Fatal("the chain holds no revision 0160")
	}
	d := newDownInstance(t)
	conn := connect(t, d.at(t, idx-1))
	ctx := context.Background()
	s := scopeSeed{ctx: ctx, conn: conn, t: t}
	existing := s.user("saml", strptr("sub-old"))

	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain[:idx+1]); err != nil {
		t.Fatalf("upgrade to 0160: %v", err)
	}
	var format, qualifier *string
	if err := conn.QueryRow(ctx, `SELECT saml_nameid_format, saml_sp_name_qualifier FROM users WHERE id = $1`, existing).Scan(&format, &qualifier); err != nil {
		t.Fatal(err)
	}
	if format != nil || qualifier != nil {
		t.Fatalf("an existing row must keep NULL: %v %v", format, qualifier)
	}
	var columns int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'users' AND column_name IN ('saml_nameid_format', 'saml_sp_name_qualifier') AND is_nullable = 'YES' AND column_default IS NULL`).Scan(&columns); err != nil || columns != 2 {
		t.Fatalf("want 2 nullable no-default columns, got %d (%v)", columns, err)
	}
}
