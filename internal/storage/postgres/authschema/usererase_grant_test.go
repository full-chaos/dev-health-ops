package authschema

import (
	"context"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// The erase sets to NULL exactly the RESTRICT and NO ACTION foreign key columns into public.users of the ops schema
// (go-api's forceDeleteUser nulls the same ones, read from the catalog): a key the erase does not null would refuse every
// erase of a user it names, and a column it nulls that is no such key is a write it has no reason for.
func TestUserEraseNullColumnsAreEveryRestrictKeyIntoPublicUsers(t *testing.T) {
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	fk := regexp.MustCompile(`(?m)^ALTER TABLE ONLY public\.(\w+)\s+ADD CONSTRAINT \w+ FOREIGN KEY \((\w+)\) REFERENCES public\.users\(id\)( ON DELETE [A-Z ]+)?;`)
	var restrict []string
	all := 0
	for _, m := range fk.FindAllStringSubmatch(strings.ReplaceAll(baseline.Schema, "\n    ADD CONSTRAINT", " ADD CONSTRAINT"), -1) {
		all++
		switch strings.TrimSpace(m[3]) {
		case "", "ON DELETE RESTRICT", "ON DELETE NO ACTION":
			restrict = append(restrict, m[1]+"."+m[2])
		}
	}
	if all < 10 {
		t.Fatalf("found %d foreign keys into public.users in the baseline, want the full set (the extraction is broken)", all)
	}
	var got []string
	for _, c := range UserEraseNullColumns() {
		got = append(got, c.Table+"."+c.Column)
	}
	sort.Strings(restrict)
	if !slices.Equal(got, restrict) {
		t.Errorf("UserEraseNullColumns = %v, want the baseline RESTRICT / NO ACTION keys %v", got, restrict)
	}
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	ref := regexp.MustCompile(`(?i)REFERENCES\s+(public\.)?"?users"?\s*\(`)
	for _, file := range chain {
		if ref.MatchString(file.SQL) {
			t.Errorf("chain migration %s adds a foreign key into public.users: decide it for the erase (UserEraseNullColumns)", file.Name)
		}
	}
}

// The erase grant while owned: DELETE on public.users, UPDATE on the four null columns, SELECT on the two erase-only
// tables' columns (impersonation_sessions' two are read already), each erase-only table cleared first; it is in the
// migrator's privilege set only then. Off: only the clearing of an erase-only table the role still holds rights on.
func TestUserEraseGrantWhileOwned(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)
	on := Options{Schema: "auth", RuntimeRole: "rt", UsersOwned: true, RequirePublicTables: true}
	got, err := userEraseStatements(ctx, presentQuerier{}, on, logger)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`GRANT DELETE ON TABLE "public"."users" TO "rt"`,
		`REVOKE ALL ON TABLE "public"."billing_audit_log" FROM "rt"`,
		`GRANT UPDATE ("actor_id") ON TABLE "public"."billing_audit_log" TO "rt"`,
		`GRANT SELECT ("actor_id") ON TABLE "public"."billing_audit_log" TO "rt"`,
		`GRANT UPDATE ("admin_user_id", "target_user_id") ON TABLE "public"."impersonation_sessions" TO "rt"`,
		`REVOKE ALL ON TABLE "public"."refunds" FROM "rt"`,
		`GRANT UPDATE ("initiated_by") ON TABLE "public"."refunds" TO "rt"`,
		`GRANT SELECT ("initiated_by") ON TABLE "public"."refunds" TO "rt"`,
	}
	if !slices.Equal(got, want) {
		t.Errorf("statements:\n got %q\nwant %q", got, want)
	}
	off := on
	off.UsersOwned = false
	got, err = userEraseStatements(ctx, presentQuerier{}, off, logger)
	if want := []string{`REVOKE ALL ON TABLE "public"."billing_audit_log" FROM "rt"`, `REVOKE ALL ON TABLE "public"."refunds" FROM "rt"`}; err != nil || !slices.Equal(got, want) {
		t.Errorf("switch off (the role still holds rights): %q, %v; want %q", got, err, want)
	}
	keys := func(set []PublicPrivilege) map[string]bool {
		out := map[string]bool{}
		for _, p := range set {
			if p.Columns == nil {
				out[p.Table+"|"+p.Privilege+"|"] = true
			}
			for _, c := range p.Columns {
				out[p.Table+"|"+p.Privilege+"|"+c] = true
			}
		}
		return out
	}
	erase := []string{"users|DELETE|", "impersonation_sessions|UPDATE|admin_user_id", "impersonation_sessions|UPDATE|target_user_id",
		"billing_audit_log|UPDATE|actor_id", "billing_audit_log|SELECT|actor_id", "refunds|UPDATE|initiated_by", "refunds|SELECT|initiated_by"}
	owned, unowned := keys(publicPrivilegeSetFor(true, false)), keys(publicPrivilegeSetFor(false, false))
	for _, k := range erase {
		if !owned[k] || unowned[k] {
			t.Errorf("%s: in the owned privilege set %v, in the not-owned set %v; want true, false", k, owned[k], unowned[k])
		}
	}
	for _, table := range userEraseOnlyTables {
		if !slices.Contains(PublicAnchorTables(), table) {
			t.Errorf("%s is no anchor table: the grants pass could not clear it", table)
		}
	}
	if !isUserEraseDelete("users") || isUserEraseDelete("memberships") || isUserEraseColumn("refunds", "UPDATE", "amount") ||
		!isUserEraseColumn("impersonation_sessions", "UPDATE", "target_user_id") || isUserEraseColumn("impersonation_sessions", "SELECT", "admin_user_id") {
		t.Error("the erase classifiers do not match the declaration")
	}
}
