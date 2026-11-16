//go:build integration

package authschema

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// userEraseOptionItem is one privilege the user erase grant needs the migrator to hold: the grant options of the
// erase (rows 1-7 of OPSTEP-13380) and the anchors of its two new tables (rows 8-9, no grant option).
type userEraseOptionItem struct {
	name   string
	revoke string // run as the table owner; it takes exactly this item from the migrator
	gap    string // the precheck's gap line; empty for an anchor
	table  string
}

func userEraseOptionItems(migrator string) []userEraseOptionItem {
	to := Quote(mustValidatedIdentifier(migrator))
	option := func(privilege, column, table string) userEraseOptionItem {
		target := "TABLE public." + table
		gap := privilege + " ON public." + table
		on := privilege
		if column != "" {
			on = privilege + " (" + column + ")"
			gap = privilege + " (" + column + ") ON public." + table
		}
		return userEraseOptionItem{name: gap, revoke: "REVOKE GRANT OPTION FOR " + on + " ON " + target + " FROM " + to + " CASCADE", gap: gap, table: table}
	}
	anchor := func(table string) userEraseOptionItem {
		return userEraseOptionItem{name: "REFERENCES anchor ON public." + table, revoke: "REVOKE REFERENCES ON TABLE public." + table + " FROM " + to + " CASCADE", table: table}
	}
	return []userEraseOptionItem{
		option("DELETE", "", "users"),
		option("UPDATE", "admin_user_id", "impersonation_sessions"),
		option("UPDATE", "target_user_id", "impersonation_sessions"),
		option("UPDATE", "actor_id", "billing_audit_log"),
		option("SELECT", "actor_id", "billing_audit_log"),
		option("UPDATE", "initiated_by", "refunds"),
		option("SELECT", "initiated_by", "refunds"),
		anchor("billing_audit_log"),
		anchor("refunds"),
	}
}

// G2 (PLAN-13380 S5, OPSTEP-13380-grantopt-1): the prod migrator holds the grant options of the set before the erase
// and none of the erase's (the prod read of 11-16). The grants pass then refuses, naming exactly the seven, and grants
// nothing; `owner-move -grants-only -apply` as the superuser gives them and the two anchors; the grants pass then passes
// and the runtime role holds the erase. With any one of the nine taken away again the grants pass refuses.
func TestGrantsPassNeedsTheUserEraseGrantOptions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))
	options := env.options(true)
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply as the migrator with the whole set: %v", err)
	}
	items := userEraseOptionItems(env.migratorRole)
	takeAway := func(t *testing.T, list []userEraseOptionItem) {
		t.Helper()
		for _, item := range list {
			if _, err := env.old.Exec(ctx, item.revoke); err != nil {
				t.Fatalf("%s: %v", item.revoke, err)
			}
		}
	}
	grantsOnly := func(t *testing.T) {
		t.Helper()
		o := env.moveOptions(true)
		o.GrantsOnly = true
		env.move(t, ctx, o)
	}
	var wantGaps []string
	for _, item := range items {
		if item.gap != "" {
			wantGaps = append(wantGaps, item.gap)
		}
	}
	slices.Sort(wantGaps)

	takeAway(t, items)
	before := env.aclSnapshot(t, ctx)
	_, err := Grants(ctx, env.migrator, options)
	if err == nil {
		t.Fatal("the grants pass passed with the prod migrator state (no erase grant option)")
	}
	requireRemedy(t, env, err)
	if got := refusedGapLines(err); !slices.Equal(got, wantGaps) {
		t.Fatalf("the refusal lists %v, want exactly %v", got, wantGaps)
	}
	requireNothingGranted(t, before, env.aclSnapshot(t, ctx))

	grantsOnly(t)
	for _, item := range items {
		var held bool
		column, privilege := "", ""
		switch {
		case item.gap == "":
			if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'public.'||$2, 'REFERENCES')`, env.migratorRole, item.table).Scan(&held); err != nil {
				t.Fatal(err)
			}
		case strings.Contains(item.gap, "("):
			privilege, column = item.gap[:strings.Index(item.gap, " (")], item.gap[strings.Index(item.gap, "(")+1:strings.Index(item.gap, ")")]
			if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.'||$2, $3, $4)`, env.migratorRole, item.table, column,
				privilege+" WITH GRANT OPTION").Scan(&held); err != nil {
				t.Fatal(err)
			}
		default:
			if err := env.admin.QueryRow(ctx, `SELECT has_table_privilege($1, 'public.'||$2, 'DELETE WITH GRANT OPTION')`, env.migratorRole, item.table).Scan(&held); err != nil {
				t.Fatal(err)
			}
		}
		if !held {
			t.Errorf("after owner-move -grants-only -apply the migrator lacks %s", item.name)
		}
	}
	if _, err := Grants(ctx, env.migrator, options); err != nil {
		t.Fatalf("the grants pass after the grants-only apply: %v", err)
	}
	for _, statement := range []string{
		`DELETE FROM public.users WHERE false`,
		`UPDATE public.impersonation_sessions SET admin_user_id = NULL, target_user_id = NULL WHERE admin_user_id = gen_random_uuid()`,
		`UPDATE public.billing_audit_log SET actor_id = NULL WHERE actor_id = gen_random_uuid()`,
		`UPDATE public.refunds SET initiated_by = NULL WHERE initiated_by = gen_random_uuid()`,
	} {
		if _, err := env.runtime.Exec(ctx, statement); err != nil {
			t.Errorf("the runtime role after the grants pass: %s: %v", statement, err)
		}
	}

	for _, item := range items {
		t.Run(item.name, func(t *testing.T) {
			takeAway(t, []userEraseOptionItem{item})
			t.Cleanup(func() { grantsOnly(t) })
			_, err := Grants(ctx, env.migrator, options)
			if err == nil {
				t.Fatalf("the grants pass passed without %s", item.name)
			}
			if item.gap == "" {
				if !strings.Contains(err.Error(), "permission denied") {
					t.Fatalf("without %s: %v, want the REVOKE ALL of public.%s refused (permission denied)", item.name, err, item.table)
				}
				return
			}
			if got := refusedGapLines(err); !slices.Equal(got, []string{item.gap}) {
				t.Fatalf("without %s the refusal lists %v, want exactly that item", item.name, got)
			}
		})
	}
}
