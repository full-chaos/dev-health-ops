//go:build integration

package policy

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/platformroleseam"
)

// preflipQuery is the file the enforcement turn-on step of docs/operate/runbooks/auth-pdp-outage-and-rollback.md runs.
const preflipQuery = "deploy/scripts/auth_preflip_platform_role_parity.sql"

// The pre-flip read sees the one state where go-api's platform flag (rule.HoldsSQL: the auth principal not disabled)
// and the PDP's snapshot (rule.PublicHoldsSQL: public.users active) disagree, which the D6651 pins keep apart: an
// active platform_admin assignment whose auth principal is disabled while public.users is active (CHAOS-13209, acp:
// flip-gate row 4 = 0 before F7/F8/F9). A clean superuser gives 0 on every check; the disabled copy gives 1 on that
// check and 0 on the others.
func TestThePreflipReadSeesADisabledAuthCopyOfAnActivePlatformAdmin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	query, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(preflipQuery)))
	if err != nil {
		t.Fatal(err)
	}
	env := platformroleseam.Start(ctx, t, true)
	read := func() map[string]int64 {
		t.Helper()
		tx, err := env.Admin.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		rows, err := tx.Query(ctx, string(query))
		if err != nil {
			t.Fatalf("%s: %v", preflipQuery, err)
		}
		got := map[string]int64{}
		for rows.Next() {
			var name string
			var n int64
			if err := rows.Scan(&name, &n); err != nil {
				t.Fatal(err)
			}
			got[name] = n
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return got
	}
	user := env.User(ctx, t, true, true)
	clean := read()
	for _, check := range []string{"superusers_without_platform_admin", "platform_admin_not_superuser", "platform_admin_on_disabled_copy_of_active_user"} {
		if n, ok := clean[check]; !ok || n != 0 {
			t.Errorf("clean state: %s = %d (present %v), want 0", check, n, ok)
		}
	}
	env.Disable(ctx, t, user)
	var active, disabled bool
	if err := env.Admin.QueryRow(ctx, `SELECT u.is_active, p.disabled_at IS NOT NULL FROM public.users u JOIN auth.principals p ON p.id = u.id WHERE u.id = $1`,
		user).Scan(&active, &disabled); err != nil || !active || !disabled {
		t.Fatalf("seed: users.is_active %v, principals.disabled_at set %v, err %v; want both true", active, disabled, err)
	}
	seeded := read()
	if n := seeded["platform_admin_on_disabled_copy_of_active_user"]; n < 1 {
		t.Errorf("seeded state: platform_admin_on_disabled_copy_of_active_user = %d, want >= 1", n)
	}
	for _, check := range []string{"superusers_without_platform_admin", "platform_admin_not_superuser"} {
		if n := seeded[check]; n != 0 {
			t.Errorf("seeded state: %s = %d, want 0", check, n)
		}
	}
}
