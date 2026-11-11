//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestRuntimeRoleHoldsNoLegacyLinkTokenPrivilege: over the real ops schema,
// with users owned or not, the runtime role can neither read nor delete go-api's
// two public link-token tables; the old purge grant is a posture violation, and
// the next Apply revokes it.
func TestRuntimeRoleHoldsNoLegacyLinkTokenPrivilege(t *testing.T) {
	for _, owned := range []bool{false, true} {
		t.Run(fmt.Sprintf("users owned %v", owned), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			env := newPublicReadsFixture(t, ctx)
			options := env.options(&bytes.Buffer{})
			options.UsersOwned = owned
			if _, err := Apply(ctx, env.migration, options); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			posture := func() []Violation {
				t.Helper()
				conn, err := env.migration.Acquire(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Release()
				found, err := VerifyRuntimePosture(ctx, conn.Conn(), options)
				if err != nil {
					t.Fatalf("VerifyRuntimePosture: %v", err)
				}
				return found
			}
			denied := func(when string) {
				t.Helper()
				for _, name := range retiredLinkTokenTables {
					table := Quote(mustValidatedIdentifier(name))
					for step, statement := range map[string]string{
						"purge":     fmt.Sprintf(`WITH d AS (DELETE FROM public.%s RETURNING id) SELECT count(*) FROM d`, table),
						"read id":   fmt.Sprintf(`SELECT count(id) FROM public.%s`, table),
						"read hash": fmt.Sprintf(`SELECT token_hash FROM public.%s`, table),
					} {
						if err := env.runtimeStatement(ctx, statement); err == nil || !strings.Contains(err.Error(), "permission denied") {
							t.Errorf("%s, %s %s: err = %v, want permission denied", when, step, name, err)
						}
					}
				}
			}
			if found := posture(); len(found) > 0 {
				t.Fatalf("posture after Apply is not clean:\n%s", describe(found))
			}
			denied("after Apply")

			role := Quote(mustValidatedIdentifier(env.runtimeRole))
			for _, name := range retiredLinkTokenTables {
				table := Quote(mustValidatedIdentifier(name))
				for _, statement := range []string{
					`GRANT DELETE ON public.` + table + ` TO ` + role,
					`GRANT SELECT (id) ON public.` + table + ` TO ` + role,
				} {
					if _, err := env.migration.Exec(ctx, statement); err != nil {
						t.Fatal(err)
					}
				}
			}
			found := describe(posture())
			for _, table := range retiredLinkTokenTables {
				if !strings.Contains(found, table) {
					t.Errorf("the old purge grant on %s was not reported:\n%s", table, found)
				}
			}
			// Revoking the held grant is a narrowing: refused with the pinned digest (the value a roll
			// acknowledges), then applied with it.
			_, err := Apply(ctx, env.migration, options)
			var narrowed *NarrowingError
			if !errors.As(err, &narrowed) {
				t.Fatalf("Apply over the old grant without the acknowledgement: err = %v, want the narrowing refusal", err)
			}
			if narrowed.Digest != retiredLinkTokensAckDigest || len(narrowed.Lost) != len(retiredLinkTokensLost()) {
				t.Fatalf("narrowing = %s, %d lost; want %s, %d lost:\n%v", narrowed.Digest, len(narrowed.Lost), retiredLinkTokensAckDigest, len(retiredLinkTokensLost()), narrowed)
			}
			acknowledged := options
			acknowledged.AcknowledgeNarrowing = retiredLinkTokensAckDigest
			if _, err := Apply(ctx, env.migration, acknowledged); err != nil {
				t.Fatalf("Apply over the old grant with the acknowledgement: %v", err)
			}
			if found := posture(); len(found) > 0 {
				t.Fatalf("posture after the second Apply is not clean:\n%s", describe(found))
			}
			denied("after the second Apply")
		})
	}
}
