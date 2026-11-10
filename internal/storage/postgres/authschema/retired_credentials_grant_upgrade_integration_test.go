//go:build integration

package authschema

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

var retiredCredentialsColumns = []string{"id", "service_name", "token_hash", "scopes", "expires_at", "revoked_at"}

// The upgrade from the live release (ops 1c3a3f8c, where PublicReadColumns still listed public.internal_service_credentials):
// after the owner move the migrator holds the old column privileges WITH GRANT OPTION and the anchor privilege, and has
// granted the six SELECT columns to the runtime role (the old grants pass). The grants pass at this tip must take them
// away as an acknowledged narrowing, and the live posture must then hold.
func TestTheLiveReleaseServiceCredentialsGrantLeavesWithTheAcknowledgedNarrowing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	env := newOwnerMoveEnv(t, ctx, false)
	env.move(t, ctx, env.moveOptions(true))

	migrator := Quote(mustValidatedIdentifier(env.migratorRole))
	runtime := Quote(mustValidatedIdentifier(env.runtimeRole))
	selectCols := `SELECT (` + strings.Join(retiredCredentialsColumns, ", ") + `)`
	for _, statement := range []string{
		`GRANT ` + selectCols + ` ON public.internal_service_credentials TO ` + migrator + ` WITH GRANT OPTION`,
		`GRANT ` + PublicAnchorPrivilege + ` ON public.internal_service_credentials TO ` + migrator,
	} {
		if _, err := env.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("live owner pass %q: %v", statement, err)
		}
	}
	if _, err := env.migrator.Exec(ctx, `GRANT `+selectCols+` ON public.internal_service_credentials TO `+runtime); err != nil {
		t.Fatalf("live grants pass: %v", err)
	}
	held := func() []string {
		t.Helper()
		var out []string
		for _, column := range retiredCredentialsColumns {
			var ok bool
			if err := env.admin.QueryRow(ctx, `SELECT has_column_privilege($1, 'public.internal_service_credentials', $2, 'SELECT')`,
				env.runtimeRole, column).Scan(&ok); err != nil {
				t.Fatal(err)
			}
			if ok {
				out = append(out, column)
			}
		}
		return out
	}
	posture := func() []Violation {
		t.Helper()
		conn, err := env.migrator.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Release()
		found, err := VerifyRuntimePosture(ctx, conn.Conn(), Options{Schema: env.schema, RuntimeRole: env.runtimeRole, UsersOwned: true})
		if err != nil {
			t.Fatalf("VerifyRuntimePosture: %v", err)
		}
		return found
	}
	if got := held(); len(got) != len(retiredCredentialsColumns) {
		t.Fatalf("the live release state is not seeded: runtime holds %v", got)
	}

	options := env.options(true)
	_, err := Apply(ctx, env.migrator, options)
	t.Logf("Apply as the migrator without the acknowledgement: err = %v", err)
	var narrowed *NarrowingError
	if !errors.As(err, &narrowed) {
		found := posture()
		var lines bytes.Buffer
		for _, v := range found {
			lines.WriteString("\n  " + v.Kind + " " + v.Schema + "." + v.Object + " " + v.Privilege + ": " + v.Detail)
		}
		t.Logf("VerifyRuntimePosture after that run: %d violation(s); runtime still holds %v on public.internal_service_credentials%s",
			len(found), held(), lines.String())
		t.Fatalf("want the narrowing refusal for the %d column privileges on public.internal_service_credentials, got %v",
			len(retiredCredentialsColumns), err)
	}
	t.Logf("narrowing digest %s, %d lost", narrowed.Digest, len(narrowed.Lost))
	if len(narrowed.Lost) != len(retiredCredentialsColumns) {
		t.Fatalf("narrowing lost %d record(s), want %d:\n%v", len(narrowed.Lost), len(retiredCredentialsColumns), narrowed)
	}
	for _, g := range narrowed.Lost {
		if g.Kind != "column" || g.Object != publicSchema+".internal_service_credentials" || g.Privilege != "SELECT" {
			t.Errorf("unexpected lost record %+v", g)
		}
	}
	if got := held(); len(got) != len(retiredCredentialsColumns) {
		t.Fatalf("a refused run changed the grants: runtime holds %v", got)
	}

	options.AcknowledgeNarrowing = narrowed.Digest
	if _, err := Apply(ctx, env.migrator, options); err != nil {
		t.Fatalf("Apply as the migrator with the acknowledgement: %v", err)
	}
	if got := held(); len(got) != 0 {
		t.Fatalf("after the acknowledged Apply the runtime still holds %v on public.internal_service_credentials", got)
	}
	if found := posture(); len(found) != 0 {
		t.Fatalf("VerifyRuntimePosture after the acknowledged Apply: %v", found)
	}
	if _, err := Apply(ctx, env.migrator, env.options(true)); err != nil {
		t.Fatalf("the next Apply, without the acknowledgement: %v", err)
	}
}
