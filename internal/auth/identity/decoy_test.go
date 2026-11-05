package identity

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const decoyAddress = "rehearsal-00000000-0000-4000-8000-000000000000@example.invalid"

// A decoy keeps its internal address only in a rehearsal; every other address goes through the register validator,
// in a rehearsal too.
func TestProvisionEmailSkipsTheValidatorForADecoyInARehearsalOnly(t *testing.T) {
	bg := context.Background()
	rehearsal := audit.WithRehearsal(bg)
	for _, tc := range []struct {
		name  string
		ctx   context.Context
		raw   string
		want  string
		error error
	}{
		{"decoy in a rehearsal", WithDecoyAddress(rehearsal), decoyAddress, decoyAddress, nil},
		{"decoy marked before the rehearsal", audit.WithRehearsal(WithDecoyAddress(bg)), decoyAddress, decoyAddress, nil},
		{"decoy outside a rehearsal", WithDecoyAddress(bg), decoyAddress, "", ErrDecoyOutsideRehearsal},
		{"invalid address in a rehearsal", rehearsal, decoyAddress, "", ErrInvalidEmail},
		{"invalid address", bg, decoyAddress, "", ErrInvalidEmail},
		{"valid address in a rehearsal", rehearsal, "Ada@Example.com", "ada@example.com", nil},
		{"valid address", bg, "Ada@Example.com", "ada@example.com", nil},
	} {
		got, err := provisionEmail(tc.ctx, tc.raw)
		if got != tc.want || !errors.Is(err, tc.error) || (tc.error == nil) != (err == nil) {
			t.Errorf("%s: provisionEmail = %q, %v; want %q, %v", tc.name, got, err, tc.want, tc.error)
		}
	}
}

// A decoy outside a rehearsal is refused before any statement: the pool points at no server.
func TestProvisionLocalRefusesADecoyOutsideARehearsalBeforeAnyStatement(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(Config{Pool: pool, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	hash := "$2b$04$C6UzMDM.H6dfI/f/IKxGhuwM4V2y6W1pYxF1Iy5lC3l3OZ0W6Q2Iu"
	_, err = store.ProvisionLocal(WithDecoyAddress(context.Background()), LocalProvisionRequest{Email: decoyAddress, PasswordHash: &hash, Actor: SelfSignUp(nil)})
	if !errors.Is(err, ErrDecoyOutsideRehearsal) {
		t.Fatalf("ProvisionLocal(decoy, no rehearsal) = %v, want ErrDecoyOutsideRehearsal", err)
	}
}
