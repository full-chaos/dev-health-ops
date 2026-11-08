package identity

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// recordingTx is an AdminTx that records each statement in order; every EXISTS read answers false.
type recordingTx struct{ statements []string }

func (r *recordingTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.statements = append(r.statements, sql)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (r *recordingTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.statements = append(r.statements, sql)
	return falseRow{}
}

type falseRow struct{}

func (falseRow) Scan(dest ...any) error {
	for _, d := range dest {
		if b, ok := d.(*bool); ok {
			*b = false
		}
	}
	return nil
}

// The platform role grant of a create runs inside the create's one Apply, after the principal, auth.users and
// public.users inserts on the same transaction, and its failure is the Apply's failure: audit.Commit then rolls the
// whole transaction back (internal/audit atomicity_integration_test.go), so a failed grant leaves no user and no
// principal. The create never commits the user first and grants after.
func TestAdminCreateGrantFaultFailsTheOneApply(t *testing.T) {
	tx := &recordingTx{}
	fault := errors.New("grant refused")
	p := planAdminCreate(AdminCreateRequest{Email: " Admin@Example.com ", Verified: true})
	granted := 0
	err := applyAdminCreate(context.Background(), tx, `"auth"`, p, nil, func(context.Context) error {
		granted++
		var inserts []string
		for _, s := range tx.statements {
			if strings.HasPrefix(strings.TrimSpace(s), "INSERT INTO") {
				inserts = append(inserts, strings.Fields(s)[2])
			}
		}
		if strings.Join(inserts, " ") != `"auth".principals "auth".users public.users` {
			t.Errorf("the grant ran after inserts %v, want the principal, auth.users and public.users rows first", inserts)
		}
		return fault
	})
	if !errors.Is(err, fault) {
		t.Fatalf("applyAdminCreate = %v, want the grant's failure as the Apply's error", err)
	}
	if granted != 1 {
		t.Fatalf("the grant ran %d times, want once", granted)
	}
}

// The create stores go-api's forms: the email lower-cased and stripped, a blank username as "" and an absent one as
// NULL, the provider "local" only when none was sent (an explicit "" is kept).
func TestPlanAdminCreateKeepsGoAPIForms(t *testing.T) {
	blank, name, empty := "   ", " Bob ", ""
	p := planAdminCreate(AdminCreateRequest{Email: " Bob@Example.COM ", Username: &name})
	if p.Email != "bob@example.com" || p.Username == nil || *p.Username != "bob" || p.Provider != "local" {
		t.Fatalf("plan = email %q username %v provider %q", p.Email, p.Username, p.Provider)
	}
	if p := planAdminCreate(AdminCreateRequest{Email: "a@b", Username: &blank, AuthProvider: &empty}); p.Username == nil || *p.Username != "" || p.Provider != "" {
		t.Fatalf("blank username %v provider %q, want \"\" and \"\"", p.Username, p.Provider)
	}
	if p := planAdminCreate(AdminCreateRequest{Email: "a@b", Username: &empty}); p.Username != nil {
		t.Fatalf("empty username %v, want NULL", p.Username)
	}
}

// The update decides as go-api's updateUser does: an email that is empty or whose lower case is the stored address
// changes nothing; a verified flag equal to the stored one changes nothing.
func TestDecideAdminUpdateComparesInTheWritePathForm(t *testing.T) {
	cur := AdminUpdateCurrent{Email: "a@x.test", Verified: false}
	upper, empty, other := "A@X.test", "", " New@X.test"
	yes, no := true, false
	if d := DecideAdminUpdate(cur, &upper, &no); !d.Empty() {
		t.Errorf("same address in upper case and the same flag: %+v, want no change", d)
	}
	if d := DecideAdminUpdate(cur, &empty, nil); !d.Empty() {
		t.Errorf("empty address: %+v, want no change", d)
	}
	d := DecideAdminUpdate(cur, &other, &yes)
	if d.Email == nil || *d.Email != "new@x.test" || d.Verified == nil || !*d.Verified {
		t.Errorf("new address and verified: %+v, want new@x.test and true", d)
	}
}

// scriptedTx answers each QueryRow with the next bool and each Exec with the next command tag.
type scriptedTx struct {
	bools []bool
	tags  []string
}

func (s *scriptedTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tag := "UPDATE 1"
	if len(s.tags) > 0 {
		tag, s.tags = s.tags[0], s.tags[1:]
	}
	return pgconn.NewCommandTag(tag), nil
}

func (s *scriptedTx) QueryRow(context.Context, string, ...any) pgx.Row {
	value := true
	if len(s.bools) > 0 {
		value, s.bools = s.bools[0], s.bools[1:]
	}
	return scriptedRow{value}
}

type scriptedRow struct{ value bool }

func (r scriptedRow) Scan(dest ...any) error {
	for _, d := range dest {
		switch v := d.(type) {
		case *bool:
			*v = r.value
		case *uuid.UUID:
			*v = uuid.Nil
		}
	}
	return nil
}

// AlreadyDone is go-api's: the user was inactive and no refresh token was left to revoke. An active user, or one with
// a live refresh token, is not already done.
func TestAdminSoftDeleteAlreadyDoneIsGoAPIs(t *testing.T) {
	for _, c := range []struct {
		active   bool
		revoked  string
		wantDone bool
	}{{false, "UPDATE 0", true}, {false, "UPDATE 2", false}, {true, "UPDATE 0", false}} {
		tx := &scriptedTx{bools: []bool{true, c.active}, tags: []string{c.revoked}}
		got, err := applyAdminSoftDelete(context.Background(), tx, `"auth"`, uuid.New())
		if err != nil {
			t.Fatal(err)
		}
		if got.WasActive != c.active || got.AlreadyDone != c.wantDone {
			t.Errorf("active %v revoked %s: %+v, want already_done %v", c.active, c.revoked, got, c.wantDone)
		}
	}
}
