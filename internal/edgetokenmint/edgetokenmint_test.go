package edgetokenmint

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	testOrg = "11111111-2222-4333-8444-555555555555"
)

var testNow = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func testPrincipal() Principal {
	return Principal{
		UserID:       ProvePrincipalID,
		Email:        "go-api-prove@service.dev-health.invalid",
		OrgID:        testOrg,
		Role:         "viewer",
		TokenVersion: 3,
	}
}

// fakeRow scans fixed values into the destinations, in order.
type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("fakeRow: column count mismatch")
	}
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.values[i]))
	}
	return nil
}

type fakeQuerier struct {
	row   fakeRow
	calls int
	sql   string
	args  []any
}

func (q *fakeQuerier) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	q.calls++
	q.sql, q.args = sql, args
	return q.row
}

func strPtr(s string) *string { return &s }

// principalRow builds the eight columns lookupSQL selects, for a row the
// minter accepts, with mutate applied.
func principalRow(mutate func(values []any)) fakeRow {
	values := []any{
		ProvePrincipalID,
		"go-api-prove@service.dev-health.invalid",
		true,  // is_active
		false, // is_superuser
		4,     // token_version
		ServiceAuthProvider,
		false, // has a password hash
		strPtr("viewer"),
	}
	if mutate != nil {
		mutate(values)
	}
	return fakeRow{values: values}
}

func TestLookupPrincipalRefusesEveryRowItMustNotMintFor(t *testing.T) {
	for name, tc := range map[string]struct {
		row  fakeRow
		want error
	}{
		"no users row":            {row: fakeRow{err: pgx.ErrNoRows}, want: ErrPrincipalNotFound},
		"a human row":             {row: principalRow(func(v []any) { v[5] = "local" }), want: ErrPrincipalNotService},
		"no auth provider":        {row: principalRow(func(v []any) { v[5] = "" }), want: ErrPrincipalNotService},
		"service with a password": {row: principalRow(func(v []any) { v[6] = true }), want: ErrPrincipalNotService},
		"inactive":                {row: principalRow(func(v []any) { v[2] = false }), want: ErrPrincipalInactive},
		"superuser":               {row: principalRow(func(v []any) { v[3] = true }), want: ErrPrincipalSuperuser},
		"no membership in org":    {row: principalRow(func(v []any) { v[7] = (*string)(nil) }), want: ErrNoMembership},
		"owner in org":            {row: principalRow(func(v []any) { v[7] = strPtr("owner") }), want: ErrRoleNotAllowed},
		"admin in org":            {row: principalRow(func(v []any) { v[7] = strPtr("admin") }), want: ErrRoleNotAllowed},
	} {
		t.Run(name, func(t *testing.T) {
			q := &fakeQuerier{row: tc.row}
			if _, err := LookupPrincipal(context.Background(), q, testOrg, Options{}); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	readErr := errors.New("connection reset")
	if _, err := LookupPrincipal(context.Background(), &fakeQuerier{row: fakeRow{err: readErr}}, testOrg, Options{}); !errors.Is(err, readErr) || errors.Is(err, ErrPrincipalNotFound) {
		t.Fatalf("a read failure must surface as itself, not as a missing row: %v", err)
	}
}

func TestLookupPrincipalReadsTheFixedPrincipalInTheRequestedOrg(t *testing.T) {
	for _, role := range []string{"viewer", "member"} {
		q := &fakeQuerier{row: principalRow(func(v []any) { v[7] = strPtr(role) })}
		principal, err := LookupPrincipal(context.Background(), q, testOrg, Options{})
		if err != nil {
			t.Fatalf("role %s: %v", role, err)
		}
		want := Principal{UserID: ProvePrincipalID, Email: "go-api-prove@service.dev-health.invalid", OrgID: testOrg, Role: role, TokenVersion: 4}
		if principal != want {
			t.Fatalf("principal = %+v, want %+v", principal, want)
		}
		if !reflect.DeepEqual(q.args, []any{ProvePrincipalID, testOrg}) {
			t.Fatalf("bound args = %v, want the fixed principal id then the org", q.args)
		}
		for _, cast := range []string{"u.id = $1::uuid", "m.org_id = $2::uuid"} {
			if !strings.Contains(q.sql, cast) {
				t.Fatalf("lookup SQL lost %q:\n%s", cast, q.sql)
			}
		}
	}
}

func TestLookupPrincipalRefusesANonUUIDOrgWithoutQuerying(t *testing.T) {
	q := &fakeQuerier{row: principalRow(nil)}
	if _, err := LookupPrincipal(context.Background(), q, "70d529e0", Options{}); err == nil {
		t.Fatal("a non-UUID org was accepted")
	}
	if q.calls != 0 {
		t.Fatalf("the database was queried %d time(s) for a malformed org", q.calls)
	}
}

func adminPrincipal() Principal {
	return Principal{
		UserID:       AdminProofPrincipalID,
		Email:        "go-api-admin-prove@service.dev-health.invalid",
		OrgID:        testOrg,
		Role:         "admin",
		TokenVersion: 5,
	}
}

func adminRow(mutate func(values []any)) fakeRow {
	return principalRow(func(v []any) {
		v[0] = AdminProofPrincipalID
		v[1] = "go-api-admin-prove@service.dev-health.invalid"
		v[7] = strPtr("admin")
		if mutate != nil {
			mutate(v)
		}
	})
}

func TestLookupAdminPrincipalReadsItsOwnRowAndRefusesEveryOtherShape(t *testing.T) {
	q := &fakeQuerier{row: adminRow(nil)}
	principal, err := LookupAdminPrincipal(context.Background(), q, testOrg, Options{})
	if err != nil {
		t.Fatalf("LookupAdminPrincipal: %v", err)
	}
	if want := (Principal{UserID: AdminProofPrincipalID, Email: "go-api-admin-prove@service.dev-health.invalid", OrgID: testOrg, Role: "admin", TokenVersion: 4}); principal != want {
		t.Fatalf("principal = %+v, want %+v", principal, want)
	}
	if !reflect.DeepEqual(q.args, []any{AdminProofPrincipalID, testOrg}) {
		t.Fatalf("bound args = %v, want the admin principal id then the org", q.args)
	}

	for name, tc := range map[string]struct {
		row  fakeRow
		want error
	}{
		"no users row":            {row: fakeRow{err: pgx.ErrNoRows}, want: ErrPrincipalNotFound},
		"a human row":             {row: adminRow(func(v []any) { v[5] = "local" }), want: ErrPrincipalNotService},
		"service with a password": {row: adminRow(func(v []any) { v[6] = true }), want: ErrPrincipalNotService},
		"inactive":                {row: adminRow(func(v []any) { v[2] = false }), want: ErrPrincipalInactive},
		"superuser":               {row: adminRow(func(v []any) { v[3] = true }), want: ErrPrincipalSuperuser},
		"no membership in org":    {row: adminRow(func(v []any) { v[7] = (*string)(nil) }), want: ErrNoMembership},
		"viewer in org":           {row: adminRow(func(v []any) { v[7] = strPtr("viewer") }), want: ErrRoleNotAllowed},
		"member in org":           {row: adminRow(func(v []any) { v[7] = strPtr("member") }), want: ErrRoleNotAllowed},
		"owner in org":            {row: adminRow(func(v []any) { v[7] = strPtr("owner") }), want: ErrRoleNotAllowed},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LookupAdminPrincipal(context.Background(), &fakeQuerier{row: tc.row}, testOrg, Options{}); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}

	// The read-level principal's lookup is unchanged: an admin row for it is
	// still refused, so the two principals cannot swap roles.
	if _, err := LookupPrincipal(context.Background(), &fakeQuerier{row: principalRow(func(v []any) { v[7] = strPtr("admin") })}, testOrg, Options{}); !errors.Is(err, ErrRoleNotAllowed) {
		t.Fatalf("the proof principal holding admin: err = %v, want ErrRoleNotAllowed", err)
	}
	// ...and the proof lookup binds the proof principal's id, never the admin's.
	proofQ := &fakeQuerier{row: principalRow(nil)}
	if _, err := LookupPrincipal(context.Background(), proofQ, testOrg, Options{}); err != nil || proofQ.args[0] != ProvePrincipalID {
		t.Fatalf("proof lookup bound %v (err %v), want %s", proofQ.args, err, ProvePrincipalID)
	}
}
