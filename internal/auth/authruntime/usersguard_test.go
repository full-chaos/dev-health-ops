package authruntime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type countsRow struct {
	values [4]int64
	err    error
}

func (r countsRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		if c, ok := d.(*string); ok {
			*c = "[]"
			continue
		}
		*(d.(*int64)) = r.values[i]
	}
	return nil
}

type countsDB struct{ row countsRow }

func (d countsDB) QueryRow(context.Context, string, ...any) pgx.Row { return d.row }

func TestCheckUsersOwnedNamesEveryGapAndOnlyCounts(t *testing.T) {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkUsersOwned(context.Background(), countsDB{countsRow{}}, schema); err != nil {
		t.Fatalf("no gap: %v", err)
	}
	for name, values := range map[string][4]int64{
		"public user without an auth user": {2, 0, 0, 0},
		"auth user without a public user":  {0, 3, 0, 0},
		"provider user without identity":   {0, 0, 4, 0},
		"all three":                        {1, 2, 3, 0},
		"unverifiable SAML subjects":       {0, 0, 2, 2},
	} {
		err := checkUsersOwned(context.Background(), countsDB{countsRow{values: values}}, schema)
		if !errors.Is(err, ErrUsersOwnedNotReady) {
			t.Fatalf("%s: err = %v, want ErrUsersOwnedNotReady", name, err)
		}
		if !strings.Contains(err.Error(), "without an auth user=") || !strings.Contains(err.Error(), "external identity=") ||
			!strings.Contains(err.Error(), fmt.Sprintf("(sso_subject_unverifiable=%d)", values[3])) {
			t.Fatalf("%s: the refusal does not name the counts: %v", name, err)
		}
	}
	err = checkUsersOwned(context.Background(), countsDB{countsRow{err: errors.New("password=hunter2 host=db")}}, schema)
	if err == nil || strings.Contains(err.Error(), "hunter2") {
		t.Fatalf("a query failure must not carry the driver text: %v", err)
	}
}

type recordingDB struct{ sql *string }

func (d recordingDB) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	*d.sql = sql
	return countsRow{}
}

func TestGuardCountsUseTheSharedSSORelinkDefinition(t *testing.T) {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	if err := checkUsersOwned(context.Background(), recordingDB{&sql}, schema); err != nil {
		t.Fatal(err)
	}
	if want := "AND NOT " + identity.SSORelinkableSQL("u"); !strings.Contains(sql, want) {
		t.Fatalf("the guard's identity count does not exclude the shared SSO relink predicate %q:\n%s", want, sql)
	}
	if want := "WHERE " + identity.SSOSubjectUnverifiableSQL("u"); !strings.Contains(sql, want) {
		t.Fatalf("the guard does not count the shared sso_subject_unverifiable bucket %q:\n%s", want, sql)
	}
}

func TestGuardCountsUseTheSharedPlaceholderDefinition(t *testing.T) {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	var sql string
	if err := checkUsersOwned(context.Background(), recordingDB{&sql}, schema); err != nil {
		t.Fatal(err)
	}
	if want := "AND NOT " + identity.ServicePlaceholderSQL("u"); !strings.Contains(sql, want) {
		t.Fatalf("the guard's identity count does not exclude the shared placeholder predicate %q:\n%s", want, sql)
	}
	if strings.Count(sql, "'service'") != 1 {
		t.Fatalf("the guard names the placeholder literal outside the shared predicate:\n%s", sql)
	}
}
