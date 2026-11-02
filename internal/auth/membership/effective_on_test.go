package membership

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, v := range r.values {
		switch d := dest[i].(type) {
		case **string:
			if v == nil {
				*d = nil
			} else {
				s := v.(string)
				*d = &s
			}
		case *string:
			*d = v.(string)
		}
	}
	return nil
}

// fakePair answers EffectiveOn's two reads: the public row, then the auth row.
type fakePair struct{ public, auth fakeRow }

func (f fakePair) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	if strings.Contains(sql, "public.memberships") {
		return f.public
	}
	return f.auth
}

func str(s string) any { return s }

func TestEffectiveOnAppliesTheEffectiveRule(t *testing.T) {
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	none := fakeRow{err: pgx.ErrNoRows}
	cases := map[string]struct {
		pair fakePair
		want Effective
	}{
		"public row wins":            {fakePair{fakeRow{values: []any{str("admin")}}, fakeRow{values: []any{"suspended", str("member")}}}, Effective{StatusActive, "admin", true}},
		"public row, no auth row":    {fakePair{fakeRow{values: []any{str("owner")}}, none}, Effective{StatusActive, "owner", true}},
		"no rows":                    {fakePair{none, none}, Effective{}},
		"auth invited only":          {fakePair{none, fakeRow{values: []any{"invited", str("member")}}}, Effective{StatusInvited, "member", true}},
		"auth suspended only":        {fakePair{none, fakeRow{values: []any{"suspended", str("admin")}}}, Effective{StatusSuspended, "admin", true}},
		"active auth row, no public": {fakePair{none, fakeRow{values: []any{"active", str("admin")}}}, Effective{StatusRemoved, "admin", true}},
		// effective() takes a *string: a public row with a NULL role is no
		// public row, so the active auth row reads as removed.
		"NULL-role public row and an active auth row": {fakePair{fakeRow{values: []any{nil}}, fakeRow{values: []any{"active", str("admin")}}}, Effective{StatusRemoved, "admin", true}},
	}
	for name, c := range cases {
		got, err := EffectiveOn(context.Background(), c.pair, schema, uuid.New(), uuid.New())
		if err != nil || got != c.want {
			t.Errorf("%s: %+v, %v; want %+v", name, got, err, c.want)
		}
	}
}

func TestEffectiveOnReadFailuresAreStoreErrors(t *testing.T) {
	schema, _ := authschema.NewValidatedIdentifier("auth")
	boom := fakeRow{err: errors.New("connection reset")}
	ok := fakeRow{values: []any{str("admin")}}
	for name, pair := range map[string]fakePair{"public": {boom, ok}, "auth": {ok, boom}} {
		if _, err := EffectiveOn(context.Background(), pair, schema, uuid.New(), uuid.New()); !errors.Is(err, ErrStore) {
			t.Errorf("%s read failure: %v, want ErrStore", name, err)
		}
	}
}
