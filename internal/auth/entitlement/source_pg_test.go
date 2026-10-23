package entitlement_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/auth/entitlement"
)

type row struct {
	vals []any
	err  error
}

func (r row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch p := d.(type) {
		case **string:
			if s, ok := r.vals[i].(string); ok {
				*p = &s
			} else {
				*p = nil
			}
		case *[]byte:
			*p, _ = r.vals[i].([]byte)
		}
	}
	return nil
}

type fakeQ struct{ rows []row }

func (q *fakeQ) QueryRow(context.Context, string, ...any) pgx.Row {
	r := q.rows[0]
	q.rows = q.rows[1:]
	return r
}

func load(t *testing.T, rows ...row) (entitlement.Content, error) {
	t.Helper()
	return entitlement.PGSource{Q: &fakeQ{rows: rows}}.Load(context.Background(), uuid.New())
}

func TestPGSource_LicenseTierGrantsRegistryFeatures(t *testing.T) {
	c, err := load(t, row{vals: []any{"team", []byte(nil)}})
	if err != nil {
		t.Fatal(err)
	}
	if c.Tier != "team" || !c.Features["webhooks"] || c.Features["sso_saml"] {
		t.Fatalf("%+v", c)
	}
}

func TestPGSource_TruthyOverrideGrantsFeature(t *testing.T) {
	c, err := load(t, row{vals: []any{"team", []byte(`{"sso_saml": true, "audit_log": false}`)}})
	if err != nil {
		t.Fatal(err)
	}
	if !c.Features["sso_saml"] || c.Features["audit_log"] {
		t.Fatalf("%+v", c.Features)
	}
}

func TestPGSource_NonObjectOverrideIsNoOverride(t *testing.T) {
	c, err := load(t, row{vals: []any{"team", []byte(`[]`)}})
	if err != nil || c.Features["sso_saml"] {
		t.Fatalf("%+v %v", c, err)
	}
}

func TestPGSource_NoLicenseRowFallsBackToOrganizationTier(t *testing.T) {
	c, err := load(t, row{err: pgx.ErrNoRows}, row{vals: []any{"enterprise"}})
	if err != nil {
		t.Fatal(err)
	}
	if c.Tier != "enterprise" || !c.Features["sso_saml"] {
		t.Fatalf("%+v", c)
	}
}

func TestPGSource_NoRowsAnywhereGrantsNothing(t *testing.T) {
	c, err := load(t, row{err: pgx.ErrNoRows}, row{err: pgx.ErrNoRows})
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range c.Features {
		if v {
			t.Fatalf("%s granted", k)
		}
	}
}

func TestPGSource_InvalidTierGrantsNothing(t *testing.T) {
	c, err := load(t, row{vals: []any{"platinum", []byte(`{"sso_saml": true}`)}})
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range c.Features {
		if v {
			t.Fatalf("%s granted", k)
		}
	}
}

func TestPGSource_DatabaseErrorIsReturned(t *testing.T) {
	if _, err := load(t, row{err: errors.New("boom")}); err == nil {
		t.Fatal("a database error must not read as no entitlement")
	}
}
