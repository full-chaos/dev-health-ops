//go:build integration

package pgmigrate_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/cli"
	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Revision 0157 adds internal_service_credential_audits.principal_id (the workload token subject of an internal acr
// request): nullable, no default, no backfill, at most one of credential_id and principal_id, at most 255 characters.

func position0157(t *testing.T, chain []pgmigrate.ChainFile) int {
	t.Helper()
	for i, file := range chain {
		if file.Revision == "0157" {
			return i
		}
	}
	t.Fatal("the chain holds no revision 0157")
	return 0
}

func TestRevision0157KeepsOldAuditRowsVerbatim(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	idx := position0157(t, chain)
	d := newDownInstance(t)
	conn := connect(t, d.at(t, idx))
	ctx := context.Background()
	credential := uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at)
VALUES ($1, 'acr', 'hash-0157', 'svc_acr_0157', '["entitlements:read"]'::json, now())`, credential); err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct {
		credential *uuid.UUID
		outcome    string
	}{{&credential, "success"}, {nil, "unknown_token"}} {
		if _, err := conn.Exec(ctx, `INSERT INTO internal_service_credential_audits (id, credential_id, requested_org_id, action, outcome, created_at)
VALUES ($1, $2, 'org-0157', 'acr_entitlement_lookup', $3, '2026-10-01T00:00:00Z')`, uuid.New(), row.credential, row.outcome); err != nil {
			t.Fatal(err)
		}
	}
	const dump = `SELECT coalesce(string_agg(concat_ws('|', id, credential_id, requested_org_id, action, outcome, created_at), ';' ORDER BY outcome), '') FROM internal_service_credential_audits`
	var before string
	if err := conn.QueryRow(ctx, dump).Scan(&before); err != nil {
		t.Fatal(err)
	}
	baseline, err := pgmigrate.LoadBaseline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgmigrate.Upgrade(ctx, conn, baseline, chain[:idx+1]); err != nil {
		t.Fatalf("upgrade to 0157: %v", err)
	}
	var after string
	var withPrincipal int
	if err := conn.QueryRow(ctx, dump).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM internal_service_credential_audits WHERE principal_id IS NOT NULL`).Scan(&withPrincipal); err != nil {
		t.Fatal(err)
	}
	if after != before || withPrincipal != 0 || before == "" {
		t.Fatalf("old rows changed or were backfilled:\nbefore %s\nafter  %s\nwith principal %d", before, after, withPrincipal)
	}
	var hasDefault bool
	if err := conn.QueryRow(ctx, `SELECT column_default IS NOT NULL FROM information_schema.columns WHERE table_name = 'internal_service_credential_audits' AND column_name = 'principal_id'`).Scan(&hasDefault); err != nil || hasDefault {
		t.Fatalf("principal_id default = %v (%v), want none", hasDefault, err)
	}
}

func TestRevision0157ChecksRefuseBothActorsAndALongPrincipal(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	position0157(t, chain)
	d := newDownInstance(t)
	conn := connect(t, d.at(t, len(chain)))
	ctx := context.Background()
	credential := uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO internal_service_credentials (id, service_name, token_hash, token_prefix, scopes, created_at)
VALUES ($1, 'acr', 'hash-0157b', 'svc_acr_0157b', '["entitlements:read"]'::json, now())`, credential); err != nil {
		t.Fatal(err)
	}
	insert := func(credentialID *uuid.UUID, principal *string) error {
		_, err := conn.Exec(ctx, `INSERT INTO internal_service_credential_audits (id, credential_id, principal_id, requested_org_id, action, outcome, created_at)
VALUES ($1, $2, $3, 'org-0157', 'acr_entitlement_lookup', 'success', now())`, uuid.New(), credentialID, principal)
		return err
	}
	prn := func(n int) *string { s := "prn_" + strings.Repeat("A", n-4); return &s }
	for name, tc := range map[string]struct {
		credential *uuid.UUID
		principal  *string
		constraint string
	}{
		"credential only":  {&credential, nil, ""},
		"principal only":   {nil, prn(22), ""},
		"neither":          {nil, nil, ""},
		"principal at 255": {nil, prn(255), ""},
		"both":             {&credential, prn(22), "ck_internal_service_credential_audits_one_actor"},
		"principal at 256": {nil, prn(256), "ck_internal_service_credential_audits_principal_id_length"},
	} {
		err := insert(tc.credential, tc.principal)
		var pgErr *pgconn.PgError
		switch {
		case tc.constraint == "" && err != nil:
			t.Errorf("%s: %v", name, err)
		case tc.constraint != "" && (!errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != tc.constraint):
			t.Errorf("%s: err = %v, want check violation %s", name, err, tc.constraint)
		}
	}
}

func TestRevision0157DowngradeRefusesWhileRowsHoldAPrincipal(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
	position := position0157(t, chain) + 1
	d := newDownInstance(t)
	uri := d.at(t, position)
	conn := connect(t, uri)
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `INSERT INTO internal_service_credential_audits (id, credential_id, principal_id, requested_org_id, action, outcome, created_at)
VALUES ($1, NULL, 'prn_AAAAAAAAAAAAAAAAAAAAAA', 'org-0157', 'acr_entitlement_lookup', 'success', now())`, uuid.New()); err != nil {
		t.Fatal(err)
	}
	before := schemaShape(t, uri)
	code, _, stderr := goDowngrade(t, uri, previousName(chain, position-1))
	if code != cli.ExitFailure || !strings.Contains(stderr, "1 internal_service_credential_audits rows hold a principal_id") || !strings.Contains(stderr, "audit rows are never changed") {
		t.Fatalf("downgrade with a principal row: exit %d stderr %q, want a refusal naming 1 row and the rule (audit rows are never changed)", code, stderr)
	}
	if after := schemaShape(t, uri); after != before {
		t.Fatalf("a refused downgrade changed the schema:\n%s", shapeDiff(before, after))
	}
	if _, err := conn.Exec(ctx, `DELETE FROM internal_service_credential_audits`); err != nil {
		t.Fatal(err)
	}
	if code, stdout, stderr := goDowngrade(t, uri, previousName(chain, position-1)); code != cli.ExitOK {
		t.Fatalf("downgrade with no principal rows: exit %d stdout %s stderr %s", code, stdout, stderr)
	}
	var columns int
	if err := conn.QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name = 'internal_service_credential_audits' AND column_name = 'principal_id'`).Scan(&columns); err != nil || columns != 0 {
		t.Fatalf("principal_id columns after the downgrade = %d (%v), want 0", columns, err)
	}
}
