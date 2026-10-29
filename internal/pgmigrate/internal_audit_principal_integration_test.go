//go:build integration

package pgmigrate_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/pgmigrate"
)

// Revision 0157 adds internal_service_credential_audits.principal_id (the workload token subject of an internal acr
// request): nullable, no default, no backfill, at most one of credential_id and principal_id, at most 255 characters.

func TestRevision0157ChecksRefuseBothActorsAndALongPrincipal(t *testing.T) {
	chain, err := pgmigrate.LoadChain()
	if err != nil {
		t.Fatal(err)
	}
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
