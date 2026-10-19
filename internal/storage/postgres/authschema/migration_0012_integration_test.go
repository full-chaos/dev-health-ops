//go:build integration

package authschema

import (
	"context"
	"strings"
	"testing"
	"time"
)

func m12Flow(t *testing.T, ctx context.Context, env *authFixture, tag string, expiresIn string) string {
	t.Helper()
	var id string
	if err := env.runtime.QueryRow(ctx, `INSERT INTO auth.sso_flow_states (protocol, provider_id, flow_hash, binding_hash, expires_at)
		VALUES ('oidc', gen_random_uuid(), $1::bytea, $2::bytea, now() + $3::interval) RETURNING id::text`,
		m10Hash(tag), m10Hash(tag+"b"), expiresIn).Scan(&id); err != nil {
		t.Fatalf("insert flow %s: %v", tag, err)
	}
	return id
}

func TestMigration0012FlowStateAndAssertionConstraints(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	flow := func(protocol, flow, binding string) error {
		var binding2 any
		if binding != "" {
			binding2 = binding
		}
		_, err := env.runtime.Exec(ctx, `INSERT INTO auth.sso_flow_states (protocol, provider_id, flow_hash, binding_hash, expires_at)
			VALUES ($1, gen_random_uuid(), $2::bytea, $3::bytea, now() + interval '10 minutes')`, protocol, flow, binding2)
		return err
	}
	for _, protocol := range []string{"oidc", "oauth", "saml"} {
		if err := flow(protocol, m10Hash("a"+protocol[:2]), ""); err != nil {
			t.Errorf("protocol %s with no binding refused: %v", protocol, err)
		}
	}
	m10Fail(t, flow("ldap", m10Hash("b1"), ""), "an unknown protocol")
	m10Fail(t, flow("oidc", m10Hash("aoi"), ""), "a duplicate flow_hash")
	m10Fail(t, flow("oidc", `\x0102`, ""), "a short flow_hash")
	m10Fail(t, flow("oidc", "plaintext-state-value", ""), "a plaintext flow_hash")
	m10Fail(t, flow("oidc", m10Hash("b2"), `\x0102`), "a short binding_hash")

	assertion := func(issuer, hash string) error {
		_, err := env.runtime.Exec(ctx, `INSERT INTO auth.saml_assertion_uses (issuer, assertion_id_hash, expires_at)
			VALUES ($1, $2::bytea, now() + interval '10 minutes')`, issuer, hash)
		return err
	}
	if err := assertion("https://idp.example", m10Hash("c1")); err != nil {
		t.Fatalf("first assertion use: %v", err)
	}
	if err := assertion("https://other-idp.example", m10Hash("c1")); err != nil {
		t.Errorf("the same assertion id under another issuer was refused: %v", err)
	}
	m10Fail(t, assertion("https://idp.example", m10Hash("c1")), "a replayed (issuer, assertion_id_hash)")
	m10Fail(t, assertion("https://idp.example", `\x0102`), "a short assertion_id_hash")
	m10Fail(t, assertion("", m10Hash("c2")), "an empty issuer")

	// The assertion ledger is insert-only for the runtime.
	if _, err := env.runtime.Exec(ctx, `UPDATE auth.saml_assertion_uses SET expires_at = now() + interval '1 year'`); err == nil {
		t.Error("the runtime role can UPDATE saml_assertion_uses")
	}
}

func TestMigration0012FlowStateIsSingleUseAndOtherwiseImmutable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)
	id := m12Flow(t, ctx, env, "d1", "10 minutes")

	consume := func() (int64, error) {
		tag, err := env.runtime.Exec(ctx, `UPDATE auth.sso_flow_states SET consumed_at = now() WHERE id = $1 AND consumed_at IS NULL`, id)
		return tag.RowsAffected(), err
	}
	if n, err := consume(); err != nil || n != 1 {
		t.Fatalf("first consume = %d, %v; want 1", n, err)
	}
	if n, err := consume(); err != nil || n != 0 {
		t.Errorf("replayed atomic consume = %d, %v; want 0 rows", n, err)
	}
	for name, statement := range map[string]string{
		"a second consume":     `UPDATE auth.sso_flow_states SET consumed_at = now() WHERE id = $1`,
		"clearing consumed_at": `UPDATE auth.sso_flow_states SET consumed_at = NULL WHERE id = $1`,
	} {
		if _, err := env.runtime.Exec(ctx, statement, id); err == nil || !strings.Contains(err.Error(), "consumed_at") {
			t.Errorf("%s: err = %v, want a refusal naming consumed_at", name, err)
		}
	}

	// Every other column is immutable, consumed or not.
	fresh := m12Flow(t, ctx, env, "d2", "10 minutes")
	immutable := map[string]string{
		"protocol":     `UPDATE auth.sso_flow_states SET protocol = 'saml' WHERE id = $1`,
		"provider_id":  `UPDATE auth.sso_flow_states SET provider_id = gen_random_uuid() WHERE id = $1`,
		"flow_hash":    `UPDATE auth.sso_flow_states SET flow_hash = decode(rpad('ee', 64, '0'), 'hex') WHERE id = $1`,
		"binding_hash": `UPDATE auth.sso_flow_states SET binding_hash = NULL WHERE id = $1`,
		"expires_at":   `UPDATE auth.sso_flow_states SET expires_at = now() + interval '1 year' WHERE id = $1`,
		"created_at":   `UPDATE auth.sso_flow_states SET created_at = now() - interval '1 year' WHERE id = $1`,
		"id":           `UPDATE auth.sso_flow_states SET id = gen_random_uuid() WHERE id = $1`,
	}
	for column, statement := range immutable {
		if _, err := env.runtime.Exec(ctx, statement, fresh); err == nil || !strings.Contains(err.Error(), "immutable") {
			t.Errorf("updating %s: err = %v, want an immutable-column refusal", column, err)
		}
	}
	// A consume that rewrites nothing else stays legal on an unconsumed row.
	if tag, err := env.runtime.Exec(ctx, `UPDATE auth.sso_flow_states SET consumed_at = now() WHERE id = $1`, fresh); err != nil || tag.RowsAffected() != 1 {
		t.Errorf("consuming a fresh flow = %v, %v; want 1 row", tag, err)
	}
}

// The sweep deletes rows expired more than an hour ago and nothing else.
func TestMigration0012SweepDeletesOnlyLongExpiredRows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	env := newM10Fixture(t, ctx)

	m12Flow(t, ctx, env, "e1", "-2 hours")
	recent := m12Flow(t, ctx, env, "e2", "-10 minutes")
	live := m12Flow(t, ctx, env, "e3", "10 minutes")
	for tag, expiresIn := range map[string]string{"f1": "-2 hours", "f2": "-10 minutes", "f3": "10 minutes"} {
		if _, err := env.runtime.Exec(ctx, `INSERT INTO auth.saml_assertion_uses (issuer, assertion_id_hash, expires_at)
			VALUES ('https://idp.example', $1::bytea, now() + $2::interval)`, m10Hash(tag), expiresIn); err != nil {
			t.Fatal(err)
		}
	}
	for _, table := range []string{"sso_flow_states", "saml_assertion_uses"} {
		tag, err := env.runtime.Exec(ctx, `DELETE FROM auth.`+table+` WHERE expires_at < now() - interval '1 hour'`)
		if err != nil || tag.RowsAffected() != 1 {
			t.Errorf("%s sweep = %v, %v; want exactly the long-expired row", table, tag, err)
		}
	}
	var remaining int
	if err := env.admin.QueryRow(ctx, `SELECT count(*) FROM auth.sso_flow_states WHERE id = ANY($1::uuid[])`, []string{recent, live}).Scan(&remaining); err != nil || remaining != 2 {
		t.Errorf("recent and live flows remaining = %d, %v; want 2", remaining, err)
	}
}
