package admin

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func strp(s string) *string { return &s }

// operatorRows answer the reads of a membership update: the user by email, the
// organization by slug, no membership yet (so the update inserts one).
func operatorRows() map[string][]any {
	return map[string][]any{
		"FROM users WHERE lower(email) = $1":         {zeroFill{}},
		"FROM organizations WHERE lower(slug) = $1":  {zeroFill{}},
		"SELECT is_superuser FROM users WHERE id = ": {false},
	}
}

var operatorMembershipUpdates = map[string]UpdateUserInput{
	"add or role": {Email: strp("a@example.test"), MembershipOrg: strp("acme"), Role: strp("admin")},
	"remove":      {Email: strp("a@example.test"), RemoveFromOrg: strp("acme")},
}

// With AUTH_PROXY_ORG_PROVISIONING on, the HTTP member routes go to
// auth-service; the CLI's membership changes and real deletes would write
// go-api's rows only, so they refuse, named, before any database call.
func TestOperatorRefusesMembershipChangesAndDeletesWithTheSwitchOn(t *testing.T) {
	for name, in := range operatorMembershipUpdates {
		db := &provisionDB{rows: operatorRows()}
		_, err := Operator{db: db, OrgProvisioning: true}.UpdateUser(context.Background(), in)
		if !errors.Is(err, ErrOrgProvisioningOn) || len(db.log) != 0 {
			t.Fatalf("%s: err %v, database calls %v; want ErrOrgProvisioningOn and none", name, err, db.log)
		}
	}
	db := &provisionDB{}
	_, err := Operator{db: db, OrgProvisioning: true}.DeleteOrg(context.Background(), provOrg.String(), false, DeleteOrgConfig{})
	if !errors.Is(err, ErrOrgProvisioningOn) || len(db.log) != 0 {
		t.Fatalf("delete: err %v, database calls %v; want ErrOrgProvisioningOn and none", err, db.log)
	}
	var refusal *OperatorError
	if !errors.As(ErrOrgProvisioningOn, &refusal) || !strings.Contains(refusal.Message, "AUTH_PROXY_ORG_PROVISIONING") {
		t.Fatalf("ErrOrgProvisioningOn = %v: want an operator refusal naming the switch", ErrOrgProvisioningOn)
	}
}

// A dry run writes nothing, so it still runs with the switch on (here it stops
// at today's ClickHouse refusal, not at the switch).
func TestOperatorDryRunDeleteStillRunsWithTheSwitchOn(t *testing.T) {
	_, err := Operator{db: &provisionDB{}, OrgProvisioning: true}.DeleteOrg(context.Background(), provOrg.String(), true, DeleteOrgConfig{})
	var refusal *OperatorError
	if errors.Is(err, ErrOrgProvisioningOn) || !errors.As(err, &refusal) || refusal.Message != wantOrgDeletionRefusedDetail {
		t.Fatalf("dry run: %v, want today's ClickHouse refusal", err)
	}
}

// A profile-only update writes no membership row: the switch does not refuse it.
func TestOperatorProfileUpdateIsNotRefusedByTheSwitch(t *testing.T) {
	db := &provisionDB{rows: operatorRows()}
	_, err := Operator{db: db, OrgProvisioning: true}.UpdateUser(context.Background(), UpdateUserInput{Email: strp("a@example.test"), FullName: strp("New Name")})
	if errors.Is(err, ErrOrgProvisioningOn) {
		t.Fatalf("a profile-only update was refused by the switch")
	}
}

// With the switch off the membership update runs today's SQL.
var operatorSwitchOffSQL = map[string][]string{
	"add or role": {"BEGIN", "tx:SELECT id, email, username, password_hash, full_name, avatar_url, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, last_login_at, created_at, updated_at FROM users WHERE lower(email) = $1", "tx:SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE lower(slug) = $1", "tx:SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2", "tx:SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2",
		"tx:INSERT INTO memberships (id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
		"COMMIT", "ROLLBACK"},
	"remove": {"BEGIN", "tx:SELECT id, email, username, password_hash, full_name, avatar_url, auth_provider, auth_provider_id, is_active, is_verified, is_superuser, token_version, last_login_at, created_at, updated_at FROM users WHERE lower(email) = $1", "tx:SELECT id, slug, name, description, settings, tier, managed_by, is_active, created_at, updated_at FROM organizations WHERE lower(slug) = $1", "tx:SELECT id, org_id, user_id, role, invited_by_id, joined_at, created_at, updated_at FROM memberships WHERE org_id = $1 AND user_id = $2",
		"tx:DELETE FROM memberships WHERE org_id = $1 AND user_id = $2",
		"tx:UPDATE refresh_tokens SET revoked_at = $3 WHERE user_id = $1 AND org_id = $2 AND revoked_at IS NULL", "COMMIT", "ROLLBACK"},
}

func TestOperatorSwitchOffRunsTodaysMembershipSQL(t *testing.T) {
	for name, in := range operatorMembershipUpdates {
		db := &provisionDB{rows: operatorRows()}
		if name == "remove" {
			// a member, so the removal runs to its delete
			db.rows["FROM memberships WHERE org_id = $1 AND user_id = $2"] = []any{zeroFill{}}
		}
		_, _ = Operator{db: db}.UpdateUser(context.Background(), in)
		if !reflect.DeepEqual(db.log, operatorSwitchOffSQL[name]) {
			t.Fatalf("%s: switch-off SQL:\n got %q\nwant %q", name, db.log, operatorSwitchOffSQL[name])
		}
	}
}
