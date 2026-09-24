//go:build integration

package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
)

// postureHarness brings up a real ClickHouse with the teams/identities/
// other_table tables and an admin connection to grant from -- CREATE USER
// and GRANT both need privileges a freshly created scratch user does not
// have, so every grant in these tests runs over the admin connection,
// targeting the scratch user by name, never self-granted.
type postureHarness struct {
	ctx         context.Context
	admin       driver.Conn
	instanceURI *url.URL
	userCounter int
}

func startPostureHarness(t *testing.T) *postureHarness {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = instance.Close(closeCtx)
	})
	// Explicitly "default", never StartClickHouse's own CLICKHOUSE_DB
	// database ("worker_test") -- this harness's CREATE TABLE/GRANT
	// statements below all say "default.<table>" literally, and
	// CheckAPIClickHouseAuthorization now resolves ITS posture from the
	// connection's real currentDatabase(), so every connection here must
	// agree on which database that is.
	parsedInstanceURI, err := url.Parse(instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	parsedInstanceURI.Path = "/default"
	adminURI := parsedInstanceURI.String()
	admin, err := Open(ctx, DefaultConfig(adminURI))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	for _, statement := range []string{
		"CREATE TABLE teams (id String, org_id String) ENGINE = ReplacingMergeTree() ORDER BY id",
		"CREATE TABLE identities (canonical_id String, org_id String) ENGINE = ReplacingMergeTree() ORDER BY canonical_id",
		"CREATE TABLE team_sync_policies (org_id String, team_id String) ENGINE = ReplacingMergeTree() ORDER BY team_id",
		"CREATE TABLE team_provider_observations (org_id String, team_id String) ENGINE = ReplacingMergeTree() ORDER BY team_id",
		"CREATE TABLE team_drift_changes (org_id String, change_id String) ENGINE = ReplacingMergeTree() ORDER BY change_id",
		"CREATE TABLE team_memberships (org_id String, member_id String) ENGINE = ReplacingMergeTree() ORDER BY member_id",
		"CREATE TABLE manual_attribution_fallbacks (org_id String, scope_id String) ENGINE = ReplacingMergeTree() ORDER BY scope_id",
		"CREATE TABLE other_table (id String) ENGINE = ReplacingMergeTree() ORDER BY id",
		"CREATE TABLE repo_metrics_daily (org_id String) ENGINE = ReplacingMergeTree() ORDER BY org_id",
		"CREATE TABLE user_metrics_daily (org_id String) ENGINE = ReplacingMergeTree() ORDER BY org_id",
		"CREATE TABLE team_metrics_daily (org_id String) ENGINE = ReplacingMergeTree() ORDER BY org_id",
		"CREATE TABLE work_item_metrics_daily (org_id String) ENGINE = ReplacingMergeTree() ORDER BY org_id",
	} {
		if err := admin.Exec(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	return &postureHarness{ctx: ctx, admin: admin, instanceURI: parsedInstanceURI}
}

// newUser creates a scratch user, grants it every statement in grants (run
// over the admin connection), then returns a connection AS that user.
func (h *postureHarness) newUser(t *testing.T, grants ...string) driver.Conn {
	t.Helper()
	h.userCounter++
	username := fmt.Sprintf("posture_test_user_%d", h.userCounter)
	password := "posture-test-password"
	if err := h.admin.Exec(h.ctx, fmt.Sprintf(
		"CREATE USER %s IDENTIFIED WITH plaintext_password BY '%s'", username, password)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = h.admin.Exec(context.Background(), "DROP USER IF EXISTS "+username) })
	for _, statement := range grants {
		if err := h.admin.Exec(h.ctx, statement+" TO "+username); err != nil {
			t.Fatal(err)
		}
	}
	userURI := url.URL{
		Scheme: h.instanceURI.Scheme,
		User:   url.UserPassword(username, password),
		Host:   h.instanceURI.Host,
		Path:   h.instanceURI.Path,
	}
	conn, err := Open(h.ctx, DefaultConfig(userURI.String()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

const (
	grantTeamsExact      = "GRANT SELECT, INSERT, ALTER DELETE ON default.teams"
	grantIdentitiesExact = "GRANT SELECT, INSERT, ALTER DELETE ON default.identities"

	// POST /teams/import's drift-projector tables (CHAOS-6311).
	grantSyncPoliciesExact = "GRANT SELECT ON default.team_sync_policies"
	grantObservationsExact = "GRANT SELECT, INSERT ON default.team_provider_observations"
	grantDriftChangesExact = "GRANT SELECT, INSERT ON default.team_drift_changes"

	// Team drift review's edge tables (CHAOS-6312).
	grantMembershipsExact = "GRANT INSERT ON default.team_memberships"
	grantFallbacksExact   = "GRANT INSERT ON default.manual_attribution_fallbacks"

	// The backfill job detail's metrics diagnostics reads (CHAOS-6439);
	// its repo_metrics_daily read rides grantMetricsExact below.
	grantRepoComplexityExact = "GRANT SELECT ON default.repo_complexity_daily"
	grantCompoundingExact    = "GRANT SELECT ON default.compounding_risk_daily"
)

// grantMetricsExact are the read-only metric-table grants of the manifest
// (organization activity for the session routes), held unchanged by every
// case below so each one varies a single grant.
var grantMetricsExact = []string{
	"GRANT SELECT ON default.repo_metrics_daily",
	"GRANT SELECT ON default.user_metrics_daily",
	"GRANT SELECT ON default.team_metrics_daily",
	"GRANT SELECT ON default.work_item_metrics_daily",
}

// importGrants is the exact grant set for every declared table other than
// teams and identities (the import tables, the backfill diagnostics reads
// and the metric tables), appended to each test's teams/identities grants so
// the whole manifest is met.
func importGrants() []string {
	return append([]string{grantSyncPoliciesExact, grantObservationsExact, grantDriftChangesExact, grantMembershipsExact, grantFallbacksExact,
		grantRepoComplexityExact, grantCompoundingExact}, grantMetricsExact...)
}

// TestCheckPostureAcceptsExactMatch proves the happy path: a user granted
// exactly APIPosture()'s manifest passes.
func TestCheckPostureAcceptsExactMatch(t *testing.T) {
	h := startPostureHarness(t)
	conn := h.newUser(t, append([]string{grantTeamsExact, grantIdentitiesExact}, importGrants()...)...)
	if err := CheckAPIClickHouseAuthorization(h.ctx, conn); err != nil {
		t.Fatalf("exact posture must pass: %v", err)
	}
}

// TestCheckPostureRejectsMissingPrivilege is the negative control for an
// under-grant: a user missing ALTER DELETE on one declared table fails.
func TestCheckPostureRejectsMissingPrivilege(t *testing.T) {
	h := startPostureHarness(t)
	conn := h.newUser(t, append([]string{"GRANT SELECT, INSERT ON default.teams", grantIdentitiesExact}, importGrants()...)...)
	err := CheckAPIClickHouseAuthorization(h.ctx, conn)
	if err == nil {
		t.Fatal("missing ALTER DELETE on teams must fail the posture check")
	}
	if !errors.Is(err, ErrPostureMismatch) {
		t.Fatalf("want ErrPostureMismatch, got: %v", err)
	}
}

// TestCheckPostureRejectsExtraGrant is the negative control for an
// over-grant: a user with an UNDECLARED table grant fails, even though
// every declared table/privilege is present -- "no more, no less".
func TestCheckPostureRejectsExtraGrant(t *testing.T) {
	h := startPostureHarness(t)
	conn := h.newUser(t, append(append([]string{grantTeamsExact, grantIdentitiesExact}, importGrants()...), "GRANT SELECT ON default.other_table")...)
	err := CheckAPIClickHouseAuthorization(h.ctx, conn)
	if err == nil {
		t.Fatal("an extra grant outside the declared manifest must fail the posture check")
	}
	if !errors.Is(err, ErrPostureMismatch) {
		t.Fatalf("want ErrPostureMismatch, got: %v", err)
	}
}

// TestCheckPostureRejectsGrantOption is the negative control for a grant
// that carries every declared privilege AND MORE: WITH GRANT OPTION lets
// the connected role grant its own privileges on to others, which no
// declared TableGrant field expresses, so it must fail exactly like any
// other over-grant. newUser has no hook for a trailing WITH GRANT OPTION
// (ClickHouse's syntax puts it after "TO <user>", which newUser appends
// itself), so this re-grants directly over the admin connection once the
// user already exists with the exact-match posture.
func TestCheckPostureRejectsGrantOption(t *testing.T) {
	h := startPostureHarness(t)
	conn := h.newUser(t, append([]string{grantTeamsExact, grantIdentitiesExact}, importGrants()...)...)
	var username string
	if err := conn.QueryRow(h.ctx, "SELECT currentUser()").Scan(&username); err != nil {
		t.Fatal(err)
	}
	if err := h.admin.Exec(h.ctx, "GRANT SELECT ON default.teams TO "+username+" WITH GRANT OPTION"); err != nil {
		t.Fatal(err)
	}
	err := CheckAPIClickHouseAuthorization(h.ctx, conn)
	if err == nil {
		t.Fatal("a WITH GRANT OPTION grant must fail the posture check")
	}
	if !errors.Is(err, ErrPostureMismatch) {
		t.Fatalf("want ErrPostureMismatch, got: %v", err)
	}
}

// TestCheckPostureRejectsMissingMetricsRead is the under-grant control for
// the read-only tables: dropping one metric table's SELECT fails.
func TestCheckPostureRejectsMissingMetricsRead(t *testing.T) {
	h := startPostureHarness(t)
	// Every other grant of the manifest is present, so the one missing
	// SELECT is the only reason the check can fail.
	all := append([]string{grantTeamsExact, grantIdentitiesExact}, importGrants()...)
	grants := all[:len(all)-1]
	conn := h.newUser(t, grants...)
	err := CheckAPIClickHouseAuthorization(h.ctx, conn)
	if err == nil {
		t.Fatal("missing SELECT on work_item_metrics_daily must fail the posture check")
	}
	if !errors.Is(err, ErrPostureMismatch) || !strings.Contains(err.Error(), "work_item_metrics_daily") {
		t.Fatalf("want ErrPostureMismatch naming work_item_metrics_daily, got: %v", err)
	}
}

// TestCheckPostureRejectsExtraPrivilegeOnDeclaredTable is the over-grant
// negative control for a DECLARED table: an extra privilege the manifest
// never asked for (UPDATE, which ClickHouse renders as ALTER UPDATE) on a
// table that otherwise matches must still fail -- "no more" applies per
// privilege, not only per table.
func TestCheckPostureRejectsExtraPrivilegeOnDeclaredTable(t *testing.T) {
	h := startPostureHarness(t)
	conn := h.newUser(t, append([]string{
		"GRANT SELECT, INSERT, ALTER DELETE, ALTER UPDATE ON default.teams", grantIdentitiesExact}, importGrants()...)...)
	err := CheckAPIClickHouseAuthorization(h.ctx, conn)
	if err == nil {
		t.Fatal("an extra privilege on a declared table must fail the posture check")
	}
	if !errors.Is(err, ErrPostureMismatch) {
		t.Fatalf("want ErrPostureMismatch, got: %v", err)
	}
}

// TestCheckPostureRejectsMissingImportTableGrant is the negative control for
// the import tables specifically: a login that has teams/identities but
// lacks a declared import-table privilege (here team_drift_changes INSERT)
// cannot run POST /teams/import, and the posture check must say so at
// startup rather than let the route 500 on first use.
func TestCheckPostureRejectsMissingImportTableGrant(t *testing.T) {
	h := startPostureHarness(t)
	conn := h.newUser(t, grantTeamsExact, grantIdentitiesExact, grantSyncPoliciesExact,
		grantObservationsExact, grantMembershipsExact, grantFallbacksExact, "GRANT SELECT ON default.team_drift_changes")
	err := CheckAPIClickHouseAuthorization(h.ctx, conn)
	if err == nil || !errors.Is(err, ErrPostureMismatch) {
		t.Fatalf("missing INSERT on team_drift_changes must fail with ErrPostureMismatch, got: %v", err)
	}
}
