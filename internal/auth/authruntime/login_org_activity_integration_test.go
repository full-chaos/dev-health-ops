//go:build integration

package authruntime

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/passwordhash"
)

// A password login for a user who holds two organizations and names none lands
// where go-api's login lands: the organization with metrics data, not the one
// joined first; with no activity row for one of them it falls back to the
// earliest join.
func TestLoginChoosesTheOrganizationWithMetricsThroughConfigure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	e := newRoutesEnv(t, ctx)
	user, first := e.seed(t, ctx)
	second := uuid.New()
	for _, statement := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO organizations (id, slug, name, tier, is_active, settings, managed_by, created_at, updated_at)
VALUES ($1, $2, 'Second Org', 'community', true, '{}', 'self', now(), now())`, []any{second, "second-" + second.String()[:8]}},
		{`INSERT INTO memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, 'member', now() + interval '1 hour', now() + interval '1 hour')`, []any{user, second}},
	} {
		if _, err := e.admin.Exec(ctx, statement.sql, statement.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if err := e.identity.ProjectOrganization(ctx, second); err != nil {
		t.Fatal(err)
	}
	hash, err := passwordhash.Hash("activity-login-password")
	if err != nil {
		t.Fatal(err)
	}
	email := "activity-" + user.String()[:8] + "@example.com"
	if _, err := e.admin.Exec(ctx, `UPDATE users SET password_hash = $2, email = $3 WHERE id = $1`, user, hash, email); err != nil {
		t.Fatal(err)
	}
	client := map[string]string{"X-Forwarded-For": "198.51.100.9", "Content-Type": "application/json"}
	login := func() string {
		t.Helper()
		got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"activity-login-password"}`, client)
		if got.status != http.StatusOK {
			t.Fatalf("login = %d %s", got.status, got.body)
		}
		return object(t, got)["user"].(map[string]any)["org_id"].(string)
	}

	// No activity rows: unknown, the earliest join (go-api without a ClickHouse answer).
	if got := login(); got != first.String() {
		t.Fatalf("no projection rows: login took %s, want the earliest join %s", got, first)
	}
	// Only one organization has a row: still unknown for the other, still the earliest join.
	if _, err := e.admin.Exec(ctx, `INSERT INTO org_activity (org_id, has_data, computed_at, refreshed_at) VALUES ($1, true, now(), now())`, second); err != nil {
		t.Fatal(err)
	}
	if got := login(); got != first.String() {
		t.Fatalf("one row missing: login took %s, want the earliest join %s", got, first)
	}
	// Both are known, the earliest has no data: go-api's choice is the organization with metrics.
	if _, err := e.admin.Exec(ctx, `INSERT INTO org_activity (org_id, has_data, computed_at, refreshed_at) VALUES ($1, false, NULL, now())`, first); err != nil {
		t.Fatal(err)
	}
	if got := login(); got != second.String() {
		t.Fatalf("known activity: login took %s, want the organization with metrics %s", got, second)
	}
	// Both have data: the newest metrics decide, whichever organization joined first.
	if _, err := e.admin.Exec(ctx, `UPDATE org_activity SET has_data = true, computed_at = now() - interval '2 days' WHERE org_id = $1`, first); err != nil {
		t.Fatal(err)
	}
	if got := login(); got != second.String() {
		t.Fatalf("both have data, the second is newer: login took %s, want %s", got, second)
	}
	if _, err := e.admin.Exec(ctx, `UPDATE org_activity SET computed_at = now() + interval '1 day' WHERE org_id = $1`, first); err != nil {
		t.Fatal(err)
	}
	if got := login(); got != first.String() {
		t.Fatalf("both have data, the first is newer: login took %s, want %s", got, first)
	}
	if strings.Contains(e.logs.String(), first.String()) && strings.Contains(e.logs.String(), "org activity unknown") {
		t.Fatalf("the unknown-activity WARN names an organization id:\n%s", e.logs.String())
	}
	// A requested organization wins over the activity.
	got := e.doWith(t, "/api/v1/auth/login", `{"email":"`+email+`","password":"activity-login-password","org_id":"`+first.String()+`"}`, client)
	if got.status != http.StatusOK || object(t, got)["user"].(map[string]any)["org_id"] != first.String() {
		t.Fatalf("requested organization: %d %s", got.status, got.body)
	}
}
