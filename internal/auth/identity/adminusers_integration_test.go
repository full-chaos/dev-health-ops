//go:build integration

package identity

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// A profile-only admin update commits: the public.users username, full_name and avatar_url and the principal's display
// name change, one auth.user.profile_updated audit row is written, and no provisioning event: no ErrNoStateChange.
func TestAdminProfileOnlyUpdateWritesOneProfileAuditRow(t *testing.T) {
	e := getEnv(t)
	ctx := context.Background()
	none := func(context.Context, audit.TxOps) error { return nil }
	actor := e.seedUser(t, ctx, uniqueEmail("profile-actor"), true, true)
	e.project(t, ctx, actor)
	created, err := e.owned.AdminCreateUser(ctx, AdminCreateRequest{Email: uniqueEmail("profile"), Actor: actor, Authorize: none})
	if err != nil {
		t.Fatal(err)
	}
	cur, err := e.owned.AdminUpdateState(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	var eventsBefore int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events`).Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}
	name, full, avatar := "Profile-"+uuid.NewString()[:8], "Profile Person", "https://example.test/p.png"
	if err := e.owned.AdminUpdateUser(ctx, AdminUpdateRequest{Principal: created.ID, Username: &name, FullName: &full, AvatarURL: &avatar,
		Actor: actor, Authorize: none}, cur); err != nil {
		t.Fatalf("profile-only update: %v", err)
	}
	var username, fullName, avatarURL, display string
	if err := e.admin.QueryRow(ctx, `SELECT u.username, u.full_name, u.avatar_url, p.display_name FROM public.users u
JOIN auth.principals p ON p.id = u.id WHERE u.id = $1`, created.ID).Scan(&username, &fullName, &avatarURL, &display); err != nil {
		t.Fatal(err)
	}
	if username != pythonLowerStrip(name) || fullName != full || avatarURL != avatar || display != full {
		t.Errorf("stored username %q full_name %q avatar_url %q display_name %q", username, fullName, avatarURL, display)
	}
	var rows int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.security_audit_events WHERE event_type = $1 AND subject_principal_id = $2`,
		audit.EventUserProfileUpdated, created.ID.String()).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Errorf("profile_updated audit rows = %d, want 1", rows)
	}
	var eventsAfter int
	if err := e.admin.QueryRow(ctx, `SELECT count(*) FROM auth.auth_outbox_events`).Scan(&eventsAfter); err != nil {
		t.Fatal(err)
	}
	if eventsAfter != eventsBefore {
		t.Errorf("outbox events %d -> %d, want none (off the stream)", eventsBefore, eventsAfter)
	}
}

func pythonLowerStrip(s string) string { return AdminStoredEmail(s) }
