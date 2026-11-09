package identity

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

type commitPick struct {
	kind, eventType, offStream string
	events                     int
}

// recordCommits swaps the two commit kinds for recorders for the test's life.
func recordCommits(t *testing.T) *[]commitPick {
	t.Helper()
	picks := &[]commitPick{}
	oldMutation, oldProjection := commitMutation, commitPublicProjection
	commitMutation = func(_ context.Context, _ *pgxpool.Pool, _ authschema.ValidatedIdentifier, m audit.Mutation) error {
		*picks = append(*picks, commitPick{kind: "commit", eventType: m.Audit.EventType, offStream: m.OffStream, events: len(m.Events)})
		return nil
	}
	commitPublicProjection = func(_ context.Context, _ *pgxpool.Pool, _ authschema.ValidatedIdentifier, event audit.AuditEvent,
		_ func(context.Context, audit.TxOps) (int64, error)) error {
		*picks = append(*picks, commitPick{kind: "public_projection", eventType: event.EventType})
		return nil
	}
	t.Cleanup(func() { commitMutation, commitPublicProjection = oldMutation, oldProjection })
	return picks
}

func lazyOwnedStore(t *testing.T) *Store {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://unit@127.0.0.1:1/unit")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	store, err := New(Config{Pool: pool, Schema: schema, UsersOwnedByAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

// A profile-only update (username, full_name, avatar_url) is one CommitPublicProjection with the profile_updated
// audit row: those columns are in no guarded table, so Commit would answer it as no change and roll it back. An
// update with an email or verified change stays one Commit with its provisioning events; a profile change with a
// platform role change is one Commit off the stream.
func TestAdminProfileOnlyUpdateTakesThePublicProjectionCommit(t *testing.T) {
	store := lazyOwnedStore(t)
	none := func(context.Context, audit.TxOps) error { return nil }
	name, full, avatar, email := "Neo", "Thomas A.", "https://example.test/a.png", "new@example.test"
	cur := AdminUpdateCurrent{Email: "old@example.test"}
	for _, c := range []struct {
		name string
		req  AdminUpdateRequest
		want commitPick
	}{
		{"profile only", AdminUpdateRequest{Username: &name, FullName: &full, AvatarURL: &avatar},
			commitPick{kind: "public_projection", eventType: audit.EventUserProfileUpdated}},
		{"email and profile", AdminUpdateRequest{Email: &email, Username: &name},
			commitPick{kind: "commit", eventType: "auth.user.email_changed", events: 1}},
		{"profile and platform role", AdminUpdateRequest{FullName: &full, Superuser: none},
			commitPick{kind: "commit", eventType: "auth.user.updated", offStream: audit.OffStreamPublicProfile}},
	} {
		picks := recordCommits(t)
		c.req.Principal, c.req.Actor, c.req.Authorize = uuid.New(), uuid.New(), none
		if err := store.AdminUpdateUser(context.Background(), c.req, cur); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(*picks) != 1 || (*picks)[0] != c.want {
			t.Errorf("%s: commits %+v, want one %+v", c.name, *picks, c.want)
		}
	}
}
