package identity

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
)

// CHAOS-13684 (pm D7015): an admin update takes is_active. Only a value that differs from the stored public.users
// is_active is a change; an equal one changes nothing (A4).
func TestDecideAdminUpdateActiveIsARealChangeOnly(t *testing.T) {
	yes, no := true, false
	if d := DecideAdminUpdate(AdminUpdateCurrent{Active: true}, AdminUpdatePatch{Active: &yes}); !d.Empty() || d.Active != nil {
		t.Errorf("active user sent true: %+v, want no change", d)
	}
	if d := DecideAdminUpdate(AdminUpdateCurrent{Active: true}, AdminUpdatePatch{Active: &no}); d.Empty() || d.Active == nil || *d.Active {
		t.Errorf("active user sent false: %+v, want a deactivate", d)
	}
	if d := DecideAdminUpdate(AdminUpdateCurrent{Active: false}, AdminUpdatePatch{Active: &yes}); d.Active == nil || !*d.Active {
		t.Errorf("inactive user sent true: %+v, want a reactivate", d)
	}
	if sameDecision(DecideAdminUpdate(AdminUpdateCurrent{Active: true}, AdminUpdatePatch{Active: &no}), AdminUpdateDecision{}) {
		t.Error("sameDecision ignores an is_active change: a concurrent change under the locks would go unseen")
	}
}

// A5: each real is_active change is one audit.Commit with one auth audit row and its outbox events: a deactivate is the
// soft delete's (auth.principal.disabled, principal.disabled + user.tokens_invalidated), a reactivate the principal's
// enable (auth.principal.enabled, principal.enabled). A4: an equal is_active is no principal event: the PATCH is the
// no-change touch, with no outbox event.
func TestAdminUpdateActiveCommitNamesThePrincipalEvent(t *testing.T) {
	store := lazyOwnedStore(t)
	none := func(context.Context, audit.TxOps) error { return nil }
	yes, no := true, false
	for _, c := range []struct {
		name string
		cur  AdminUpdateCurrent
		req  AdminUpdateRequest
		want commitPick
	}{
		{"deactivate", AdminUpdateCurrent{Email: "a@example.test", Active: true}, AdminUpdateRequest{Active: &no},
			commitPick{kind: "commit", eventType: "auth.principal.disabled", events: 2}},
		{"reactivate", AdminUpdateCurrent{Email: "a@example.test", Active: false}, AdminUpdateRequest{Active: &yes},
			commitPick{kind: "commit", eventType: "auth.principal.enabled", events: 1}},
		{"no-op", AdminUpdateCurrent{Email: "a@example.test", Active: true}, AdminUpdateRequest{Active: &yes},
			commitPick{kind: "public_projection", eventType: audit.EventUserTouched}},
	} {
		picks := recordCommits(t)
		c.req.Principal, c.req.Actor, c.req.Authorize = uuid.New(), uuid.New(), none
		if err := store.AdminUpdateUser(context.Background(), c.req, c.cur); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if len(*picks) != 1 || (*picks)[0] != c.want {
			t.Errorf("%s: commits %+v, want one %+v", c.name, *picks, c.want)
		}
	}
}

// A deactivate is the soft delete's writes: public.users is_active false with token_version + 1 (refresh refused, A1),
// the principal's disabled_at, auth.users legacy_token_version + 1.
func TestAdminDeactivateWritesBumpTheTokenVersions(t *testing.T) {
	tx := &recordingTx{}
	if _, err := applyAdminSoftDelete(context.Background(), tx, `"auth"`, uuid.New()); err != nil {
		t.Fatal(err)
	}
	all := strings.Join(tx.statements, "\n")
	for _, want := range []string{
		"UPDATE public.users SET is_active = false, token_version = token_version + 1",
		`UPDATE "auth".principals SET disabled_at = COALESCE(disabled_at, clock_timestamp())`,
		`UPDATE "auth".users SET legacy_token_version = legacy_token_version + 1`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("deactivate statements lack %q:\n%s", want, all)
		}
	}
}

// A reactivate clears the deactivate (A3): public.users is_active true and the principal's disabled_at NULL, and leaves
// token_version as legacy's update left it.
func TestAdminReactivateClearsDisabledAtAndKeepsTheTokenVersion(t *testing.T) {
	tx := &recordingTx{}
	if err := applyAdminReactivate(context.Background(), tx, `"auth"`, uuid.New()); err != nil {
		t.Fatal(err)
	}
	all := strings.Join(tx.statements, "\n")
	for _, want := range []string{
		"UPDATE public.users SET is_active = true",
		`UPDATE "auth".principals SET disabled_at = NULL`,
	} {
		if !strings.Contains(all, want) {
			t.Errorf("reactivate statements lack %q:\n%s", want, all)
		}
	}
	if strings.Contains(all, "token_version") {
		t.Errorf("reactivate writes a token version:\n%s", all)
	}
}
