package admin

import (
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func memberFixture(name, email *string) *memberWithUser {
	now := time.Date(2026, time.October, 8, 12, 0, 0, 0, time.UTC)
	return &memberWithUser{
		membership: membership{
			ID: uuid.New(), OrgID: uuid.New(), UserID: uuid.New(), Role: "member",
			CreatedAt: now, UpdatedAt: now,
		},
		UserName: name, UserEmail: email,
	}
}

func memberField(t *testing.T, o *pyjson.Object, key string) any {
	t.Helper()
	v, ok := o.Get(key)
	if !ok {
		t.Fatalf("member item has no %s key", key)
	}
	return v
}

func TestMemberItemServesUserNameAndEmail(t *testing.T) {
	name, email := "Ari Admin", "ari@example.com"
	o := memberWithUserResponseObject(memberFixture(&name, &email))
	if got := memberField(t, o, "user_name"); got != "Ari Admin" {
		t.Fatalf("user_name = %#v", got)
	}
	if got := memberField(t, o, "user_email"); got != "ari@example.com" {
		t.Fatalf("user_email = %#v", got)
	}
}

func TestMemberItemNamesAreNullWhenThereIsNoUserRowOrBlankName(t *testing.T) {
	blank, email := "  ", "only-email@example.com"
	for label, m := range map[string]*memberWithUser{
		"no user row": memberFixture(nil, nil),
		"blank name":  memberFixture(&blank, &email),
	} {
		o := memberWithUserResponseObject(m)
		if got := memberField(t, o, "user_name"); got != nil {
			t.Fatalf("%s: user_name = %#v, want null", label, got)
		}
	}
	if got := memberField(t, memberWithUserResponseObject(memberFixture(nil, nil)), "user_email"); got != nil {
		t.Fatalf("user_email = %#v, want null", got)
	}
}

func TestAddAndPatchMemberItemsKeepThePythonShape(t *testing.T) {
	o := membershipResponseObject(&memberFixture(nil, nil).membership)
	for _, key := range []string{"user_name", "user_email"} {
		if _, ok := o.Get(key); ok {
			t.Fatalf("membershipResponseObject carries %s", key)
		}
	}
}

func TestMemberListQueryReadsUsersOnlyThroughTheCallersOrgMemberships(t *testing.T) {
	for _, want := range []string{"FROM memberships m", "LEFT JOIN users u ON u.id = m.user_id", "WHERE m.org_id = $1"} {
		if !strings.Contains(listMembersWithUsersQuery, want) {
			t.Fatalf("member list query missing %q:\n%s", want, listMembersWithUsersQuery)
		}
	}
}
