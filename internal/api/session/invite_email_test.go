package session

import "testing"

func TestInviteEmailRefusal(t *testing.T) {
	for _, tc := range []struct {
		name, user, invite string
		verified           bool
		want               string
	}{
		{"match", "ada@example.com", "ada@example.com", true, ""},
		{"case and space variant", " Ada@Example.com ", "ada@example.COM", true, ""},
		{"different", "ada@example.com", "grace@example.com", true, reasonInviteEmailMismatch},
		{"unverified", "ada@example.com", "ada@example.com", false, reasonInviteEmailUnverified},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := inviteEmailRefusal(&pendingInvite{Email: tc.invite}, &userRow{Email: tc.user, IsVerified: tc.verified})
			if got != tc.want {
				t.Fatalf("refusal = %q, want %q", got, tc.want)
			}
		})
	}
}
