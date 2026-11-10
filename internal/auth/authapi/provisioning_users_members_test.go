package authapi

import (
	"slices"
	"testing"
)

// Each exported admit list is a copy: a caller that appends to or edits what it got changes neither the next call nor
// the list the route admits with.
func TestUsersAdminMembersFuncsReturnACopy(t *testing.T) {
	for name, c := range map[string]struct {
		get    func() []string
		shared []string
	}{
		"create":   {UsersCreateMembers, usersCreateMembers},
		"update":   {UsersUpdateMembers, usersUpdateMembers},
		"password": {UsersPasswordMembers, usersPasswordMembers},
		"delete":   {UsersDeleteMembers, usersDeleteMembers},
	} {
		want := slices.Clone(c.shared)
		got := c.get()
		if !slices.Equal(got, want) {
			t.Errorf("%s members %v, want %v", name, got, want)
			continue
		}
		got[0] = "edited"
		_ = append(got[:1], "appended")
		if again := c.get(); !slices.Equal(again, want) || !slices.Equal(c.shared, want) {
			t.Errorf("%s: after an edit of the returned slice the next call is %v and the admit list %v, want %v", name, again, c.shared, want)
		}
	}
}
