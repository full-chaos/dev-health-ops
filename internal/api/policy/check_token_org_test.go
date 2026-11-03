package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestCheckTokenOrg(t *testing.T) {
	own := uuid.MustParse(ownOrg)
	for name, tc := range map[string]struct {
		mutate  func(*fakeStore)
		user    User
		wantErr func(error) bool
		reads   int
	}{
		"member": {nil, User{UserID: userID.String(), OrgID: ownOrg}, func(err error) bool { return err == nil }, 1},
		"removed member": {func(s *fakeStore) { delete(s.members, [2]uuid.UUID{userID, own}) },
			User{UserID: userID.String(), OrgID: ownOrg}, IsRefusal, 1},
		"store down": {func(s *fakeStore) { s.errMember = errors.Join(ErrUnavailable, errors.New("down")) },
			User{UserID: userID.String(), OrgID: ownOrg}, func(err error) bool { return errors.Is(err, ErrUnavailable) && !IsRefusal(err) }, 1},
		"superuser skips the read": {func(s *fakeStore) { delete(s.members, [2]uuid.UUID{userID, own}) },
			User{UserID: userID.String(), OrgID: ownOrg, IsSuperuser: true}, func(err error) bool { return err == nil }, 0},
		"no org reads nothing": {nil, User{UserID: userID.String()}, func(err error) bool { return err == nil }, 0},
		"org claim that is no uuid": {nil, User{UserID: userID.String(), OrgID: "org-1"}, IsRefusal, 0},
	} {
		t.Run(name, func(t *testing.T) {
			store := newStore()
			if tc.mutate != nil {
				tc.mutate(store)
			}
			err := authenticator(t, store).CheckTokenOrg(context.Background(), &tc.user)
			if !tc.wantErr(err) {
				t.Fatalf("err = %v", err)
			}
			if len(store.membershipSeen) != tc.reads {
				t.Fatalf("membership reads = %d, want %d", len(store.membershipSeen), tc.reads)
			}
		})
	}
}
