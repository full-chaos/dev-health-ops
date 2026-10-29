package server

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// queryAPIStoreBase is what the query-api users store wraps: the live users,
// membership, impersonation and session reads (policy.PGStore in production).
type queryAPIStoreBase interface {
	policy.Store
	policy.SessionStore
}

// queryAPIUserStore is query-api's live users-row store. Its AccessUser, the
// users read of a user access token, reads only the users columns the
// query-api role is granted (postgres.QueryAPIAuthorization): it is UserState
// (is_active, is_superuser, token_version). policy.PGStore's own AccessUser
// also selects email, username and full_name, which the role cannot read.
// No query-api answer carries those three.
type queryAPIUserStore struct{ queryAPIStoreBase }

func newQueryAPIUserStore(store policy.PGStore) queryAPIUserStore {
	return queryAPIUserStore{queryAPIStoreBase: store}
}

// AccessUser implements policy.AccessUserStore over UserState.
func (s queryAPIUserStore) AccessUser(ctx context.Context, id uuid.UUID) (policy.AccessUserRow, bool, error) {
	state, found, err := s.UserState(ctx, id)
	if err != nil || !found {
		return policy.AccessUserRow{}, false, err
	}
	return policy.AccessUserRow{State: state}, true, nil
}
