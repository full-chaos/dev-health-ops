package authapi

import (
	"context"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
)

// fakeUsersAdmin runs each write's Authorize (and a create's Grant) on an empty transaction, as the store does
// after its locks, then answers. The acting admin's password is usersAdminFixturePassword.
type fakeUsersAdmin struct {
	calls     int
	create    identity.AdminCreateRequest
	update    identity.AdminUpdateRequest
	password  identity.AdminPasswordRequest
	soft      identity.AdminSoftDeleteRequest
	current   identity.AdminUpdateCurrent
	err       error
	softDone  map[uuid.UUID]bool
	createdID uuid.UUID
}

func (f *fakeUsersAdmin) AdminCreateUser(ctx context.Context, req identity.AdminCreateRequest) (identity.AdminUser, error) {
	f.calls++
	f.create = req
	if err := req.Authorize(ctx, audit.TxOps{}); err != nil {
		return identity.AdminUser{}, err
	}
	f.createdID = uuid.New()
	if req.Grant != nil {
		if err := req.Grant(ctx, audit.TxOps{}, f.createdID); err != nil {
			return identity.AdminUser{}, err
		}
	}
	if f.err != nil {
		return identity.AdminUser{}, f.err
	}
	return identity.AdminUser{ID: f.createdID, Email: identity.AdminStoredEmail(req.Email)}, nil
}

func (f *fakeUsersAdmin) AdminUpdateState(context.Context, uuid.UUID) (identity.AdminUpdateCurrent, error) {
	return f.current, nil
}

func (f *fakeUsersAdmin) AdminUpdateUser(ctx context.Context, req identity.AdminUpdateRequest, _ identity.AdminUpdateCurrent) error {
	f.calls++
	f.update = req
	if err := req.Authorize(ctx, audit.TxOps{}); err != nil {
		return err
	}
	if req.Superuser != nil {
		if err := req.Superuser(ctx, audit.TxOps{}); err != nil {
			return err
		}
	}
	return f.err
}

func (f *fakeUsersAdmin) VerifyAdminPassword(_ context.Context, _ uuid.UUID, password string) (bool, error) {
	return password == usersAdminFixturePassword, nil
}

func (f *fakeUsersAdmin) AdminSetPassword(ctx context.Context, req identity.AdminPasswordRequest) error {
	f.calls++
	f.password = req
	if err := req.Authorize(ctx, audit.TxOps{}); err != nil {
		return err
	}
	return f.err
}

func (f *fakeUsersAdmin) AdminSoftDeleteUser(ctx context.Context, req identity.AdminSoftDeleteRequest) (identity.AdminSoftDeleteResult, error) {
	f.calls++
	f.soft = req
	if err := req.Authorize(ctx, audit.TxOps{}); err != nil {
		return identity.AdminSoftDeleteResult{}, err
	}
	if f.softDone == nil {
		f.softDone = map[uuid.UUID]bool{}
	}
	done := f.softDone[req.Principal]
	f.softDone[req.Principal] = true
	return identity.AdminSoftDeleteResult{WasActive: !done, AlreadyDone: done}, f.err
}

// fakePlatformRoles records the platform role writes; its locks do nothing.
type fakePlatformRoles struct {
	locks, sharedLocks                int
	grants, revokes                   []uuid.UUID
	grantInputs                       []platformrole.GrantInput
	direct                            int // the store's own Grant or Revoke (its own commit), not GrantOn or RevokeOn
	grantErr, revokeErr, lastAdminErr error
	lastAdminChecks                   []uuid.UUID
	held                              map[uuid.UUID]bool // stored platform_admin assignments, whatever the principal's state
	heldReads                         []uuid.UUID
}

func (f *fakePlatformRoles) HeldOn(_ context.Context, _ platformrole.RowQuerier, principal uuid.UUID, role string) (bool, error) {
	f.heldReads = append(f.heldReads, principal)
	return role == platformrole.PlatformAdmin && f.held[principal], nil
}

func (f *fakePlatformRoles) RefuseLastPlatformAdmin(_ context.Context, _ audit.TxOps, principal uuid.UUID) error {
	f.lastAdminChecks = append(f.lastAdminChecks, principal)
	return f.lastAdminErr
}

func (f *fakePlatformRoles) LockWrites(context.Context, audit.TxOps) error { f.locks++; return nil }
func (f *fakePlatformRoles) LockWritesShared(context.Context, audit.TxOps) error {
	f.sharedLocks++
	return nil
}

func (f *fakePlatformRoles) GrantOn(_ context.Context, _ audit.TxOps, in platformrole.GrantInput) (uuid.UUID, error) {
	f.grants = append(f.grants, in.Principal)
	f.grantInputs = append(f.grantInputs, in)
	return uuid.New(), f.grantErr
}

func (f *fakePlatformRoles) RevokeOn(_ context.Context, _ audit.TxOps, in platformrole.RevokeInput) error {
	f.revokes = append(f.revokes, in.Principal)
	return f.revokeErr
}

func (f *fakePlatformRoles) Grant(_ context.Context, in platformrole.GrantInput) (uuid.UUID, error) {
	f.direct++
	f.grants = append(f.grants, in.Principal)
	f.grantInputs = append(f.grantInputs, in)
	return uuid.New(), f.grantErr
}

func (f *fakePlatformRoles) Revoke(_ context.Context, in platformrole.RevokeInput) error {
	f.direct++
	f.revokes = append(f.revokes, in.Principal)
	return f.revokeErr
}
