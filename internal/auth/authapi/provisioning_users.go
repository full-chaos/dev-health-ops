package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// The user-admin routes: the writes of go-api's /api/v1/admin/users routes while auth-service owns users. go-api
// keeps its route, its guards and its body checks, then calls these with its workload token; each writes the auth
// rows and go-api's public rows in one commit, and go-api answers from its read of public.users. They mount only
// with their endpoint-profile rows.
const (
	PatternUsersCreate   = "/internal/v1/users/create"
	PatternUsersUpdate   = "/internal/v1/users/update"
	PatternUsersPassword = "/internal/v1/users/password"
	PatternUsersDelete   = "/internal/v1/users/delete"
)

const (
	opUsersCreate   = "users_create"
	opUsersUpdate   = "users_update"
	opUsersPassword = "users_password"
	opUsersDelete   = "users_delete"
)

// Refusal codes of the user-admin routes.
const (
	reasonEmailTaken           = "email_taken"
	reasonUsernameTaken        = "username_taken"
	reasonAdminPasswordInvalid = "admin_password_invalid"
	reasonLastPlatformAdmin    = "last_platform_admin"
	reasonProviderIDInvalid    = "provider_id_invalid"
)

// usersAdminPatterns are the user-admin routes; they mount together, only when each has its row (UsersAdminRoutes).
var usersAdminPatterns = []string{PatternUsersCreate, PatternUsersUpdate, PatternUsersPassword, PatternUsersDelete}

// UsersAdminRouteCount is the number of user-admin routes.
func UsersAdminRouteCount() int { return len(usersAdminPatterns) }

// UsersAdminRowsError is nil when the endpoint-profile document (nil: the one the binary embeds) holds, for every
// user-admin route, a row that admits exactly the go-api workload token; otherwise it is the *ProvisioningRowError of
// the first route without one.
func UsersAdminRowsError(raw []byte) error {
	if raw == nil {
		raw = authv1.AuthServiceEndpointProfiles
	}
	return rowsExactFor(raw, usersAdminPatterns, []string{workloadTokenClass})
}

// maxUsersAdminBody bounds a user-admin body: a create carries a verifier and request metadata.
const maxUsersAdminBody = 8 << 10

// The members each user-admin route admits; any other member is malformed. The exported funcs below return copies for
// go-api's client tests.
var (
	usersCreateMembers = []string{"actor_principal_id", "email", "password_hash", "username", "full_name", "auth_provider",
		"auth_provider_id", "is_verified", "grant_platform_admin", "organization_id", "role", "request_metadata"}
	usersUpdateMembers = []string{"actor_principal_id", "principal_id", "organization_id", "email", "is_verified", "is_superuser",
		"username", "full_name", "avatar_url", "is_active"}
	usersPasswordMembers = []string{"actor_principal_id", "principal_id", "organization_id", "admin_password", "password_hash",
		"request_metadata"}
	usersDeleteMembers = []string{"actor_principal_id", "principal_id", "organization_id", "mode"}
)

// UsersCreateMembers returns a copy of the members the users create route admits.
func UsersCreateMembers() []string { return append([]string(nil), usersCreateMembers...) }

// UsersUpdateMembers returns a copy of the members the users update route admits.
func UsersUpdateMembers() []string { return append([]string(nil), usersUpdateMembers...) }

// UsersPasswordMembers returns a copy of the members the users password route admits.
func UsersPasswordMembers() []string { return append([]string(nil), usersPasswordMembers...) }

// UsersDeleteMembers returns a copy of the members the users delete route admits.
func UsersDeleteMembers() []string { return append([]string(nil), usersDeleteMembers...) }

// usersAdminOperations are the user-admin routes' operations. They take the go-api workload token only, as the
// workloadOperations do; they are listed apart because their rows are a separate gate (withUsersAdminRoutes).
var usersAdminOperations = map[string]bool{opUsersCreate: true, opUsersUpdate: true, opUsersPassword: true, opUsersDelete: true}

// UsersAdmin is the user-admin store (*identity.Store).
type UsersAdmin interface {
	AdminCreateUser(ctx context.Context, req identity.AdminCreateRequest) (identity.AdminUser, error)
	AdminUpdateState(ctx context.Context, principal uuid.UUID) (identity.AdminUpdateCurrent, error)
	AdminUpdateUser(ctx context.Context, req identity.AdminUpdateRequest, cur identity.AdminUpdateCurrent) error
	VerifyAdminPassword(ctx context.Context, actor uuid.UUID, password string) (bool, error)
	AdminSetPassword(ctx context.Context, req identity.AdminPasswordRequest) error
	AdminSoftDeleteUser(ctx context.Context, req identity.AdminSoftDeleteRequest) (identity.AdminSoftDeleteResult, error)
}

// UsersAdminStores are the user-admin routes' stores, a part of ProvisioningDeps; with either nil, or without the
// routes' endpoint-profile rows, those routes are not mounted.
type UsersAdminStores struct {
	UsersAdmin    UsersAdmin
	PlatformRoles PlatformRoles
}

// PlatformRoles is the platform role store as the user-admin routes use it (PlatformRoleStore wraps
// *platformrole.Store).
type PlatformRoles interface {
	LockWrites(ctx context.Context, tx audit.TxOps) error
	LockWritesShared(ctx context.Context, tx audit.TxOps) error
	GrantOn(ctx context.Context, tx audit.TxOps, in platformrole.GrantInput) (uuid.UUID, error)
	RevokeOn(ctx context.Context, tx audit.TxOps, in platformrole.RevokeInput) error
	// HeldOn reads the stored assignment, whatever the principal's state: the update's role change is decided from it.
	HeldOn(ctx context.Context, q platformrole.RowQuerier, principal uuid.UUID, role string) (bool, error)
	Grant(ctx context.Context, in platformrole.GrantInput) (uuid.UUID, error)
	Revoke(ctx context.Context, in platformrole.RevokeInput) error
	// RefuseLastPlatformAdmin refuses an erase of the last active platform_admin, after LockWrites.
	RefuseLastPlatformAdmin(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error
}

// PlatformRoleStore is PlatformRoles over the store.
type PlatformRoleStore struct{ *platformrole.Store }

func (PlatformRoleStore) LockWrites(ctx context.Context, tx audit.TxOps) error {
	return platformrole.LockWrites(ctx, tx)
}

func (PlatformRoleStore) LockWritesShared(ctx context.Context, tx audit.TxOps) error {
	return platformrole.LockWritesShared(ctx, tx)
}

// UsersAdminRoutes returns the user-admin routes, apart from ProvisioningRoutes and its AUTH_MEMBERSHIP_ROUTES switch:
// auth-service mounts them while it owns users. They take the go-api workload token on the provisioning routes'
// workload path. With no endpoint-profile row for any one of them, none is built and nothing is refused: the routes
// stay dark until their rows ship. deps needs Users, Reader, Organizations and both UsersAdminStores.
func UsersAdminRoutes(deps ProvisioningDeps) ([]httpapi.Route, error) {
	if deps.Users == nil || deps.Reader == nil || deps.Organizations == nil || deps.UsersAdmin == nil || deps.PlatformRoles == nil {
		return nil, errors.New("authapi: the user-admin routes need users, a reader, organizations, the user-admin store and the platform role store")
	}
	if UsersAdminRowsError(deps.ProfileRows) != nil {
		return nil, nil
	}
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	meter := deps.Meter
	if meter == nil {
		meter = otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/authapi")
	}
	rec, err := newRecorder(deps.Logger, meter)
	if err != nil {
		return nil, err
	}
	retired, err := meter.Int64Counter("auth.provisioning.retired_credential_refusals",
		metric.WithDescription("Provisioning route requests refused because they presented the retired session issuer credential, by operation."))
	if err != nil {
		return nil, err
	}
	h := provisioningHandlers{ProvisioningDeps: deps, rec: rec, retiredRefusals: retired}
	return []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternUsersCreate, Handler: http.HandlerFunc(h.usersCreate)},
		{Method: http.MethodPost, Pattern: PatternUsersUpdate, Handler: http.HandlerFunc(h.usersUpdate)},
		{Method: http.MethodPost, Pattern: PatternUsersPassword, Handler: http.HandlerFunc(h.usersPassword)},
		{Method: http.MethodPost, Pattern: PatternUsersDelete, Handler: http.HandlerFunc(h.usersDelete)},
	}, nil
}

// usersActorRule is go-api's guard of an admin users write, read live on q: an active actor holding admin.users
// passes; otherwise the write needs an organization the actor owns or administers, no platform admin grant, and
// (for a write on a user) a target that is a member of that organization and is no superuser.
func (h provisioningHandlers) usersActorRule(ctx context.Context, q session.Queryer, actor uuid.UUID, orgID *uuid.UUID,
	superuserWrite bool, target *uuid.UUID) error {
	state, found, err := h.Users.StateOn(ctx, q, actor)
	if err != nil {
		return err
	}
	if !found || !state.IsActive {
		return errActorRefused{reasonDeactivated}
	}
	if state.Holds(actionAdminUsers) {
		return nil
	}
	if superuserWrite || orgID == nil {
		return errActorRefused{reasonSuperuserRequired}
	}
	role, member, err := h.Users.RoleOn(ctx, q, actor, *orgID)
	if err != nil {
		return err
	}
	if !member || role == nil || (*role != "owner" && *role != "admin") {
		return errActorRefused{reasonAdminRequired}
	}
	if target == nil {
		return nil
	}
	if _, inOrg, err := h.Users.RoleOn(ctx, q, *target, *orgID); err != nil {
		return err
	} else if !inOrg {
		return errActorRefused{reasonPrincipalNotFound}
	}
	return h.targetsRule(ctx, q, state, []uuid.UUID{*target})
}

// usersPrecheck runs usersActorRule on the pool before anything else; the store runs it again under its locks.
func (h provisioningHandlers) usersPrecheck(w http.ResponseWriter, r *http.Request, op string, actor uuid.UUID, orgID *uuid.UUID,
	superuserWrite bool, target *uuid.UUID) bool {
	err := h.usersActorRule(r.Context(), h.Reader, actor, orgID, superuserWrite, target)
	if err == nil {
		return true
	}
	h.answerUsersError(w, r, op, err)
	return false
}

// usersAuthorize is the store's Authorize: the platform role write lock (exclusive when the write changes a
// platform role, shared otherwise) before any row, then usersActorRule on the transaction with grantRequested, the
// platform_admin grant the request asks for (go-api guards.go:106-109: a non-superuser is refused for a grant or a
// superuser target, never for an unchanged false). An is_active change runs no last platform_admin guard: legacy's
// PATCH set is_active with none (users.py UserService.update, CHAOS-13684, product rule 1); the soft delete keeps it.
func (h provisioningHandlers) usersAuthorize(actor uuid.UUID, orgID *uuid.UUID, roleChange, grantRequested bool, target *uuid.UUID) func(context.Context, audit.TxOps) error {
	return func(ctx context.Context, tx audit.TxOps) error {
		lock := h.PlatformRoles.LockWritesShared
		if roleChange {
			lock = h.PlatformRoles.LockWrites
		}
		if err := lock(ctx, tx); err != nil {
			return err
		}
		return h.usersActorRule(ctx, tx, actor, orgID, grantRequested, target)
	}
}

// usersDeleteAuthorize is a delete's Authorize: the platform role write lock exclusive (an erase can remove the last
// platform_admin, so it serializes with every grant and revoke), usersActorRule on the transaction, then the last
// platform_admin guard (pm D6878).
func (h provisioningHandlers) usersDeleteAuthorize(actor uuid.UUID, orgID *uuid.UUID, target uuid.UUID) func(context.Context, audit.TxOps) error {
	return func(ctx context.Context, tx audit.TxOps) error {
		if err := h.PlatformRoles.LockWrites(ctx, tx); err != nil {
			return err
		}
		if err := h.usersActorRule(ctx, tx, actor, orgID, false, &target); err != nil {
			return err
		}
		return h.PlatformRoles.RefuseLastPlatformAdmin(ctx, tx, target)
	}
}

// answerUsersError writes the answer of a refused or failed user-admin write.
func (h provisioningHandlers) answerUsersError(w http.ResponseWriter, r *http.Request, op string, err error) {
	var refused errActorRefused
	answer := unavailable(reasonStoreUnavailable)
	switch {
	case errors.As(err, &refused):
		answer = refusal{http.StatusForbidden, refused.reason}
		if refused.reason == reasonPrincipalNotFound {
			answer.status = http.StatusNotFound
		}
	case errors.Is(err, identity.ErrEmailTaken):
		answer = refusal{http.StatusConflict, reasonEmailTaken}
	case errors.Is(err, identity.ErrUsernameTaken):
		answer = refusal{http.StatusConflict, reasonUsernameTaken}
	case errors.Is(err, identity.ErrInvalidProviderID):
		answer = refusal{http.StatusBadRequest, reasonProviderIDInvalid}
	case errors.Is(err, identity.ErrUserNotFound):
		answer = refusal{http.StatusNotFound, reasonPrincipalNotFound}
	case errors.Is(err, identity.ErrStateChangedUnderLock):
		answer = refusal{http.StatusConflict, reasonStateChanged}
	case errors.Is(err, identity.ErrOrganizationNotFound):
		answer = refusal{http.StatusNotFound, reasonOrganizationNotFound}
	case errors.Is(err, identity.ErrOrganizationSuspended):
		answer = refusal{http.StatusForbidden, reasonOrganizationInactive}
	case errors.Is(err, platformrole.ErrGranterNotHeld):
		answer = refusal{http.StatusForbidden, reasonSuperuserRequired}
	case errors.Is(err, platformrole.ErrLastPlatformAdmin):
		answer = refusal{http.StatusConflict, reasonLastPlatformAdmin}
	case errors.Is(err, platformrole.ErrAlreadyHeld), errors.Is(err, platformrole.ErrNotHeld):
		answer = refusal{http.StatusConflict, reasonStateChanged}
	case errors.Is(err, platformrole.ErrPrincipalOff):
		answer = refusal{http.StatusForbidden, reasonDeactivated}
	}
	h.rec.refuse(w, r, op, answer, failure(answer, err))
}

func (h provisioningHandlers) malformedUsers(w http.ResponseWriter, r *http.Request, op string) {
	h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
}

type usersCreateAnswer struct {
	PrincipalID string `json:"principal_id"`
	Email       string `json:"email"`
}

func (h provisioningHandlers) usersCreate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admitUpTo(w, r, opUsersCreate, maxUsersAdminBody, usersCreateMembers...)
	if !ok {
		return
	}
	actor, err1 := body.uuid("actor_principal_id")
	email, err2 := body.anyText("email", maxShortTextBytes)
	hash, err3 := body.optionalText("password_hash", maxShortTextBytes)
	username, err4 := body.optionalAnyText("username", maxShortTextBytes)
	fullName, err5 := body.optionalAnyText("full_name", maxShortTextBytes)
	provider, err6 := body.optionalAnyText("auth_provider", maxShortTextBytes)
	providerID, err7 := body.optionalAnyText("auth_provider_id", maxShortTextBytes)
	verified, err8 := body.optionalBool("is_verified")
	grant, err9 := body.optionalBool("grant_platform_admin")
	orgID, err10 := body.optionalUUID("organization_id")
	role, err11 := body.optionalText("role", maxShortTextBytes)
	metadata, err12 := body.optionalObject("request_metadata", maxRequestMetadataBytes)
	if errors.Join(err1, err2, err3, err4, err5, err6, err7, err8, err9, err10, err11, err12) != nil || (orgID == nil) != (role == nil) {
		h.malformedUsers(w, r, opUsersCreate)
		return
	}
	if !h.usersPrecheck(w, r, opUsersCreate, actor, orgID, grant, nil) {
		return
	}
	ctx := r.Context()
	if orgID != nil {
		if err := h.Organizations.EnsureOrganization(ctx, *orgID); err != nil {
			h.answerUsersError(w, r, opUsersCreate, err)
			return
		}
	}
	req := identity.AdminCreateRequest{Email: email, Username: username, FullName: fullName, PasswordHash: hash, AuthProvider: provider,
		AuthProviderID: providerID, Verified: verified, OrgID: orgID, RequestMetadata: metadata, Actor: actor,
		RequestID: requestid.Audit(ctx), Caller: caller, Authorize: h.usersAuthorize(actor, orgID, grant, grant, nil)}
	if role != nil {
		req.Role = *role
	}
	if grant {
		req.Grant = func(ctx context.Context, tx audit.TxOps, principal uuid.UUID) error {
			_, err := h.PlatformRoles.GrantOn(ctx, tx, platformrole.GrantInput{Actor: actor, Principal: principal, Role: platformrole.PlatformAdmin})
			return err
		}
	}
	created, err := h.UsersAdmin.AdminCreateUser(ctx, req)
	if err != nil {
		h.answerUsersError(w, r, opUsersCreate, err)
		return
	}
	h.rec.record(ctx, opUsersCreate, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, usersCreateAnswer{PrincipalID: created.ID.String(), Email: created.Email})
}

type usersChangeAnswer struct {
	PrincipalID string `json:"principal_id"`
	Changed     bool   `json:"changed"`
}

func (h provisioningHandlers) usersUpdate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opUsersUpdate, usersUpdateMembers...)
	if !ok {
		return
	}
	actor, err1 := body.uuid("actor_principal_id")
	target, err2 := body.uuid("principal_id")
	orgID, err3 := body.optionalUUID("organization_id")
	email, err4 := body.optionalAnyText("email", maxShortTextBytes)
	verified, err5 := optionalBoolPointer(body, "is_verified")
	superuser, err6 := optionalBoolPointer(body, "is_superuser")
	username, err7 := body.optionalAnyText("username", maxShortTextBytes)
	fullName, err8 := body.optionalAnyText("full_name", maxShortTextBytes)
	avatarURL, err9 := body.optionalAnyText("avatar_url", maxShortTextBytes)
	active, err10 := optionalBoolPointer(body, "is_active")
	if errors.Join(err1, err2, err3, err4, err5, err6, err7, err8, err9, err10) != nil {
		h.malformedUsers(w, r, opUsersUpdate)
		return
	}
	ctx := r.Context()
	grantRequested := superuser != nil && *superuser
	if !h.usersPrecheck(w, r, opUsersUpdate, actor, orgID, grantRequested, &target) {
		return
	}
	cur, err := h.UsersAdmin.AdminUpdateState(ctx, target)
	if err != nil {
		h.answerUsersError(w, r, opUsersUpdate, err)
		return
	}
	// The stored assignment, not the active authority: an inactive user who still holds platform_admin and is sent
	// is_superuser true is no change, and sent false is a revoke, as go-api writes its stored column.
	held, err := h.PlatformRoles.HeldOn(ctx, h.Reader, target, platformrole.PlatformAdmin)
	if err != nil {
		h.answerUsersError(w, r, opUsersUpdate, err)
		return
	}
	roleChange := superuser != nil && *superuser != held
	decided := identity.DecideAdminUpdate(cur, identity.AdminUpdatePatch{Email: email, Verified: verified, Username: username,
		FullName: fullName, AvatarURL: avatarURL, Active: active})
	var superuserWrite func(context.Context, audit.TxOps) error
	if roleChange {
		grantIt := *superuser
		superuserWrite = func(ctx context.Context, tx audit.TxOps) error {
			if grantIt {
				_, err := h.PlatformRoles.GrantOn(ctx, tx, platformrole.GrantInput{Actor: actor, Principal: target, Role: platformrole.PlatformAdmin,
					AllowDisabledTarget: true})
				return err
			}
			return h.PlatformRoles.RevokeOn(ctx, tx, platformrole.RevokeInput{Actor: actor, Principal: target, Role: platformrole.PlatformAdmin})
		}
	}
	// Every PATCH, a no-op and a platform role change only included, is one AdminUpdateUser: one lock order, and
	// public.users.updated_at moves as go-api's PATCH moves it (pm D6915).
	err = h.UsersAdmin.AdminUpdateUser(ctx, identity.AdminUpdateRequest{Principal: target, Email: email, Verified: verified,
		Username: username, FullName: fullName, AvatarURL: avatarURL, Active: active, Actor: actor,
		RequestID: requestid.Audit(ctx), Caller: caller, Authorize: h.usersAuthorize(actor, orgID, roleChange, grantRequested, &target),
		Superuser: superuserWrite}, cur)
	if err != nil {
		h.answerUsersError(w, r, opUsersUpdate, err)
		return
	}
	h.rec.record(ctx, opUsersUpdate, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, usersChangeAnswer{PrincipalID: target.String(), Changed: !decided.Empty() || roleChange})
}

// optionalBoolPointer reads an absent or null member as nil, else a JSON boolean.
func optionalBoolPointer(o strictObject, name string) (*bool, error) {
	if o.isNull(name) {
		return nil, nil
	}
	v, err := o.optionalBool(name)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

func (h provisioningHandlers) usersPassword(w http.ResponseWriter, r *http.Request) {
	body, _, ok := h.admit(w, r, opUsersPassword, usersPasswordMembers...)
	if !ok {
		return
	}
	actor, err1 := body.uuid("actor_principal_id")
	target, err2 := body.uuid("principal_id")
	orgID, err3 := body.optionalUUID("organization_id")
	adminPassword, err4 := body.text("admin_password", maxShortTextBytes)
	hash, err5 := body.text("password_hash", maxShortTextBytes)
	metadata, err6 := body.optionalObject("request_metadata", maxRequestMetadataBytes)
	if errors.Join(err1, err2, err3, err4, err5, err6) != nil {
		h.malformedUsers(w, r, opUsersPassword)
		return
	}
	ctx := r.Context()
	if !h.usersPrecheck(w, r, opUsersPassword, actor, orgID, false, &target) {
		return
	}
	match, err := h.UsersAdmin.VerifyAdminPassword(ctx, actor, adminPassword)
	if err != nil {
		h.answerUsersError(w, r, opUsersPassword, err)
		return
	}
	if !match {
		h.rec.refuse(w, r, opUsersPassword, refusal{http.StatusForbidden, reasonAdminPasswordInvalid}, nil)
		return
	}
	err = h.UsersAdmin.AdminSetPassword(ctx, identity.AdminPasswordRequest{Principal: target, Hash: hash, Actor: actor, AuditOrg: orgID, RequestMetadata: metadata, RequestID: requestid.Audit(ctx),
		Authorize: h.usersAuthorize(actor, orgID, false, false, &target)})
	if err != nil {
		h.answerUsersError(w, r, opUsersPassword, err)
		return
	}
	h.rec.record(ctx, opUsersPassword, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, usersChangeAnswer{PrincipalID: target.String(), Changed: true})
}

type usersDeleteAnswer struct {
	PrincipalID string `json:"principal_id"`
	WasActive   bool   `json:"was_active"`
	AlreadyDone bool   `json:"already_done"`
}

func (h provisioningHandlers) usersDelete(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opUsersDelete, usersDeleteMembers...)
	if !ok {
		return
	}
	actor, err1 := body.uuid("actor_principal_id")
	target, err2 := body.uuid("principal_id")
	orgID, err3 := body.optionalUUID("organization_id")
	mode, err4 := body.optionalText("mode", maxShortTextBytes)
	if errors.Join(err1, err2, err3, err4) != nil || mode != nil && *mode != "soft" {
		h.malformedUsers(w, r, opUsersDelete)
		return
	}
	ctx := r.Context()
	if !h.usersPrecheck(w, r, opUsersDelete, actor, orgID, false, &target) {
		return
	}
	result, err := h.UsersAdmin.AdminSoftDeleteUser(ctx, identity.AdminSoftDeleteRequest{Principal: target, Actor: actor,
		RequestID: requestid.Audit(ctx), Caller: caller, Authorize: h.usersDeleteAuthorize(actor, orgID, target)})
	if err != nil {
		h.answerUsersError(w, r, opUsersDelete, err)
		return
	}
	h.rec.record(ctx, opUsersDelete, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, usersDeleteAnswer{PrincipalID: target.String(), WasActive: result.WasActive, AlreadyDone: result.AlreadyDone})
}
