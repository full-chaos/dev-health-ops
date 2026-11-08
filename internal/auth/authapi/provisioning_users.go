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
)

// usersAdminPatterns are the user-admin routes; they mount together, only when each has its row (UsersAdminRoutes).
var usersAdminPatterns = []string{PatternUsersCreate, PatternUsersUpdate, PatternUsersPassword, PatternUsersDelete}

// maxUsersAdminBody bounds a user-admin body: a create carries a verifier and request metadata.
const maxUsersAdminBody = 8 << 10

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
	Grant(ctx context.Context, in platformrole.GrantInput) (uuid.UUID, error)
	Revoke(ctx context.Context, in platformrole.RevokeInput) error
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
	rows := deps.ProfileRows
	if rows == nil {
		rows = authv1.AuthServiceEndpointProfiles
	}
	if rowsExactFor(rows, usersAdminPatterns, []string{workloadTokenClass}) != nil {
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
// platform role, shared otherwise) before any row, then usersActorRule on the transaction.
func (h provisioningHandlers) usersAuthorize(actor uuid.UUID, orgID *uuid.UUID, superuserWrite bool, target *uuid.UUID) func(context.Context, audit.TxOps) error {
	return func(ctx context.Context, tx audit.TxOps) error {
		lock := h.PlatformRoles.LockWritesShared
		if superuserWrite {
			lock = h.PlatformRoles.LockWrites
		}
		if err := lock(ctx, tx); err != nil {
			return err
		}
		return h.usersActorRule(ctx, tx, actor, orgID, superuserWrite, target)
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
	body, caller, ok := h.admitUpTo(w, r, opUsersCreate, maxUsersAdminBody, "actor_principal_id", "email", "password_hash", "username",
		"full_name", "auth_provider", "auth_provider_id", "is_verified", "grant_platform_admin", "organization_id", "role", "request_metadata")
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
		RequestID: requestid.Audit(ctx), Caller: caller, Authorize: h.usersAuthorize(actor, orgID, grant, nil)}
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
	body, caller, ok := h.admit(w, r, opUsersUpdate, "actor_principal_id", "principal_id", "organization_id", "email", "is_verified", "is_superuser")
	if !ok {
		return
	}
	actor, err1 := body.uuid("actor_principal_id")
	target, err2 := body.uuid("principal_id")
	orgID, err3 := body.optionalUUID("organization_id")
	email, err4 := body.optionalAnyText("email", maxShortTextBytes)
	verified, err5 := optionalBoolPointer(body, "is_verified")
	superuser, err6 := optionalBoolPointer(body, "is_superuser")
	if errors.Join(err1, err2, err3, err4, err5, err6) != nil {
		h.malformedUsers(w, r, opUsersUpdate)
		return
	}
	ctx := r.Context()
	if !h.usersPrecheck(w, r, opUsersUpdate, actor, orgID, superuser != nil, &target) {
		return
	}
	cur, err := h.UsersAdmin.AdminUpdateState(ctx, target)
	if err != nil {
		h.answerUsersError(w, r, opUsersUpdate, err)
		return
	}
	state, _, err := h.Users.StateOn(ctx, h.Reader, target)
	if err != nil {
		h.answerUsersError(w, r, opUsersUpdate, err)
		return
	}
	roleChange := superuser != nil && *superuser != state.Holds(actionAdminUsers)
	decided := identity.DecideAdminUpdate(cur, email, verified)
	var superuserWrite func(context.Context, audit.TxOps) error
	if roleChange {
		grantIt := *superuser
		superuserWrite = func(ctx context.Context, tx audit.TxOps) error {
			if grantIt {
				_, err := h.PlatformRoles.GrantOn(ctx, tx, platformrole.GrantInput{Actor: actor, Principal: target, Role: platformrole.PlatformAdmin})
				return err
			}
			return h.PlatformRoles.RevokeOn(ctx, tx, platformrole.RevokeInput{Actor: actor, Principal: target, Role: platformrole.PlatformAdmin})
		}
	}
	switch {
	case decided.Empty() && !roleChange:
		h.rec.record(ctx, opUsersUpdate, "success", reasonAccepted, nil)
		writeJSON(w, http.StatusOK, usersChangeAnswer{PrincipalID: target.String()})
		return
	case decided.Empty():
		if *superuser {
			_, err = h.PlatformRoles.Grant(ctx, platformrole.GrantInput{Actor: actor, Principal: target, Role: platformrole.PlatformAdmin})
		} else {
			err = h.PlatformRoles.Revoke(ctx, platformrole.RevokeInput{Actor: actor, Principal: target, Role: platformrole.PlatformAdmin})
		}
	default:
		err = h.UsersAdmin.AdminUpdateUser(ctx, identity.AdminUpdateRequest{Principal: target, Email: email, Verified: verified, Actor: actor,
			RequestID: requestid.Audit(ctx), Caller: caller, Authorize: h.usersAuthorize(actor, orgID, roleChange, &target),
			Superuser: superuserWrite}, cur)
	}
	if err != nil {
		h.answerUsersError(w, r, opUsersUpdate, err)
		return
	}
	h.rec.record(ctx, opUsersUpdate, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, usersChangeAnswer{PrincipalID: target.String(), Changed: true})
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
	body, _, ok := h.admit(w, r, opUsersPassword, "actor_principal_id", "principal_id", "organization_id", "admin_password", "password_hash",
		"request_metadata")
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
		Authorize: h.usersAuthorize(actor, orgID, false, &target)})
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
	body, caller, ok := h.admit(w, r, opUsersDelete, "actor_principal_id", "principal_id", "organization_id", "mode")
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
		RequestID: requestid.Audit(ctx), Caller: caller, Authorize: h.usersAuthorize(actor, orgID, false, &target)})
	if err != nil {
		h.answerUsersError(w, r, opUsersDelete, err)
		return
	}
	h.rec.record(ctx, opUsersDelete, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, usersDeleteAnswer{PrincipalID: target.String(), WasActive: result.WasActive, AlreadyDone: result.AlreadyDone})
}
