package authapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/invite"
	"github.com/full-chaos/dev-health-ops/internal/auth/membership"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/orgdeprovision"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/servicecred"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// Internal provisioning route patterns: the membership, invite and
// organization deprovision writes go-api makes through auth-service while
// auth-service owns users. go-api is the policy enforcement point and calls
// them with its workload token, or its session issuer credential holding
// memberships:write; the go-api proxy forwards only /api/v1/auth routes.
const (
	PatternMembershipAdd      = "/internal/v1/memberships/add"
	PatternMembershipRole     = "/internal/v1/memberships/role"
	PatternMembershipRemove   = "/internal/v1/memberships/remove"
	PatternMembershipTransfer = "/internal/v1/memberships/transfer"
	PatternInviteCreate       = "/internal/v1/invites/create"
	PatternInviteAccept       = "/internal/v1/invites/accept"
	PatternOrgDeprovision     = "/internal/v1/organizations/deprovision"
	PatternOrgReactivate      = "/internal/v1/organizations/reactivate"
)

// Outcome-metric operations of the provisioning routes.
const (
	opMembershipAdd      = "membership_add"
	opMembershipRole     = "membership_role"
	opMembershipRemove   = "membership_remove"
	opMembershipTransfer = "membership_transfer"
	opInviteCreate       = "invite_create"
	opInviteAccept       = "invite_accept"
	opOrgDeprovision     = "organization_deprovision"
	opOrgReactivate      = "organization_reactivate"
)

var provisioningOperations = []string{opMembershipAdd, opMembershipRole, opMembershipRemove, opMembershipTransfer,
	opInviteCreate, opInviteAccept, opOrgDeprovision, opOrgReactivate, opOrgSuspend, opInviteRevoke, opOrgState}

// Provisioning refusal codes beyond the shared ones.
const (
	reasonAdminRequired        = "admin_required"
	reasonSuperuserRequired    = "superuser_required"
	reasonOrganizationNotFound = "organization_not_found"
	reasonOrganizationInactive = "organization_suspended"
	reasonOrganizationConflict = "organization_conflict"
	reasonMembershipNotFound   = "membership_not_found"
	reasonAlreadyMember        = "already_member"
	reasonMembershipSuspended  = "membership_suspended"
	reasonLastOwner            = "last_owner"
	reasonNotOwner             = "not_owner"
	reasonTargetNotMember      = "target_not_member"
	reasonRoleInvalid          = "role_invalid"
	reasonRoleMissing          = "role_missing"
	reasonInviterNotFound      = "inviter_not_found"
	reasonInviteInvalid        = "invite_invalid"
	reasonEmailUnverified      = "email_unverified"
	reasonEmailMismatch        = "email_mismatch"
	reasonEmailInvalid         = "email_invalid"
	reasonTTLInvalid           = "ttl_invalid"
	reasonInvitePending        = "invite_pending"
	reasonUsersNotOwned        = "users_not_owned"
	reasonStateChanged         = "state_changed"
)

// ProvisioningCredentials checks the bearer of the provisioning routes
// (*servicecred.PG).
type ProvisioningCredentials interface {
	CheckScope(ctx context.Context, token contracts.SecretString, scope string) (servicecred.Credential, servicecred.Refusal, error)
}

// ProvisioningUsers reads live public.users and membership rows on the
// caller's connection or transaction (PGUsers): the actor rule reads with it
// before the verb and again inside the verb's transaction, after its locks.
type ProvisioningUsers interface {
	StateOn(ctx context.Context, q session.Queryer, userID uuid.UUID) (UserState, bool, error)
	RoleOn(ctx context.Context, q session.Queryer, userID, orgID uuid.UUID) (*string, bool, error)
}

// Memberships is the membership aggregate (*membership.Store).
type Memberships interface {
	Add(ctx context.Context, c membership.Change) (*membership.Membership, error)
	ChangeRole(ctx context.Context, c membership.Change) (*membership.Membership, error)
	Remove(ctx context.Context, c membership.Change) (*membership.Membership, error)
	TransferOwnership(ctx context.Context, t membership.Transfer) error
}

// Invites is the invite aggregate (*invite.Store).
type Invites interface {
	Create(ctx context.Context, in invite.CreateInput) (*invite.Created, error)
	Accept(ctx context.Context, in invite.AcceptInput) (*invite.Accepted, error)
	Revoke(ctx context.Context, in invite.RevokeInput) error
}

// Deprovisioner ends an organization (*orgdeprovision.Service).
type Deprovisioner interface {
	Deprovision(ctx context.Context, in orgdeprovision.Input) (*orgdeprovision.Result, error)
}

// OrganizationReactivator ends an organization's suspension, authorizing the
// actor under the row lock (*org.Store).
type OrganizationReactivator interface {
	ReactivateAuthorized(ctx context.Context, id uuid.UUID, ifRevision *int64, a org.Actor, authz org.Authorize) (changed bool, err error)
}

// OrganizationEnsurer projects go-api's organization when the auth schema
// has none yet, never changing an existing row (*identity.Store).
type OrganizationEnsurer interface {
	EnsureOrganization(ctx context.Context, orgID uuid.UUID) error
}

// ProvisioningDeps are the provisioning routes' collaborators.
type ProvisioningDeps struct {
	Credentials   ProvisioningCredentials
	Users         ProvisioningUsers
	Reader        session.Queryer
	Organizations OrganizationEnsurer
	Memberships   Memberships
	Invites       Invites
	Deprovision   Deprovisioner
	Orgs          OrganizationLifecycle
	// Workload is the go-api workload token path the session issue route
	// takes too (one subject); nil refuses every workload token.
	Workload *SessionIssueWorkload
	Logger   *slog.Logger
	Meter    metric.Meter
	// ProfileRows is the endpoint-profile document the routes are checked
	// against; nil is the one the binary embeds. Tests plant others.
	ProfileRows []byte
}

type provisioningHandlers struct {
	ProvisioningDeps
	rec recorder
	// provisioningUses counts every request a session issuer credential with
	// memberships:write was accepted on, by operation: the compatibility
	// credential these routes take until it is retired; a workload call is not counted.
	provisioningUses metric.Int64Counter
}

// ProvisioningRoutes returns the internal membership, invite and organization
// deprovision routes. Each registration is on a line of its own.
func ProvisioningRoutes(deps ProvisioningDeps) ([]httpapi.Route, error) {
	if deps.Credentials == nil || deps.Users == nil || deps.Reader == nil || deps.Organizations == nil ||
		deps.Memberships == nil || deps.Invites == nil || deps.Deprovision == nil || deps.Orgs == nil {
		return nil, errors.New("authapi: provisioning routes need credentials, users, a reader, organizations, memberships, invites, a deprovisioner and the organization reactivate and suspend verbs")
	}
	rows := deps.ProfileRows
	if rows == nil {
		rows = authv1.AuthServiceEndpointProfiles
	}
	if err := provisioningRowsPresent(rows); err != nil {
		return nil, err
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
	uses, err := meter.Int64Counter("auth.provisioning.compat_credential_uses",
		metric.WithDescription("Provisioning route requests accepted on the session issuer credential's memberships:write scope, by operation."))
	if err != nil {
		return nil, err
	}
	for _, op := range provisioningOperations {
		uses.Add(context.Background(), 0, metric.WithAttributes(attribute.String("operation", op)))
	}
	h := provisioningHandlers{ProvisioningDeps: deps, rec: rec, provisioningUses: uses}
	routes := []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternMembershipAdd, Handler: http.HandlerFunc(h.membershipAdd)},
		{Method: http.MethodPost, Pattern: PatternMembershipRole, Handler: http.HandlerFunc(h.membershipRole)},
		{Method: http.MethodPost, Pattern: PatternMembershipRemove, Handler: http.HandlerFunc(h.membershipRemove)},
		{Method: http.MethodPost, Pattern: PatternMembershipTransfer, Handler: http.HandlerFunc(h.membershipTransfer)},
		{Method: http.MethodPost, Pattern: PatternInviteCreate, Handler: http.HandlerFunc(h.inviteCreate)},
		{Method: http.MethodPost, Pattern: PatternInviteAccept, Handler: http.HandlerFunc(h.inviteAccept)},
		{Method: http.MethodPost, Pattern: PatternOrgDeprovision, Handler: http.HandlerFunc(h.orgDeprovision)},
		{Method: http.MethodPost, Pattern: PatternOrgReactivate, Handler: http.HandlerFunc(h.orgReactivate)},
		h.revokeRoute(h.Invites),
	}
	// The suspend route is mounted only when the profile document holds its row: without it the route answers 404, the
	// routes above are unaffected and the service starts. One line names the route and nothing else.
	if rowsPresentFor(rows, darkPatterns) == nil {
		routes = append(routes, httpapi.Route{Method: http.MethodPost, Pattern: PatternOrgSuspend, Handler: http.HandlerFunc(h.orgSuspend)})
	} else {
		deps.Logger.Info("auth provisioning route not mounted: no endpoint-profile row", "route", PatternOrgSuspend)
	}
	routes = h.withStateRoute(routes, rows)
	return routes, nil
}

// admit checks the bearer and reads the body as one strict object of the
// allowed members. It writes the refusal and returns false when either fails.
func (h provisioningHandlers) admit(w http.ResponseWriter, r *http.Request, op string, allowed ...string) (strictObject, audit.Caller, bool) {
	token, ok := bearerToken(r)
	if !ok {
		h.rec.refuse(w, r, op, unauthorized(reasonInvalidCredentials), nil)
		return nil, audit.Caller{}, false
	}
	caller, ok := h.caller(w, r, op, token)
	if !ok {
		return nil, audit.Caller{}, false
	}
	body, err := readStrictObject(http.MaxBytesReader(w, r.Body, maxProvisioningBody), allowed...)
	if err != nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return nil, audit.Caller{}, false
	}
	return body, caller, true
}

// workloadOperations are the operations that take the go-api workload token. The
// list is default-deny: any other operation (the invite revoke and organization
// suspend among them) refuses a workload token before the key set is read.
var workloadOperations = map[string]bool{opMembershipAdd: true, opMembershipRole: true, opMembershipRemove: true,
	opMembershipTransfer: true, opInviteCreate: true, opInviteAccept: true, opOrgDeprovision: true, opOrgReactivate: true, opOrgState: true}

// caller authorizes the bearer, its class read from the text before any store
// or key-set read: the session issuer credential holding memberships:write
// (counted until it is retired), or, on workloadOperations only, the go-api
// workload token (audience auth-service, the configured go-api subject). Every
// refusal is the one 401; the reason is only logged. On a refusal it has
// already answered.
func (h provisioningHandlers) caller(w http.ResponseWriter, r *http.Request, op, bearer string) (audit.Caller, bool) {
	ctx := r.Context()
	class, reason := classifyIssueBearer(bearer)
	switch class {
	case issueClassCompat:
		credential, refused, err := h.Credentials.CheckScope(ctx, contracts.NewSecretString(bearer), servicecred.ScopeMembershipsWrite)
		if err != nil {
			h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
			return audit.Caller{}, false
		}
		if refused == "" {
			h.provisioningUses.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", op)))
			return audit.Caller{CredentialID: credential.ID}, true
		}
		h.rec.logger.WarnContext(ctx, "provisioning route refused: service credential", "operation", op, "refusal", string(refused))
	case issueClassWorkload:
		if !workloadOperations[op] {
			h.rec.logger.WarnContext(ctx, "provisioning route refused: workload token", "operation", op, "refusal", "operation")
			break
		}
		workload, why, err := h.Workload.authorize(bearer)
		if err != nil {
			h.rec.refuse(w, r, op, unavailable(reasonDependency), err)
			return audit.Caller{}, false
		}
		if why == "" {
			return audit.Caller{Workload: workload.workload, JTI: workload.jti}, true
		}
		h.rec.logger.WarnContext(ctx, "provisioning route refused: workload token", "operation", op, "refusal", why)
	default:
		h.rec.logger.WarnContext(ctx, "provisioning route refused: bearer", "operation", op, "refusal", reason)
	}
	h.rec.refuse(w, r, op, unauthorized(reasonInvalidCredentials), nil)
	return audit.Caller{}, false
}

// errActorRefused is the actor rule's refusal; reason names which rule.
type errActorRefused struct{ reason string }

func (e errActorRefused) Error() string { return "authapi: actor refused: " + e.reason }

// actorRule is go-api's PEP rule, read live on q: the actor is an active
// account and a superuser, or (unless superuserOnly) an owner or admin member
// of the organization (ensureOrgAdminAccess). A store failure is returned as
// it is.
func (h provisioningHandlers) actorRule(ctx context.Context, q session.Queryer, actor, orgID uuid.UUID, superuserOnly bool) (UserState, error) {
	state, found, err := h.Users.StateOn(ctx, q, actor)
	if err != nil {
		return UserState{}, err
	}
	if !found || !state.IsActive {
		return UserState{}, errActorRefused{reasonDeactivated}
	}
	if state.IsSuperuser {
		return state, nil
	}
	if superuserOnly {
		return UserState{}, errActorRefused{reasonSuperuserRequired}
	}
	role, member, err := h.Users.RoleOn(ctx, q, actor, orgID)
	if err != nil {
		return UserState{}, err
	}
	if !member || role == nil || (*role != "owner" && *role != "admin") {
		return UserState{}, errActorRefused{reasonAdminRequired}
	}
	return state, nil
}

// targetsRule is go-api's refuseSuperuserWrite for an actor that is no
// superuser: no target may be a superuser. It reads the targets' users rows,
// which the membership verb holds FOR UPDATE (step 2).
func (h provisioningHandlers) targetsRule(ctx context.Context, q session.Queryer, actor UserState, targets []uuid.UUID) error {
	if actor.IsSuperuser {
		return nil
	}
	for _, target := range targets {
		state, _, err := h.Users.StateOn(ctx, q, target)
		if err != nil {
			return err
		}
		if state.IsSuperuser {
			return errActorRefused{reasonSuperuserRequired}
		}
	}
	return nil
}

// precheck runs the actor rule on the pool before anything is read or
// written for the request, so a refused actor learns nothing about the rows.
// The verb runs it again under its locks.
func (h provisioningHandlers) precheck(w http.ResponseWriter, r *http.Request, op string, actor, orgID uuid.UUID, superuserOnly bool, targets ...uuid.UUID) bool {
	ctx := r.Context()
	state, err := h.actorRule(ctx, h.Reader, actor, orgID, superuserOnly)
	if err == nil {
		err = h.targetsRule(ctx, h.Reader, state, targets)
	}
	var refused errActorRefused
	switch {
	case errors.As(err, &refused):
		h.rec.refuse(w, r, op, refusal{http.StatusForbidden, refused.reason}, nil)
		return false
	case err != nil:
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return false
	}
	if err := h.Organizations.EnsureOrganization(ctx, orgID); err != nil {
		answer := unavailable(reasonDependency)
		switch {
		case errors.Is(err, identity.ErrOrganizationNotFound):
			answer, err = refusal{http.StatusNotFound, reasonOrganizationNotFound}, nil
		case errors.Is(err, identity.ErrOrganizationSlugCollision):
			answer, err = refusal{http.StatusConflict, reasonOrganizationConflict}, nil
		}
		h.rec.refuse(w, r, op, answer, err)
		return false
	}
	return true
}

// membershipAuthorize is the verb's Authorize: the actor rule and the targets
// rule on the verb's transaction, after its locks.
func (h provisioningHandlers) membershipAuthorize(actor uuid.UUID) membership.Authorize {
	return func(ctx context.Context, tx audit.TxOps, l membership.Locked) error {
		state, err := h.actorRule(ctx, tx, actor, l.OrgID, false)
		if err == nil {
			err = h.targetsRule(ctx, tx, state, l.Principals)
		}
		var refused errActorRefused
		if errors.As(err, &refused) {
			if refused.reason == reasonSuperuserRequired {
				return membership.ErrSuperuserRequired
			}
			return membership.ErrDenied
		}
		return err
	}
}

func actorOf(ctx context.Context, actor uuid.UUID, caller audit.Caller) membership.Actor {
	return membership.Actor{PrincipalID: &actor, RequestID: requestid.Audit(ctx), Caller: caller}
}

func (h provisioningHandlers) membershipAdd(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opMembershipAdd, "organization_id", "principal_id", "actor_principal_id", "role", "invited_by_principal_id")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	principal, err2 := body.uuid("principal_id")
	actor, err3 := body.uuid("actor_principal_id")
	role, err4 := body.text("role", maxShortTextBytes)
	inviter, err5 := body.optionalUUID("invited_by_principal_id")
	if errors.Join(err1, err2, err3, err4, err5) != nil {
		h.rec.refuse(w, r, opMembershipAdd, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opMembershipAdd, actor, orgID, false, principal) {
		return
	}
	m, err := h.Memberships.Add(r.Context(), membership.Change{OrgID: orgID, PrincipalID: principal, Role: role, InvitedBy: inviter,
		Actor: actorOf(r.Context(), actor, caller), Authorize: h.membershipAuthorize(actor)})
	h.answerMembership(w, r, opMembershipAdd, m, err)
}

func (h provisioningHandlers) membershipRole(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opMembershipRole, "organization_id", "principal_id", "actor_principal_id", "role", "expected_revision")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	principal, err2 := body.uuid("principal_id")
	actor, err3 := body.uuid("actor_principal_id")
	role, err4 := body.text("role", maxShortTextBytes)
	revision, err5 := body.optionalInt("expected_revision", 1, 9007199254740991)
	if errors.Join(err1, err2, err3, err4, err5) != nil {
		h.rec.refuse(w, r, opMembershipRole, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opMembershipRole, actor, orgID, false, principal) {
		return
	}
	m, err := h.Memberships.ChangeRole(r.Context(), membership.Change{OrgID: orgID, PrincipalID: principal, Role: role, ExpectedRevision: revision,
		Actor: actorOf(r.Context(), actor, caller), Authorize: h.membershipAuthorize(actor)})
	h.answerMembership(w, r, opMembershipRole, m, err)
}

func (h provisioningHandlers) membershipRemove(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opMembershipRemove, "organization_id", "principal_id", "actor_principal_id")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	principal, err2 := body.uuid("principal_id")
	actor, err3 := body.uuid("actor_principal_id")
	if errors.Join(err1, err2, err3) != nil {
		h.rec.refuse(w, r, opMembershipRemove, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opMembershipRemove, actor, orgID, false, principal) {
		return
	}
	m, err := h.Memberships.Remove(r.Context(), membership.Change{OrgID: orgID, PrincipalID: principal,
		Actor: actorOf(r.Context(), actor, caller), Authorize: h.membershipAuthorize(actor)})
	h.answerMembership(w, r, opMembershipRemove, m, err)
}

type transferAnswer struct {
	OrganizationID   string `json:"organization_id"`
	FromPrincipalID  string `json:"from_principal_id"`
	OwnerPrincipalID string `json:"owner_principal_id"`
}

func (h provisioningHandlers) membershipTransfer(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opMembershipTransfer, "organization_id", "from_principal_id", "to_principal_id", "actor_principal_id")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	from, err2 := body.uuid("from_principal_id")
	to, err3 := body.uuid("to_principal_id")
	actor, err4 := body.uuid("actor_principal_id")
	if errors.Join(err1, err2, err3, err4) != nil {
		h.rec.refuse(w, r, opMembershipTransfer, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opMembershipTransfer, actor, orgID, false, from, to) {
		return
	}
	err := h.Memberships.TransferOwnership(r.Context(), membership.Transfer{OrgID: orgID, From: from, To: to,
		Actor: actorOf(r.Context(), actor, caller), Authorize: h.membershipAuthorize(actor)})
	if err != nil {
		answer := membershipRefusal(err)
		h.rec.refuse(w, r, opMembershipTransfer, answer, failure(answer, err))
		return
	}
	h.rec.record(r.Context(), opMembershipTransfer, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, transferAnswer{OrganizationID: orgID.String(), FromPrincipalID: from.String(), OwnerPrincipalID: to.String()})
}

type membershipAnswer struct {
	MembershipID   string  `json:"membership_id"`
	OrganizationID string  `json:"organization_id"`
	PrincipalID    string  `json:"principal_id"`
	Role           *string `json:"role"`
	Status         string  `json:"status"`
	JoinedAt       *string `json:"joined_at"`
	Revision       int64   `json:"revision"`
}

func (h provisioningHandlers) answerMembership(w http.ResponseWriter, r *http.Request, op string, m *membership.Membership, err error) {
	if err != nil {
		answer := membershipRefusal(err)
		h.rec.refuse(w, r, op, answer, failure(answer, err))
		return
	}
	out := membershipAnswer{MembershipID: m.ID.String(), OrganizationID: m.OrgID.String(), PrincipalID: m.PrincipalID.String(),
		Role: m.Role, Status: string(m.Status), Revision: m.Revision}
	if m.JoinedAt != nil {
		joined := m.JoinedAt.UTC().Format(time.RFC3339Nano)
		out.JoinedAt = &joined
	}
	h.rec.record(r.Context(), op, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, out)
}

// membershipRefusal maps a membership store error to its answer.
func membershipRefusal(err error) refusal {
	for _, c := range []struct {
		err    error
		answer refusal
	}{
		{membership.ErrDenied, refusal{http.StatusForbidden, reasonAdminRequired}},
		{membership.ErrSuperuserRequired, refusal{http.StatusForbidden, reasonSuperuserRequired}},
		{membership.ErrNotOwned, refusal{http.StatusConflict, reasonUsersNotOwned}},
		{membership.ErrRoleInvalid, refusal{http.StatusBadRequest, reasonRoleInvalid}},
		{membership.ErrRoleMissing, refusal{http.StatusConflict, reasonRoleMissing}},
		{membership.ErrNotMember, refusal{http.StatusNotFound, reasonMembershipNotFound}},
		{membership.ErrAlreadyMember, refusal{http.StatusConflict, reasonAlreadyMember}},
		{membership.ErrSuspended, refusal{http.StatusConflict, reasonMembershipSuspended}},
		{membership.ErrLastOwner, refusal{http.StatusConflict, reasonLastOwner}},
		{membership.ErrNotOwner, refusal{http.StatusConflict, reasonNotOwner}},
		{membership.ErrTargetNotMember, refusal{http.StatusConflict, reasonTargetNotMember}},
		{membership.ErrRevisionConflict, refusal{http.StatusConflict, reasonRevisionStale}},
		{membership.ErrOrgNotFound, refusal{http.StatusNotFound, reasonOrganizationNotFound}},
		{membership.ErrOrgSuspended, refusal{http.StatusConflict, reasonOrganizationInactive}},
		{membership.ErrPrincipalNotFound, refusal{http.StatusNotFound, reasonPrincipalNotFound}},
		{membership.ErrInviterNotFound, refusal{http.StatusNotFound, reasonInviterNotFound}},
		{membership.ErrStateMoved, refusal{http.StatusConflict, reasonStateChanged}},
	} {
		if errors.Is(err, c.err) {
			return c.answer
		}
	}
	return unavailable(reasonStoreUnavailable)
}

// failure is the error to log with answer: the store error behind a 503,
// nothing for a named refusal.
func failure(answer refusal, err error) error {
	if answer.status == http.StatusServiceUnavailable {
		return err
	}
	return nil
}

type inviteAnswer struct {
	InviteID  string `json:"invite_id"`
	Token     string `json:"token"`
	ExpiresAt string `json:"expires_at"`
}

func (h provisioningHandlers) inviteCreate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opInviteCreate, "organization_id", "email", "role", "ttl_seconds", "actor_principal_id",
		"description", "request_metadata")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	email, err2 := body.text("email", maxShortTextBytes)
	role, err3 := body.text("role", maxShortTextBytes)
	ttlSeconds, err4 := body.optionalInt("ttl_seconds", 1, 1<<31)
	actor, err5 := body.uuid("actor_principal_id")
	description, err6 := body.optionalText("description", maxDescriptionBytes)
	metadata, err7 := body.optionalObject("request_metadata", maxRequestMetadataBytes)
	if errors.Join(err1, err2, err3, err4, err5, err6, err7) != nil {
		h.rec.refuse(w, r, opInviteCreate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opInviteCreate, actor, orgID, false) {
		return
	}
	var ttl *time.Duration
	if ttlSeconds != nil {
		d := time.Duration(*ttlSeconds) * time.Second
		ttl = &d
	}
	created, err := h.Invites.Create(r.Context(), invite.CreateInput{OrgID: orgID, Email: email, Role: role, TTL: ttl, InvitedBy: actor,
		Actor:       invite.Actor{PrincipalID: &actor, RequestID: requestid.Audit(r.Context()), Caller: caller},
		Authorize:   h.inviteAuthorize(actor),
		PublicAudit: &invite.PublicAudit{Description: description, RequestMetadata: metadata}})
	if err != nil {
		answer := inviteRefusal(err)
		h.rec.refuse(w, r, opInviteCreate, answer, failure(answer, err))
		return
	}
	h.rec.record(r.Context(), opInviteCreate, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, inviteAnswer{InviteID: created.ID.String(), Token: created.Token,
		ExpiresAt: created.ExpiresAt.UTC().Format(time.RFC3339Nano)})
}

func (h provisioningHandlers) inviteAuthorize(actor uuid.UUID) invite.Authorize {
	return func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error {
		_, err := h.actorRule(ctx, tx, actor, orgID, false)
		var refused errActorRefused
		if errors.As(err, &refused) {
			return invite.ErrDenied
		}
		return err
	}
}

type acceptAnswer struct {
	InviteID       string `json:"invite_id"`
	OrganizationID string `json:"organization_id"`
	MembershipID   string `json:"membership_id"`
	Role           string `json:"role"`
}

// inviteAccept takes no actor rule: the token and the account's verified
// address authorize the join (the invite aggregate checks both under its
// locks). The account must be active. The invite aggregate projects the
// invite's organization when the auth schema has none yet, once the token
// names a pending, unexpired invite.
func (h provisioningHandlers) inviteAccept(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opInviteAccept, "token", "principal_id", "description", "request_metadata")
	if !ok {
		return
	}
	token, err1 := body.text("token", maxTokenBytes)
	principal, err2 := body.uuid("principal_id")
	description, err3 := body.optionalText("description", maxDescriptionBytes)
	metadata, err4 := body.optionalObject("request_metadata", maxRequestMetadataBytes)
	if errors.Join(err1, err2, err3, err4) != nil {
		h.rec.refuse(w, r, opInviteAccept, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	ctx := r.Context()
	state, found, err := h.Users.StateOn(ctx, h.Reader, principal)
	switch {
	case err != nil:
		h.rec.refuse(w, r, opInviteAccept, unavailable(reasonStoreUnavailable), err)
		return
	case !found || !state.IsActive:
		h.rec.refuse(w, r, opInviteAccept, refusal{http.StatusForbidden, reasonDeactivated}, nil)
		return
	}
	accepted, err := h.Invites.Accept(ctx, invite.AcceptInput{Token: token, PrincipalID: principal,
		Actor:              invite.Actor{PrincipalID: &principal, RequestID: requestid.Audit(ctx), Caller: caller},
		PublicAudit:        &invite.PublicAudit{Description: description, RequestMetadata: metadata},
		EnsureOrganization: h.Organizations.EnsureOrganization})
	if err != nil {
		answer := inviteRefusal(err)
		h.rec.refuse(w, r, opInviteAccept, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opInviteAccept, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, acceptAnswer{InviteID: accepted.InviteID.String(), OrganizationID: accepted.OrgID.String(),
		MembershipID: accepted.MembershipID.String(), Role: accepted.Role})
}

// inviteRefusal maps an invite store error to its answer. An unusable token
// has the one answer, whatever made it unusable.
func inviteRefusal(err error) refusal {
	for _, c := range []struct {
		err    error
		answer refusal
	}{
		{invite.ErrInviteInvalid, refusal{http.StatusBadRequest, reasonInviteInvalid}},
		{invite.ErrAccountInactive, refusal{http.StatusForbidden, reasonDeactivated}},
		{invite.ErrDenied, refusal{http.StatusForbidden, reasonAdminRequired}},
		{invite.ErrNotOwned, refusal{http.StatusConflict, reasonUsersNotOwned}},
		{invite.ErrEmailUnverified, refusal{http.StatusForbidden, reasonEmailUnverified}},
		{invite.ErrEmailMismatch, refusal{http.StatusForbidden, reasonEmailMismatch}},
		{invite.ErrEmailInvalid, refusal{http.StatusBadRequest, reasonEmailInvalid}},
		{invite.ErrRoleInvalid, refusal{http.StatusBadRequest, reasonRoleInvalid}},
		{invite.ErrTTLInvalid, refusal{http.StatusBadRequest, reasonTTLInvalid}},
		{invite.ErrPendingExists, refusal{http.StatusConflict, reasonInvitePending}},
		{invite.ErrAlreadyMember, refusal{http.StatusConflict, reasonAlreadyMember}},
		{invite.ErrOrgNotFound, refusal{http.StatusNotFound, reasonOrganizationNotFound}},
		{invite.ErrOrgSuspended, refusal{http.StatusConflict, reasonOrganizationInactive}},
		{invite.ErrOrgConflict, refusal{http.StatusConflict, reasonOrganizationConflict}},
		{invite.ErrNotFound, refusal{http.StatusNotFound, reasonInviteNotFound}},
		{invite.ErrNotPending, refusal{http.StatusConflict, reasonInviteNotPending}},
		{invite.ErrInviterNotFound, refusal{http.StatusNotFound, reasonInviterNotFound}},
		{membership.ErrStateMoved, refusal{http.StatusConflict, reasonStateChanged}},
	} {
		if errors.Is(err, c.err) {
			return c.answer
		}
	}
	return unavailable(reasonStoreUnavailable)
}

type deprovisionAnswer struct {
	OrganizationID     string `json:"organization_id"`
	RemovedMemberships int    `json:"removed_memberships"`
	RevokedInvites     int    `json:"revoked_invites"`
	Suspended          bool   `json:"suspended"`
}

// orgDeprovision is go-api's organization deletion step in the auth schema:
// a superuser actor only (go-api's DELETE /orgs/{org_id} is policy.Superuser).
func (h provisioningHandlers) orgDeprovision(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opOrgDeprovision, "organization_id", "actor_principal_id")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	if errors.Join(err1, err2) != nil {
		h.rec.refuse(w, r, opOrgDeprovision, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opOrgDeprovision, actor, orgID, true) {
		return
	}
	ctx := r.Context()
	res, err := h.Deprovision.Deprovision(ctx, orgdeprovision.Input{OrgID: orgID,
		Actor: orgdeprovision.Actor{PrincipalID: &actor, RequestID: requestid.Audit(ctx), Caller: caller},
		Authorize: func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error {
			_, err := h.actorRule(ctx, tx, actor, orgID, true)
			var refused errActorRefused
			if errors.As(err, &refused) {
				return orgdeprovision.ErrDenied
			}
			return err
		}})
	if err != nil {
		answer := unavailable(reasonStoreUnavailable)
		switch {
		case errors.Is(err, orgdeprovision.ErrDenied):
			answer = refusal{http.StatusForbidden, reasonSuperuserRequired}
		case errors.Is(err, orgdeprovision.ErrNotFound):
			answer = refusal{http.StatusNotFound, reasonOrganizationNotFound}
		case errors.Is(err, orgdeprovision.ErrNotOwned):
			answer = refusal{http.StatusConflict, reasonUsersNotOwned}
		case errors.Is(err, orgdeprovision.ErrStateMoved):
			answer = refusal{http.StatusConflict, reasonStateChanged}
		}
		h.rec.refuse(w, r, opOrgDeprovision, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opOrgDeprovision, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, deprovisionAnswer{OrganizationID: orgID.String(), RemovedMemberships: res.RemovedMemberships,
		RevokedInvites: res.RevokedInvites, Suspended: res.Suspended})
}

type reactivateAnswer struct {
	OrganizationID string `json:"organization_id"`
	Status         string `json:"status"`
	Reactivated    bool   `json:"reactivated"`
}

// orgReactivate ends an organization's suspension: a superuser actor only
// (go-api's PATCH /admin/orgs/{org_id} is policy.Superuser), read through the
// users seam before anything is read for the request and again inside the
// verb's transaction after the row lock. An organization that is not
// suspended answers 200 with reactivated false and writes nothing.
func (h provisioningHandlers) orgReactivate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opOrgReactivate, "organization_id", "actor_principal_id", "if_revision")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	ifRevision, err3 := body.optionalInt("if_revision", 0, maxIfRevision)
	if errors.Join(err1, err2, err3) != nil {
		h.rec.refuse(w, r, opOrgReactivate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opOrgReactivate, actor, orgID, true) {
		return
	}
	ctx := r.Context()
	changed, err := h.Orgs.ReactivateAuthorized(ctx, orgID, ifRevision, org.Actor{PrincipalID: &actor, RequestID: requestid.Audit(ctx), Caller: caller},
		func(ctx context.Context, tx audit.TxOps, orgID uuid.UUID) error {
			_, err := h.actorRule(ctx, tx, actor, orgID, true)
			var refused errActorRefused
			if errors.As(err, &refused) {
				return org.ErrDenied
			}
			return err
		})
	if err != nil {
		answer := unavailable(reasonStoreUnavailable)
		switch {
		case errors.Is(err, org.ErrDenied):
			answer = refusal{http.StatusForbidden, reasonSuperuserRequired}
		case errors.Is(err, org.ErrNotFound):
			answer = refusal{http.StatusNotFound, reasonOrganizationNotFound}
		case errors.Is(err, org.ErrRevisionConflict):
			answer = refusal{http.StatusConflict, reasonStateChanged}
		}
		h.rec.refuse(w, r, opOrgReactivate, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opOrgReactivate, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, reactivateAnswer{OrganizationID: orgID.String(), Status: "active", Reactivated: changed})
}
