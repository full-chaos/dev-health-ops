package authapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
)

// The organization service account routes, served with the other provisioning routes: their endpoint-profile rows are
// on the same startup gate (provisioningPatterns), so a document without a row builds none of them.
const PatternServiceAccountCreate = "/internal/v1/service-accounts/create"
const PatternServiceAccountDisable = "/internal/v1/service-accounts/disable"
const PatternServiceAccountEnable = "/internal/v1/service-accounts/enable"
const PatternServiceAccountOwner = "/internal/v1/service-accounts/owner"

const (
	opServiceAccountCreate  = "service_account_create"
	opServiceAccountDisable = "service_account_disable"
	opServiceAccountEnable  = "service_account_enable"
	opServiceAccountOwner   = "service_account_owner"
)

// Refusal codes of the service account routes.
const (
	reasonServiceAccountNotFound = "service_account_not_found"
	reasonNameTaken              = "name_taken"
	reasonOwnerInvalid           = "owner_invalid"
	reasonOwnerNotMember         = "owner_not_member"
	reasonAlreadyInState         = "already_in_state"
)

// maxServiceAccountNameBytes bounds the name member: the longest name the store's pattern admits.
const maxServiceAccountNameBytes = 63

// ServiceAccounts is the service account aggregate (*serviceaccount.Store).
type ServiceAccounts interface {
	Create(ctx context.Context, in serviceaccount.CreateInput, a serviceaccount.Actor) (uuid.UUID, error)
	Disable(ctx context.Context, id uuid.UUID, a serviceaccount.Actor) error
	Enable(ctx context.Context, id uuid.UUID, a serviceaccount.Actor) error
	SetOwner(ctx context.Context, id, newOwner uuid.UUID, a serviceaccount.Actor) error
	Lookup(ctx context.Context, id uuid.UUID) (*serviceaccount.Info, error)
	ServiceAccountKeys
}

type serviceAccountAnswer struct {
	ServiceAccountID string  `json:"service_account_id"`
	OrganizationID   *string `json:"organization_id,omitempty"`
	Name             *string `json:"name,omitempty"`
	Status           *string `json:"status,omitempty"`
	OwnerPrincipalID *string `json:"owner_principal_id,omitempty"`
}

func answerText(s string) *string { return &s }

// serviceAccountActor is the verb's Actor: the actor rule (go-api's PEP rule: an active superuser, or an owner or
// admin member of the organization) read on the verb's transaction after its locks, and the targets rule on the
// owners the change names (and, for an owner change, the account's current owner as the verb locked it). The
// transaction takes no lock for these reads; a demotion that commits during it counts as after it. The refusal's
// reason is kept in denied for the answer.
func (h provisioningHandlers) serviceAccountActor(ctx context.Context, caller audit.Caller, actor uuid.UUID, named []uuid.UUID, withCurrent bool, denied *errActorRefused) serviceaccount.Actor {
	return serviceaccount.Actor{PrincipalID: &actor, RequestID: requestid.Audit(ctx), Caller: caller,
		Authorize: func(ctx context.Context, tx audit.TxOps, l serviceaccount.Locked) error {
			targets := append([]uuid.UUID(nil), named...)
			if withCurrent {
				targets = append(targets, l.Owner)
			}
			state, err := h.actorRule(ctx, tx, actor, l.OrgID, false)
			if err == nil {
				err = h.targetsRule(ctx, tx, state, targets)
			}
			var refused errActorRefused
			if errors.As(err, &refused) {
				*denied = refused
				return serviceaccount.ErrDenied
			}
			return err
		}}
}

// serviceAccountRefusal maps a service account store error to its answer.
func serviceAccountRefusal(err error, denied errActorRefused) refusal {
	if errors.Is(err, serviceaccount.ErrDenied) {
		reason := denied.reason
		if reason == "" {
			reason = reasonAdminRequired
		}
		return refusal{http.StatusForbidden, reason}
	}
	for _, c := range []struct {
		err    error
		answer refusal
	}{
		{serviceaccount.ErrNotOwned, refusal{http.StatusConflict, reasonUsersNotOwned}},
		{serviceaccount.ErrNotFound, refusal{http.StatusNotFound, reasonServiceAccountNotFound}},
		{serviceaccount.ErrNotAccount, refusal{http.StatusNotFound, reasonServiceAccountNotFound}},
		{serviceaccount.ErrOrgNotFound, refusal{http.StatusNotFound, reasonOrganizationNotFound}},
		{serviceaccount.ErrOrgNotActive, refusal{http.StatusConflict, reasonOrganizationInactive}},
		{serviceaccount.ErrNameInvalid, refusal{http.StatusBadRequest, reasonNameInvalid}},
		{serviceaccount.ErrNameTaken, refusal{http.StatusConflict, reasonNameTaken}},
		{serviceaccount.ErrOwnerNotUser, refusal{http.StatusConflict, reasonOwnerInvalid}},
		{serviceaccount.ErrOwnerDisabled, refusal{http.StatusConflict, reasonOwnerInvalid}},
		{serviceaccount.ErrOwnerNotMember, refusal{http.StatusConflict, reasonOwnerNotMember}},
		{serviceaccount.ErrAlreadyInState, refusal{http.StatusConflict, reasonAlreadyInState}},
		{serviceaccount.ErrStateChanged, refusal{http.StatusConflict, reasonStateChanged}},
		{serviceaccount.ErrAccountDisabled, refusal{http.StatusConflict, reasonServiceAccountDisabled}},
		{serviceaccount.ErrKeyLive, refusal{http.StatusConflict, reasonKeyLive}},
		{serviceaccount.ErrKeyRotating, refusal{http.StatusConflict, reasonKeyRotating}},
		{serviceaccount.ErrKeyNone, refusal{http.StatusConflict, reasonKeyNone}},
		{serviceaccount.ErrKeyNotFound, refusal{http.StatusNotFound, reasonKeyNotFound}},
		{serviceaccount.ErrKeyExpiry, refusal{http.StatusBadRequest, reasonExpiryInvalid}},
		{serviceaccount.ErrKeyOverlap, refusal{http.StatusBadRequest, reasonOverlapInvalid}},
		{serviceaccount.ErrKeyReason, refusal{http.StatusBadRequest, reasonRevokeReasonInvalid}},
	} {
		if errors.Is(err, c.err) {
			return c.answer
		}
	}
	return unavailable(reasonStoreUnavailable)
}

// serviceAccountInfo reads the account a route names, before any verb runs, for the organization the actor rule needs.
func (h provisioningHandlers) serviceAccountInfo(w http.ResponseWriter, r *http.Request, op string, id, actor uuid.UUID) (*serviceaccount.Info, bool) {
	return h.accountOf(w, r, op, id, actor, reasonServiceAccountNotFound)
}

// accountOf reads an account. An account that does not exist can be authorized for no organization (refuseUnknown).
func (h provisioningHandlers) accountOf(w http.ResponseWriter, r *http.Request, op string, id, actor uuid.UUID, notFound string) (*serviceaccount.Info, bool) {
	info, err := h.ServiceAccounts.Lookup(r.Context(), id)
	switch {
	case err == nil:
		return info, true
	case errors.Is(err, serviceaccount.ErrNotFound):
		h.refuseUnknown(w, r, op, actor, notFound)
	default:
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
	}
	return nil, false
}

// refuseUnknown answers a row that does not exist. The actor rule runs on the nil organization, so a non-superuser gets
// the refusal it would get for a row of another organization, and only an active superuser is told the row is unknown
// (404 with reason). It writes the answer.
func (h provisioningHandlers) refuseUnknown(w http.ResponseWriter, r *http.Request, op string, actor uuid.UUID, reason string) {
	_, rule := h.actorRule(r.Context(), h.Reader, actor, uuid.Nil, false)
	var refused errActorRefused
	switch {
	case errors.As(rule, &refused):
		h.rec.refuse(w, r, op, refusal{http.StatusForbidden, refused.reason}, nil)
	case rule != nil:
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), rule)
	default:
		h.rec.refuse(w, r, op, refusal{http.StatusNotFound, reason}, nil)
	}
}

func (h provisioningHandlers) serviceAccountDone(w http.ResponseWriter, r *http.Request, op string, err error, denied errActorRefused, answer serviceAccountAnswer) {
	if err != nil {
		a := serviceAccountRefusal(err, denied)
		h.rec.refuse(w, r, op, a, failure(a, err))
		return
	}
	h.rec.record(r.Context(), op, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, answer)
}

func (h provisioningHandlers) serviceAccountCreate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opServiceAccountCreate, "organization_id", "actor_principal_id", "owner_principal_id", "name")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	owner, err3 := body.uuid("owner_principal_id")
	name, err4 := body.text("name", maxServiceAccountNameBytes)
	if errors.Join(err1, err2, err3, err4) != nil {
		h.rec.refuse(w, r, opServiceAccountCreate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opServiceAccountCreate, actor, orgID, false, owner) {
		return
	}
	var denied errActorRefused
	id, err := h.ServiceAccounts.Create(r.Context(), serviceaccount.CreateInput{OrgID: orgID, Name: name, Owner: owner},
		h.serviceAccountActor(r.Context(), caller, actor, []uuid.UUID{owner}, false, &denied))
	h.serviceAccountDone(w, r, opServiceAccountCreate, err, denied, serviceAccountAnswer{ServiceAccountID: id.String(),
		OrganizationID: answerText(orgID.String()), Name: answerText(name), Status: answerText("active"), OwnerPrincipalID: answerText(owner.String())})
}

func (h provisioningHandlers) serviceAccountSwitch(op string, enable bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, caller, ok := h.admit(w, r, op, "service_account_id", "actor_principal_id")
		if !ok {
			return
		}
		id, err1 := body.uuid("service_account_id")
		actor, err2 := body.uuid("actor_principal_id")
		if errors.Join(err1, err2) != nil {
			h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
			return
		}
		info, ok := h.serviceAccountInfo(w, r, op, id, actor)
		if !ok || !h.precheck(w, r, op, actor, info.OrgID, false) {
			return
		}
		var denied errActorRefused
		a := h.serviceAccountActor(r.Context(), caller, actor, nil, false, &denied)
		status := "disabled"
		var err error
		if enable {
			status = "active"
			err = h.ServiceAccounts.Enable(r.Context(), id, a)
		} else {
			err = h.ServiceAccounts.Disable(r.Context(), id, a)
		}
		h.serviceAccountDone(w, r, op, err, denied, serviceAccountAnswer{ServiceAccountID: id.String(), Status: answerText(status)})
	}
}

func (h provisioningHandlers) serviceAccountOwner(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opServiceAccountOwner, "service_account_id", "actor_principal_id", "owner_principal_id")
	if !ok {
		return
	}
	id, err1 := body.uuid("service_account_id")
	actor, err2 := body.uuid("actor_principal_id")
	owner, err3 := body.uuid("owner_principal_id")
	if errors.Join(err1, err2, err3) != nil {
		h.rec.refuse(w, r, opServiceAccountOwner, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	info, ok := h.serviceAccountInfo(w, r, opServiceAccountOwner, id, actor)
	if !ok || !h.precheck(w, r, opServiceAccountOwner, actor, info.OrgID, false, owner, info.Owner) {
		return
	}
	var denied errActorRefused
	err := h.ServiceAccounts.SetOwner(r.Context(), id, owner, h.serviceAccountActor(r.Context(), caller, actor, []uuid.UUID{owner}, true, &denied))
	h.serviceAccountDone(w, r, opServiceAccountOwner, err, denied, serviceAccountAnswer{ServiceAccountID: id.String(), OwnerPrincipalID: answerText(owner.String())})
}
