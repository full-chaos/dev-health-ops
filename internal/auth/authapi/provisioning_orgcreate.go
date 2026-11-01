package authapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/org"
	"github.com/full-chaos/dev-health-ops/internal/auth/platformrole"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

const (
	// PatternOrgOnboard is the route of a user's first organization. It takes
	// no slug, tier or settings.
	PatternOrgOnboard = "/internal/v1/organizations/onboard"
	// PatternOrgCreate is the route of an organization a superuser creates.
	PatternOrgCreate = "/internal/v1/organizations/create"
)

// withOrgCreateRoutes mounts each organization route only when the profile
// document holds its row: a row's source line must exist at the document's
// source_commit, so a route lands before its row.
func (h provisioningHandlers) withOrgCreateRoutes(routes []httpapi.Route, rows []byte) []httpapi.Route {
	for _, route := range []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternOrgOnboard, Handler: http.HandlerFunc(h.orgOnboard)},
		{Method: http.MethodPost, Pattern: PatternOrgCreate, Handler: http.HandlerFunc(h.orgCreate)},
	} {
		if rowsAcceptFor(rows, []string{route.Pattern}, []string{serviceSessionIssuerClass}) == nil {
			routes = append(routes, route)
		}
	}
	return routes
}

const (
	opOrgOnboard      = "organization_onboard"
	opOrgCreate       = "organization_create"
	reasonNameInvalid = "name_invalid"

	maxOrganizationNameBytes = 400
	maxOrgCreateBody         = 64 << 10
	maxOrgSettingsBytes      = 60 << 10
)

type orgBornAnswer struct {
	OrganizationID string  `json:"organization_id"`
	MembershipID   *string `json:"membership_id,omitempty"`
}

func answerBorn(w http.ResponseWriter, born *identity.BornOrganization) {
	out := orgBornAnswer{OrganizationID: born.ID.String()}
	if born.MembershipID != nil {
		id := born.MembershipID.String()
		out.MembershipID = &id
	}
	writeJSON(w, http.StatusOK, out)
}

// birthRefusal maps a birth's store error to its answer; notFound is the
// answer of a missing principal.
func birthRefusal(err error, notFound refusal) refusal {
	switch {
	case errors.Is(err, identity.ErrOrganizationMember):
		return refusal{http.StatusConflict, reasonAlreadyMember}
	case errors.Is(err, identity.ErrAccountInactive):
		return refusal{http.StatusForbidden, reasonDeactivated}
	case errors.Is(err, identity.ErrUserNotFound):
		return notFound
	case errors.Is(err, identity.ErrOrganizationSlugCollision):
		return refusal{http.StatusConflict, reasonOrganizationConflict}
	case errors.Is(err, identity.ErrSignupNotAvailable):
		return refusal{http.StatusConflict, reasonUsersNotOwned}
	}
	return unavailable(reasonStoreUnavailable)
}

// orgOnboard takes no actor rule: go-api names the signed-in user, and the
// verb refuses, under its locks, an inactive account or one with a membership.
func (h provisioningHandlers) orgOnboard(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opOrgOnboard, "principal_id", "organization_name", "description", "request_metadata")
	if !ok {
		return
	}
	principal, err1 := body.uuid("principal_id")
	rawName, err2 := body.text("organization_name", maxOrganizationNameBytes)
	description, err3 := body.optionalText("description", maxDescriptionBytes)
	metadata, err4 := body.optionalObject("request_metadata", maxRequestMetadataBytes)
	if errors.Join(err1, err2, err3, err4) != nil {
		h.rec.refuse(w, r, opOrgOnboard, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	name, err := org.NormalizeName(rawName)
	if err != nil {
		h.rec.refuse(w, r, opOrgOnboard, refusal{http.StatusUnprocessableEntity, reasonNameInvalid}, nil)
		return
	}
	ctx := r.Context()
	born, err := h.Organizations.OnboardOrganization(ctx, identity.OnboardOrganizationRequest{Principal: principal, Name: name,
		RequestID: requestid.Audit(ctx), Caller: caller, Description: description, RequestMetadata: metadata})
	if err != nil {
		answer := birthRefusal(err, refusal{http.StatusForbidden, reasonDeactivated})
		h.rec.refuse(w, r, opOrgOnboard, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opOrgOnboard, "success", reasonAccepted, nil)
	answerBorn(w, born)
}

// orgCreate is a superuser's create: the rule runs on the pool first, so a
// refused actor learns nothing, then again under the verb's locks.
func (h provisioningHandlers) orgCreate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admitUpTo(w, r, opOrgCreate, maxOrgCreateBody, "actor_principal_id", "organization_name", "slug",
		"organization_description", "settings", "tier", "managed_by", "owner_principal_id")
	if !ok {
		return
	}
	actor, err1 := body.uuid("actor_principal_id")
	rawName, err2 := body.text("organization_name", maxOrganizationNameBytes)
	slug, err3 := body.anyText("slug", maxShortTextBytes)
	description, err4 := body.optionalAnyText("organization_description", maxDescriptionBytes)
	settings, err5 := body.optionalObject("settings", maxOrgSettingsBytes)
	tier, err6 := body.anyText("tier", maxShortTextBytes)
	managedBy, err7 := body.text("managed_by", maxShortTextBytes)
	owner, err8 := body.optionalUUID("owner_principal_id")
	if errors.Join(err1, err2, err3, err4, err5, err6, err7, err8) != nil {
		h.rec.refuse(w, r, opOrgCreate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	name, err := org.NormalizeName(rawName)
	if err != nil {
		h.rec.refuse(w, r, opOrgCreate, refusal{http.StatusUnprocessableEntity, reasonNameInvalid}, nil)
		return
	}
	ctx := r.Context()
	_, err = h.actorRule(ctx, h.Reader, actor, uuid.Nil, true)
	var refused errActorRefused
	switch {
	case errors.As(err, &refused):
		h.rec.refuse(w, r, opOrgCreate, refusal{http.StatusForbidden, refused.reason}, nil)
		return
	case err != nil:
		h.rec.refuse(w, r, opOrgCreate, unavailable(reasonStoreUnavailable), err)
		return
	}
	denied := reasonSuperuserRequired
	born, err := h.Organizations.CreateOrganization(ctx, identity.CreateOrganizationRequest{Actor: actor, RequestID: requestid.Audit(ctx), Caller: caller,
		Slug: slug, Name: name, Description: description, Settings: settings, Tier: tier, ManagedBy: managedBy, Owner: owner,
		LockRoles: platformrole.LockWritesShared,
		Authorize: func(ctx context.Context, tx audit.TxOps) error {
			_, err := h.actorRule(ctx, tx, actor, uuid.Nil, true)
			var refused errActorRefused
			if errors.As(err, &refused) {
				denied = refused.reason
				return identity.ErrOrganizationDenied
			}
			return err
		}})
	if err != nil {
		answer := birthRefusal(err, refusal{http.StatusNotFound, reasonPrincipalNotFound})
		if errors.Is(err, identity.ErrOrganizationDenied) {
			answer = refusal{http.StatusForbidden, denied}
		}
		h.rec.refuse(w, r, opOrgCreate, answer, failure(answer, err))
		return
	}
	h.rec.record(ctx, opOrgCreate, "success", reasonAccepted, nil)
	answerBorn(w, born)
}
