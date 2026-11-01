package authapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/grant"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
)

// The repository grant routes. They mount only when the endpoint-profile document holds a row for each (withGrantRoutes) and
// are not on the startup gate (provisioningPatterns): on a build without the rows each answers 404 and the service starts.
const (
	PatternGrantCreate = "/internal/v1/grants/create"
	PatternGrantUpdate = "/internal/v1/grants/update"
	PatternGrantRevoke = "/internal/v1/grants/revoke"
)

const (
	opGrantCreate = "grant_create"
	opGrantUpdate = "grant_update"
	opGrantRevoke = "grant_revoke"
)

// Refusal codes of the grant routes.
const (
	reasonGrantNotFound    = "grant_not_found"
	reasonGranteeNotMember = "grantee_not_member"
	reasonAlreadyGranted   = "already_granted"
	reasonGrantRevoked     = "grant_revoked"
	reasonGrantExpiry      = "expiry_not_future"
	reasonGrantKeyInvalid  = "resource_key_invalid"
	reasonGrantNothing     = "nothing_to_change"
	reasonRevisionClash    = "revision_conflict"
)

func init() {
	provisioningOperations = append(provisioningOperations, opGrantCreate, opGrantUpdate, opGrantRevoke)
}

// maxGrantKeyBytes bounds resource_key: two 100-byte segments and the slash.
const maxGrantKeyBytes = 201

// Grants is the resource grant aggregate (*grant.Store).
type Grants interface {
	Create(ctx context.Context, in grant.CreateInput) (*grant.Grant, error)
	Update(ctx context.Context, in grant.UpdateInput) (*grant.Grant, error)
	Revoke(ctx context.Context, in grant.RevokeInput) (*grant.Grant, error)
}

type grantAnswer struct {
	GrantID        string  `json:"grant_id"`
	OrganizationID string  `json:"organization_id"`
	PrincipalID    string  `json:"principal_id"`
	Role           string  `json:"role"`
	ResourceKey    string  `json:"resource_key"`
	ExpiresAt      *string `json:"expires_at,omitempty"`
	Status         string  `json:"status"`
	Revision       int64   `json:"revision"`
}

func answerOf(g *grant.Grant) grantAnswer {
	a := grantAnswer{GrantID: g.ID.String(), OrganizationID: g.OrgID.String(), PrincipalID: g.PrincipalID.String(), Role: g.Role,
		ResourceKey: g.Key, Status: "active", Revision: g.Revision}
	if g.ExpiresAt != nil {
		t := g.ExpiresAt.UTC().Format(time.RFC3339Nano)
		a.ExpiresAt = &t
	}
	if !g.Live() {
		a.Status = "revoked"
	}
	return a
}

// withGrantRoutes mounts the grant routes when the profile document holds a row listing every class each admits, else logs
// one line naming the route. Without a Grants store it mounts none.
func (h provisioningHandlers) withGrantRoutes(routes []httpapi.Route, rows []byte) []httpapi.Route {
	if h.Grants == nil {
		return routes
	}
	for _, r := range []struct {
		pattern string
		handler http.HandlerFunc
	}{
		{PatternGrantCreate, h.grantCreate},
		{PatternGrantUpdate, h.grantUpdate},
		{PatternGrantRevoke, h.grantRevoke},
	} {
		if rowsAcceptFor(rows, []string{r.pattern}, admittedClasses(r.pattern, h.Workload != nil)) != nil {
			h.Logger.Info("auth provisioning route not mounted: no endpoint-profile row", "route", r.pattern)
			continue
		}
		routes = append(routes, httpapi.Route{Method: http.MethodPost, Pattern: r.pattern, Handler: r.handler})
	}
	return routes
}

// grantActor is the verb's Actor: the acting principal, the request id and the calling credential.
func (h provisioningHandlers) grantActor(ctx context.Context, caller audit.Caller, actor uuid.UUID) grant.Actor {
	return grant.Actor{PrincipalID: &actor, RequestID: requestid.Audit(ctx), Caller: caller}
}

// grantAuthorize is the verb's Authorize: the actor rule (an active superuser, or an owner or admin member of the
// organization) and the targets rule on the grantee, read on the verb's transaction after its locks. The refusal's reason is
// kept in denied for the answer.
func (h provisioningHandlers) grantAuthorize(actor uuid.UUID, denied *errActorRefused) grant.Authorize {
	return func(ctx context.Context, tx audit.TxOps, l grant.Locked) error {
		state, err := h.actorRule(ctx, tx, actor, l.OrgID, false)
		if err == nil {
			err = h.targetsRule(ctx, tx, state, l.Principals)
		}
		var refused errActorRefused
		if errors.As(err, &refused) {
			*denied = refused
			return grant.ErrDenied
		}
		return err
	}
}

// grantRefusal maps a grant store error to its answer. No answer carries the key or any request member.
func grantRefusal(err error, denied errActorRefused) refusal {
	if errors.Is(err, grant.ErrDenied) || errors.Is(err, grant.ErrSuperuserRequired) {
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
		{grant.ErrNotOwned, refusal{http.StatusConflict, reasonUsersNotOwned}},
		{grant.ErrNotFound, refusal{http.StatusNotFound, reasonGrantNotFound}},
		{grant.ErrOrgNotFound, refusal{http.StatusNotFound, reasonOrganizationNotFound}},
		{grant.ErrOrgSuspended, refusal{http.StatusConflict, reasonOrganizationInactive}},
		{grant.ErrPrincipalNotFound, refusal{http.StatusNotFound, reasonPrincipalNotFound}},
		{grant.ErrNotMember, refusal{http.StatusConflict, reasonGranteeNotMember}},
		{grant.ErrAlreadyGranted, refusal{http.StatusConflict, reasonAlreadyGranted}},
		{grant.ErrRevoked, refusal{http.StatusConflict, reasonGrantRevoked}},
		{grant.ErrRevisionConflict, refusal{http.StatusConflict, reasonRevisionClash}},
		{grant.ErrRoleInvalid, refusal{http.StatusBadRequest, reasonRoleInvalid}},
		{grant.ErrKeyInvalid, refusal{http.StatusBadRequest, reasonGrantKeyInvalid}},
		{grant.ErrExpiryPast, refusal{http.StatusBadRequest, reasonGrantExpiry}},
		{grant.ErrNothingToSet, refusal{http.StatusBadRequest, reasonGrantNothing}},
		{grant.ErrActorRequired, refusal{http.StatusBadRequest, reasonMalformed}},
		{grant.ErrOriginInvalid, refusal{http.StatusBadRequest, reasonMalformed}},
	} {
		if errors.Is(err, c.err) {
			return c.answer
		}
	}
	return unavailable(reasonStoreUnavailable)
}

func (h provisioningHandlers) grantDone(w http.ResponseWriter, r *http.Request, op string, g *grant.Grant, err error, denied errActorRefused) {
	if err != nil {
		a := grantRefusal(err, denied)
		h.rec.refuse(w, r, op, a, failure(a, err))
		return
	}
	h.rec.record(r.Context(), op, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, answerOf(g))
}

// optionalTime reads an absent or null member as nil, else an RFC 3339 timestamp.
func (o strictObject) optionalTime(name string) (*time.Time, error) {
	text, err := o.optionalText(name, maxShortTextBytes)
	if err != nil || text == nil {
		return nil, err
	}
	t, perr := time.Parse(time.RFC3339Nano, *text)
	if perr != nil {
		return nil, errProvisioningBody
	}
	return &t, nil
}

// optionalBool reads an absent or null member as false, else a JSON boolean.
func (o strictObject) optionalBool(name string) (bool, error) {
	if o.isNull(name) {
		return false, nil
	}
	var b bool
	if err := json.Unmarshal(o[name], &b); err != nil {
		return false, errProvisioningBody
	}
	return b, nil
}

func (h provisioningHandlers) grantCreate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opGrantCreate, "organization_id", "actor_principal_id", "principal_id", "role", "resource_key", "expires_at")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	principal, err3 := body.uuid("principal_id")
	role, err4 := body.text("role", maxShortTextBytes)
	key, err5 := body.text("resource_key", maxGrantKeyBytes)
	expires, err6 := body.optionalTime("expires_at")
	if errors.Join(err1, err2, err3, err4, err5, err6) != nil {
		h.rec.refuse(w, r, opGrantCreate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opGrantCreate, actor, orgID, false, principal) {
		return
	}
	var denied errActorRefused
	g, err := h.Grants.Create(r.Context(), grant.CreateInput{OrgID: orgID, PrincipalID: principal, Role: role, Key: key, ExpiresAt: expires,
		Actor: h.grantActor(r.Context(), caller, actor), Authorize: h.grantAuthorize(actor, &denied)})
	h.grantDone(w, r, opGrantCreate, g, err, denied)
}

func (h provisioningHandlers) grantUpdate(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opGrantUpdate, "organization_id", "actor_principal_id", "grant_id", "role", "expires_at", "clear_expiry", "expected_revision")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	id, err3 := body.uuid("grant_id")
	role, err4 := body.optionalText("role", maxShortTextBytes)
	expires, err5 := body.optionalTime("expires_at")
	clear, err6 := body.optionalBool("clear_expiry")
	revision, err7 := body.optionalInt("expected_revision", 1, 9007199254740991)
	if errors.Join(err1, err2, err3, err4, err5, err6, err7) != nil {
		h.rec.refuse(w, r, opGrantUpdate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opGrantUpdate, actor, orgID, false) {
		return
	}
	var denied errActorRefused
	g, err := h.Grants.Update(r.Context(), grant.UpdateInput{OrgID: orgID, GrantID: id, Role: role, ExpiresAt: expires, ClearExpiry: clear,
		ExpectedRevision: revision, Actor: h.grantActor(r.Context(), caller, actor), Authorize: h.grantAuthorize(actor, &denied)})
	h.grantDone(w, r, opGrantUpdate, g, err, denied)
}

func (h provisioningHandlers) grantRevoke(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admit(w, r, opGrantRevoke, "organization_id", "actor_principal_id", "grant_id", "expected_revision")
	if !ok {
		return
	}
	orgID, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	id, err3 := body.uuid("grant_id")
	revision, err4 := body.optionalInt("expected_revision", 1, 9007199254740991)
	if errors.Join(err1, err2, err3, err4) != nil {
		h.rec.refuse(w, r, opGrantRevoke, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.precheck(w, r, opGrantRevoke, actor, orgID, false) {
		return
	}
	var denied errActorRefused
	g, err := h.Grants.Revoke(r.Context(), grant.RevokeInput{OrgID: orgID, GrantID: id, ExpectedRevision: revision,
		Actor: h.grantActor(r.Context(), caller, actor), Authorize: h.grantAuthorize(actor, &denied)})
	h.grantDone(w, r, opGrantRevoke, g, err, denied)
}
