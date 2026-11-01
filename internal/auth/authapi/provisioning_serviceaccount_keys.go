package authapi

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/serviceaccount"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
)

// The service account key routes. Each is mounted only when the endpoint-profile document holds a row accepting every
// class it admits (withServiceAccountKeyRoutes); without the row it answers 404 and the service starts. They are not on
// the startup gate (provisioningPatterns) until their rows ship.
const PatternServiceAccountKeyIssue = "/internal/v1/service-accounts/keys/issue"
const PatternServiceAccountKeyRotate = "/internal/v1/service-accounts/keys/rotate"
const PatternServiceAccountKeyRevoke = "/internal/v1/service-accounts/keys/revoke"
const PatternServiceAccountKeyList = "/internal/v1/service-accounts/keys/list"

const (
	opServiceAccountKeyIssue  = "service_account_key_issue"
	opServiceAccountKeyRotate = "service_account_key_rotate"
	opServiceAccountKeyRevoke = "service_account_key_revoke"
	opServiceAccountKeyList   = "service_account_key_list"
)

// Refusal codes of the key routes.
const (
	reasonServiceAccountDisabled = "service_account_disabled"
	reasonKeyLive                = "key_live"
	reasonKeyRotating            = "key_rotating"
	reasonKeyNone                = "key_none"
	reasonKeyNotFound            = "key_not_found"
	reasonExpiryInvalid          = "expiry_invalid"
	reasonOverlapInvalid         = "overlap_invalid"
	reasonRevokeReasonInvalid    = "reason_invalid"
)

// The body bounds of the key routes, the store's own ceilings in seconds.
const (
	maxKeyExpirySeconds  = int64(serviceaccount.KeyExpiryCeiling / time.Second)
	maxKeyOverlapSeconds = int64(serviceaccount.KeyOverlapCeiling / time.Second)
	maxRevokeReasonBytes = 32
)

// ServiceAccountKeys are the key verbs of the service account aggregate (*serviceaccount.Store).
type ServiceAccountKeys interface {
	IssueKey(ctx context.Context, id uuid.UUID, expiresIn time.Duration, a serviceaccount.Actor) (serviceaccount.IssuedKey, error)
	RotateKey(ctx context.Context, id uuid.UUID, overlap, expiresIn time.Duration, a serviceaccount.Actor) (serviceaccount.IssuedKey, error)
	RevokeKey(ctx context.Context, credentialID uuid.UUID, reason string, a serviceaccount.Actor) error
	ListKeys(ctx context.Context, id uuid.UUID) ([]serviceaccount.Key, error)
	KeyByID(ctx context.Context, id uuid.UUID) (*serviceaccount.Key, error)
}

// withServiceAccountKeyRoutes mounts each key route whose profile row lists every class it admits, and logs one line
// naming each route it leaves out.
func (h provisioningHandlers) withServiceAccountKeyRoutes(routes []httpapi.Route, rows []byte) []httpapi.Route {
	for _, r := range []httpapi.Route{
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyIssue, Handler: http.HandlerFunc(h.serviceAccountKeyIssue)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyRotate, Handler: http.HandlerFunc(h.serviceAccountKeyRotate)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyRevoke, Handler: http.HandlerFunc(h.serviceAccountKeyRevoke)},
		{Method: http.MethodPost, Pattern: PatternServiceAccountKeyList, Handler: http.HandlerFunc(h.serviceAccountKeyList)},
	} {
		if rowsAcceptFor(rows, []string{r.Pattern}, admittedClasses(r.Pattern, h.Workload != nil)) != nil {
			h.Logger.Info("auth provisioning route not mounted: no endpoint-profile row", "route", r.Pattern)
			continue
		}
		routes = append(routes, r)
	}
	return routes
}

func keyWireID(id uuid.UUID) string { return "crd_" + id.String() }

// keyBody is a key's registry metadata as the credential wire forms carry it. status follows the rule the schemas state:
// revoked_at set is revoked; else expires_at at or before now is expired; else active.
func keyBody(k serviceaccount.Key, now time.Time) contracts.CredentialBody {
	body := contracts.CredentialBody{
		ID: keyWireID(k.ID), PrincipalID: session.WirePrincipalID(k.Principal), CredentialClass: serviceaccount.KeyClass,
		DisplayPrefix: serviceaccount.KeyPrefix, Fingerprint: k.Fingerprint, DomainOwner: "platform",
		Audiences: []string{}, Scopes: []string{}, Status: "active",
		CreatedAt: contracts.Timestamp{Time: k.CreatedAt}, UpdatedAt: contracts.Timestamp{Time: k.UpdatedAt},
		ExpiresAt: &contracts.Timestamp{Time: k.ExpiresAt},
	}
	if k.ParentID != nil {
		body.RotationParentID = keyWireID(*k.ParentID)
	}
	if k.CreatedBy != nil {
		body.CreatedBy = session.WirePrincipalID(*k.CreatedBy)
	}
	switch {
	case k.RevokedAt != nil:
		body.Status = "revoked"
		body.RevokedAt = &contracts.Timestamp{Time: *k.RevokedAt}
		if k.RevokedReason != nil {
			body.RevokedReason = *k.RevokedReason
		}
	case !k.ExpiresAt.After(now):
		body.Status = "expired"
	}
	return body
}

func keyReceipt(kind string, issued serviceaccount.IssuedKey) contracts.CredentialReceipt {
	return contracts.CredentialReceipt{SchemaVersion: contracts.CredentialReceiptSurface, Kind: kind,
		Credential: keyBody(issued.Key, time.Now()), Secret: issued.Secret.Reveal()}
}

// keyExpiry is the expires_in_seconds member: at most the store's ceiling, the default when absent, malformed when null.
func keyExpiry(body strictObject) (time.Duration, error) {
	if _, present := body["expires_in_seconds"]; !present {
		return serviceaccount.KeyExpiryDefault, nil
	}
	seconds, err := body.optionalInt("expires_in_seconds", 1, maxKeyExpirySeconds)
	if err != nil {
		return 0, err
	}
	if seconds == nil {
		return 0, errProvisioningBody
	}
	return time.Duration(*seconds) * time.Second, nil
}

// credentialID reads a credential id in its wire form, crd_ and a canonical uuid.
func credentialID(body strictObject, name string) (uuid.UUID, error) {
	text, err := body.text(name, len("crd_")+len(uuid.Nil.String()))
	if err != nil {
		return uuid.Nil, err
	}
	bare, found := strings.CutPrefix(text, "crd_")
	id, err := uuid.Parse(bare)
	if !found || err != nil || id.String() != bare {
		return uuid.Nil, errors.New("not a credential id")
	}
	return id, nil
}

// serviceAccountKeyDone answers a verb's refusal; it reports whether the verb went through.
func (h provisioningHandlers) serviceAccountKeyDone(w http.ResponseWriter, r *http.Request, op string, err error, denied errActorRefused) bool {
	if err != nil {
		a := serviceAccountRefusal(err, denied)
		h.rec.refuse(w, r, op, a, failure(a, err))
		return false
	}
	h.rec.record(r.Context(), op, "success", reasonAccepted, nil)
	return true
}

// serviceAccountKeyTarget reads a route's account and runs the precheck: for issue and rotate the owner is a target, so
// a non-superuser may not act on a superuser's account.
func (h provisioningHandlers) serviceAccountKeyTarget(w http.ResponseWriter, r *http.Request, op string, id, actor uuid.UUID, ownerIsTarget bool) (*serviceaccount.Info, bool) {
	info, ok := h.serviceAccountInfo(w, r, op, id, actor)
	if !ok {
		return nil, false
	}
	var targets []uuid.UUID
	if ownerIsTarget {
		targets = []uuid.UUID{info.Owner}
	}
	return info, h.precheck(w, r, op, actor, info.OrgID, false, targets...)
}

func (h provisioningHandlers) serviceAccountKeyIssue(w http.ResponseWriter, r *http.Request) {
	const op = opServiceAccountKeyIssue
	body, caller, ok := h.admit(w, r, op, "service_account_id", "actor_principal_id", "expires_in_seconds")
	if !ok {
		return
	}
	id, err1 := body.uuid("service_account_id")
	actor, err2 := body.uuid("actor_principal_id")
	expires, err3 := keyExpiry(body)
	if errors.Join(err1, err2, err3) != nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if _, ok := h.serviceAccountKeyTarget(w, r, op, id, actor, true); !ok {
		return
	}
	var denied errActorRefused
	issued, err := h.ServiceAccounts.IssueKey(r.Context(), id, expires, h.serviceAccountActor(r.Context(), caller, actor, nil, true, &denied))
	if h.serviceAccountKeyDone(w, r, op, err, denied) {
		writeJSON(w, http.StatusOK, keyReceipt("issue", issued))
	}
}

func (h provisioningHandlers) serviceAccountKeyRotate(w http.ResponseWriter, r *http.Request) {
	const op = opServiceAccountKeyRotate
	body, caller, ok := h.admit(w, r, op, "service_account_id", "actor_principal_id", "overlap_seconds", "expires_in_seconds")
	if !ok {
		return
	}
	id, err1 := body.uuid("service_account_id")
	actor, err2 := body.uuid("actor_principal_id")
	expires, err3 := keyExpiry(body)
	overlap, err4 := body.optionalInt("overlap_seconds", 0, maxKeyOverlapSeconds)
	if errors.Join(err1, err2, err3, err4) != nil || overlap == nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if _, ok := h.serviceAccountKeyTarget(w, r, op, id, actor, true); !ok {
		return
	}
	var denied errActorRefused
	issued, err := h.ServiceAccounts.RotateKey(r.Context(), id, time.Duration(*overlap)*time.Second, expires,
		h.serviceAccountActor(r.Context(), caller, actor, nil, true, &denied))
	if h.serviceAccountKeyDone(w, r, op, err, denied) {
		writeJSON(w, http.StatusOK, keyReceipt("rotate", issued))
	}
}

// serviceAccountKeyRevoke is the kill path: the owner is no target. A repeat answers the entry and writes nothing.
func (h provisioningHandlers) serviceAccountKeyRevoke(w http.ResponseWriter, r *http.Request) {
	const op = opServiceAccountKeyRevoke
	body, caller, ok := h.admit(w, r, op, "credential_id", "actor_principal_id", "reason")
	if !ok {
		return
	}
	id, err1 := credentialID(body, "credential_id")
	actor, err2 := body.uuid("actor_principal_id")
	reason, err3 := body.text("reason", maxRevokeReasonBytes)
	if errors.Join(err1, err2, err3) != nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	ctx := r.Context()
	key, err := h.ServiceAccounts.KeyByID(ctx, id)
	if errors.Is(err, serviceaccount.ErrKeyNotFound) {
		h.refuseUnknown(w, r, op, actor, reasonKeyNotFound)
		return
	}
	if err != nil {
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return
	}
	info, ok := h.accountOf(w, r, op, key.Principal, actor, reasonKeyNotFound)
	if !ok || !h.precheck(w, r, op, actor, info.OrgID, false) {
		return
	}
	var denied errActorRefused
	err = h.ServiceAccounts.RevokeKey(ctx, id, reason, h.serviceAccountActor(ctx, caller, actor, nil, false, &denied))
	if err != nil {
		a := serviceAccountRefusal(err, denied)
		h.rec.refuse(w, r, op, a, failure(a, err))
		return
	}
	after, err := h.ServiceAccounts.KeyByID(ctx, id)
	if err != nil {
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return
	}
	h.rec.record(ctx, op, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, contracts.CredentialEntry{SchemaVersion: contracts.CredentialEntrySurface, CredentialBody: keyBody(*after, time.Now())})
}

func (h provisioningHandlers) serviceAccountKeyList(w http.ResponseWriter, r *http.Request) {
	const op = opServiceAccountKeyList
	body, _, ok := h.admit(w, r, op, "service_account_id", "actor_principal_id")
	if !ok {
		return
	}
	id, err1 := body.uuid("service_account_id")
	actor, err2 := body.uuid("actor_principal_id")
	if errors.Join(err1, err2) != nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if _, ok := h.serviceAccountKeyTarget(w, r, op, id, actor, false); !ok {
		return
	}
	keys, err := h.ServiceAccounts.ListKeys(r.Context(), id)
	if err != nil {
		h.rec.refuse(w, r, op, unavailable(reasonStoreUnavailable), err)
		return
	}
	now := time.Now()
	items := make([]contracts.CredentialBody, 0, len(keys))
	for _, k := range keys {
		items = append(items, keyBody(k, now))
	}
	h.rec.record(r.Context(), op, "success", reasonAccepted, nil)
	writeJSON(w, http.StatusOK, contracts.CredentialList{SchemaVersion: contracts.CredentialListSurface, Items: items})
}
