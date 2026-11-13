package authapi

import (
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/ssoprovider"
)

const (
	maxSSOName    = 255
	maxSSOListMax = 1000
	maxSSOText    = 4096
)

// ssoIDs reads the organization and the actor every SSO provider body names.
func ssoIDs(body strictObject) (org, actor uuid.UUID, err error) {
	org, err1 := body.uuid("organization_id")
	actor, err2 := body.uuid("actor_principal_id")
	return org, actor, errors.Join(err1, err2)
}

// ssoWriteAdmit is the shared head of a provider write: the go-api workload token and the body. go-api is the decision
// point (owner or admin of the organization, its own feature gate); this route adds no actor rule of its own, and the
// actor only names the audit row.
func (h provisioningHandlers) ssoWriteAdmit(w http.ResponseWriter, r *http.Request, op string, members ...string) (strictObject, uuid.UUID, uuid.UUID, ssoprovider.Actor, bool) {
	body, caller, ok := h.admitUpTo(w, r, op, maxSSOProviderBody, members...)
	if !ok {
		return nil, uuid.Nil, uuid.Nil, ssoprovider.Actor{}, false
	}
	org, actor, err := ssoIDs(body)
	if err != nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return nil, uuid.Nil, uuid.Nil, ssoprovider.Actor{}, false
	}
	return body, org, actor, h.ssoActor(r, actor, caller), true
}

func (h provisioningHandlers) ssoProviderList(w http.ResponseWriter, r *http.Request) {
	body, caller, ok := h.admitUpTo(w, r, opSSOProviderList, maxProvisioningBody, ssoListMembers...)
	if !ok {
		return
	}
	_ = caller
	org, _, err := ssoIDs(body)
	protocol, err2 := body.optionalAnyText("protocol", maxShortTextBytes)
	status, err3 := body.optionalAnyText("status", maxShortTextBytes)
	limit, err4 := body.optionalInt("limit", 0, 1<<31)
	offset, err5 := body.optionalInt("offset", 0, 1<<31)
	if errors.Join(err, err2, err3, err4, err5) != nil {
		h.rec.refuse(w, r, opSSOProviderList, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	filter := ssoprovider.Filter{Limit: 50}
	if protocol != nil {
		filter.Protocol = *protocol
	}
	if status != nil {
		filter.Status = *status
	}
	if limit != nil {
		filter.Limit = min(*limit, maxSSOListMax)
	}
	if offset != nil {
		filter.Offset = *offset
	}
	if !h.ensureOrganization(w, r, opSSOProviderList, org) {
		return
	}
	items, total, err := h.SSOProviders.List(r.Context(), org, filter)
	if err != nil {
		h.ssoStoreFailure(w, r, opSSOProviderList, err)
		return
	}
	answer := ssoProviderListAnswer{Items: make([]ssoProviderAnswer, 0, len(items)), Total: total, Limit: filter.Limit, Offset: filter.Offset}
	for _, p := range items {
		answer.Items = append(answer.Items, ssoAnswer(p))
	}
	h.rec.record(r.Context(), opSSOProviderList, "success", reasonAccepted, nil)
	writeProviderJSON(w, http.StatusOK, answer)
}

func (h provisioningHandlers) ssoProviderGet(w http.ResponseWriter, r *http.Request) {
	body, _, ok := h.admitUpTo(w, r, opSSOProviderGet, maxProvisioningBody, ssoByIDMembers...)
	if !ok {
		return
	}
	org, _, err := ssoIDs(body)
	id, err2 := body.uuid("provider_id")
	if errors.Join(err, err2) != nil {
		h.rec.refuse(w, r, opSSOProviderGet, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.ensureOrganization(w, r, opSSOProviderGet, org) {
		return
	}
	p, err := h.SSOProviders.Get(r.Context(), org, id)
	if err != nil {
		h.ssoStoreFailure(w, r, opSSOProviderGet, err)
		return
	}
	h.rec.record(r.Context(), opSSOProviderGet, "success", reasonAccepted, nil)
	writeProviderJSON(w, http.StatusOK, ssoAnswer(*p))
}

func (h provisioningHandlers) ssoProviderCreate(w http.ResponseWriter, r *http.Request) {
	body, org, _, who, ok := h.ssoWriteAdmit(w, r, opSSOProviderCreate, ssoCreateMembers...)
	if !ok {
		return
	}
	name, err1 := body.text("name", maxSSOName)
	protocolText, err2 := body.text("protocol", maxShortTextBytes)
	saml, err3 := body.optionalObject("saml_config", maxSSOProviderBody)
	oidc, err4 := body.optionalObject("oidc_config", maxSSOProviderBody)
	isDefault, err5 := body.optionalBoolPtr("is_default")
	idp, err6 := body.optionalBoolPtr("allow_idp_initiated")
	auto, err7 := body.optionalBoolPtr("auto_provision_users")
	role, err8 := body.optionalAnyText("default_role", maxShortTextBytes)
	domains, err9 := body.optionalDomains("allowed_domains")
	if errors.Join(err1, err2, err3, err4, err5, err6, err7, err8, err9) != nil || !ssoProtocolPattern.MatchString(protocolText) {
		h.rec.refuse(w, r, opSSOProviderCreate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.ensureOrganization(w, r, opSSOProviderCreate, org) {
		return
	}
	switch {
	case protocolText == ssoprovider.ProtocolSAML && saml == nil:
		h.rec.refuse(w, r, opSSOProviderCreate, refusal{http.StatusBadRequest, reasonSAMLConfigRequired}, nil)
		return
	case protocolText == ssoprovider.ProtocolOIDC && oidc == nil:
		h.rec.refuse(w, r, opSSOProviderCreate, refusal{http.StatusBadRequest, reasonOIDCConfigRequired}, nil)
		return
	}
	config, err := h.buildConfig(saml, oidc)
	if err != nil || config == nil {
		answer := refusal{http.StatusBadRequest, reasonMalformed}
		if !errors.Is(err, errProvisioningBody) && err != nil {
			answer = unavailable(reasonStoreUnavailable)
		}
		h.rec.refuse(w, r, opSSOProviderCreate, answer, failure(answer, err))
		return
	}
	in := ssoprovider.CreateInput{Name: name, Protocol: protocolText, Config: *config, DefaultRole: "member", AutoProvision: true, AllowedDomains: []string{}}
	if isDefault != nil {
		in.IsDefault = *isDefault
	}
	if idp != nil {
		in.AllowIdpInitiated = *idp
	}
	if auto != nil {
		in.AutoProvision = *auto
	}
	if role != nil {
		in.DefaultRole = *role
	}
	if domains != nil {
		in.AllowedDomains = *domains
	}
	p, err := h.SSOProviders.Create(r.Context(), org, in, who)
	if err != nil {
		h.ssoStoreFailure(w, r, opSSOProviderCreate, err)
		return
	}
	h.rec.record(r.Context(), opSSOProviderCreate, "success", reasonAccepted, nil)
	writeProviderJSON(w, http.StatusCreated, ssoAnswer(*p))
}

func (h provisioningHandlers) ssoProviderUpdate(w http.ResponseWriter, r *http.Request) {
	body, org, _, who, ok := h.ssoWriteAdmit(w, r, opSSOProviderUpdate, ssoUpdateMembers...)
	if !ok {
		return
	}
	id, err1 := body.uuid("provider_id")
	name, err2 := body.optionalAnyText("name", maxSSOName)
	saml, err3 := body.optionalObject("saml_config", maxSSOProviderBody)
	oidc, err4 := body.optionalObject("oidc_config", maxSSOProviderBody)
	isDefault, err5 := body.optionalBoolPtr("is_default")
	idp, err6 := body.optionalBoolPtr("allow_idp_initiated")
	auto, err7 := body.optionalBoolPtr("auto_provision_users")
	role, err8 := body.optionalAnyText("default_role", maxShortTextBytes)
	domains, err9 := body.optionalDomains("allowed_domains")
	if errors.Join(err1, err2, err3, err4, err5, err6, err7, err8, err9) != nil {
		h.rec.refuse(w, r, opSSOProviderUpdate, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.ensureOrganization(w, r, opSSOProviderUpdate, org) {
		return
	}
	config, err := h.buildConfig(saml, oidc)
	if err != nil {
		answer := refusal{http.StatusBadRequest, reasonMalformed}
		if !errors.Is(err, errProvisioningBody) {
			answer = unavailable(reasonStoreUnavailable)
		}
		h.rec.refuse(w, r, opSSOProviderUpdate, answer, failure(answer, err))
		return
	}
	p, err := h.SSOProviders.Update(r.Context(), org, id, ssoprovider.UpdateInput{Name: name, Config: config, IsDefault: isDefault,
		AllowIdpInitiated: idp, AutoProvision: auto, DefaultRole: role, AllowedDomains: domains}, who)
	if err != nil {
		h.ssoStoreFailure(w, r, opSSOProviderUpdate, err)
		return
	}
	h.rec.record(r.Context(), opSSOProviderUpdate, "success", reasonAccepted, nil)
	writeProviderJSON(w, http.StatusOK, ssoAnswer(*p))
}

func (h provisioningHandlers) ssoProviderDelete(w http.ResponseWriter, r *http.Request) {
	body, org, _, who, ok := h.ssoWriteAdmit(w, r, opSSOProviderDelete, ssoByIDMembers...)
	if !ok {
		return
	}
	id, err := body.uuid("provider_id")
	if err != nil {
		h.rec.refuse(w, r, opSSOProviderDelete, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.ensureOrganization(w, r, opSSOProviderDelete, org) {
		return
	}
	if err := h.SSOProviders.Delete(r.Context(), org, id, who); err != nil {
		h.ssoStoreFailure(w, r, opSSOProviderDelete, err)
		return
	}
	h.rec.record(r.Context(), opSSOProviderDelete, "success", reasonAccepted, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h provisioningHandlers) ssoProviderActivate(w http.ResponseWriter, r *http.Request) {
	h.ssoSetStatus(w, r, opSSOProviderActivate, ssoprovider.StatusActive)
}

func (h provisioningHandlers) ssoProviderDeactivate(w http.ResponseWriter, r *http.Request) {
	h.ssoSetStatus(w, r, opSSOProviderDeactivate, ssoprovider.StatusInactive)
}

func (h provisioningHandlers) ssoSetStatus(w http.ResponseWriter, r *http.Request, op, status string) {
	body, org, _, who, ok := h.ssoWriteAdmit(w, r, op, ssoByIDMembers...)
	if !ok {
		return
	}
	id, err := body.uuid("provider_id")
	if err != nil {
		h.rec.refuse(w, r, op, refusal{http.StatusBadRequest, reasonMalformed}, nil)
		return
	}
	if !h.ensureOrganization(w, r, op, org) {
		return
	}
	p, err := h.SSOProviders.SetStatus(r.Context(), org, id, status, who)
	if err != nil {
		h.ssoStoreFailure(w, r, op, err)
		return
	}
	h.rec.record(r.Context(), op, "success", reasonAccepted, nil)
	writeProviderJSON(w, http.StatusOK, ssoAnswer(*p))
}
