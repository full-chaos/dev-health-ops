package sso

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/authprovision"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// The provider routes while auth-service owns the SSO providers (Deps.Providers, AUTH_SSO_PROVIDERS_OWNED there): go-api
// keeps each route, its guard, its validation and the 402 gate, and calls auth-service for the write. Every provider
// answer is re-rendered here with providerResponse, so its bytes (times in pydantic's form, the masked config, the key
// order) are the ones activate and deactivate answered before the move. Auth-service answers its own refusals as reason
// codes; the legacy texts are mapped here. A fault is a named 503, never the stub's 500 and never a partial answer.

// providersUnavailableText is the detail of the 503 a client fault gets.
const providersUnavailableText = "SSO provider service unavailable"

const (
	textAdminRequired       = "Admin access required"
	textProviderNotFound    = "SSO provider not found"
	textSAMLConfigRequired  = "SAML config required for SAML providers"
	textOIDCConfigRequired  = "OIDC config required for OIDC providers"
	errShapeAnswerFormat    = "%w: auth-service answered a provider that is not the model"
	rfc3339ProviderTimeForm = time.RFC3339Nano
)

// providerAnswer is the provider object auth-service answers (the legacy SSOProviderResponse).
type providerAnswer struct {
	ID                 string          `json:"id"`
	OrgID              string          `json:"org_id"`
	Name               string          `json:"name"`
	Protocol           string          `json:"protocol"`
	Status             string          `json:"status"`
	IsDefault          bool            `json:"is_default"`
	AllowIdpInitiated  bool            `json:"allow_idp_initiated"`
	AutoProvisionUsers bool            `json:"auto_provision_users"`
	DefaultRole        string          `json:"default_role"`
	Config             json.RawMessage `json:"config"`
	AllowedDomains     json.RawMessage `json:"allowed_domains"`
	LastMetadataSyncAt *string         `json:"last_metadata_sync_at"`
	LastLoginAt        *string         `json:"last_login_at"`
	LastError          *string         `json:"last_error"`
	LastErrorAt        *string         `json:"last_error_at"`
	CreatedAt          string          `json:"created_at"`
	UpdatedAt          string          `json:"updated_at"`
}

func parseProviderTime(text *string) (*time.Time, error) {
	if text == nil {
		return nil, nil
	}
	at, err := time.Parse(rfc3339ProviderTimeForm, *text)
	if err != nil {
		return nil, err
	}
	return &at, nil
}

// rawConfig is the config member as auth-service wrote it, kept as it is (null or absent is the empty object, as the legacy read).
func rawConfig(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return json.RawMessage("{}")
	}
	return raw
}

func jsonText(raw json.RawMessage) *string {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	text := string(raw)
	return &text
}

// providerRowOf is the providerRow providerResponse renders for an auth-service answer.
func providerRowOf(raw json.RawMessage) (*providerRow, json.RawMessage, error) {
	var a providerAnswer
	if err := json.Unmarshal(raw, &a); err != nil {
		return nil, nil, fmt.Errorf(errShapeAnswerFormat, errShape)
	}
	if len(a.Config) != 0 && !json.Valid(a.Config) {
		return nil, nil, fmt.Errorf(errShapeAnswerFormat, errShape)
	}
	row := &providerRow{ID: a.ID, OrgID: a.OrgID, Name: a.Name, Protocol: a.Protocol, Status: a.Status, DefaultRole: a.DefaultRole,
		IsDefault: a.IsDefault, AllowIdpInitiated: a.AllowIdpInitiated, AutoProvision: a.AutoProvisionUsers,
		AllowedDomains: jsonText(a.AllowedDomains), LastError: a.LastError}
	var err error
	if row.LastMetadataSyncAt, err = parseProviderTime(a.LastMetadataSyncAt); err != nil {
		return nil, nil, err
	}
	if row.LastLoginAt, err = parseProviderTime(a.LastLoginAt); err != nil {
		return nil, nil, err
	}
	if row.LastErrorAt, err = parseProviderTime(a.LastErrorAt); err != nil {
		return nil, nil, err
	}
	if row.CreatedAt, err = time.Parse(rfc3339ProviderTimeForm, a.CreatedAt); err != nil {
		return nil, nil, err
	}
	if row.UpdatedAt, err = time.Parse(rfc3339ProviderTimeForm, a.UpdatedAt); err != nil {
		return nil, nil, err
	}
	return row, rawConfig(a.Config), nil
}

func (h handlers) writeProvider(w http.ResponseWriter, r *http.Request, status int, raw json.RawMessage) {
	row, config, err := providerRowOf(raw)
	if err == nil {
		var out *pyjson.Object
		if out, err = renderProvider(row, config); err == nil {
			policy.WriteModel(w, status, out, nil)
			return
		}
	}
	h.providerFault(w, r, "render provider", err)
}

// providerFault answers a refusal of auth-service with the legacy text it stands for and anything else with the named 503.
func (h handlers) providerFault(w http.ResponseWriter, r *http.Request, step string, err error) {
	var refused *authprovision.Refused
	if errors.As(err, &refused) {
		switch refused.Reason {
		case authprovision.ReasonSSOProviderNotFound:
			policy.WriteDetail(w, http.StatusNotFound, textProviderNotFound, nil)
			return
		case authprovision.ReasonSAMLConfigRequired:
			policy.WriteDetail(w, http.StatusBadRequest, textSAMLConfigRequired, nil)
			return
		case authprovision.ReasonOIDCConfigRequired:
			policy.WriteDetail(w, http.StatusBadRequest, textOIDCConfigRequired, nil)
			return
		}
	}
	h.Logger.WarnContext(r.Context(), "api sso providers: auth-service call failed", "path", r.Pattern, "step", step, "error", err.Error())
	policy.WriteDetail(w, http.StatusServiceUnavailable, policy.ErrorDetail(providersUnavailableText), nil)
}

// providerScope is the caller's organization (the token's, never the body's) and the caller. A caller whose token org does
// not parse as a UUID is the bare 500 the legacy handlers gave (uuid.UUID(user.org_id) is not caught). A write needs the
// owner or admin role first (router.py: user.role not in ("owner", "admin")).
func (h handlers) providerScope(w http.ResponseWriter, r *http.Request, write bool) (org, actor uuid.UUID, ok bool) {
	caller := policy.UserFrom(r.Context())
	if write && caller.Role != "owner" && caller.Role != "admin" {
		policy.WriteDetail(w, http.StatusForbidden, textAdminRequired, nil)
		return uuid.Nil, uuid.Nil, false
	}
	org, err := pythonparity.ParseUUID(caller.OrgID)
	if err != nil {
		h.fail(w, r, "parse org id", err)
		return uuid.Nil, uuid.Nil, false
	}
	return org, caller.ID, true
}

func (h handlers) providerID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := pythonparity.ParseUUID(r.PathValue("provider_id"))
	if err != nil {
		h.fail(w, r, "parse provider id", err)
		return uuid.Nil, false
	}
	return id, true
}

// bodyObject is the validated JSON object of the request.
func bodyObject(r *http.Request) *pyjson.Object {
	body, _ := policy.BodyFrom(r.Context())
	object, _ := body.Value.(*pyjson.Object)
	return object
}

func rawMember(object *pyjson.Object, name string) json.RawMessage {
	value, present := object.Get(name)
	if !present || value == nil {
		return nil
	}
	raw, err := pyjson.Marshal(value)
	if err != nil {
		return nil
	}
	return raw
}

func textMember(object *pyjson.Object, name string) *string {
	if value, present := object.Get(name); present {
		if text, ok := value.(string); ok {
			return &text
		}
	}
	return nil
}

func boolMember(object *pyjson.Object, name string) *bool {
	if value, present := object.Get(name); present {
		if flag, ok := value.(bool); ok {
			return &flag
		}
	}
	return nil
}

func domainsMember(object *pyjson.Object, name string) *[]string {
	value, present := object.Get(name)
	if !present {
		return nil
	}
	items, ok := value.([]pyjson.Value)
	if !ok {
		return nil
	}
	domains := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			domains = append(domains, text)
		}
	}
	return &domains
}

// listProviders is GET /sso/providers over auth-service. limit and offset are Python ints: past int64 or negative, the
// legacy query failed in the database (a bare 500), so no call is made. auth-service clamps a limit to 1000; the answer
// echoes the limit and offset asked, as the legacy list did.
func (h handlers) listProviders(w http.ResponseWriter, r *http.Request) {
	org, actor, ok := h.providerScope(w, r, false)
	if !ok {
		return
	}
	var errs pybody.Errors
	query := r.URL.Query()
	limit, _ := errs.QueryInt("limit", pybody.LastQueryValue(query, "limit"), 50, nil, nil)
	offset, _ := errs.QueryInt("offset", pybody.LastQueryValue(query, "offset"), 0, nil, nil)
	if limit == nil || offset == nil || !limit.IsInt64() || !offset.IsInt64() || limit.Sign() < 0 || offset.Sign() < 0 {
		h.fail(w, r, "list bounds", errShape)
		return
	}
	request := authprovision.SSOListRequest{Org: org, Actor: actor}
	if protocol := pybody.LastQueryValue(query, "protocol"); protocol != nil && *protocol != "" {
		request.Protocol = protocol
	}
	if status := pybody.LastQueryValue(query, "status"); status != nil && *status != "" {
		request.Status = status
	}
	asked, askedOffset := limit.Int64(), offset.Int64()
	request.Limit, request.Offset = &asked, &askedOffset
	raw, err := h.Providers.List(r.Context(), request)
	if err != nil {
		h.providerFault(w, r, "list", err)
		return
	}
	var answer struct {
		Items []json.RawMessage `json:"items"`
		Total int64             `json:"total"`
	}
	if err := json.Unmarshal(raw, &answer); err != nil {
		h.providerFault(w, r, "list answer", err)
		return
	}
	items := make([]pyjson.Value, 0, len(answer.Items))
	for _, item := range answer.Items {
		row, config, err := providerRowOf(item)
		if err != nil {
			h.providerFault(w, r, "render provider", err)
			return
		}
		out, err := renderProvider(row, config)
		if err != nil {
			h.providerFault(w, r, "render provider", err)
			return
		}
		items = append(items, out)
	}
	out := pyjson.NewObject()
	out.Set("items", items)
	out.Set("total", answer.Total)
	out.Set("limit", pyjson.Int{Int: new(big.Int).Set(limit)})
	out.Set("offset", pyjson.Int{Int: new(big.Int).Set(offset)})
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// createProvider is POST /sso/providers over auth-service (201).
func (h handlers) createProvider(w http.ResponseWriter, r *http.Request) {
	org, actor, ok := h.providerScope(w, r, true)
	if !ok {
		return
	}
	object := bodyObject(r)
	if object == nil {
		h.fail(w, r, "body", errShape)
		return
	}
	protocol := ""
	if text := textMember(object, "protocol"); text != nil {
		protocol = *text
	}
	saml, oidc := rawMember(object, "saml_config"), rawMember(object, "oidc_config")
	switch {
	case protocol == "saml" && saml == nil:
		policy.WriteDetail(w, http.StatusBadRequest, textSAMLConfigRequired, nil)
		return
	case protocol == "oidc" && oidc == nil:
		policy.WriteDetail(w, http.StatusBadRequest, textOIDCConfigRequired, nil)
		return
	}
	name := ""
	if text := textMember(object, "name"); text != nil {
		name = *text
	}
	raw, err := h.Providers.Create(r.Context(), authprovision.SSOCreateRequest{Org: org, Actor: actor, Name: name, Protocol: protocol,
		SAMLConfig: saml, OIDCConfig: oidc, IsDefault: boolMember(object, "is_default"), AllowIdpInitiated: boolMember(object, "allow_idp_initiated"),
		AutoProvision: boolMember(object, "auto_provision_users"), DefaultRole: textMember(object, "default_role"),
		AllowedDomains: domainsMember(object, "allowed_domains")})
	if err != nil {
		h.providerFault(w, r, "create", err)
		return
	}
	h.writeProvider(w, r, http.StatusCreated, raw)
}

// getProvider is GET /sso/providers/{provider_id} over auth-service.
func (h handlers) getProvider(w http.ResponseWriter, r *http.Request) {
	org, actor, ok := h.providerScope(w, r, false)
	if !ok {
		return
	}
	id, ok := h.providerID(w, r)
	if !ok {
		return
	}
	raw, err := h.Providers.Get(r.Context(), org, actor, id)
	if err != nil {
		h.providerFault(w, r, "get", err)
		return
	}
	h.writeProvider(w, r, http.StatusOK, raw)
}

// updateProvider is PATCH /sso/providers/{provider_id} over auth-service: only the members the body names change.
func (h handlers) updateProvider(w http.ResponseWriter, r *http.Request) {
	org, actor, ok := h.providerScope(w, r, true)
	if !ok {
		return
	}
	id, ok := h.providerID(w, r)
	if !ok {
		return
	}
	object := bodyObject(r)
	if object == nil {
		h.fail(w, r, "body", errShape)
		return
	}
	raw, err := h.Providers.Update(r.Context(), authprovision.SSOUpdateRequest{Org: org, Actor: actor, Provider: id, Name: textMember(object, "name"),
		SAMLConfig: rawMember(object, "saml_config"), OIDCConfig: rawMember(object, "oidc_config"), IsDefault: boolMember(object, "is_default"),
		AllowIdpInitiated: boolMember(object, "allow_idp_initiated"), AutoProvision: boolMember(object, "auto_provision_users"),
		DefaultRole: textMember(object, "default_role"), AllowedDomains: domainsMember(object, "allowed_domains")})
	if err != nil {
		h.providerFault(w, r, "update", err)
		return
	}
	h.writeProvider(w, r, http.StatusOK, raw)
}

// deleteProvider is DELETE /sso/providers/{provider_id} over auth-service (204).
func (h handlers) deleteProvider(w http.ResponseWriter, r *http.Request) {
	org, actor, ok := h.providerScope(w, r, true)
	if !ok {
		return
	}
	id, ok := h.providerID(w, r)
	if !ok {
		return
	}
	if err := h.Providers.Delete(r.Context(), org, actor, id); err != nil {
		h.providerFault(w, r, "delete", err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// setStatusViaProviders is activate and deactivate over auth-service, in place of the direct UPDATE.
func (h handlers) setStatusViaProviders(status string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		org, actor, ok := h.providerScope(w, r, true)
		if !ok {
			return
		}
		id, ok := h.providerID(w, r)
		if !ok {
			return
		}
		var raw json.RawMessage
		var err error
		if status == "active" {
			raw, err = h.Providers.Activate(r.Context(), org, actor, id)
		} else {
			raw, err = h.Providers.Deactivate(r.Context(), org, actor, id)
		}
		if err != nil {
			h.providerFault(w, r, "set status", err)
			return
		}
		h.writeProvider(w, r, http.StatusOK, raw)
	})
}
