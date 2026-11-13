package authprovision

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

// The SSO provider routes auth-service serves to go-api (AUTH_SSO_PROVIDERS_OWNED): the provider management go-api's own
// routes stand in front of. go-api decides who may call and answers the legacy status and text; auth-service writes.
const (
	PathSSOProviderList       = "/internal/v1/sso/providers/list"
	PathSSOProviderCreate     = "/internal/v1/sso/providers/create"
	PathSSOProviderGet        = "/internal/v1/sso/providers/get"
	PathSSOProviderUpdate     = "/internal/v1/sso/providers/update"
	PathSSOProviderDelete     = "/internal/v1/sso/providers/delete"
	PathSSOProviderActivate   = "/internal/v1/sso/providers/activate"
	PathSSOProviderDeactivate = "/internal/v1/sso/providers/deactivate"
)

// Reason codes of the SSO provider routes' refusals (error.v1 reason_code), as auth-service names them.
const (
	ReasonSSOProviderNotFound = "sso_provider_not_found"
	ReasonSAMLConfigRequired  = "saml_config_required"
	ReasonOIDCConfigRequired  = "oidc_config_required"
	ReasonOrganizationMissing = "organization_not_found"
)

// SSOListRequest is the list's filter; a nil field is not sent and auth-service applies the legacy default.
type SSOListRequest struct {
	Org, Actor       uuid.UUID
	Protocol, Status *string
	Limit, Offset    *int64
}

// SSOCreateRequest is a create after go-api's own body checks. SAMLConfig and OIDCConfig are the validated config
// objects as JSON (nil: absent); a nil pointer is not sent and auth-service applies the legacy default.
type SSOCreateRequest struct {
	Org, Actor                                  uuid.UUID
	Name, Protocol                              string
	SAMLConfig, OIDCConfig                      json.RawMessage
	IsDefault, AllowIdpInitiated, AutoProvision *bool
	DefaultRole                                 *string
	AllowedDomains                              *[]string
}

// SSOUpdateRequest is an update; only the members set are sent, so only they change (the legacy PATCH).
type SSOUpdateRequest struct {
	Org, Actor, Provider                        uuid.UUID
	Name                                        *string
	SAMLConfig, OIDCConfig                      json.RawMessage
	IsDefault, AllowIdpInitiated, AutoProvision *bool
	DefaultRole                                 *string
	AllowedDomains                              *[]string
}

// SSOProviders is the SSO provider routes as go-api calls them. Every method that answers a provider returns the legacy
// SSOProviderResponse JSON as auth-service wrote it (the secret is masked there); a refusal is a *Refused whose
// reason is one of the Reason constants above.
type SSOProviders interface {
	List(ctx context.Context, in SSOListRequest) (json.RawMessage, error)
	Get(ctx context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error)
	Create(ctx context.Context, in SSOCreateRequest) (json.RawMessage, error)
	Update(ctx context.Context, in SSOUpdateRequest) (json.RawMessage, error)
	Delete(ctx context.Context, org, actor, provider uuid.UUID) error
	Activate(ctx context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error)
	Deactivate(ctx context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error)
}

// SSOProvidersClient calls the SSO provider routes of one auth-service.
type SSOProvidersClient struct{ core *Core }

var _ SSOProviders = (*SSOProvidersClient)(nil)

// NewSSOProvidersClient is an SSOProvidersClient over core.
func NewSSOProvidersClient(core *Core) *SSOProvidersClient { return &SSOProvidersClient{core: core} }

func ssoBase(org, actor uuid.UUID) map[string]any {
	return map[string]any{"organization_id": org.String(), "actor_principal_id": actor.String()}
}

func ssoByID(org, actor, provider uuid.UUID) map[string]any {
	request := ssoBase(org, actor)
	request["provider_id"] = provider.String()
	return request
}

// answerProvider posts and returns the provider JSON, refusing an answer that is not a provider object of this org.
func (c *SSOProvidersClient) answerProvider(ctx context.Context, path string, request map[string]any, org uuid.UUID, status int) (json.RawMessage, error) {
	var answer json.RawMessage
	if err := c.core.PostExpecting(ctx, path, request, &answer, status); err != nil {
		return nil, err
	}
	var provider struct {
		ID    string `json:"id"`
		OrgID string `json:"org_id"`
	}
	if err := json.Unmarshal(answer, &provider); err != nil || provider.OrgID != org.String() {
		return nil, fmt.Errorf("%w: answer is not a provider of the organization", ErrUnavailable)
	}
	if _, err := uuid.Parse(provider.ID); err != nil {
		return nil, fmt.Errorf("%w: answer is not a provider", ErrUnavailable)
	}
	return answer, nil
}

func setConfigs(request map[string]any, saml, oidc json.RawMessage) {
	if len(saml) > 0 {
		request["saml_config"] = saml
	}
	if len(oidc) > 0 {
		request["oidc_config"] = oidc
	}
}

func setFlags(request map[string]any, isDefault, idp, auto *bool, role *string, domains *[]string) {
	if isDefault != nil {
		request["is_default"] = *isDefault
	}
	if idp != nil {
		request["allow_idp_initiated"] = *idp
	}
	if auto != nil {
		request["auto_provision_users"] = *auto
	}
	setText(request, "default_role", role)
	if domains != nil {
		request["allowed_domains"] = *domains
	}
}

// List returns the legacy SSOProviderListResponse JSON (items, total, limit, offset).
func (c *SSOProvidersClient) List(ctx context.Context, in SSOListRequest) (json.RawMessage, error) {
	request := ssoBase(in.Org, in.Actor)
	setText(request, "protocol", in.Protocol)
	setText(request, "status", in.Status)
	if in.Limit != nil {
		request["limit"] = *in.Limit
	}
	if in.Offset != nil {
		request["offset"] = *in.Offset
	}
	var answer json.RawMessage
	if err := c.core.PostExpecting(ctx, PathSSOProviderList, request, &answer, http.StatusOK); err != nil {
		return nil, err
	}
	var list struct {
		Items []json.RawMessage `json:"items"`
		Total *int64            `json:"total"`
	}
	if err := json.Unmarshal(answer, &list); err != nil || list.Items == nil || list.Total == nil {
		return nil, fmt.Errorf("%w: answer is not a provider list", ErrUnavailable)
	}
	return answer, nil
}

// Get returns one provider.
func (c *SSOProvidersClient) Get(ctx context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error) {
	return c.answerProvider(ctx, PathSSOProviderGet, ssoByID(org, actor, provider), org, http.StatusOK)
}

// Create creates a pending_setup provider; auth-service answers 201.
func (c *SSOProvidersClient) Create(ctx context.Context, in SSOCreateRequest) (json.RawMessage, error) {
	request := ssoBase(in.Org, in.Actor)
	request["name"] = in.Name
	request["protocol"] = in.Protocol
	setConfigs(request, in.SAMLConfig, in.OIDCConfig)
	setFlags(request, in.IsDefault, in.AllowIdpInitiated, in.AutoProvision, in.DefaultRole, in.AllowedDomains)
	return c.answerProvider(ctx, PathSSOProviderCreate, request, in.Org, http.StatusCreated)
}

// Update changes the members set.
func (c *SSOProvidersClient) Update(ctx context.Context, in SSOUpdateRequest) (json.RawMessage, error) {
	request := ssoByID(in.Org, in.Actor, in.Provider)
	setText(request, "name", in.Name)
	setConfigs(request, in.SAMLConfig, in.OIDCConfig)
	setFlags(request, in.IsDefault, in.AllowIdpInitiated, in.AutoProvision, in.DefaultRole, in.AllowedDomains)
	return c.answerProvider(ctx, PathSSOProviderUpdate, request, in.Org, http.StatusOK)
}

// Delete removes the provider; auth-service answers 204 with no body.
func (c *SSOProvidersClient) Delete(ctx context.Context, org, actor, provider uuid.UUID) error {
	return c.core.PostExpecting(ctx, PathSSOProviderDelete, ssoByID(org, actor, provider), nil, http.StatusNoContent)
}

// Activate sets the status active.
func (c *SSOProvidersClient) Activate(ctx context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error) {
	return c.answerProvider(ctx, PathSSOProviderActivate, ssoByID(org, actor, provider), org, http.StatusOK)
}

// Deactivate sets the status inactive.
func (c *SSOProvidersClient) Deactivate(ctx context.Context, org, actor, provider uuid.UUID) (json.RawMessage, error) {
	return c.answerProvider(ctx, PathSSOProviderDeactivate, ssoByID(org, actor, provider), org, http.StatusOK)
}
