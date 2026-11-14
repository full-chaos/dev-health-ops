package authapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"regexp"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
	"github.com/full-chaos/dev-health-ops/internal/auth/requestid"
	"github.com/full-chaos/dev-health-ops/internal/auth/ssoprovider"
)

// The SSO provider routes: the management of public.sso_providers (go-api's provider CRUD, which is a stub there).
// auth-service is the one writer. Bodies and answers are the legacy schemas (api/auth/schemas.py at 64efc37bb3^);
// the client secret is sealed with the SSO cipher and is never in an answer. They mount only with their
// endpoint-profile rows, on the provisioning routes' workload path.
const (
	PatternSSOProviderList       = "/internal/v1/sso/providers/list"
	PatternSSOProviderCreate     = "/internal/v1/sso/providers/create"
	PatternSSOProviderGet        = "/internal/v1/sso/providers/get"
	PatternSSOProviderUpdate     = "/internal/v1/sso/providers/update"
	PatternSSOProviderDelete     = "/internal/v1/sso/providers/delete"
	PatternSSOProviderActivate   = "/internal/v1/sso/providers/activate"
	PatternSSOProviderDeactivate = "/internal/v1/sso/providers/deactivate"
)

const (
	opSSOProviderList       = "sso_provider_list"
	opSSOProviderCreate     = "sso_provider_create"
	opSSOProviderGet        = "sso_provider_get"
	opSSOProviderUpdate     = "sso_provider_update"
	opSSOProviderDelete     = "sso_provider_delete"
	opSSOProviderActivate   = "sso_provider_activate"
	opSSOProviderDeactivate = "sso_provider_deactivate"
)

// Refusal codes of the SSO provider routes: the 404 and the two 400s of the legacy router.
const (
	reasonSSOProviderNotFound = "sso_provider_not_found"
	reasonSAMLConfigRequired  = "saml_config_required"
	reasonOIDCConfigRequired  = "oidc_config_required"
)

// Legacy texts of those refusals, as go-api's caller answers them.
const (
	TextSSOProviderNotFound = "SSO provider not found"
	TextSAMLConfigRequired  = "SAML config required for SAML providers"
	TextOIDCConfigRequired  = "OIDC config required for OIDC providers"
)

var ssoProviderPatterns = []string{PatternSSOProviderList, PatternSSOProviderCreate, PatternSSOProviderGet, PatternSSOProviderUpdate,
	PatternSSOProviderDelete, PatternSSOProviderActivate, PatternSSOProviderDeactivate}

// SSOProviderPatterns returns a copy of the SSO provider routes, for tests that plant their endpoint-profile rows.
func SSOProviderPatterns() []string { return append([]string(nil), ssoProviderPatterns...) }

// SSOProviderRouteCount is the number of SSO provider routes.
func SSOProviderRouteCount() int { return len(ssoProviderPatterns) }

// SSOProviderRowsError is nil when the endpoint-profile document (nil: the embedded one) holds, for every SSO provider
// route, a row that admits exactly the go-api workload token; otherwise the *ProvisioningRowError of the first without.
func SSOProviderRowsError(raw []byte) error {
	if raw == nil {
		raw = authv1.AuthServiceEndpointProfiles
	}
	return rowsExactFor(raw, ssoProviderPatterns, []string{workloadTokenClass})
}

// maxSSOProviderBody bounds a provider body: a certificate and a claim mapping.
const maxSSOProviderBody = 64 << 10

var (
	ssoListMembers     = []string{"organization_id", "actor_principal_id", "protocol", "status", "limit", "offset"}
	ssoCreateMembers   = []string{"organization_id", "actor_principal_id", "name", "protocol", "saml_config", "oidc_config", "is_default", "allow_idp_initiated", "auto_provision_users", "default_role", "allowed_domains"}
	ssoUpdateMembers   = []string{"organization_id", "actor_principal_id", "provider_id", "name", "saml_config", "oidc_config", "is_default", "allow_idp_initiated", "auto_provision_users", "default_role", "allowed_domains"}
	ssoByIDMembers     = []string{"organization_id", "actor_principal_id", "provider_id"}
	ssoProtocolPattern = regexp.MustCompile(`^(saml|oidc)$`)
)

// SSOProviderCreateMembers returns a copy of the members the create route admits.
func SSOProviderCreateMembers() []string { return append([]string(nil), ssoCreateMembers...) }

// SSOProviderUpdateMembers returns a copy of the members the update route admits.
func SSOProviderUpdateMembers() []string { return append([]string(nil), ssoUpdateMembers...) }

// SSOProviderListMembers returns a copy of the members the list route admits.
func SSOProviderListMembers() []string { return append([]string(nil), ssoListMembers...) }

// SSOProviderByIDMembers returns a copy of the members the get, delete, activate and deactivate routes admit.
func SSOProviderByIDMembers() []string { return append([]string(nil), ssoByIDMembers...) }

// ssoProviderOperations take the go-api workload token only.
var ssoProviderOperations = map[string]bool{opSSOProviderList: true, opSSOProviderCreate: true, opSSOProviderGet: true,
	opSSOProviderUpdate: true, opSSOProviderDelete: true, opSSOProviderActivate: true, opSSOProviderDeactivate: true}

// SSOProviders is the provider store (*ssoprovider.Store).
type SSOProviders interface {
	List(ctx context.Context, org uuid.UUID, f ssoprovider.Filter) ([]ssoprovider.Provider, int64, error)
	Get(ctx context.Context, org, id uuid.UUID) (*ssoprovider.Provider, error)
	Create(ctx context.Context, org uuid.UUID, in ssoprovider.CreateInput, a ssoprovider.Actor) (*ssoprovider.Provider, error)
	Update(ctx context.Context, org, id uuid.UUID, in ssoprovider.UpdateInput, a ssoprovider.Actor) (*ssoprovider.Provider, error)
	SetStatus(ctx context.Context, org, id uuid.UUID, status string, a ssoprovider.Actor) (*ssoprovider.Provider, error)
	Delete(ctx context.Context, org, id uuid.UUID, a ssoprovider.Actor) error
}

// SecretSealer seals a provider's client secret with the SSO cipher; the sealed text is what the sign-in reader opens.
type SecretSealer interface {
	Seal(plain string) (string, error)
}

// SSOProviderStores are the SSO provider routes' stores, a part of ProvisioningDeps; with either nil, or without the
// routes' endpoint-profile rows, those routes are not mounted.
type SSOProviderStores struct {
	SSOProviders SSOProviders
	SSOSealer    SecretSealer
}

// SSOProviderRoutes returns the SSO provider routes, or none (and no refusal) while any row is missing: the routes stay
// dark until their rows ship. deps needs Organizations, Workload and both SSOProviderStores.
func SSOProviderRoutes(deps ProvisioningDeps) ([]httpapi.Route, error) {
	if deps.Organizations == nil || deps.SSOProviders == nil || deps.SSOSealer == nil {
		return nil, errors.New("authapi: the SSO provider routes need users, a reader, organizations, the provider store and the secret sealer")
	}
	if SSOProviderRowsError(deps.ProfileRows) != nil {
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
		{Method: http.MethodPost, Pattern: PatternSSOProviderList, Handler: http.HandlerFunc(h.ssoProviderList)},
		{Method: http.MethodPost, Pattern: PatternSSOProviderCreate, Handler: http.HandlerFunc(h.ssoProviderCreate)},
		{Method: http.MethodPost, Pattern: PatternSSOProviderGet, Handler: http.HandlerFunc(h.ssoProviderGet)},
		{Method: http.MethodPost, Pattern: PatternSSOProviderUpdate, Handler: http.HandlerFunc(h.ssoProviderUpdate)},
		{Method: http.MethodPost, Pattern: PatternSSOProviderDelete, Handler: http.HandlerFunc(h.ssoProviderDelete)},
		{Method: http.MethodPost, Pattern: PatternSSOProviderActivate, Handler: http.HandlerFunc(h.ssoProviderActivate)},
		{Method: http.MethodPost, Pattern: PatternSSOProviderDeactivate, Handler: http.HandlerFunc(h.ssoProviderDeactivate)},
	}, nil
}

// ssoProviderAnswer is the legacy SSOProviderResponse.
type ssoProviderAnswer struct {
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
	AllowedDomains     []string        `json:"allowed_domains"`
	LastMetadataSyncAt *string         `json:"last_metadata_sync_at"`
	LastLoginAt        *string         `json:"last_login_at"`
	LastError          *string         `json:"last_error"`
	LastErrorAt        *string         `json:"last_error_at"`
	CreatedAt          string          `json:"created_at"`
	UpdatedAt          string          `json:"updated_at"`
}

// ssoProviderListAnswer is the legacy SSOProviderListResponse.
type ssoProviderListAnswer struct {
	Items  []ssoProviderAnswer `json:"items"`
	Total  int64               `json:"total"`
	Limit  int64               `json:"limit"`
	Offset int64               `json:"offset"`
}

func ssoTime(t *time.Time) *string {
	if t == nil {
		return nil
	}
	s := pydanticTime(*t)
	return &s
}

// samlConfigInput and oidcConfigInput are the legacy SAMLConfigInput and OIDCConfigInput: the stored config is the
// model dump, with every optional member present (null when absent) and the defaults filled.
type samlConfigInput struct {
	EntityID         string            `json:"entity_id"`
	SSOURL           string            `json:"sso_url"`
	Certificate      string            `json:"certificate"`
	SLOURL           *string           `json:"slo_url"`
	NameIDFormat     string            `json:"name_id_format"`
	AttributeMapping map[string]string `json:"attribute_mapping"`
}

type oidcConfigInput struct {
	ClientID              string            `json:"client_id"`
	ClientSecret          string            `json:"client_secret"`
	Issuer                string            `json:"issuer"`
	AuthorizationEndpoint *string           `json:"authorization_endpoint"`
	TokenEndpoint         *string           `json:"token_endpoint"`
	UserinfoEndpoint      *string           `json:"userinfo_endpoint"`
	JWKSURI               *string           `json:"jwks_uri"`
	Scopes                []string          `json:"scopes"`
	ClaimMapping          map[string]string `json:"claim_mapping"`
}

// decodeConfig reads raw as one object of exactly the members of target (a pointer to a struct), refusing others and
// a null member where the legacy model has a non-null one.
func decodeConfig(raw []byte, target any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	return dec.Decode(target)
}

func readSAMLConfig(raw []byte) (*samlConfigInput, error) {
	in := samlConfigInput{NameIDFormat: "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress", AttributeMapping: map[string]string{}}
	if err := decodeConfig(raw, &in); err != nil || in.EntityID == "" && !bytes.Contains(raw, []byte(`"entity_id"`)) ||
		!bytes.Contains(raw, []byte(`"sso_url"`)) || !bytes.Contains(raw, []byte(`"certificate"`)) {
		return nil, errProvisioningBody
	}
	if in.AttributeMapping == nil {
		in.AttributeMapping = map[string]string{}
	}
	return &in, nil
}

func readOIDCConfig(raw []byte) (*oidcConfigInput, error) {
	in := oidcConfigInput{Scopes: []string{"openid", "profile", "email"}, ClaimMapping: map[string]string{}}
	if err := decodeConfig(raw, &in); err != nil || !bytes.Contains(raw, []byte(`"client_id"`)) ||
		!bytes.Contains(raw, []byte(`"client_secret"`)) || !bytes.Contains(raw, []byte(`"issuer"`)) {
		return nil, errProvisioningBody
	}
	if in.Scopes == nil {
		in.Scopes = []string{"openid", "profile", "email"}
	}
	if in.ClaimMapping == nil {
		in.ClaimMapping = map[string]string{}
	}
	return &in, nil
}

// buildConfig is the legacy config choice of a create or update: a SAML config wins over an OIDC one; an OIDC config
// moves its client_secret out of the config into the sealed secrets object.
func (h provisioningHandlers) buildConfig(saml, oidc []byte) (*ssoprovider.Config, error) {
	switch {
	case saml != nil:
		if _, err := readSAMLConfig(saml); err != nil {
			return nil, err
		}
		config, err := orderedConfig(saml, samlConfigMembers)
		return &ssoprovider.Config{JSON: config}, err
	case oidc != nil:
		in, err := readOIDCConfig(oidc)
		if err != nil {
			return nil, err
		}
		sealed, err := h.SSOSealer.Seal(in.ClientSecret)
		if err != nil {
			return nil, err
		}
		config, err := orderedConfig(oidc, oidcConfigMembers)
		if err != nil {
			return nil, err
		}
		secrets, err := json.Marshal(map[string]string{"client_secret": sealed})
		if err != nil {
			return nil, err
		}
		text := string(secrets)
		return &ssoprovider.Config{JSON: config, Secrets: &text}, nil
	}
	return nil, nil
}

func (o strictObject) optionalBoolPtr(name string) (*bool, error) {
	if o.isNull(name) {
		return nil, nil
	}
	var v bool
	if err := json.Unmarshal(o[name], &v); err != nil {
		return nil, errProvisioningBody
	}
	return &v, nil
}

func (o strictObject) optionalDomains(name string) (*[]string, error) {
	if o.isNull(name) {
		return nil, nil
	}
	var list []string
	if err := json.Unmarshal(o[name], &list); err != nil || len(list) > 256 {
		return nil, errProvisioningBody
	}
	if list == nil {
		list = []string{}
	}
	return &list, nil
}

func (h provisioningHandlers) ensureOrganization(w http.ResponseWriter, r *http.Request, op string, orgID uuid.UUID) bool {
	if err := h.Organizations.EnsureOrganization(r.Context(), orgID); err != nil {
		answer := unavailable(reasonDependency)
		if errors.Is(err, identity.ErrOrganizationNotFound) {
			answer, err = refusal{http.StatusNotFound, reasonOrganizationNotFound}, nil
		}
		h.rec.refuse(w, r, op, answer, err)
		return false
	}
	return true
}

func (h provisioningHandlers) ssoStoreFailure(w http.ResponseWriter, r *http.Request, op string, err error) {
	answer := unavailable(reasonStoreUnavailable)
	if errors.Is(err, ssoprovider.ErrNotFound) {
		answer = refusal{http.StatusNotFound, reasonSSOProviderNotFound}
	}
	h.rec.refuse(w, r, op, answer, failure(answer, err))
}

func (h provisioningHandlers) ssoActor(r *http.Request, actor uuid.UUID, caller audit.Caller) ssoprovider.Actor {
	return ssoprovider.Actor{PrincipalID: &actor, RequestID: requestid.Audit(r.Context()), Caller: caller}
}
