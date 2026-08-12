package providerfoundation

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
)

const pagerDutyOAuthRenewalWindow = 5 * time.Minute

var pagerDutyOperationalReadScopes = strings.Fields(pagerDutyReadScopes)

// PagerDutyOAuthTokenRecord is the encrypted token reference loaded for one
// exact organization, provider, credential name, and OAuth binding.
type PagerDutyOAuthTokenRecord struct {
	Ciphertext secrets.Value
	Version    int
	BindingID  string
}

// PagerDutyOAuthTokenRotation is the optimistic replacement written after a
// successful refresh. It never carries plaintext token values.
type PagerDutyOAuthTokenRotation struct {
	ExpectedVersion   int
	ExpectedBindingID string
	Ciphertext        secrets.Value
	ExpiresAt         time.Time
	GrantedScopes     []string
	HasRefreshToken   bool
}

type PagerDutyOAuthTokenRepository interface {
	Load(context.Context, string, string) (PagerDutyOAuthTokenRecord, error)
	Rotate(context.Context, string, string, PagerDutyOAuthTokenRotation) (bool, error)
}

// PagerDutyOAuthHydrator resolves a tokenless OAuth descriptor through the
// Python-owned provider_oauth_credentials store. The access token exists only
// on the returned credential copy and is never added to the descriptor row,
// process environment, logs, or job payload.
type PagerDutyOAuthHydrator struct {
	Repository      PagerDutyOAuthTokenRepository
	Cipher          CredentialCipher
	Doer            HTTPDoer
	AppClientID     secrets.Value
	AppClientSecret secrets.Value
	Now             func() time.Time
}

type pagerDutyOAuthTokens struct {
	AccessToken   string    `json:"access_token"`
	RefreshToken  *string   `json:"refresh_token"`
	ExpiresAt     time.Time `json:"expires_at"`
	GrantedScopes []string  `json:"granted_scopes"`
}

func (h PagerDutyOAuthHydrator) Hydrate(
	ctx context.Context,
	lease LeaseGuard,
	scope TenantScope,
	credential Credential,
) (Credential, error) {
	if credential.Provider != "pagerduty" || credentialValue(credential, "auth_mode") != "oauth" {
		return credential, nil
	}
	if token, exists := credential.Secret("access_token"); exists && token.Configured() {
		return credential, nil
	}
	if ctx == nil || lease == nil || h.Repository == nil || h.Cipher == nil {
		return Credential{}, ErrCredentialInvalid
	}
	credentialName := credentialValue(credential, "oauth_credential_name")
	bindingID := credentialValue(credential, "oauth_binding_id")
	if scope.Provider != "pagerduty" || credentialName == "" || bindingID == "" {
		return Credential{}, ErrCredentialInvalid
	}
	if err := lease.Assert(ctx); err != nil {
		return Credential{}, err
	}
	record, err := h.Repository.Load(ctx, scope.OrgID, credentialName)
	if err != nil {
		return Credential{}, err
	}
	tokens, err := h.decode(ctx, lease, record, bindingID)
	if err != nil {
		return Credential{}, err
	}
	if !hasPagerDutyOperationalReadScopes(tokens.GrantedScopes) {
		return Credential{}, ErrCredentialInvalid
	}
	if tokens.ExpiresAt.After(h.now().Add(pagerDutyOAuthRenewalWindow)) {
		return credential.WithEphemeralSecret(
			"access_token", secrets.NewValue(tokens.AccessToken),
		)
	}
	return h.refresh(ctx, lease, scope.OrgID, credentialName, credential, record, tokens)
}

func (h PagerDutyOAuthHydrator) decode(
	ctx context.Context,
	lease LeaseGuard,
	record PagerDutyOAuthTokenRecord,
	expectedBindingID string,
) (pagerDutyOAuthTokens, error) {
	if record.Version < 1 || record.BindingID != expectedBindingID ||
		!record.Ciphertext.Configured() {
		return pagerDutyOAuthTokens{}, ErrCredentialInvalid
	}
	if err := lease.Assert(ctx); err != nil {
		return pagerDutyOAuthTokens{}, err
	}
	plaintext, err := h.Cipher.Decrypt(record.Ciphertext)
	if err != nil {
		return pagerDutyOAuthTokens{}, ErrCredentialInvalid
	}
	var tokens pagerDutyOAuthTokens
	if json.Unmarshal(plaintext, &tokens) != nil ||
		strings.TrimSpace(tokens.AccessToken) == "" || tokens.ExpiresAt.IsZero() {
		return pagerDutyOAuthTokens{}, ErrCredentialInvalid
	}
	tokens.GrantedScopes = normalizedPagerDutyScopes(tokens.GrantedScopes)
	return tokens, nil
}

func (h PagerDutyOAuthHydrator) refresh(
	ctx context.Context,
	lease LeaseGuard,
	orgID string,
	credentialName string,
	credential Credential,
	record PagerDutyOAuthTokenRecord,
	current pagerDutyOAuthTokens,
) (Credential, error) {
	if h.Doer == nil || !h.AppClientID.Configured() || current.RefreshToken == nil ||
		strings.TrimSpace(*current.RefreshToken) == "" {
		return Credential{}, ErrCredentialInvalid
	}
	if err := lease.Assert(ctx); err != nil {
		return Credential{}, err
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {h.AppClientID.Reveal()},
		"client_secret": {h.AppClientSecret.Reveal()},
		"refresh_token": {*current.RefreshToken},
	}
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, pagerDutyTokenURL, strings.NewReader(form.Encode()),
	)
	if err != nil {
		return Credential{}, ErrCredentialInvalid
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := h.Doer.Do(request)
	if err != nil {
		return Credential{}, &ProviderError{Class: ErrorTransient}
	}
	defer response.Body.Close()
	if classification := ClassifyHTTP("pagerduty", response.StatusCode, response.Header); classification != nil {
		return Credential{}, classification
	}
	var payload struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    any    `json:"expires_in"`
		Scope        string `json:"scope"`
	}
	if json.NewDecoder(io.LimitReader(response.Body, maxProviderErrorBody)).Decode(&payload) != nil ||
		strings.TrimSpace(payload.AccessToken) == "" {
		return Credential{}, ErrCredentialInvalid
	}
	expiresIn := pagerDutyExpiresIn(payload.ExpiresIn)
	refreshed := pagerDutyOAuthTokens{
		AccessToken: payload.AccessToken,
		ExpiresAt:   h.now().Add(time.Duration(expiresIn) * time.Second),
	}
	if payload.RefreshToken == "" {
		refreshed.RefreshToken = current.RefreshToken
	} else {
		refreshed.RefreshToken = &payload.RefreshToken
	}
	if strings.TrimSpace(payload.Scope) == "" {
		refreshed.GrantedScopes = current.GrantedScopes
	} else {
		refreshed.GrantedScopes = normalizedPagerDutyScopes(strings.Fields(payload.Scope))
	}
	if !hasPagerDutyOperationalReadScopes(refreshed.GrantedScopes) {
		return Credential{}, ErrCredentialInvalid
	}
	encoded, err := json.Marshal(refreshed)
	if err != nil {
		return Credential{}, ErrCredentialInvalid
	}
	ciphertext, err := h.Cipher.Encrypt(encoded)
	if err != nil {
		return Credential{}, ErrCredentialInvalid
	}
	if err := lease.Assert(ctx); err != nil {
		return Credential{}, err
	}
	rotated, err := h.Repository.Rotate(ctx, orgID, credentialName, PagerDutyOAuthTokenRotation{
		ExpectedVersion: record.Version, ExpectedBindingID: record.BindingID,
		Ciphertext: ciphertext, ExpiresAt: refreshed.ExpiresAt,
		GrantedScopes: refreshed.GrantedScopes, HasRefreshToken: refreshed.RefreshToken != nil,
	})
	if err != nil {
		return Credential{}, err
	}
	if rotated {
		return credential.WithEphemeralSecret(
			"access_token", secrets.NewValue(refreshed.AccessToken),
		)
	}
	// A concurrent worker won the optimistic rotation. Use only its freshly
	// persisted token and re-check the binding and scopes before proceeding.
	latest, err := h.Repository.Load(ctx, orgID, credentialName)
	if err != nil {
		return Credential{}, err
	}
	latestTokens, err := h.decode(ctx, lease, latest, record.BindingID)
	if err != nil || !hasPagerDutyOperationalReadScopes(latestTokens.GrantedScopes) ||
		!latestTokens.ExpiresAt.After(h.now().Add(pagerDutyOAuthRenewalWindow)) {
		return Credential{}, ErrCredentialInvalid
	}
	return credential.WithEphemeralSecret(
		"access_token", secrets.NewValue(latestTokens.AccessToken),
	)
}

func (h PagerDutyOAuthHydrator) now() time.Time {
	if h.Now != nil {
		return h.Now().UTC()
	}
	return time.Now().UTC()
}

func normalizedPagerDutyScopes(scopes []string) []string {
	result := make([]string, 0, len(scopes))
	seen := make(map[string]struct{}, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "" {
			continue
		}
		if _, exists := seen[scope]; exists {
			continue
		}
		seen[scope] = struct{}{}
		result = append(result, scope)
	}
	sort.Strings(result)
	return result
}

func hasPagerDutyOperationalReadScopes(scopes []string) bool {
	normalized := normalizedPagerDutyScopes(scopes)
	for _, required := range pagerDutyOperationalReadScopes {
		if !slices.Contains(normalized, required) {
			return false
		}
	}
	return true
}

func pagerDutyExpiresIn(value any) int {
	var seconds int
	switch typed := value.(type) {
	case float64:
		seconds = int(typed)
	case string:
		seconds, _ = strconv.Atoi(typed)
	}
	if seconds < 1 {
		return 3600
	}
	return seconds
}
