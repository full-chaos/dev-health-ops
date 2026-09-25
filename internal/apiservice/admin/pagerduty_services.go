// PagerDuty services list (api/admin/routers/pagerduty_services.py): the
// organisation's PagerDuty services, read live from PagerDuty with the stored
// credential. It builds the request's authentication (an API token, an OAuth
// access token that is renewed and rotated when it is about to expire, or a
// client-credentials token), pages through GET /services the way
// providers/pagerduty/client.py does (through providers/_http.py's retrying
// core) and answers the services sorted by name.
package admin

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

func (h *handlers) pagerDutyServicesRoutes() []httpapi.Route {
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: pagerDutyPrefix + "/services", Allow: "GET", Handler: h.guard.Wrap(policy.Admin, http.HandlerFunc(h.listPagerDutyServices))},
	}
}

// pagerDutyRenewalWindow is DEFAULT_RENEWAL_WINDOW.
const pagerDutyRenewalWindow = 5 * time.Minute

// servicesFailure is what the route answers in place of a list: an
// HTTPException (a status, a detail and maybe a header) or the generic 500 of
// an exception the route does not catch.
type servicesFailure struct {
	status  int
	detail  string
	header  http.Header
	err     error // non-nil: the unhandled-exception 500
	logNote string
}

func servicesHTTPError(status int, detail string) *servicesFailure {
	return &servicesFailure{status: status, detail: detail}
}

func servicesInternal(note string, err error) *servicesFailure {
	return &servicesFailure{status: http.StatusInternalServerError, err: err, logNote: note}
}

func (h *handlers) writeServicesFailure(ctx context.Context, w http.ResponseWriter, f *servicesFailure) {
	if f.err != nil {
		h.internalError(ctx, w, f.logNote, f.err)
		return
	}
	policy.WriteDetail(w, f.status, f.detail, f.header)
}

func (h *handlers) listPagerDutyServices(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	name, present := queryLastValue(r.URL.Query(), "credential_name")
	if !present {
		name = "default"
	} else if name == "" {
		limit := pyjson.NewObject()
		limit.Set("min_length", int64(1))
		writeValidation(w, pybody.Errors{{Type: "string_too_short", Loc: []pyjson.Value{"query", "credential_name"},
			Msg: "String should have at least 1 character", Input: "", Ctx: limit}})
		return
	}
	normalized := pythonparity.Strip(name)
	if normalized == "" {
		policy.WriteDetail(w, http.StatusUnprocessableEntity, "credential_name must not be blank", nil)
		return
	}

	values, failure := h.loadPagerDutyDescriptorValues(ctx, orgID, normalized)
	if failure != nil {
		h.writeServicesFailure(ctx, w, failure)
		return
	}
	auth, failure := h.buildPagerDutyAuth(ctx, orgID, values)
	if failure != nil {
		h.writeServicesFailure(ctx, w, failure)
		return
	}
	region, failure := requiredDescriptorString(values, "region")
	if failure != nil {
		h.writeServicesFailure(ctx, w, failure)
		return
	}
	services, failure := h.fetchPagerDutyServices(ctx, auth, region)
	if failure != nil {
		h.writeServicesFailure(ctx, w, failure)
		return
	}

	sort.SliceStable(services, func(i, j int) bool {
		left, right := pythonparity.Fold(services[i].displayName), pythonparity.Fold(services[j].displayName)
		if left != right {
			return left < right
		}
		return services[i].id < services[j].id
	})
	list := make([]pyjson.Value, 0, len(services))
	for _, service := range services {
		item := pyjson.NewObject()
		item.Set("external_id", service.id)
		item.Set("display_name", service.displayName)
		item.Set("name_resolved", service.nameResolved)
		if service.status == nil {
			item.Set("status", nil)
		} else {
			item.Set("status", *service.status)
		}
		list = append(list, item)
	}
	out := pyjson.NewObject()
	out.Set("credential_name", normalized)
	out.Set("services", list)
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// loadPagerDutyDescriptorValues is the descriptor lookup and
// get_decrypted_credentials: an absent, inactive or unreadable credential is
// the 404; a stored payload that is not an object fails later, as Python's
// `.get` on it does.
func (h *handlers) loadPagerDutyDescriptorValues(ctx context.Context, orgID, name string) (*pyjson.Object, *servicesFailure) {
	var active bool
	var encrypted *string
	err := h.store.Pool.QueryRow(ctx,
		`SELECT is_active, credentials_encrypted FROM integration_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND name = $2`,
		orgID, name).Scan(&active, &encrypted)
	notFound := servicesHTTPError(http.StatusNotFound, "PagerDuty credential was not found")
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound
	}
	if err != nil {
		return nil, servicesInternal("load pagerduty credential", err)
	}
	if !active || encrypted == nil || *encrypted == "" {
		return nil, notFound
	}
	plaintext, err := h.decryptor.Decrypt(secrets.NewValue(*encrypted))
	if err != nil {
		return nil, notFound
	}
	decoded, err := pyjson.Decode(plaintext)
	if err != nil {
		return nil, notFound
	}
	object, isObject := decoded.(*pyjson.Object)
	if !isObject {
		return nil, servicesInternal("pagerduty credential payload is not an object", errors.New("not an object"))
	}
	return object, nil
}

// requiredDescriptorString is _required_string: a non-blank string, stripped.
func requiredDescriptorString(values *pyjson.Object, key string) (string, *servicesFailure) {
	raw, _ := values.Get(key)
	text, isString := raw.(string)
	if !isString || pythonparity.Strip(text) == "" {
		return "", servicesHTTPError(http.StatusConflict, "PagerDuty credential is missing required "+key)
	}
	return pythonparity.Strip(text), nil
}

// pagerDutyRequestAuth is the one Authorization header a request carries.
type pagerDutyRequestAuth struct{ header string }

// buildPagerDutyAuth is _build_auth.
func (h *handlers) buildPagerDutyAuth(ctx context.Context, orgID string, values *pyjson.Object) (pagerDutyRequestAuth, *servicesFailure) {
	mode, failure := requiredDescriptorString(values, "auth_mode")
	if failure != nil {
		return pagerDutyRequestAuth{}, failure
	}
	switch mode {
	case "api_token":
		token, failure := requiredDescriptorString(values, "api_token")
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		return pagerDutyRequestAuth{header: "Token token=" + token}, nil
	case "oauth":
		if h.pagerDuty.ClientID == "" {
			return pagerDutyRequestAuth{}, servicesHTTPError(http.StatusConflict, "PagerDuty OAuth app is not configured")
		}
		credentialName, failure := requiredDescriptorString(values, "oauth_credential_name")
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		bindingID, failure := requiredDescriptorString(values, "oauth_binding_id")
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		access, failure := h.validPagerDutyAccessToken(ctx, orgID, credentialName, bindingID)
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		return pagerDutyRequestAuth{header: "Bearer " + access}, nil
	case "client_credentials":
		clientID, failure := requiredDescriptorString(values, "client_id")
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		clientSecret, failure := requiredDescriptorString(values, "client_secret")
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		subdomain, failure := requiredDescriptorString(values, "subdomain")
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		region, failure := requiredDescriptorString(values, "region")
		if failure != nil {
			return pagerDutyRequestAuth{}, failure
		}
		config := providerfoundation.PagerDutyRevokeConfig{ClientID: clientID, ClientSecret: clientSecret, TokenURL: h.pagerDuty.TokenURL}
		tokens, err := providerfoundation.RequestPagerDutyClientCredentialsToken(ctx, h.upstreamDoer, config, subdomain, region, h.store.now().UTC())
		switch {
		case errors.Is(err, providerfoundation.ErrPagerDutyExchangeMalformed):
			return pagerDutyRequestAuth{}, servicesInternal("pagerduty client credentials response", err)
		case err != nil:
			return pagerDutyRequestAuth{}, servicesHTTPError(http.StatusBadGateway, "PagerDuty authentication is temporarily unavailable")
		}
		return pagerDutyRequestAuth{header: "Bearer " + tokens.AccessToken}, nil
	}
	return pagerDutyRequestAuth{}, servicesHTTPError(http.StatusConflict, "PagerDuty credential uses an unsupported authentication mode")
}

const pagerDutyReconnect = "PagerDuty OAuth credential must be reconnected"

// oauthRow is one provider_oauth_credentials row's token columns.
type oauthRow struct {
	encrypted string
	version   int64
	bindingID *string
}

func loadOAuthRow(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, orgID, credentialName string, lock bool) (*oauthRow, error) {
	sql := `SELECT token_encrypted, version, binding_id FROM provider_oauth_credentials WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2`
	if lock {
		sql += ` FOR UPDATE`
	}
	var row oauthRow
	err := q.QueryRow(ctx, sql, orgID, credentialName).Scan(&row.encrypted, &row.version, &row.bindingID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &row, nil
}

// storedTokens is _versioned_tokens: the binding must be the expected one and
// the payload a valid OAuthTokens; either failure is the reconnect 409.
func (h *handlers) storedTokens(row *oauthRow, expectedBinding string) (pagerDutyStoredTokens, bool) {
	if row.bindingID == nil || *row.bindingID != expectedBinding {
		return pagerDutyStoredTokens{}, false
	}
	plaintext, err := h.decryptor.Decrypt(secrets.NewValue(row.encrypted))
	if err != nil {
		return pagerDutyStoredTokens{}, false
	}
	tokens, err := parsePagerDutyOAuthTokens(plaintext)
	if err != nil {
		return pagerDutyStoredTokens{}, false
	}
	return tokens, true
}

// validPagerDutyAccessToken is get_valid_access_token plus the route's commit:
// the stored token when it is not about to expire, else a renewed one whose
// rotation is stored under the row lock.
func (h *handlers) validPagerDutyAccessToken(ctx context.Context, orgID, credentialName, bindingID string) (string, *servicesFailure) {
	reconnect := servicesHTTPError(http.StatusConflict, pagerDutyReconnect)
	tx, err := h.store.Pool.Begin(ctx)
	if err != nil {
		return "", servicesInternal("begin pagerduty token renewal", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	now := h.store.now().UTC()
	due := func(tokens pagerDutyStoredTokens) (bool, *servicesFailure) {
		if tokens.ExpiresNaive {
			// A naive expires_at cannot be compared with an aware clock.
			return false, servicesInternal("pagerduty oauth expiry has no offset", errors.New("naive expires_at"))
		}
		return !tokens.ExpiresAt.After(now.Add(pagerDutyRenewalWindow)), nil
	}

	row, err := loadOAuthRow(ctx, tx, orgID, credentialName, false)
	if err != nil {
		return "", servicesInternal("load pagerduty oauth row", err)
	}
	if row == nil {
		return "", reconnect
	}
	tokens, ok := h.storedTokens(row, bindingID)
	if !ok {
		return "", reconnect
	}
	needs, failure := due(tokens)
	if failure != nil {
		return "", failure
	}
	if !needs {
		if err := tx.Commit(ctx); err != nil {
			return "", servicesInternal("commit pagerduty token read", err)
		}
		return tokens.Access, nil
	}

	locked, err := loadOAuthRow(ctx, tx, orgID, credentialName, true)
	if err != nil {
		return "", servicesInternal("lock pagerduty oauth row", err)
	}
	if locked == nil {
		return "", reconnect
	}
	current, ok := h.storedTokens(locked, bindingID)
	if !ok {
		return "", reconnect
	}
	needs, failure = due(current)
	if failure != nil {
		return "", failure
	}
	if !needs {
		if err := tx.Commit(ctx); err != nil {
			return "", servicesInternal("commit pagerduty token read", err)
		}
		return current.Access, nil
	}
	if !current.HasRefresh || locked.bindingID == nil {
		return "", reconnect
	}

	config := h.pagerDuty
	refreshed, err := providerfoundation.RefreshPagerDutyOAuthTokens(ctx, h.upstreamDoer, config, current.Refresh, h.store.now().UTC())
	switch {
	case errors.Is(err, providerfoundation.ErrPagerDutyExchangeMalformed):
		return "", servicesInternal("pagerduty refresh response", err)
	case err != nil:
		return "", servicesHTTPError(http.StatusBadGateway, "PagerDuty OAuth renewal is temporarily unavailable")
	}
	// The renewed token keeps the old refresh token and scopes when the
	// answer carries none.
	refresh := refreshed.RefreshToken
	if refresh == "" {
		refresh = current.Refresh
	}
	scopes := refreshed.GrantedScopes
	if len(scopes) == 0 {
		scopes = current.Scopes
	}
	scopes = append([]string(nil), scopes...)
	sort.Strings(scopes)
	payload, err := storedPagerDutyTokenJSON(refreshed.AccessToken, refresh, refreshed.ExpiresAt, scopes)
	if err != nil {
		return "", servicesInternal("encode pagerduty tokens", err)
	}
	sealed, err := h.decryptor.Encrypt(payload)
	if err != nil {
		return "", servicesInternal("encrypt pagerduty tokens", err)
	}
	scopeText, err := pyjson.Dumps(stringValues(scopes))
	if err != nil {
		return "", servicesInternal("encode pagerduty scopes", err)
	}
	var rotated int64
	err = tx.QueryRow(ctx, `UPDATE provider_oauth_credentials SET token_encrypted = $5, version = version + 1, expires_at = $6, granted_scopes = $7::json, has_refresh_token = $8, updated_at = $9
WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2 AND version = $3 AND binding_id = $4 RETURNING version`,
		orgID, credentialName, locked.version, *locked.bindingID, sealed.Reveal(), refreshed.ExpiresAt, scopeText, refresh != "", now).Scan(&rotated)
	if errors.Is(err, pgx.ErrNoRows) {
		// A rotation conflict: use whatever token is stored now.
		latest, loadErr := loadOAuthRow(ctx, tx, orgID, credentialName, false)
		if loadErr != nil {
			return "", servicesInternal("reload pagerduty oauth row", loadErr)
		}
		if latest == nil {
			return "", reconnect
		}
		latestTokens, ok := h.storedTokens(latest, bindingID)
		if !ok {
			return "", reconnect
		}
		if err := tx.Commit(ctx); err != nil {
			return "", servicesInternal("commit pagerduty token read", err)
		}
		return latestTokens.Access, nil
	}
	if err != nil {
		return "", servicesInternal("rotate pagerduty oauth tokens", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", servicesInternal("commit pagerduty token rotation", err)
	}
	return refreshed.AccessToken, nil
}

// pagerDutyService is one validated service of a page.
type pagerDutyService struct {
	id, displayName string
	nameResolved    bool
	status          *string
}

// fetchPagerDutyServices is PagerDutyClient.list_services followed by the
// route's error mapping.
func (h *handlers) fetchPagerDutyServices(ctx context.Context, auth pagerDutyRequestAuth, region string) ([]pagerDutyService, *servicesFailure) {
	base := h.pagerDuty.APIBase(region)
	client := pagerDutyServicesClient(h.upstreamDoer)
	var services []pagerDutyService
	offset := 0
	for {
		query := url.Values{"limit": {"100"}, "offset": {strconv.Itoa(offset)}}
		body, failure := h.pagerDutyGET(ctx, client, base+"/services?"+query.Encode(), auth)
		if failure != nil {
			return nil, failure
		}
		decoded, err := pyjson.Decode(body)
		if err != nil {
			return nil, servicesInternal("pagerduty services response is not JSON", err)
		}
		payload, isObject := decoded.(*pyjson.Object)
		if !isObject {
			return nil, servicesHTTPError(http.StatusBadGateway, "PagerDuty services are temporarily unavailable")
		}
		rawPage, _ := payload.Get("services")
		page, isList := rawPage.([]pyjson.Value)
		if !isList {
			return nil, servicesHTTPError(http.StatusBadGateway, "PagerDuty services are temporarily unavailable")
		}
		rawMore, _ := payload.Get("more")
		more, isBool := rawMore.(bool)
		if !isBool {
			return nil, servicesHTTPError(http.StatusBadGateway, "PagerDuty services are temporarily unavailable")
		}
		for _, item := range page {
			service, err := validatePagerDutyService(item)
			if err != nil {
				return nil, servicesInternal("pagerduty service failed validation", err)
			}
			services = append(services, service)
		}
		if !more {
			return services, nil
		}
		if len(page) == 0 {
			return nil, servicesHTTPError(http.StatusBadGateway, "PagerDuty services are temporarily unavailable")
		}
		offset += len(page)
	}
}

// pagerDutyServicesClient is the instrumented core's client: 30s timeout, no
// redirect following. A supplied doer (a test's transport) is used as is.
func pagerDutyServicesClient(doer providerfoundation.HTTPDoer) providerfoundation.HTTPDoer {
	if doer != nil {
		return doer
	}
	return &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

const (
	pagerDutyMaxAttempts    = 5
	pagerDutyInitialBackoff = time.Second
	pagerDutyMaxBackoff     = 60 * time.Second
)

var pagerDutyRetryable = map[int]bool{429: true, 500: true, 502: true, 503: true, 504: true}

// pagerDutyGET is InstrumentedRESTCore.request for one GET: transparent
// retries with Retry-After aware backoff, then the terminal classification.
func (h *handlers) pagerDutyGET(ctx context.Context, client providerfoundation.HTTPDoer, target string, auth pagerDutyRequestAuth) ([]byte, *servicesFailure) {
	unavailable := servicesHTTPError(http.StatusBadGateway, "PagerDuty services are temporarily unavailable")
	delay := pagerDutyInitialBackoff
	sleep := func(d time.Duration) bool {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for attempt := 0; attempt < pagerDutyMaxAttempts; attempt++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, servicesInternal("build pagerduty request", err)
		}
		request.Header.Set("Accept", "application/vnd.pagerduty+json;version=2")
		request.Header.Set("Authorization", auth.header)
		response, err := client.Do(request)
		if err != nil {
			if !pagerDutyRetryableTransportError(err) {
				return nil, servicesInternal("pagerduty request failed", err)
			}
			if attempt < pagerDutyMaxAttempts-1 {
				if !sleep(delay) {
					return nil, servicesInternal("pagerduty request cancelled", ctx.Err())
				}
				delay = minDuration(delay*2, pagerDutyMaxBackoff)
				continue
			}
			return nil, unavailable
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 8<<20))
		_ = response.Body.Close()
		if readErr != nil {
			// A body cut short is an httpx read error, which nothing catches.
			return nil, servicesInternal("pagerduty response body", readErr)
		}
		status := response.StatusCode
		if status < 300 {
			return body, nil
		}
		if status < 400 {
			return nil, unavailable
		}
		if pagerDutyRetryable[status] && attempt < pagerDutyMaxAttempts-1 {
			wait := delay
			if seconds, ok := resolveRetryAfter(response.Header, h.store.now().UTC()); ok && seconds > 0 {
				wait = time.Duration(seconds * float64(time.Second))
			}
			if !sleep(wait) {
				return nil, servicesInternal("pagerduty request cancelled", ctx.Err())
			}
			delay = minDuration(delay*2, pagerDutyMaxBackoff)
			continue
		}
		return nil, classifyPagerDutyTerminal(status, response.Header, h.store.now().UTC())
	}
	return nil, unavailable
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// pagerDutyRetryableTransportError is httpx.TimeoutException or ConnectError.
func pagerDutyRetryableTransportError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	var opErr *net.OpError
	return errors.As(err, &opErr) && opErr.Op == "dial"
}

// classifyPagerDutyTerminal is _classify_pagerduty_error followed by
// _raise_for_status, then the route's except clauses: 403 is the missing
// scope, 401 and 429 keep their status (429 with its Retry-After), anything
// else is the 502.
func classifyPagerDutyTerminal(status int, header http.Header, now time.Time) *servicesFailure {
	switch status {
	case http.StatusForbidden:
		return servicesHTTPError(http.StatusForbidden, "PagerDuty credential is missing services.read permission")
	case http.StatusUnauthorized:
		return servicesHTTPError(http.StatusUnauthorized, "PagerDuty credential is no longer authorized")
	case http.StatusTooManyRequests:
		failure := servicesHTTPError(http.StatusTooManyRequests, "PagerDuty rate limit exceeded")
		if seconds, ok := resolveRetryAfter(header, now); ok {
			failure.header = http.Header{"Retry-After": {strconv.FormatInt(int64(math.Max(0, math.Ceil(seconds))), 10)}}
		}
		return failure
	}
	return servicesHTTPError(http.StatusBadGateway, "PagerDuty services are temporarily unavailable")
}

// resolveRetryAfter is resolve_retry_after_seconds with PagerDuty's reset
// header (ratelimit-reset): Retry-After as delta seconds or an HTTP date
// (negative results clamped to 0), else the epoch-seconds reset header.
func resolveRetryAfter(header http.Header, now time.Time) (float64, bool) {
	if raw := strings.TrimSpace(header.Get("Retry-After")); raw != "" {
		if seconds, err := strconv.ParseFloat(raw, 64); err == nil && !math.IsNaN(seconds) {
			return math.Max(0, seconds), true
		}
		if when, err := http.ParseTime(raw); err == nil {
			return math.Max(0, when.Sub(now).Seconds()), true
		}
	}
	if raw := header.Get("Ratelimit-Reset"); raw != "" {
		if epoch, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil && !math.IsNaN(epoch) {
			return math.Max(0, epoch-float64(now.UnixNano())/1e9), true
		}
	}
	return 0, false
}
