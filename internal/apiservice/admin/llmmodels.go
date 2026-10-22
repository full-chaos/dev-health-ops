package admin

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

const (
	llmModelListTTL       = time.Hour
	llmModelListMaxBytes  = 4 << 20
	llmModelListMaxPages  = 5
	llmModelListMaxCached = 1024
	llmModelIDMaxLen      = 128
	llmCursorMaxLen       = 512

	llmModelsSourceLive        = "live"
	llmModelsSourceCache       = "cache"
	llmModelsSourceCatalog     = "catalog"
	llmModelsSourceUnavailable = "unavailable"
	llmModelsSourceSaved       = "saved"

	llmModelsReasonNotConfigured    = "not_configured"
	llmModelsReasonAPIKeyRequired   = "api_key_required"
	llmModelsReasonBaseURLRequired  = "base_url_required"
	llmModelsReasonInvalidBaseURL   = "invalid_base_url"
	llmModelsReasonProviderRejected = "provider_rejected"
	llmModelsReasonProviderDown     = "provider_unavailable"
	llmModelsReasonInvalidResponse  = "invalid_response"

	anthropicVersionHeader = "2023-06-01"
)

var llmModelIDSecretShape = regexp.MustCompile(`sk-[A-Za-z0-9_-]{16,}|AIza[0-9A-Za-z_-]{20,}|(?i:bearer\s)`)

var llmModelIDShape = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+-]*$`)

// llmCursorShape is the same allowlist plus the base64 characters real page
// tokens use; a cursor outside it is never copied into a request.
var llmCursorShape = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+=-]*$`)

type llmModelListCache struct {
	mu      sync.Mutex
	entries map[string]llmModelListEntry
}

type llmModelListEntry struct {
	orgID     string
	models    []string
	fetchedAt time.Time
}

func newLLMModelListCache() *llmModelListCache {
	return &llmModelListCache{entries: map[string]llmModelListEntry{}}
}

// llmModelListKeySecret keys the cache-key digest so the digest of a saved
// key cannot be recomputed outside this process; it is never persisted.
var llmModelListKeySecret = func() []byte {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		panic("llm model list: no randomness: " + err.Error())
	}
	return secret
}()

func llmModelListKey(orgID, provider, baseURL, apiKey string) string {
	mac := hmac.New(sha256.New, llmModelListKeySecret)
	mac.Write([]byte(baseURL + "\x00" + apiKey))
	return orgID + "|" + provider + "|" + hex.EncodeToString(mac.Sum(nil)[:8])
}

func (c *llmModelListCache) get(key string, now time.Time) (llmModelListEntry, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[key]
	if !ok || now.Sub(entry.fetchedAt) >= llmModelListTTL {
		delete(c.entries, key)
		return llmModelListEntry{}, false
	}
	return entry, true
}

func (c *llmModelListCache) put(key string, entry llmModelListEntry, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= llmModelListMaxCached {
		for k, e := range c.entries {
			if now.Sub(e.fetchedAt) >= llmModelListTTL {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= llmModelListMaxCached {
			c.entries = map[string]llmModelListEntry{}
		}
	}
	c.entries[key] = entry
}

func (c *llmModelListCache) invalidateOrg(orgID string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for k, e := range c.entries {
		if e.orgID == orgID {
			delete(c.entries, k)
		}
	}
}

type llmModelListRequest struct {
	method  string
	url     string
	headers map[string]string
}

// llmModelListFirstRequest builds the provider's list-models request, or a
// reason code when the saved configuration cannot make one.
func llmModelListFirstRequest(provider, baseURL, apiKey string) (llmModelListRequest, string) {
	if llmorgsettings.CarriesKey(llmorgsettings.KeyLeakForms(apiKey), baseURL) {
		return llmModelListRequest{}, llmModelsReasonInvalidBaseURL
	}
	base := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	switch provider {
	case "anthropic":
		if base == "" {
			base = "https://api.anthropic.com"
		}
		return llmModelListRequest{url: base + "/v1/models?limit=1000", headers: map[string]string{
			"x-api-key": apiKey, "anthropic-version": anthropicVersionHeader,
		}}, ""
	case "gemini":
		if base == "" {
			base = "https://generativelanguage.googleapis.com/v1beta"
		}
		return llmModelListRequest{url: base + "/models?pageSize=1000", headers: map[string]string{
			"x-goog-api-key": apiKey,
		}}, ""
	case "openai":
		if base == "" {
			base = "https://api.openai.com/v1"
		}
	default:
		if base == "" {
			return llmModelListRequest{}, llmModelsReasonBaseURLRequired
		}
	}
	headers := map[string]string{}
	if apiKey != "" {
		headers["Authorization"] = "Bearer " + apiKey
	}
	return llmModelListRequest{url: base + "/models", headers: headers}, ""
}

type llmModelPage struct {
	ids  []string
	next string
}

func parseLLMModelPage(provider string, body []byte) (llmModelPage, bool) {
	var page llmModelPage
	switch provider {
	case "gemini":
		var parsed struct {
			Models *[]struct {
				Name    string   `json:"name"`
				Methods []string `json:"supportedGenerationMethods"`
			} `json:"models"`
			NextPageToken string `json:"nextPageToken"`
		}
		if json.Unmarshal(body, &parsed) != nil || parsed.Models == nil {
			return page, false
		}
		for _, m := range *parsed.Models {
			generates := false
			for _, method := range m.Methods {
				generates = generates || method == "generateContent"
			}
			if id := strings.TrimPrefix(m.Name, "models/"); generates && id != "" {
				page.ids = append(page.ids, id)
			}
		}
		page.next = parsed.NextPageToken
	case "anthropic":
		var parsed struct {
			Data *[]struct {
				ID string `json:"id"`
			} `json:"data"`
			HasMore bool   `json:"has_more"`
			LastID  string `json:"last_id"`
		}
		if json.Unmarshal(body, &parsed) != nil || parsed.Data == nil {
			return page, false
		}
		for _, m := range *parsed.Data {
			if m.ID != "" {
				page.ids = append(page.ids, m.ID)
			}
		}
		if parsed.HasMore {
			page.next = parsed.LastID
		}
	default:
		var parsed struct {
			Data *[]struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if json.Unmarshal(body, &parsed) != nil || parsed.Data == nil {
			return page, false
		}
		for _, m := range *parsed.Data {
			if m.ID != "" {
				page.ids = append(page.ids, m.ID)
			}
		}
	}
	return page, true
}

func llmModelListNextURL(provider, firstURL, cursor string) string {
	parsed, err := url.Parse(firstURL)
	if err != nil {
		return ""
	}
	query := parsed.Query()
	if provider == "gemini" {
		query.Set("pageToken", cursor)
	} else {
		query.Set("after_id", cursor)
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// listProviderModels calls the provider's list-models API with the saved key
// and returns model ids, or a reason code. The key only rides the request
// headers; no error text from the provider is returned.
func listProviderModels(ctx context.Context, client providerfoundation.HTTPDoer, provider, baseURL, apiKey string) ([]string, string) {
	first, reason := llmModelListFirstRequest(provider, baseURL, apiKey)
	if reason != "" {
		return nil, reason
	}
	doer := httpguard.NoRedirectsDoer(client)
	forms := llmorgsettings.KeyLeakForms(apiKey)
	seen := map[string]struct{}{}
	var ids []string
	var joined strings.Builder
	target := first.url
	for pageNumber := 0; pageNumber < llmModelListMaxPages && target != ""; pageNumber++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			return nil, llmModelsReasonProviderDown
		}
		for name, value := range first.headers {
			req.Header.Set(name, value)
		}
		resp, err := doer.Do(req)
		if err != nil {
			return nil, llmModelsReasonProviderDown
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, llmModelListMaxBytes))
		_ = resp.Body.Close()
		if readErr != nil {
			return nil, llmModelsReasonProviderDown
		}
		switch {
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			return nil, llmModelsReasonProviderRejected
		case resp.StatusCode < 200 || resp.StatusCode >= 300:
			return nil, llmModelsReasonProviderDown
		}
		page, ok := parseLLMModelPage(provider, body)
		if !ok {
			return nil, llmModelsReasonInvalidResponse
		}
		for _, id := range page.ids {
			if llmorgsettings.CarriesKey(forms, id) {
				continue
			}
			joined.WriteString(id)
			if len(id) > llmModelIDMaxLen || !llmModelIDShape.MatchString(id) || llmModelIDSecretShape.MatchString(id) {
				continue
			}
			if _, dup := seen[id]; !dup {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}
		if llmorgsettings.CarriesKey(forms, joined.String()) {
			return nil, llmModelsReasonInvalidResponse
		}
		target = ""
		if page.next != "" && len(page.next) <= llmCursorMaxLen && llmCursorShape.MatchString(page.next) && !llmorgsettings.CarriesKey(forms, page.next) {
			target = llmModelListNextURL(provider, first.url, page.next)
		}
	}
	if len(ids) == 0 {
		return nil, llmModelsReasonInvalidResponse
	}
	if provider != "anthropic" {
		sort.Strings(ids)
	}
	return ids, ""
}

// llmModelItem is one entry of the model list: where the id came from and
// whether the provider's own list confirmed it.
type llmModelItem struct {
	id       string
	source   string
	verified bool
}

func llmModelsResponse(provider string, models []llmModelItem, source, reason string, fetchedAt *time.Time) *pyjson.Object {
	out := pyjson.NewObject()
	if provider == "" {
		out.Set("provider", nil)
	} else {
		out.Set("provider", provider)
	}
	list := make([]pyjson.Value, 0, len(models))
	for _, model := range models {
		item := pyjson.NewObject()
		item.Set("id", model.id)
		item.Set("source", model.source)
		item.Set("verified", model.verified)
		list = append(list, item)
	}
	out.Set("models", list)
	out.Set("source", source)
	if reason == "" {
		out.Set("reason", nil)
	} else {
		out.Set("reason", reason)
	}
	if fetchedAt == nil {
		out.Set("fetched_at", nil)
	} else {
		out.Set("fetched_at", fetchedAt.UTC().Format(time.RFC3339))
	}
	return out
}

// mergeLLMModels is the list a select shows: the provider's own ids first
// (verified), then the platform catalog and the saved model, each id once; the
// verified ids and the rest are each in alphabetical order.
func mergeLLMModels(live, catalog []string, saved string) []llmModelItem {
	seen := map[string]struct{}{}
	var items []llmModelItem
	add := func(id, source string, verified bool) {
		if _, dup := seen[id]; dup {
			return
		}
		seen[id] = struct{}{}
		items = append(items, llmModelItem{id: id, source: source, verified: verified})
	}
	for _, id := range live {
		add(id, llmModelsSourceLive, true)
	}
	for _, id := range catalog {
		add(id, llmModelsSourceCatalog, false)
	}
	if saved != "" {
		add(saved, llmModelsSourceSaved, false)
	}
	sort.SliceStable(items, func(a, b int) bool {
		if items[a].verified != items[b].verified {
			return items[a].verified
		}
		return items[a].id < items[b].id
	})
	return items
}

// savedModelListable is whether the saved model may be listed: the same id
// filters the provider's ids pass, and never one that carries the saved key.
func savedModelListable(model, apiKey string) bool {
	return model != "" && len(model) <= llmModelIDMaxLen && llmModelIDShape.MatchString(model) &&
		!llmModelIDSecretShape.MatchString(model) && !carriesKey(keyLeakForms(apiKey), model)
}

// getLLMSettingsModels serves a provider's model list: the platform catalog,
// merged with the provider's own list when the saved key belongs to that
// provider. The request carries no key: the saved, decrypted key is the only
// credential used, and only for the saved provider.
func (h *handlers) getLLMSettingsModels(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	orgID, ok := adminOrgID(w, policy.UserFrom(ctx))
	if !ok {
		return
	}
	if !h.requireBYOLLMAccess(ctx, w, orgID, false) {
		return
	}
	requested := ""
	if values, present := r.URL.Query()["provider"]; present {
		requested = pythonparity.Lower(pythonparity.Strip(values[len(values)-1]))
	}
	if requested != "" && !slices.Contains(llmbudget.CatalogProviders(), requested) {
		llmDetail(http.StatusBadRequest, w,
			"error", "unknown_provider",
			"feature", byoLLMFeature,
			"message", "provider must be one of "+strings.Join(llmbudget.CatalogProviders(), ", "))
		return
	}
	cfg, err := h.loadReadinessBYOConfig(ctx, orgID)
	if err != nil {
		h.internalError(ctx, w, "read byo llm config", err)
		return
	}
	out, err := h.modelList(ctx, orgID, cfg, requested)
	if err != nil {
		h.internalError(ctx, w, "validate byo base url", err)
		return
	}
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// modelList answers the model list for a loaded BYO config and an optional
// requested provider (blank = the saved one). The saved key and base url go to
// the saved provider only; the base url is vetted against the key before any
// resolution.
func (h *handlers) modelList(ctx context.Context, orgID string, cfg readinessBYOConfig, requested string) (*pyjson.Object, error) {
	saved := cfg.provider
	if saved == "" && cfg.apiKey.Configured() {
		saved = "openai"
	}
	provider := saved
	if requested != "" {
		provider = requested
	}
	apiKey := cfg.apiKey.Reveal()
	if provider == "" {
		return llmModelsResponse("", nil, llmModelsSourceUnavailable, llmModelsReasonNotConfigured, nil), nil
	}
	catalog := llmbudget.CatalogModels(provider)
	savedModel := ""
	if provider == saved && savedModelListable(cfg.model, apiKey) {
		savedModel = cfg.model
	}
	respond := func(live []string, source, reason string, fetchedAt *time.Time) *pyjson.Object {
		models := mergeLLMModels(live, catalog, savedModel)
		if len(models) == 0 {
			return llmModelsResponse(provider, nil, llmModelsSourceUnavailable, reason, nil)
		}
		return llmModelsResponse(provider, models, source, reason, fetchedAt)
	}
	if provider != saved {
		return respond(nil, llmModelsSourceCatalog, "", nil), nil
	}
	if !llmorgsettings.CredentialsComplete(provider, apiKey) {
		reason := ""
		if len(catalog) == 0 && savedModel == "" {
			reason = llmModelsReasonAPIKeyRequired
		}
		return respond(nil, llmModelsSourceCatalog, reason, nil), nil
	}
	valid, _, _, err := h.checkBaseURL(ctx, cfg.baseURL, apiKey)
	if err != nil {
		return nil, err
	}
	if !valid {
		return respond(nil, llmModelsSourceCatalog, llmModelsReasonInvalidBaseURL, nil), nil
	}
	now := h.store.now().UTC()
	key := llmModelListKey(orgID, provider, cfg.baseURL, apiKey)
	if entry, hit := h.modelCache.get(key, now); hit {
		return respond(entry.models, llmModelsSourceCache, "", &entry.fetchedAt), nil
	}
	client := newOpenAICompatibleReadinessProber(h.upstreamDoer).client
	models, reason := listProviderModels(ctx, client, provider, cfg.baseURL, apiKey)
	if reason != "" {
		return respond(nil, llmModelsSourceCatalog, reason, nil), nil
	}
	h.modelCache.put(key, llmModelListEntry{orgID: orgID, models: models, fetchedAt: now}, now)
	return respond(models, llmModelsSourceLive, "", &now), nil
}

// checkBaseURL is the one door to the base url validator for every reader in
// this package: a url holding any of the keys is refused before a lookup.
func (h *handlers) checkBaseURL(ctx context.Context, baseURL string, keys ...string) (valid bool, reason string, keyHit bool, err error) {
	validate := llmorgsettings.BaseURLValidator(llmorgsettings.ValidateBaseURLChecked)
	if h.validateURL != nil {
		validate = h.validateURL
	}
	return llmorgsettings.CheckBaseURL(ctx, baseURL, keys, validate)
}

func (h *handlers) storedLLMKey(ctx context.Context, orgID string) (string, error) {
	if h.storedKey != nil {
		return h.storedKey(ctx, orgID)
	}
	cfg, err := h.loadReadinessBYOConfig(ctx, orgID)
	if err != nil {
		return "", err
	}
	return cfg.apiKey.Reveal(), nil
}
