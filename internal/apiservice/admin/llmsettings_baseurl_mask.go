package admin

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

const baseURLMaskedValue = "***"

// llmKeyLikeQueryParams are the base_url query parameter names whose value is
// masked on GET whatever it holds (lower-cased).
var llmKeyLikeQueryParams = []string{"api_key", "apikey", "key", "token", "access_token", "secret", "auth", "password"}

func keyLikeParam(pair string) bool {
	name, _, _ := strings.Cut(pair, "=")
	if decoded, err := url.QueryUnescape(name); err == nil {
		name = decoded
	}
	name = strings.ToLower(name)
	for _, known := range llmKeyLikeQueryParams {
		if name == known {
			return true
		}
	}
	return false
}

func anyKeyInText(text string, keys []string) bool {
	for _, key := range keys {
		if llmorgsettings.BaseURLCarriesKey(text, key) {
			return true
		}
	}
	return false
}

// maskBaseURL is a stored base_url as the admin GET reports it: the userinfo
// and every key-like or key-carrying query value are masked, and a url that
// still carries a stored key (host, path, fragment) or cannot be parsed while
// carrying one reads "********". A clean url is returned byte-identical.
func maskBaseURL(raw string, keys []string) string {
	if raw == "" {
		return raw
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		if anyKeyInText(raw, keys) {
			return "********"
		}
		return raw
	}
	changed := false
	if parsed.User != nil {
		parsed.User = url.User("masked")
		changed = true
	}
	if parsed.RawQuery != "" {
		pairs := strings.Split(parsed.RawQuery, "&")
		for i, pair := range pairs {
			name, value, hasValue := strings.Cut(pair, "=")
			if hasValue && value != "" && (keyLikeParam(pair) || anyKeyInText(pair, keys)) {
				pairs[i] = name + "=" + baseURLMaskedValue
				changed = true
			}
		}
		parsed.RawQuery = strings.Join(pairs, "&")
	}
	out := raw
	if changed {
		out = parsed.String()
	}
	if anyKeyInText(out, keys) {
		return "********"
	}
	return out
}

// llmMaskKeys are the stored api keys of every scope, the default row's and
// each override's; an unreadable key reads as absent (the response already
// names it).
func (h *handlers) llmMaskKeys(ctx context.Context, db pgExecer, orgID string) ([]string, error) {
	var keys []string
	for _, scope := range llmLeakScopes() {
		field := "api_key"
		if scope != "" {
			field = llmorgsettings.PurposeKey(scope, field)
		}
		value, err := h.llmSettingValue(ctx, db, orgID, scope, field)
		var unreadable *undecryptableSettingsError
		if errors.As(err, &unreadable) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if value != nil && *value != "" {
			keys = append(keys, *value)
		}
	}
	return keys, nil
}

// setMaskedBaseURL sets base_url on out from a stored value.
func setMaskedBaseURL(out *pyjson.Object, stored string, keys []string) {
	out.Set("base_url", maskBaseURL(stored, keys))
}

// carriesBaseURLMask is whether a PUT base_url holds the text a masked GET
// shows: the whole "********", the masked userinfo, or a "***" query value.
func carriesBaseURLMask(text string) bool {
	if text == "********" {
		return true
	}
	parsed, err := url.Parse(text)
	if err != nil {
		return false
	}
	if parsed.User != nil && parsed.User.Username() == "masked" {
		return true
	}
	for _, pair := range strings.Split(parsed.RawQuery, "&") {
		if _, value, ok := strings.Cut(pair, "="); ok && value == baseURLMaskedValue {
			return true
		}
	}
	return false
}

// resolveBaseURL maps a PUT base_url that is the very text the GET showed for
// the stored value of scope back to that stored value (a form saved without
// editing the field). A base_url that carries the mask text but is not the
// masked form of the current stored value is refused: a masked value is never
// stored. db nil reads through the handler's readers; a tx reads its own rows.
func (h *handlers) resolveBaseURL(ctx context.Context, db pgExecer, orgID string, scope llmorgsettings.Purpose, sent *string) (resolved *string, refused bool, err error) {
	if sent == nil || *sent == "" || !carriesBaseURLMask(*sent) {
		return sent, false, nil
	}
	stored, keys, err := h.storedBaseURLAndKeys(ctx, db, orgID, scope)
	if err != nil {
		return nil, false, err
	}
	if stored != "" {
		if masked := maskBaseURL(stored, keys); masked != stored && masked == *sent {
			return &stored, false, nil
		}
	}
	return nil, true, nil
}

func (h *handlers) storedBaseURLAndKeys(ctx context.Context, db pgExecer, orgID string, scope llmorgsettings.Purpose) (string, []string, error) {
	if db == nil {
		stored, err := h.storedScopeBaseURLOf(ctx, orgID, scope)
		if err != nil {
			return "", nil, &llmStepError{"read stored llm base url", err}
		}
		keys, err := h.storedLeakKeys(ctx, orgID)
		return stored, keys, err
	}
	field := "base_url"
	if scope != "" {
		field = llmorgsettings.PurposeKey(scope, field)
	}
	value, err := h.llmSettingValue(ctx, db, orgID, scope, field)
	var unreadable *undecryptableSettingsError
	if err != nil && !errors.As(err, &unreadable) {
		return "", nil, &llmStepError{"read stored llm base url", err}
	}
	keys, err := h.llmMaskKeys(ctx, db, orgID)
	if err != nil {
		return "", nil, err
	}
	if value == nil {
		return "", keys, nil
	}
	return *value, keys, nil
}

func sameBaseURL(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func maskedBaseURLRefusal() *llmRefusal {
	return newLLMRefusal(http.StatusBadRequest, "error", "invalid_base_url", "feature", byoLLMFeature, "message", "Invalid LLM base_url")
}
