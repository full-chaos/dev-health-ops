package admin

import (
	"context"
	"errors"
	"net/http"

	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// llmLeakScopes are the default row ("") and every purpose override, categorize included.
func llmLeakScopes() []llmorgsettings.Purpose {
	return append(append([]llmorgsettings.Purpose{""}, llmorgsettings.Purposes...), llmorgsettings.PurposeCategorize)
}

// llmSettingsSaveLockPrefix + org id is the advisory lock key text every LLM
// settings save of the org takes first, so its key checks read what each
// earlier save of the org committed (CHAOS-14247).
const llmSettingsSaveLockPrefix = "llm-settings-save:"

// llmSettingsSaveLockTimeout bounds the wait on another save of the org: 10x
// the measured p99 of one save (under 40 ms on the venue), floor 2 s.
const llmSettingsSaveLockTimeout = "2000ms"

const (
	llmSettingsSaveLockTimeoutReadSQL = `SELECT current_setting('lock_timeout')`
	llmSettingsSaveLockTimeoutSQL     = `SET LOCAL lock_timeout = '` + llmSettingsSaveLockTimeout + `'`
	llmSettingsSaveLockSQL            = `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`
	// The bound is for the save lock only: later row and budget lock waits get
	// the session's own value back (TO DEFAULT would drop a session SET).
	llmSettingsSaveLockTimeoutRestoreSQL = `SELECT set_config('lock_timeout', $1, true)`
)

const pgLockNotAvailable = "55P03"

var llmSettingsSaveLockTimeouts = func() metric.Int64Counter {
	meter := otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/admin")
	counter, err := meter.Int64Counter("devhealth_api_llm_settings_save_lock_timeout_total",
		metric.WithDescription("LLM settings saves refused with 409 setting_changed because another save of the org held the save lock past the lock timeout, by scope."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_api_llm_settings_save_lock_timeout_total")
	}
	return counter
}()

// lockLLMSettingsSave takes the org's save lock as the first statements of a
// save transaction. A wait past the timeout is the 409 setting_changed.
func (h *handlers) lockLLMSettingsSave(ctx context.Context, tx pgExecer, orgID string, scope llmorgsettings.Purpose) (*llmRefusal, error) {
	var previous string
	if err := tx.QueryRow(ctx, llmSettingsSaveLockTimeoutReadSQL).Scan(&previous); err != nil {
		return nil, &llmStepError{"read lock timeout", err}
	}
	if _, err := tx.Exec(ctx, llmSettingsSaveLockTimeoutSQL); err != nil {
		return nil, &llmStepError{"bound llm settings save lock", err}
	}
	if _, err := tx.Exec(ctx, llmSettingsSaveLockSQL, llmSettingsSaveLockPrefix+orgID); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == pgLockNotAvailable {
			name := scopeLabel(scope)
			llmSettingsSaveLockTimeouts.Add(ctx, 1, metric.WithAttributes(attribute.String("scope", name)))
			h.logger.WarnContext(ctx, "admin: llm settings save lock timed out; another save of the org holds it",
				"org_id", orgID, "scope", name)
			return newLLMRefusal(http.StatusConflict,
				"error", "setting_changed",
				"message", "The setting changed while saving. Reload and try again."), nil
		}
		return nil, &llmStepError{"lock llm settings save", err}
	}
	if _, err := tx.Exec(ctx, llmSettingsSaveLockTimeoutRestoreSQL, previous); err != nil {
		return nil, &llmStepError{"restore lock timeout", err}
	}
	return nil, nil
}

func scopeLabel(scope llmorgsettings.Purpose) string {
	if scope == "" {
		return "default"
	}
	return string(scope)
}

// storedLeakKeys is every stored api key of the org, the default row's and each
// purpose override's, for the save-time base_url check. An undecryptable key
// reads as "".
func (h *handlers) storedLeakKeys(ctx context.Context, orgID string) ([]string, error) {
	return h.storedLeakKeysOn(ctx, h.store.Pool, orgID)
}

// storedLeakKeysOn is storedLeakKeys over db (the pool or the save transaction).
func (h *handlers) storedLeakKeysOn(ctx context.Context, db pgExecer, orgID string) ([]string, error) {
	stored, err := readableStoredKey(h.storedDefaultKeyOn(ctx, db, orgID))
	if err != nil {
		return nil, &llmStepError{"read stored llm key", err}
	}
	keys := []string{stored}
	for _, purpose := range llmLeakScopes()[1:] {
		key, err := readableStoredKey(h.storedPurposeAPIKeyOn(ctx, db, orgID, purpose))
		if err != nil {
			return nil, &llmStepError{"read stored llm purpose key", err}
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// readerLeakKeys are the keys a default-row reader checks its stored base_url
// against: the keys it holds and, for a non-empty url, every stored key of the
// org (storedLeakKeys), so a url that carries another scope's key is refused
// before any lookup.
func (h *handlers) readerLeakKeys(ctx context.Context, orgID, baseURL string, held ...string) ([]string, error) {
	keys := append([]string{}, held...)
	if baseURL == "" {
		return keys, nil
	}
	stored, err := h.storedLeakKeys(ctx, orgID)
	if err != nil {
		return nil, err
	}
	return append(keys, stored...), nil
}

// storedDefaultKeyOn is storedLLMKey's key over db: the stored default key
// whatever its binding.
func (h *handlers) storedDefaultKeyOn(ctx context.Context, db pgExecer, orgID string) (string, error) {
	if h.storedKey != nil {
		return h.storedKey(ctx, orgID)
	}
	key, err := h.scopeKey(ctx, db, orgID, "", false, true)
	return key.ForChecks(), err
}

func (h *handlers) storedScopeBaseURLOf(ctx context.Context, db pgExecer, orgID string, purpose llmorgsettings.Purpose) (string, error) {
	if h.storedScopeBaseURL != nil {
		return h.storedScopeBaseURL(ctx, orgID, purpose)
	}
	scoped, err := h.scopeKey(ctx, db, orgID, purpose, false, false)
	return scoped.BaseURL, err
}

// otherBaseURLCarriesKey is whether a stored base_url of any scope but own
// carries apiKey. The own scope is skipped: the write replaces its base_url
// with the body's, which is checked against apiKey on its own. The default
// row counts for every purpose write: a purpose with no base_url inherits it.
func (h *handlers) otherBaseURLCarriesKey(ctx context.Context, db pgExecer, orgID string, own llmorgsettings.Purpose, apiKey *string) (bool, error) {
	if apiKey == nil || *apiKey == "" {
		return false, nil
	}
	for _, scope := range llmLeakScopes() {
		if scope == own {
			continue
		}
		baseURL, err := h.storedScopeBaseURLOf(ctx, db, orgID, scope)
		if err != nil {
			return false, &llmStepError{"read stored llm base url", err}
		}
		if llmorgsettings.BaseURLCarriesKey(baseURL, *apiKey) {
			return true, nil
		}
	}
	return false, nil
}

// saveKeyRefusal is the save-time key check of one scope over db: the body's
// base_url carries a stored key or the body's key, or another scope's stored
// base_url carries the body's key. resolve also runs the SSRF validator after
// it; the pass inside the save lock does not, so no DNS lookup holds the lock.
func (h *handlers) saveKeyRefusal(ctx context.Context, db pgExecer, orgID string, own llmorgsettings.Purpose, apiKey, baseURL *string, resolve bool) (*llmRefusal, error) {
	baseURLText := ""
	if baseURL != nil {
		baseURLText = *baseURL
	}
	var keys []string
	if apiKey != nil {
		keys = append(keys, *apiKey)
	}
	if baseURLText != "" {
		stored, err := h.storedLeakKeysOn(ctx, db, orgID)
		if err != nil {
			return nil, err
		}
		keys = append(keys, stored...)
	}
	carried, err := h.otherBaseURLCarriesKey(ctx, db, orgID, own, apiKey)
	if err != nil {
		return nil, err
	}
	if carried {
		return baseURLKeyRefusal(), nil
	}
	validate := llmorgsettings.BaseURLValidator(func(context.Context, string) (bool, string, error) { return true, "", nil })
	if resolve {
		validate = h.baseURLValidator()
	}
	valid, reason, keyHit, splitErr := llmorgsettings.CheckBaseURL(ctx, baseURLText, keys, validate)
	if keyHit {
		return baseURLKeyRefusal(), nil
	}
	if splitErr != nil {
		// urlsplit's ValueError escapes validate_llm_base_url: an unhandled 500.
		return nil, &llmStepError{"validate base url", splitErr}
	}
	if !valid {
		if reason == "" {
			reason = llmorgsettings.BaseURLKeyRefusal
		}
		return newLLMRefusal(http.StatusBadRequest, "error", "invalid_base_url", "feature", byoLLMFeature, "message", reason), nil
	}
	return nil, nil
}

func baseURLKeyRefusal() *llmRefusal {
	return newLLMRefusal(http.StatusBadRequest, "error", "invalid_base_url", "feature", byoLLMFeature, "message", llmorgsettings.BaseURLKeyRefusal)
}
