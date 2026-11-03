package admin

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// CHAOS-12172: a stored, encrypted BYO setting that the configured
// SETTINGS_ENCRYPTION_KEY cannot open (it was sealed under a key since
// replaced, or restored from another deployment) is not a transient failure:
// a retry never helps, only replacing or removing the setting does. Its reads
// answer their own 409 instead of the generic 500, naming the settings and
// never a value, and a write that brings a new key does not need the old one.

const (
	llmUndecryptableCode    = "llm_settings_undecryptable"
	llmUndecryptableMessage = "The saved key can no longer be read. Replace or remove it."
)

// undecryptableSettingsError names the stored settings (their keys, never a
// value) that the configured key cannot decrypt.
type undecryptableSettingsError struct{ keys []string }

func (e *undecryptableSettingsError) Error() string {
	return "stored llm settings cannot be decrypted: " + strings.Join(e.keys, ", ")
}

// undecryptable is the error settingValue returns for a row whose ciphertext
// the configured key cannot open; any other decrypt error is returned as is.
func undecryptable(key string, err error) error {
	if errors.Is(err, providerfoundation.ErrCredentialInvalid) {
		return &undecryptableSettingsError{keys: []string{key}}
	}
	return err
}

// collectUndecryptable adds the settings an undecryptable error names to
// into and reports whether err was one, so a read can name every unreadable
// setting instead of stopping at the first.
func collectUndecryptable(err error, into *[]string) bool {
	var u *undecryptableSettingsError
	if !errors.As(err, &u) {
		return false
	}
	*into = append(*into, u.keys...)
	return true
}

// undecryptableOrNil is the error for the collected keys, nil when none.
func undecryptableOrNil(keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	return &undecryptableSettingsError{keys: keys}
}

// readableStoredKey is a stored key as the base-URL key check reads it: a key
// that cannot be decrypted cannot be compared and is left out, so a write
// that replaces it (or only touches an override) still goes through. A write
// that keeps it fails later, on its own response read, with the 409.
func readableStoredKey(stored string, err error) (string, error) {
	var u *undecryptableSettingsError
	if errors.As(err, &u) {
		return "", nil
	}
	return stored, err
}

// purposeOfKey is the purpose an override's settings key belongs to.
func purposeOfKey(key string) (string, bool) {
	rest, ok := strings.CutPrefix(key, "purpose:")
	if !ok {
		return "", false
	}
	purpose, _, ok := strings.Cut(rest, ":")
	if !ok || purpose == "" {
		return "", false
	}
	return purpose, true
}

// writeUndecryptable answers an undecryptable stored setting with its own
// 409 and reports whether it did. The detail and the log line carry the
// setting keys and their purposes only.
func (h *handlers) writeUndecryptable(ctx context.Context, w http.ResponseWriter, what string, err error) bool {
	var u *undecryptableSettingsError
	if !errors.As(err, &u) {
		return false
	}
	keys := slices.Clone(u.keys)
	slices.Sort(keys)
	keys = slices.Compact(keys)
	settings := make([]pyjson.Value, 0, len(keys))
	purposes := []pyjson.Value{}
	seen := map[string]bool{}
	for _, key := range keys {
		settings = append(settings, key)
		if purpose, ok := purposeOfKey(key); ok && !seen[purpose] {
			seen[purpose] = true
			purposes = append(purposes, purpose)
		}
	}
	h.logger.WarnContext(ctx, "admin: "+what+": stored llm settings cannot be decrypted with the configured key", "settings", keys)
	detail := pyjson.NewObject()
	detail.Set("error", llmUndecryptableCode)
	detail.Set("feature", byoLLMFeature)
	detail.Set("message", llmUndecryptableMessage)
	detail.Set("settings", settings)
	detail.Set("purposes", purposes)
	policy.WriteDetail(w, http.StatusConflict, detail, nil)
	return true
}

// byoReadFailed is internalError for a BYO route: the 409 for undecryptable
// stored settings, the generic 500 for everything else.
func (h *handlers) byoReadFailed(ctx context.Context, w http.ResponseWriter, what string, err error) {
	if h.writeUndecryptable(ctx, w, what, err) {
		return
	}
	h.internalError(ctx, w, what, err)
}
