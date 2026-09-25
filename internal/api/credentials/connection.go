package credentials

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// lookupOutcome is CredentialLookupOutcome: why a by-id lookup came back
// unusable. NOT_FOUND alone is a 404; the other two are reason-coded 422s.
type lookupOutcome int

const (
	outcomeOK lookupOutcome = iota
	outcomeNotFound
	outcomeNoPayload
	outcomeDecryptFailed
)

// storedCredential is what a lookup returns: the row's identity and its
// decrypted payload.
type storedCredential struct {
	provider, name string
	creds          *pyjson.Object
}

// decryptPayload is decrypt_value + json.loads. ok is false where Python's
// ValueError / JSONDecodeError is caught (a wrong key, a malformed token, a
// payload that is not JSON); a missing key is Python's RuntimeError, which
// is not caught, so it is returned as an error. A payload that is valid JSON
// but not an object is readable (ok) with nil creds.
func (h handlers) decryptPayload(ciphertext string) (creds *pyjson.Object, ok bool, err error) {
	decoded, ok, err := h.decryptValue(ciphertext)
	if err != nil || !ok {
		return nil, ok, err
	}
	// Valid JSON that is not an object decrypted and parsed: Python's
	// json.loads gives it back with the OK outcome, and never counts it as
	// a decrypt failure. It carries no credentials here (nil), which the
	// route answers as "Credential not found", as Python does for a falsy
	// value such as [] (`if not creds`).
	object, _ := decoded.(*pyjson.Object)
	return object, true, nil
}

// decryptValue is DecryptStoredValue on the handlers' cipher.
func (h handlers) decryptValue(ciphertext string) (decoded pyjson.Value, ok bool, err error) {
	return DecryptStoredValue(h.cipher, ciphertext)
}

// DecryptStoredValue is decrypt_value + json.loads, the decoded value
// whatever its type (nil for JSON null), with decryptPayload's ok and error
// rules: ok is false where Python's ValueError / JSONDecodeError is caught
// (a wrong key, a malformed token, a payload that is not JSON) and a missing
// key is Python's RuntimeError, not caught, returned as an error. It is the
// one implementation of the stored-payload read, shared by every route that
// reads integration_credentials.credentials_encrypted.
func DecryptStoredValue(cipher Cipher, ciphertext string) (decoded pyjson.Value, ok bool, err error) {
	if cipher == nil || !cipher.Configured() {
		return nil, false, errors.New("SETTINGS_ENCRYPTION_KEY environment variable is required for encryption")
	}
	plain, decryptErr := cipher.Decrypt(secrets.NewValue(ciphertext))
	if decryptErr != nil {
		return nil, false, nil
	}
	decoded, parseErr := pyjson.DecodeString(string(plain))
	if parseErr != nil {
		return nil, false, nil
	}
	return decoded, true, nil
}

// lookupByID is get_decrypted_credentials_by_id_with_outcome.
func (h handlers) lookupByID(ctx context.Context, orgID, credentialID string) (*storedCredential, lookupOutcome, error) {
	id, ok := policy.ParsePyUUID(credentialID)
	if !ok {
		return nil, outcomeNotFound, nil
	}
	var provider, name string
	var ciphertext *string
	err := h.pool.QueryRow(ctx, `SELECT provider, name, credentials_encrypted FROM integration_credentials WHERE org_id = $1 AND id = $2`,
		orgID, id).Scan(&provider, &name, &ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, outcomeNotFound, nil
	}
	if err != nil {
		return nil, outcomeNotFound, err
	}
	row := &storedCredential{provider: provider, name: name}
	if ciphertext == nil || *ciphertext == "" {
		return row, outcomeNoPayload, nil
	}
	creds, readable, err := h.decryptPayload(*ciphertext)
	if err != nil {
		return nil, outcomeNotFound, err
	}
	if !readable {
		RecordDecryptFailed(ctx, provider)
		return row, outcomeDecryptFailed, nil
	}
	row.creds = creds
	return row, outcomeOK, nil
}

// lookupByName is get_decrypted_credentials: nil when the row, its payload
// or a readable payload is missing.
func (h handlers) lookupByName(ctx context.Context, orgID, provider, name string) (*pyjson.Object, error) {
	var ciphertext *string
	err := h.pool.QueryRow(ctx, `SELECT credentials_encrypted FROM integration_credentials WHERE org_id = $1 AND provider = $2 AND name = $3`,
		orgID, provider, name).Scan(&ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if ciphertext == nil || *ciphertext == "" {
		return nil, nil
	}
	creds, readable, err := h.decryptPayload(*ciphertext)
	if err != nil {
		return nil, err
	}
	if !readable {
		return nil, nil
	}
	return creds, nil
}

// testConnection is POST /credentials/test.
func (h handlers) testConnection(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	provider, _ := problems.RequiredString(object, "provider", 0, 0)
	name, hasName := problems.DefaultedString(object, "name", 0, 0)
	if !hasName {
		name = "default"
	}
	credentialID, hasID := problems.OptionalString(object, "credential_id", 0, 0)
	inline, hasInline := problems.OptionalAnyDict(object, "credentials")
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	ctx := r.Context()
	orgID := orgIDOf(ctx)

	var creds *pyjson.Object
	if hasInline {
		creds = inline
	}
	var stored *storedCredential
	if creds == nil || creds.Len() == 0 {
		creds = nil
		if hasID && credentialID != "" {
			row, outcome, err := h.lookupByID(ctx, orgID, credentialID)
			if err != nil {
				h.internal(w, r, "look up credential", err)
				return
			}
			stored = row
			if outcome != outcomeOK {
				if outcome == outcomeNotFound {
					policy.WriteDetail(w, http.StatusNotFound, "Credential not found", nil)
					return
				}
				reason := "credential_missing_payload"
				if outcome == outcomeDecryptFailed {
					reason = "credential_unreadable"
				}
				detail := pyjson.NewObject()
				detail.Set("message", "Stored credential exists but cannot be used for a test connection")
				detail.Set("reason_code", reason)
				policy.WriteDetail(w, http.StatusUnprocessableEntity, detail, nil)
				return
			}
			creds = row.creds
		} else {
			var err error
			if creds, err = h.lookupByName(ctx, orgID, provider, name); err != nil {
				h.internal(w, r, "look up credential", err)
				return
			}
		}
		if creds == nil || creds.Len() == 0 {
			policy.WriteDetail(w, http.StatusNotFound, "Credential not found", nil)
			return
		}
	}

	success, details, probeErr := h.probe(ctx, provider, creds)
	if errors.Is(probeErr, errPagerDutyNotServed) {
		// PagerDuty hydrates OAuth tokens before its live read, which this
		// plane does not do: refuse the request outright rather than answer a
		// success=false test result (and record it on the stored row) that
		// Python would not give.
		policy.WriteDetail(w, http.StatusNotImplemented, errPagerDutyNotServed.Error(), nil)
		return
	}
	var errorText *string
	if probeErr != nil {
		h.logger.ErrorContext(ctx, "test connection failed", "provider", strings.NewReplacer("\r", "", "\n", "").Replace(provider), "error", probeErr.Error())
		success, details = false, nil
		text := probeErr.Error()
		errorText = &text
	}
	// The error reaches two sinks -- the persisted last_test_error and the
	// response -- so it is redacted before either (CHAOS-2780).
	if errorText != nil {
		redacted := pythonparity.SanitizeErrorText(*errorText, 4000)
		errorText = &redacted
	}
	if stored == nil {
		row, err := h.rowIdentity(ctx, orgID, provider, name)
		if err != nil {
			h.internal(w, r, "look up credential row", err)
			return
		}
		stored = row
	}
	if stored != nil {
		if err := h.recordTestResult(ctx, orgID, stored.provider, stored.name, success, errorText); err != nil {
			h.internal(w, r, "record test result", err)
			return
		}
	}
	out := pyjson.NewObject()
	out.Set("success", success)
	if errorText != nil {
		out.Set("error", *errorText)
	} else {
		out.Set("error", nil)
	}
	if details != nil && details.Len() > 0 {
		out.Set("details", details)
	} else {
		out.Set("details", nil)
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// rowIdentity is `svc.get(provider, name)` for a test whose credentials came
// inline: the stored row, if any, still receives the result.
func (h handlers) rowIdentity(ctx context.Context, orgID, provider, name string) (*storedCredential, error) {
	var id uuid.UUID
	err := h.pool.QueryRow(ctx, `SELECT id FROM integration_credentials WHERE org_id = $1 AND provider = $2 AND name = $3`, orgID, provider, name).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &storedCredential{provider: provider, name: name}, nil
}

// recordTestResult is update_test_result: the row named provider/name gets
// last_test_at/success/error (the error redacted again, a no-op on clean
// text) and a fresh updated_at.
func (h handlers) recordTestResult(ctx context.Context, orgID, provider, name string, success bool, errorText *string) error {
	var stored any
	if errorText != nil {
		redacted := pythonparity.SanitizeErrorText(*errorText, 4000)
		stored = redacted
	}
	// last_test_at and the onupdate updated_at are separate now() calls.
	testedAt := h.now().UTC().Truncate(time.Microsecond)
	updatedAt := h.now().UTC().Truncate(time.Microsecond)
	_, err := h.pool.Exec(ctx, `UPDATE integration_credentials SET last_test_at = $1, last_test_success = $2, last_test_error = $3, updated_at = $4
		WHERE org_id = $5 AND provider = $6 AND name = $7`, testedAt, success, stored, updatedAt, orgID, provider, name)
	return err
}
