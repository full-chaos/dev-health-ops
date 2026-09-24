package customerpush

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/externalingest"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// validSystems is customer_push.py _VALID_SYSTEMS, sorted as its message
// prints them.
var validSystems = []string{"atlassian", "custom", "github", "gitlab", "jira", "linear", "pagerduty"}

// sourceModes is IngestSourceMode's values in declaration order.
var sourceModes = []string{"fullchaos_sync", "customer_push", "disabled"}

const (
	tokenPrefix        = "fcpush_"
	tokenPrefixDisplay = 12
	scopeIngestWrite   = "ingest:write"
)

// failure is an HTTPException a write step raised: status and detail.
type failure struct {
	status int
	detail pyjson.Value
}

func fail(status int, detail pyjson.Value) *failure { return &failure{status: status, detail: detail} }

// codeDetail is a {"code", "message"} detail object.
func codeDetail(code, message string) *pyjson.Object {
	detail := pyjson.NewObject()
	detail.Set("code", code)
	detail.Set("message", message)
	return detail
}

// validateSystem is _validate_system.
func validateSystem(system string) (string, *failure) {
	normalized := pythonparity.Lower(pythonparity.Strip(system))
	for _, valid := range validSystems {
		if normalized == valid {
			return normalized, nil
		}
	}
	return "", fail(http.StatusBadRequest, "Invalid system '"+system+"'; must be one of "+pyListRepr(validSystems))
}

// validateMode is _validate_mode.
func validateMode(mode string) (string, *failure) {
	for _, valid := range sourceModes {
		if mode == valid {
			return mode, nil
		}
	}
	return "", fail(http.StatusBadRequest, "Invalid mode '"+mode+"'; must be one of "+pyListRepr(sourceModes))
}

// rejectFullchaosHosted is _reject_fullchaos_hosted_webhook_mode.
func rejectFullchaosHosted(webhookMode string) *failure {
	if webhookMode == "fullchaos_hosted" {
		return fail(http.StatusBadRequest, "fullchaos_hosted webhook mode is not available yet")
	}
	return nil
}

// userUUID is _user_uuid: nil for an empty id; uuid.UUID() of it otherwise
// (a refused id is Python's unhandled 500).
func userUUID(user *policy.User) (*uuid.UUID, error) {
	if user.UserID == "" {
		return nil, nil
	}
	id, err := pythonparity.ParseUUID(user.UserID)
	if err != nil {
		return nil, err
	}
	return &id, nil
}

// pythonNow is datetime.now(timezone.utc): microsecond precision.
func (h *handlers) pythonNow() time.Time { return h.now().UTC().Truncate(time.Microsecond) }

// generateToken is generate_ingest_token: "fcpush_" + token_urlsafe(32).
func generateToken() (string, error) {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return "", err
	}
	return tokenPrefix + base64.RawURLEncoding.EncodeToString(secret), nil
}

// hashToken is hash_ingest_token.
func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// jsonColumn is a stored JSON column's text as SQLAlchemy's JSON type
// writes it (json.dumps with its defaults).
func jsonColumn(value pyjson.Value) ([]byte, error) {
	text, err := pyjson.Dumps(value)
	return []byte(text), err
}

// writeTx runs fn in one transaction, committed when fn returns no failure
// and no error, as the request's session commits.
func (h *handlers) writeTx(ctx context.Context, fn func(pgx.Tx) (*failure, error)) (*failure, error) {
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	failed, err := fn(tx)
	if err != nil || failed != nil {
		return failed, err
	}
	return nil, tx.Commit(ctx)
}

// answer writes a step's outcome: the error as the generic 500, the
// failure as its HTTPException.
func (h *handlers) answer(w http.ResponseWriter, r *http.Request, operation string, failed *failure, err error) bool {
	if err != nil {
		h.internal(w, r, operation, err)
		return true
	}
	if failed != nil {
		policy.WriteDetail(w, failed.status, failed.detail, nil)
		return true
	}
	return false
}

// bodyOrAnswer validates the body BodyFirst read with parse, after the
// admin dependency, as FastAPI reports body errors; false when answered.
func bodyOrAnswer[T any](w http.ResponseWriter, r *http.Request, parse func(pybody.Body) (T, []pybody.Error)) (T, bool) {
	body, _ := policy.BodyFrom(r.Context())
	value, errs := parse(body)
	if len(errs) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(errs), nil)
		return value, false
	}
	return value, true
}

// resolveOwnership is _resolve_ownership: a managed source that matches and
// is enabled under an active integration is 409
// source_owned_by_fullchaos_sync; otherwise the first match's id is kept,
// and an active integration for the provider adds a warning.
func (h *handlers) resolveOwnership(ctx context.Context, tx pgx.Tx, orgID, system, instance, entityFamily string) (*uuid.UUID, []string, *failure, error) {
	warnings := []string{}
	if system == "custom" {
		return nil, warnings, nil, nil
	}
	matches, err := externalingest.FindMatchingManagedSources(ctx, tx, h.getenv, orgID, system, instance, entityFamily)
	if errors.Is(err, externalingest.ErrOwnershipResolutionUnavailable) {
		return nil, nil, fail(http.StatusConflict, codeDetail("ownership_resolution_unavailable",
			externalingest.OwnershipResolutionUnavailableMessage)), nil
	}
	if err != nil {
		return nil, nil, nil, err
	}
	for _, match := range matches {
		if match.Enabled && match.IntegrationActive {
			return nil, nil, fail(http.StatusConflict, codeDetail("source_owned_by_fullchaos_sync",
				"A managed "+system+" sync source already owns '"+instance+"' in this organization; "+
					"disable it before enabling customer-push for the same instance.")), nil
		}
	}
	var matched *uuid.UUID
	if len(matches) > 0 {
		id := matches[0].ID
		matched = &id
	}
	var active uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM integrations
		WHERE org_id = $1 AND lower(provider) = $2 AND is_active IS true LIMIT 1`, orgID, system).Scan(&active)
	switch {
	case err == nil:
		warnings = append(warnings, "Managed sync is also configured for provider '"+system+"' in this "+
			"organization -- verify this is a different repository/workspace.")
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, nil, nil, err
	}
	return matched, warnings, nil, nil
}

// auditEntry is emit_audit_log's row for this area.
func auditEntry(r *http.Request, orgID string, user *uuid.UUID, action audit.Action, resource audit.ResourceType,
	resourceID, description string, changes pyjson.Value) (audit.Entry, error) {
	org, err := pythonparity.ParseUUID(orgID)
	if err != nil {
		return audit.Entry{}, err
	}
	entry := audit.Entry{OrgID: org, UserID: user, Action: action, ResourceType: resource, ResourceID: resourceID,
		Description: &description}
	if changes != nil {
		if entry.Changes, err = jsonColumn(changes); err != nil {
			return audit.Entry{}, err
		}
	}
	entry.RequestMetadata, err = audit.RequestMetadata(r)
	return entry, err
}

func (h *handlers) writeAudit(ctx context.Context, tx pgx.Tx, entry audit.Entry) error {
	_, err := audit.PGWriter{Now: h.pythonNow}.Write(ctx, tx, entry)
	return err
}

func (h *handlers) createSource(w http.ResponseWriter, r *http.Request) {
	payload, ok := bodyOrAnswer(w, r, parseSourceCreate)
	if !ok {
		return
	}
	user := policy.UserFrom(r.Context())
	orgID := user.OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	var response *pyjson.Object
	failed, err := h.writeTx(r.Context(), func(tx pgx.Tx) (*failure, error) {
		ctx := r.Context()
		system, failed := validateSystem(payload.System)
		if failed != nil {
			return failed, nil
		}
		mode, failed := validateMode(payload.Mode)
		if failed != nil {
			return failed, nil
		}
		if failed := rejectFullchaosHosted(payload.WebhookMode); failed != nil {
			return failed, nil
		}
		var variant string
		err := tx.QueryRow(ctx, `SELECT instance FROM external_ingest_sources
			WHERE org_id = $1 AND system = $2 AND lower(instance) = $3 AND entity_family = $4`,
			orgID, system, pythonparity.Lower(pythonparity.Strip(payload.Instance)), payload.EntityFamily).Scan(&variant)
		switch {
		case err == nil:
			return fail(http.StatusConflict, "A source is already registered for system='"+system+"' instance='"+variant+
				"' in this organization (instance identifiers are case-insensitive)"), nil
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, err
		}
		var matched *uuid.UUID
		warnings := []string{}
		if mode == "customer_push" {
			var failed *failure
			matched, warnings, failed, err = h.resolveOwnership(ctx, tx, orgID, system, payload.Instance, payload.EntityFamily)
			if failed != nil || err != nil {
				return failed, err
			}
		}
		createdBy, err := userUUID(user)
		if err != nil {
			return nil, err
		}
		now := h.pythonNow()
		source := sourceRow{ID: uuid.New(), OrgID: orgID, System: system, Instance: payload.Instance,
			EntityFamily: payload.EntityFamily, DisplayName: payload.DisplayName, Mode: mode, Enabled: true,
			WebhookMode: payload.WebhookMode, MatchedIntegrationSourceID: matched, CreatedAt: now, UpdatedAt: now}
		// begin_nested(): the unique constraint's IntegrityError rolls back
		// the savepoint only and answers 409.
		savepoint, err := tx.Begin(ctx)
		if err != nil {
			return nil, err
		}
		_, err = savepoint.Exec(ctx, `INSERT INTO external_ingest_sources
			(id, org_id, system, instance, entity_family, display_name, mode, enabled, webhook_mode,
			 webhook_secret_id, matched_integration_source_id, created_by_user_id, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, $10, $11, $12, $13)`,
			source.ID, orgID, system, source.Instance, source.EntityFamily, source.DisplayName, mode, true,
			source.WebhookMode, matched, createdBy, now, now)
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			_ = savepoint.Rollback(ctx)
			return fail(http.StatusConflict, "A source is already registered for system='"+system+"' instance='"+
				payload.Instance+"' in this organization"), nil
		}
		if err != nil {
			return nil, err
		}
		if err := savepoint.Commit(ctx); err != nil {
			return nil, err
		}
		changes := pyjson.NewObject()
		changes.Set("system", system)
		changes.Set("instance", payload.Instance)
		changes.Set("entity_family", payload.EntityFamily)
		changes.Set("mode", mode)
		entry, err := auditEntry(r, orgID, createdBy, "ingest_source_registered", "ingest_source", source.ID.String(),
			"Registered customer-push source "+system+"/"+payload.Instance, changes)
		if err != nil {
			return nil, err
		}
		if err := h.writeAudit(ctx, tx, entry); err != nil {
			return nil, err
		}
		response = sourceResponse(source, warnings)
		return nil, nil
	})
	if h.answer(w, r, "create source", failed, err) {
		return
	}
	policy.WriteModel(w, http.StatusCreated, response, nil)
}

func (h *handlers) patchSource(w http.ResponseWriter, r *http.Request) {
	payload, ok := bodyOrAnswer(w, r, parseSourcePatch)
	if !ok {
		return
	}
	user := policy.UserFrom(r.Context())
	orgID := user.OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	source, ok := h.loadSource(w, r, orgID, r.PathValue("source_id"))
	if !ok {
		return
	}
	var response *pyjson.Object
	failed, err := h.writeTx(r.Context(), func(tx pgx.Tx) (*failure, error) {
		ctx := r.Context()
		changes := pyjson.NewObject()
		change := func(name string, old, new pyjson.Value) {
			pair := pyjson.NewObject()
			pair.Set("old", old)
			pair.Set("new", new)
			changes.Set(name, pair)
		}
		modeChanged, enabledChanged := false, false
		if payload.Mode != nil {
			mode, failed := validateMode(*payload.Mode)
			if failed != nil {
				return failed, nil
			}
			if mode != source.Mode {
				change("mode", source.Mode, mode)
				modeChanged = true
			}
			source.Mode = mode
		}
		if payload.Enabled != nil && *payload.Enabled != source.Enabled {
			change("enabled", source.Enabled, *payload.Enabled)
			enabledChanged = true
			source.Enabled = *payload.Enabled
		}
		if payload.DisplayName != nil && (source.DisplayName == nil || *payload.DisplayName != *source.DisplayName) {
			change("display_name", optionalString(source.DisplayName), *payload.DisplayName)
			value := *payload.DisplayName
			source.DisplayName = &value
		}
		if payload.WebhookMode != nil && *payload.WebhookMode != source.WebhookMode {
			if failed := rejectFullchaosHosted(*payload.WebhookMode); failed != nil {
				return failed, nil
			}
			change("webhook_mode", source.WebhookMode, *payload.WebhookMode)
			source.WebhookMode = *payload.WebhookMode
		}
		warnings := []string{}
		if source.Enabled && source.Mode == "customer_push" && (modeChanged || enabledChanged) {
			matched, found, failed, err := h.resolveOwnership(ctx, tx, orgID, source.System, source.Instance, source.EntityFamily)
			if failed != nil || err != nil {
				return failed, err
			}
			warnings = found
			source.MatchedIntegrationSourceID = matched
		}
		if changes.Len() > 0 {
			source.UpdatedAt = h.pythonNow()
			_, err := tx.Exec(ctx, `UPDATE external_ingest_sources
				SET mode = $2, enabled = $3, display_name = $4, webhook_mode = $5,
				    matched_integration_source_id = $6, updated_at = $7
				WHERE id = $1`,
				source.ID, source.Mode, source.Enabled, source.DisplayName, source.WebhookMode,
				source.MatchedIntegrationSourceID, source.UpdatedAt)
			if err != nil {
				return nil, err
			}
			actor, err := userUUID(user)
			if err != nil {
				return nil, err
			}
			entry, err := auditEntry(r, orgID, actor, "ingest_source_mode_changed", "ingest_source", source.ID.String(),
				"Updated customer-push source "+source.System+"/"+source.Instance, changes)
			if err != nil {
				return nil, err
			}
			if err := h.writeAudit(ctx, tx, entry); err != nil {
				return nil, err
			}
		}
		response = sourceResponse(source, warnings)
		return nil, nil
	})
	if h.answer(w, r, "patch source", failed, err) {
		return
	}
	policy.WriteModel(w, http.StatusOK, response, nil)
}

// newToken is one IngestToken to insert, and the create response's fields.
type newToken struct {
	ID        uuid.UUID
	OrgID     string
	SourceID  *uuid.UUID
	Name      string
	Plaintext string
	Scopes    []string
	ExpiresAt *pytime.DateTime
	CreatedAt time.Time
}

// insertToken inserts t with its hash and display prefix.
func (h *handlers) insertToken(ctx context.Context, tx pgx.Tx, t newToken, createdBy *uuid.UUID) error {
	scopes := make([]pyjson.Value, len(t.Scopes))
	for index, scope := range t.Scopes {
		scopes[index] = scope
	}
	scopesJSON, err := jsonColumn(scopes)
	if err != nil {
		return err
	}
	var expires *time.Time
	if t.ExpiresAt != nil {
		// A naive value is stored as UTC wall time (asyncpg's encoding).
		at := t.ExpiresAt.Time.UTC()
		expires = &at
	}
	_, err = tx.Exec(ctx, `INSERT INTO external_ingest_tokens
		(id, org_id, source_id, name, token_hash, token_prefix, scopes, created_by_user_id,
		 expires_at, revoked_at, last_used_at, last_used_ip, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, NULL, NULL, $10)`,
		t.ID, t.OrgID, t.SourceID, t.Name, hashToken(t.Plaintext), t.Plaintext[:tokenPrefixDisplay], string(scopesJSON),
		createdBy, expires, t.CreatedAt)
	return err
}

// tokenCreateResponse is IngestTokenCreateResponse.
func tokenCreateResponse(t newToken) *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", t.ID.String())
	out.Set("org_id", t.OrgID)
	out.Set("source_id", optionalUUID(t.SourceID))
	out.Set("name", t.Name)
	out.Set("token", t.Plaintext)
	out.Set("token_prefix", t.Plaintext[:tokenPrefixDisplay])
	scopes := make([]pyjson.Value, len(t.Scopes))
	for index, scope := range t.Scopes {
		scopes[index] = scope
	}
	out.Set("scopes", scopes)
	if t.ExpiresAt == nil {
		out.Set("expires_at", nil)
	} else {
		out.Set("expires_at", pytime.Pydantic(*t.ExpiresAt))
	}
	out.Set("created_at", pydanticTime(t.CreatedAt))
	return out
}

// createToken is _create_token inside tx.
func (h *handlers) createToken(ctx context.Context, tx pgx.Tx, r *http.Request, user *policy.User, orgID string,
	sourceID *uuid.UUID, payload tokenCreate) (*pyjson.Object, error) {
	plaintext, err := generateToken()
	if err != nil {
		return nil, err
	}
	createdBy, err := userUUID(user)
	if err != nil {
		return nil, err
	}
	token := newToken{ID: uuid.New(), OrgID: orgID, SourceID: sourceID, Name: payload.Name, Plaintext: plaintext,
		Scopes: payload.Scopes, ExpiresAt: payload.ExpiresAt, CreatedAt: h.pythonNow()}
	if err := h.insertToken(ctx, tx, token, createdBy); err != nil {
		return nil, err
	}
	changes := pyjson.NewObject()
	changes.Set("name", payload.Name)
	scopes := make([]pyjson.Value, len(payload.Scopes))
	for index, scope := range payload.Scopes {
		scopes[index] = scope
	}
	changes.Set("scopes", scopes)
	changes.Set("source_id", optionalUUID(sourceID))
	entry, err := auditEntry(r, orgID, createdBy, "ingest_token_created", "ingest_token", token.ID.String(),
		"Created ingest token '"+payload.Name+"'", changes)
	if err != nil {
		return nil, err
	}
	if err := h.writeAudit(ctx, tx, entry); err != nil {
		return nil, err
	}
	return tokenCreateResponse(token), nil
}

func (h *handlers) createSourceToken(w http.ResponseWriter, r *http.Request) {
	payload, ok := bodyOrAnswer(w, r, parseTokenCreate)
	if !ok {
		return
	}
	user := policy.UserFrom(r.Context())
	orgID := user.OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	source, ok := h.loadSource(w, r, orgID, r.PathValue("source_id"))
	if !ok {
		return
	}
	var response *pyjson.Object
	failed, err := h.writeTx(r.Context(), func(tx pgx.Tx) (*failure, error) {
		var err error
		response, err = h.createToken(r.Context(), tx, r, user, orgID, &source.ID, payload)
		return nil, err
	})
	if h.answer(w, r, "create source token", failed, err) {
		return
	}
	policy.WriteModel(w, http.StatusCreated, response, nil)
}

func (h *handlers) createOrgToken(w http.ResponseWriter, r *http.Request) {
	payload, ok := bodyOrAnswer(w, r, parseTokenCreate)
	if !ok {
		return
	}
	user := policy.UserFrom(r.Context())
	orgID := user.OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	for _, scope := range payload.Scopes {
		if scope == scopeIngestWrite {
			policy.WriteDetail(w, http.StatusBadRequest, "ingest:write requires a source-bound token; create it via "+
				"POST /customer-push/sources/{source_id}/tokens", nil)
			return
		}
	}
	var response *pyjson.Object
	failed, err := h.writeTx(r.Context(), func(tx pgx.Tx) (*failure, error) {
		var err error
		response, err = h.createToken(r.Context(), tx, r, user, orgID, nil, payload)
		return nil, err
	})
	if h.answer(w, r, "create org token", failed, err) {
		return
	}
	policy.WriteModel(w, http.StatusCreated, response, nil)
}

// lockToken is _get_org_token(for_update=True) inside tx.
func lockToken(ctx context.Context, tx pgx.Tx, orgID, rawID string) (tokenRow, *failure, error) {
	id, err := pythonparity.ParseUUID(rawID)
	if err != nil {
		return tokenRow{}, fail(http.StatusNotFound, "Token not found"), nil
	}
	token, err := scanToken(tx.QueryRow(ctx, `SELECT `+tokenColumns+` FROM external_ingest_tokens
		WHERE id = $1 AND org_id = $2 FOR UPDATE`, id, orgID))
	if errors.Is(err, pgx.ErrNoRows) {
		return tokenRow{}, fail(http.StatusNotFound, "Token not found"), nil
	}
	return token, nil, err
}

func (h *handlers) rotateToken(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	orgID := user.OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	var response *pyjson.Object
	failed, err := h.writeTx(r.Context(), func(tx pgx.Tx) (*failure, error) {
		ctx := r.Context()
		old, failed, err := lockToken(ctx, tx, orgID, r.PathValue("token_id"))
		if failed != nil || err != nil {
			return failed, err
		}
		if old.RevokedAt != nil {
			return fail(http.StatusBadRequest, "Token already revoked"), nil
		}
		now := h.pythonNow()
		var expires *pytime.DateTime
		if old.ExpiresAt != nil {
			at := pytime.UTC(now.Add(old.ExpiresAt.Sub(old.CreatedAt)))
			expires = &at
		}
		if _, err := tx.Exec(ctx, `UPDATE external_ingest_tokens SET revoked_at = $2 WHERE id = $1`, old.ID, now); err != nil {
			return nil, err
		}
		scopes, err := scopesValue(old.Scopes)
		if err != nil {
			return nil, err
		}
		list := scopes.([]pyjson.Value)
		names := make([]string, len(list))
		for index, scope := range list {
			names[index] = scope.(string)
		}
		plaintext, err := generateToken()
		if err != nil {
			return nil, err
		}
		createdBy, err := userUUID(user)
		if err != nil {
			return nil, err
		}
		token := newToken{ID: uuid.New(), OrgID: orgID, SourceID: old.SourceID, Name: old.Name, Plaintext: plaintext,
			Scopes: names, ExpiresAt: expires, CreatedAt: h.pythonNow()}
		if err := h.insertToken(ctx, tx, token, createdBy); err != nil {
			return nil, err
		}
		changes := pyjson.NewObject()
		changes.Set("old_token_id", old.ID.String())
		changes.Set("new_token_id", token.ID.String())
		entry, err := auditEntry(r, orgID, createdBy, "ingest_token_rotated", "ingest_token", token.ID.String(),
			"Rotated ingest token '"+old.Name+"'", changes)
		if err != nil {
			return nil, err
		}
		if err := h.writeAudit(ctx, tx, entry); err != nil {
			return nil, err
		}
		response = tokenCreateResponse(token)
		return nil, nil
	})
	if h.answer(w, r, "rotate token", failed, err) {
		return
	}
	policy.WriteModel(w, http.StatusOK, response, nil)
}

func (h *handlers) revokeToken(w http.ResponseWriter, r *http.Request) {
	user := policy.UserFrom(r.Context())
	orgID := user.OrgID
	if !h.requireAccess(w, r, orgID) {
		return
	}
	var response *pyjson.Object
	failed, err := h.writeTx(r.Context(), func(tx pgx.Tx) (*failure, error) {
		ctx := r.Context()
		token, failed, err := lockToken(ctx, tx, orgID, r.PathValue("token_id"))
		if failed != nil || err != nil {
			return failed, err
		}
		if token.RevokedAt == nil {
			now := h.pythonNow()
			token.RevokedAt = &now
			if _, err := tx.Exec(ctx, `UPDATE external_ingest_tokens SET revoked_at = $2 WHERE id = $1`, token.ID, now); err != nil {
				return nil, err
			}
			actor, err := userUUID(user)
			if err != nil {
				return nil, err
			}
			entry, err := auditEntry(r, orgID, actor, "ingest_token_revoked", "ingest_token", token.ID.String(),
				"Revoked ingest token '"+token.Name+"'", nil)
			if err != nil {
				return nil, err
			}
			if err := h.writeAudit(ctx, tx, entry); err != nil {
				return nil, err
			}
		}
		response, err = tokenResponse(token)
		return nil, err
	})
	if h.answer(w, r, "revoke token", failed, err) {
		return
	}
	policy.WriteModel(w, http.StatusOK, response, nil)
}
