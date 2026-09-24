// Package credentials is the integration credential admin
// (api/admin/routers/credentials.py and IntegrationCredentialsService):
// list, read, create-or-replace and update of an org's stored provider
// credentials. Go is the writer of integration_credentials here, so every
// ciphertext it stores must open with Python's core.encryption.decrypt_value
// and with the Go reader (providerfoundation.FernetDecryptor).
package credentials

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/netip"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/externalurl"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Cipher seals and opens a credential payload the way core.encryption's
// encrypt_value and decrypt_value do; providerfoundation.FernetDecryptor is
// the production one. Configured is false when no encryption key is set.
type Cipher interface {
	Encrypt(plaintext []byte) (secrets.Value, error)
	Decrypt(ciphertext secrets.Value) ([]byte, error)
	Configured() bool
}

// Deps is what the routes need.
type Deps struct {
	Pool   *pgxpool.Pool
	Guard  *policy.Guard
	Cipher Cipher
	Logger *slog.Logger
	Now    func() time.Time
	// HTTPClient carries the connection-test probes (nil: a client with no
	// redirect following and the probes' own timeouts); HostLookup resolves
	// hostnames for the SSRF guard (nil: the system resolver). Tests replace
	// both to reach a stub provider.
	HTTPClient *http.Client
	HostLookup func(context.Context, string) ([]netip.Addr, error)
}

// Routes returns the credential routes. Python registers the list route
// (GET) before create (POST) on /credentials and get before patch on
// /credentials/{provider}/{name}, so a 405 names the first route's method.
func Routes(deps Deps) []httpapi.Route {
	logger := deps.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := deps.Now
	if now == nil {
		now = time.Now
	}
	client := deps.HTTPClient
	if client == nil {
		client = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	}
	lookup := deps.HostLookup
	if lookup == nil {
		lookup = externalurl.ResolveHostAddrs
	}
	h := handlers{pool: deps.Pool, cipher: deps.Cipher, logger: logger, now: now, client: client, lookup: lookup}
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/api/v1/admin/credentials", Allow: http.MethodGet,
			Handler: deps.Guard.Wrap(policy.AdminOrg, http.HandlerFunc(h.list))},
		{Method: http.MethodPost, Pattern: "/api/v1/admin/credentials",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.create))},
		{Method: http.MethodPost, Pattern: "/api/v1/admin/credentials/test",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.testConnection))},
		{Method: http.MethodGet, Pattern: "/api/v1/admin/credentials/{provider}/{name}", Allow: http.MethodGet,
			Handler: deps.Guard.Wrap(policy.AdminOrg, http.HandlerFunc(h.get))},
		{Method: http.MethodPatch, Pattern: "/api/v1/admin/credentials/{provider}/{name}",
			Handler: deps.Guard.BodyFirst(policy.AdminOrg, http.HandlerFunc(h.update))},
	}
}

type handlers struct {
	pool   *pgxpool.Pool
	cipher Cipher
	logger *slog.Logger
	now    func() time.Time
	client *http.Client
	lookup func(context.Context, string) ([]netip.Addr, error)
}

func (h handlers) internal(w http.ResponseWriter, r *http.Request, what string, err error) {
	h.logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
		slog.String("step", what), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

func orgIDOf(ctx context.Context) string { return policy.UserFrom(ctx).OrgID }

// credential mirrors the IntegrationCredential row the routes expose.
type credential struct {
	ID                   uuid.UUID
	Provider, Name       string
	IsActive             bool
	EncryptedPresent     bool
	Config               *pyjson.Object // nil = SQL NULL
	LastTestAt           *time.Time
	LastTestSuccess      *bool
	LastTestError        *string
	CreatedAt, UpdatedAt time.Time
}

const credentialColumns = `id, provider, name, is_active, credentials_encrypted IS NOT NULL, config::text, last_test_at, last_test_success, last_test_error, created_at, updated_at`

func scanCredential(row pgx.Row) (credential, error) {
	var c credential
	var config *string
	if err := row.Scan(&c.ID, &c.Provider, &c.Name, &c.IsActive, &c.EncryptedPresent, &config, &c.LastTestAt, &c.LastTestSuccess, &c.LastTestError, &c.CreatedAt, &c.UpdatedAt); err != nil {
		return credential{}, err
	}
	if config != nil {
		decoded, err := pyjson.DecodeString(*config)
		if err != nil {
			return credential{}, err
		}
		if object, ok := decoded.(*pyjson.Object); ok {
			c.Config = object
		}
	}
	return c, nil
}

// responseJSON is _integration_credential_response: config falls back to {}
// when NULL or empty.
func (c credential) responseJSON() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("id", c.ID.String())
	out.Set("provider", c.Provider)
	out.Set("name", c.Name)
	out.Set("is_active", c.IsActive)
	config := c.Config
	if config == nil {
		config = pyjson.NewObject()
	}
	out.Set("config", config)
	out.Set("last_test_at", timeOrNil(c.LastTestAt))
	if c.LastTestSuccess != nil {
		out.Set("last_test_success", *c.LastTestSuccess)
	} else {
		out.Set("last_test_success", nil)
	}
	if c.LastTestError != nil {
		out.Set("last_test_error", *c.LastTestError)
	} else {
		out.Set("last_test_error", nil)
	}
	out.Set("created_at", pytime.Pydantic(pytime.UTC(c.CreatedAt)))
	out.Set("updated_at", pytime.Pydantic(pytime.UTC(c.UpdatedAt)))
	return out
}

func timeOrNil(at *time.Time) pyjson.Value {
	if at == nil {
		return nil
	}
	return pytime.Pydantic(pytime.UTC(*at))
}

// list is GET /credentials: a truthy `provider` lists that provider's rows
// (active or not), otherwise every row, only active ones when active_only.
func (h handlers) list(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	var problems pybody.Errors
	activeOnly, _ := problems.QueryBool("active_only", pybody.LastQuery(query, "active_only"), false)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	sql := `SELECT ` + credentialColumns + ` FROM integration_credentials WHERE org_id = $1`
	args := []any{orgIDOf(r.Context())}
	if provider := pybody.LastQuery(query, "provider"); provider != nil && *provider != "" {
		sql += ` AND provider = $2`
		args = append(args, *provider)
	} else if activeOnly {
		sql += ` AND is_active = true`
	}
	rows, err := h.pool.Query(r.Context(), sql, args...)
	if err != nil {
		h.internal(w, r, "list credentials", err)
		return
	}
	defer rows.Close()
	out := []pyjson.Value{}
	for rows.Next() {
		c, err := scanCredential(rows)
		if err != nil {
			h.internal(w, r, "scan credential", err)
			return
		}
		out = append(out, c.responseJSON())
	}
	if err := rows.Err(); err != nil {
		h.internal(w, r, "list credentials", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func (h handlers) load(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, orgID, provider, name string, lock bool) (*credential, error) {
	sql := `SELECT ` + credentialColumns + ` FROM integration_credentials WHERE org_id = $1 AND provider = $2 AND name = $3`
	if lock {
		sql += ` FOR UPDATE`
	}
	c, err := scanCredential(q.QueryRow(ctx, sql, orgID, provider, name))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &c, nil
}

// get is GET /credentials/{provider}/{name}.
func (h handlers) get(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("name") == "repos" {
		h.reposShadow(w, r)
		return
	}
	c, err := h.load(r.Context(), h.pool, orgIDOf(r.Context()), r.PathValue("provider"), r.PathValue("name"), false)
	if err != nil {
		h.internal(w, r, "get credential", err)
		return
	}
	if c == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Credential not found", nil)
		return
	}
	policy.WriteModel(w, http.StatusOK, c.responseJSON(), nil)
}

// reposShadow answers GET /credentials/{x}/repos, where Python's
// /credentials/{credential_id}/repos route is registered before the read
// route and wins the same-shaped path. That route looks the segment up as a
// credential id: a segment that is not a UUID, or names no credential of the
// org, is Python's 404 "Credential not found" -- answered here identically --
// and an id that names one is repo listing, which is not served on this
// plane (501, never a credential read of the wrong shape).
func (h handlers) reposShadow(w http.ResponseWriter, r *http.Request) {
	id, ok := policy.ParsePyUUID(r.PathValue("provider"))
	if ok {
		var found bool
		err := h.pool.QueryRow(r.Context(), `SELECT EXISTS (SELECT 1 FROM integration_credentials WHERE org_id = $1 AND id = $2)`,
			orgIDOf(r.Context()), id).Scan(&found)
		if err != nil {
			h.internal(w, r, "look up credential for repo listing", err)
			return
		}
		ok = found
	}
	if !ok {
		policy.WriteDetail(w, http.StatusNotFound, "Credential not found", nil)
		return
	}
	policy.WriteDetail(w, http.StatusNotImplemented, "Repository listing is not served by this API plane", nil)
}

const pagerDutyDedicated = "Use the dedicated PagerDuty setup endpoints"

// requiredDict is a required `dict[str, Any]` body field.
func requiredDict(problems *pybody.Errors, object *pyjson.Object, name string) (*pyjson.Object, bool) {
	raw, ok := object.Get(name)
	if !ok {
		*problems = append(*problems, pybody.Error{Type: "missing", Loc: []pyjson.Value{"body", name}, Msg: "Field required", Input: object})
		return nil, false
	}
	nested, isObject := raw.(*pyjson.Object)
	if !isObject {
		*problems = append(*problems, pybody.Error{Type: "dict_type", Loc: []pyjson.Value{"body", name}, Msg: "Input should be a valid dictionary", Input: raw})
		return nil, false
	}
	return nested, true
}

// credentialKeyMap is _CREDENTIAL_KEY_MAP.
var credentialKeyMap = map[string]map[string]string{
	"linear":    {"apiKey": "api_key"},
	"jira":      {"apiToken": "api_token", "baseUrl": "base_url"},
	"github":    {"appId": "app_id", "baseUrl": "base_url", "installationId": "installation_id", "privateKey": "private_key"},
	"gitlab":    {"baseUrl": "base_url"},
	"atlassian": {"apiToken": "api_token", "cloudId": "cloud_id"},
}

// normalizeCredentialKeys is _normalize_credential_keys: camelCase keys of
// the provider's map become snake_case, in the dict's own order (a later
// duplicate overwrites the value and keeps the first position).
func normalizeCredentialKeys(provider string, credentials *pyjson.Object) *pyjson.Object {
	keyMap := credentialKeyMap[pythonparity.Lower(provider)]
	if len(keyMap) == 0 {
		return credentials
	}
	out := pyjson.NewObject()
	for _, key := range credentials.Keys() {
		value, _ := credentials.Get(key)
		if renamed, ok := keyMap[key]; ok {
			key = renamed
		}
		out.Set(key, value)
	}
	return out
}

// seal is encrypt_value(json.dumps(credentials)).
func (h handlers) seal(credentials *pyjson.Object) (string, error) {
	plain, err := pyjson.Dumps(credentials)
	if err != nil {
		return "", err
	}
	sealed, err := h.cipher.Encrypt([]byte(plain))
	if err != nil {
		return "", err
	}
	return sealed.Reveal(), nil
}

func configText(config *pyjson.Object) (string, error) { return pyjson.Dumps(config) }

// create is POST /credentials: IntegrationCredentialsService.set with the
// default is_active=True. An existing (provider, name) row is replaced in
// place, its config only when one is given.
func (h handlers) create(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	provider, _ := problems.RequiredString(object, "provider", 1, 0)
	name, hasName := problems.DefaultedString(object, "name", 1, 0)
	if !hasName {
		name = "default"
	}
	credentials, _ := requiredDict(&problems, object, "credentials")
	config, hasConfig := problems.OptionalAnyDict(object, "config")
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	if provider == "pagerduty" {
		policy.WriteDetail(w, http.StatusBadRequest, pagerDutyDedicated, nil)
		return
	}
	var configArg *pyjson.Object
	if hasConfig {
		configArg = config
	}
	saved, err := h.set(r.Context(), orgIDOf(r.Context()), provider, name, credentials, configArg, true)
	if err != nil {
		h.internal(w, r, "save credential", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, saved.responseJSON(), nil)
}

// set is IntegrationCredentialsService.set inside one transaction.
func (h handlers) set(ctx context.Context, orgID, provider, name string, credentials, config *pyjson.Object, isActive bool) (credential, error) {
	sealed, err := h.seal(normalizeCredentialKeys(provider, credentials))
	if err != nil {
		return credential{}, err
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		return credential{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now := h.now().UTC().Truncate(time.Microsecond)
	existing, err := h.load(ctx, tx, orgID, provider, name, true)
	if err != nil {
		return credential{}, err
	}
	if existing == nil {
		stored := config
		if stored == nil || stored.Len() == 0 {
			stored = pyjson.NewObject()
		}
		text, err := configText(stored)
		if err != nil {
			return credential{}, err
		}
		// The model's two column defaults are separate now() calls, so a
		// new row's updated_at is a moment after its created_at.
		updatedAt := h.now().UTC().Truncate(time.Microsecond)
		created := credential{ID: uuid.New(), Provider: provider, Name: name, IsActive: isActive, Config: stored, CreatedAt: now, UpdatedAt: updatedAt}
		if _, err := tx.Exec(ctx, `INSERT INTO integration_credentials (id, org_id, provider, name, is_active, credentials_encrypted, config, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8, $9)`,
			created.ID, orgID, provider, name, isActive, sealed, text, now, updatedAt); err != nil {
			return credential{}, err
		}
		return created, tx.Commit(ctx)
	}
	// An UPDATE always follows: the fresh ciphertext is a change. config is
	// written only when given and not equal to the stored value (SQLAlchemy
	// skips an unchanged attribute, so the stored text keeps its own key
	// order), yet the response carries the value the caller sent.
	sets := []string{"credentials_encrypted = $1", "is_active = $2", "updated_at = $3"}
	args := []any{sealed, isActive, now}
	response := *existing
	response.IsActive = isActive
	response.UpdatedAt = now
	if config != nil {
		if existing.Config == nil || !pyEqual(existing.Config, config) {
			text, err := configText(config)
			if err != nil {
				return credential{}, err
			}
			args = append(args, text)
			sets = append(sets, "config = $"+itoa(len(args))+"::json")
		}
		response.Config = config
	}
	args = append(args, existing.ID)
	if _, err := tx.Exec(ctx, `UPDATE integration_credentials SET `+join(sets)+` WHERE id = $`+itoa(len(args)), args...); err != nil {
		return credential{}, err
	}
	return response, tx.Commit(ctx)
}

// update is PATCH /credentials/{provider}/{name}.
func (h handlers) update(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	credentials, hasCredentials := problems.OptionalAnyDict(object, "credentials")
	config, hasConfig := problems.OptionalAnyDict(object, "config")
	isActive, hasActive := problems.OptionalBool(object, "is_active")
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	provider, name := r.PathValue("provider"), r.PathValue("name")
	if provider == "pagerduty" {
		policy.WriteDetail(w, http.StatusBadRequest, pagerDutyDedicated, nil)
		return
	}
	ctx := r.Context()
	orgID := orgIDOf(ctx)
	existing, err := h.load(ctx, h.pool, orgID, provider, name, false)
	if err != nil {
		h.internal(w, r, "get credential", err)
		return
	}
	if existing == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Credential not found", nil)
		return
	}
	if hasCredentials {
		// set(): the stored config is passed through when none was sent (a
		// NULL stored config passes None, which leaves the column alone),
		// is_active likewise falls back to the stored flag.
		var configArg *pyjson.Object
		if hasConfig {
			configArg = config
		} else {
			configArg = existing.Config
		}
		active := existing.IsActive
		if hasActive {
			active = isActive
		}
		saved, err := h.set(ctx, orgID, provider, name, credentials, configArg, active)
		if err != nil {
			h.internal(w, r, "update credential", err)
			return
		}
		policy.WriteModel(w, http.StatusOK, saved.responseJSON(), nil)
		return
	}
	saved, err := h.patchFlags(ctx, orgID, *existing, config, hasConfig, isActive, hasActive)
	if err != nil {
		h.internal(w, r, "update credential", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, saved.responseJSON(), nil)
}

// patchFlags assigns config and is_active without touching the ciphertext.
// SQLAlchemy writes only attributes whose value changed (and stamps
// updated_at with them); nothing changed means no UPDATE at all.
func (h handlers) patchFlags(ctx context.Context, orgID string, existing credential, config *pyjson.Object, hasConfig, isActive, hasActive bool) (credential, error) {
	response := existing
	var sets []string
	var args []any
	if hasConfig {
		response.Config = config
		if existing.Config == nil || !pyEqual(existing.Config, config) {
			text, err := configText(config)
			if err != nil {
				return credential{}, err
			}
			args = append(args, text)
			sets = append(sets, "config = $"+itoa(len(args))+"::json")
		}
	}
	if hasActive {
		response.IsActive = isActive
		if isActive != existing.IsActive {
			args = append(args, isActive)
			sets = append(sets, "is_active = $"+itoa(len(args)))
		}
	}
	if len(sets) == 0 {
		return response, nil
	}
	now := h.now().UTC().Truncate(time.Microsecond)
	args = append(args, now)
	sets = append(sets, "updated_at = $"+itoa(len(args)))
	args = append(args, existing.ID, orgID)
	if _, err := h.pool.Exec(ctx, `UPDATE integration_credentials SET `+join(sets)+` WHERE id = $`+itoa(len(args)-1)+` AND org_id = $`+itoa(len(args)), args...); err != nil {
		return credential{}, err
	}
	response.UpdatedAt = now
	return response, nil
}
