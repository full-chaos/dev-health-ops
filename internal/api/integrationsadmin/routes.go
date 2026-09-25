// Package integrationsadmin serves the generic integration admin routes,
// ported route for route from api/admin/routers/integrations.py and
// api/services/integrations.py:
//
//	GET   /api/v1/admin/integrations
//	POST  /api/v1/admin/integrations
//	GET   /api/v1/admin/integrations/{integration_id}
//	PATCH /api/v1/admin/integrations/{integration_id}
//	GET   /api/v1/admin/integrations/{integration_id}/sources
//	PATCH /api/v1/admin/integrations/{integration_id}/sources/{source_id}
//	GET   /api/v1/admin/integrations/{integration_id}/datasets
//	PATCH /api/v1/admin/integrations/{integration_id}/datasets
//	POST  /api/v1/admin/integrations/{integration_id}/discover
//
// The sync and backfill triggers are separate routes on the same prefix. Every route is Depends(get_admin_org_id): policy.AdminOrg, and the
// org is the caller's own org_id claim. The write routes validate a pydantic
// body first (FastAPI validates the body before any dependency), and run in
// one transaction that rolls back on any refusal, as get_postgres_session
// does when the handler raises.
package integrationsadmin

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	schedsync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
	"github.com/full-chaos/dev-health-ops/internal/synclimits"
)

const prefix = "/api/v1/admin/integrations"

// Deps is what the routes need. Pool is the api role's pool.
type Deps struct {
	Pool   *pgxpool.Pool
	Guard  *policy.Guard
	Logger *slog.Logger
	// Discovery is the source discovery the discover route runs (nil: the
	// route answers its 503, as a Python discovery that cannot run does).
	Discovery schedsync.SourceDiscoveryExecutor
	// Now is the clock of the row timestamps Python takes from
	// datetime.now(timezone.utc) (nil: time.Now).
	Now func() time.Time
}

type handlers struct{ Deps }

// Routes returns the integration CRUD, source and dataset routes.
func Routes(deps Deps) []httpapi.Route {
	if deps.Logger == nil {
		deps.Logger = slog.Default()
	}
	if deps.Now == nil {
		deps.Now = time.Now
	}
	h := handlers{deps}
	read := func(handler http.HandlerFunc) http.Handler { return deps.Guard.Wrap(policy.AdminOrg, handler) }
	write := func(handler http.HandlerFunc) http.Handler { return deps.Guard.BodyFirst(policy.AdminOrg, handler) }
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: prefix, Handler: read(h.list)},
		{Method: http.MethodPost, Pattern: prefix, Handler: write(h.create)},
		{Method: http.MethodGet, Pattern: prefix + "/{integration_id}", Handler: read(h.get)},
		{Method: http.MethodPatch, Pattern: prefix + "/{integration_id}", Handler: write(h.update)},
		{Method: http.MethodGet, Pattern: prefix + "/{integration_id}/sources", Handler: read(h.listSources)},
		{Method: http.MethodPatch, Pattern: prefix + "/{integration_id}/sources/{source_id}", Handler: write(h.updateSource)},
		{Method: http.MethodGet, Pattern: prefix + "/{integration_id}/datasets", Handler: read(h.listDatasets)},
		{Method: http.MethodPatch, Pattern: prefix + "/{integration_id}/datasets", Handler: write(h.updateDatasets)},
		{Method: http.MethodPost, Pattern: prefix + "/{integration_id}/discover", Handler: read(h.discover)},
	}
}

// refusal is an HTTPException the handler raises: the transaction rolls back
// and the route answers {"detail": message}.
type refusal struct {
	status  int
	message string
}

func (r refusal) Error() string { return r.message }

func refuse(status int, message string) error { return refusal{status: status, message: message} }

func (h handlers) fail(w http.ResponseWriter, r *http.Request, step string, err error) {
	var denied refusal
	if errors.As(err, &denied) {
		policy.WriteDetail(w, denied.status, denied.message, nil)
		return
	}
	h.Logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
		slog.String("step", step), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

// inTx runs fn in one transaction, committed only when fn returns nil.
func (h handlers) inTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func orgOf(r *http.Request) string { return policy.UserFrom(r.Context()).OrgID }

// pathID is uuid.UUID(raw) for a path parameter: ok is false for a value
// Python's constructor refuses (the service then reports "not found").
func pathID(raw string) (uuid.UUID, bool) {
	id, err := pythonparity.ParseUUID(raw)
	return id, err == nil
}

// requireIntegration is the `if get_by_id(...) is None: 404` every nested
// route starts with.
func requireIntegration(ctx context.Context, q querier, orgID, raw string) (*integration, uuid.UUID, error) {
	id, ok := pathID(raw)
	if !ok {
		return nil, uuid.Nil, refuse(http.StatusNotFound, "Integration not found")
	}
	row, err := getIntegration(ctx, q, orgID, id, false)
	if err != nil {
		return nil, uuid.Nil, err
	}
	if row == nil {
		return nil, uuid.Nil, refuse(http.StatusNotFound, "Integration not found")
	}
	return row, id, nil
}

func (h handlers) list(w http.ResponseWriter, r *http.Request) {
	rows, err := listIntegrations(r.Context(), h.Pool, orgOf(r))
	if err != nil {
		h.fail(w, r, "list integrations", err)
		return
	}
	out := make([]pyjson.Value, 0, len(rows))
	for _, row := range rows {
		object, err := integrationObject(row)
		if err != nil {
			h.fail(w, r, "render integration", err)
			return
		}
		out = append(out, object)
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func (h handlers) get(w http.ResponseWriter, r *http.Request) {
	row, _, err := requireIntegration(r.Context(), h.Pool, orgOf(r), r.PathValue("integration_id"))
	if err != nil {
		h.fail(w, r, "get integration", err)
		return
	}
	object, err := integrationObject(*row)
	if err != nil {
		h.fail(w, r, "render integration", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, object, nil)
}

// resolveCredential is IntegrationService._resolve_credential_id.
func resolveCredential(ctx context.Context, q querier, orgID, provider, raw string) (uuid.UUID, error) {
	id, ok := pathID(raw)
	if !ok {
		return uuid.Nil, refuse(http.StatusBadRequest, "Invalid credential_id: "+raw)
	}
	owned, err := credentialOwned(ctx, q, orgID, provider, id)
	if err != nil {
		return uuid.Nil, err
	}
	if !owned {
		return uuid.Nil, refuse(http.StatusBadRequest, "credential_id does not reference a credential for this organization and provider")
	}
	return id, nil
}

func dumps(value pyjson.Value) (string, error) { return pyjson.Dumps(value) }

// createBody is IntegrationCreate.
type createBody struct {
	name, provider string
	credentialID   *string
	config         *pyjson.Object
	isActive       bool
	scheduleCron   *string
	timezone       *string
}

func optionalText(problems *pybody.Errors, object *pyjson.Object, name string) *string {
	if value, present := problems.OptionalString(object, name, 0, 0); present {
		return &value
	}
	return nil
}

func parseCreate(body pybody.Body) (createBody, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return createBody{}, problems
	}
	out := createBody{config: pyjson.NewObject(), isActive: true}
	out.name, _ = problems.RequiredString(object, "name", 1, 255)
	out.provider, _ = problems.RequiredString(object, "provider", 1, 0)
	out.credentialID = optionalText(&problems, object, "credential_id")
	if config, present := problems.DefaultedAnyDict(object, "config"); present {
		out.config = config
	}
	if value, present := problems.DefaultedBool(object, "is_active"); present {
		out.isActive = value
	}
	out.scheduleCron = optionalText(&problems, object, "schedule_cron")
	out.timezone = optionalText(&problems, object, "timezone")
	return out, problems
}

func (h handlers) create(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	request, problems := parseCreate(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	orgID := orgOf(r)
	config, err := dumps(request.config)
	if err != nil {
		h.fail(w, r, "encode config", err)
		return
	}
	var object *pyjson.Object
	err = h.inTx(r.Context(), func(tx pgx.Tx) error {
		var credential *uuid.UUID
		if request.credentialID != nil {
			id, err := resolveCredential(r.Context(), tx, orgID, request.provider, *request.credentialID)
			if err != nil {
				return err
			}
			credential = &id
		}
		created, err := scanIntegration(tx.QueryRow(r.Context(), `
INSERT INTO public.integrations (id, org_id, provider, credential_id, name, config, is_active, schedule_cron, timezone, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, $7, $8, $9, $10, $11) RETURNING `+integrationColumns,
			uuid.New(), orgID, request.provider, credential, request.name, config, request.isActive,
			request.scheduleCron, request.timezone, h.Now().UTC(), h.Now().UTC()))
		if err != nil {
			return fmt.Errorf("insert integration: %w", err)
		}
		// The response is built before the commit: a row the response model
		// cannot render rolls the write back, as the Python route's raise
		// inside the session does.
		object, err = integrationObject(created)
		return err
	})
	if err != nil {
		h.fail(w, r, "create integration", err)
		return
	}
	policy.WriteModel(w, http.StatusCreated, object, nil)
}

// updateBody is IntegrationUpdate: every field optional, and None means
// "leave unchanged".
type updateBody struct {
	name, credentialID, scheduleCron, timezone *string
	config                                     *pyjson.Object
	isActive                                   *bool
}

func parseUpdate(body pybody.Body) (updateBody, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return updateBody{}, problems
	}
	var out updateBody
	out.name = optionalText(&problems, object, "name")
	out.credentialID = optionalText(&problems, object, "credential_id")
	if config, present := problems.OptionalAnyDict(object, "config"); present {
		out.config = config
	}
	if value, present := problems.OptionalBool(object, "is_active"); present {
		out.isActive = &value
	}
	out.scheduleCron = optionalText(&problems, object, "schedule_cron")
	out.timezone = optionalText(&problems, object, "timezone")
	return out, problems
}

func (h handlers) update(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	request, problems := parseUpdate(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	orgID := orgOf(r)
	var object *pyjson.Object
	err := h.inTx(r.Context(), func(tx pgx.Tx) error {
		current, id, err := requireIntegration(r.Context(), tx, orgID, r.PathValue("integration_id"))
		if err != nil {
			return err
		}
		updated := *current
		// The response is the ORM object in memory: it carries every value
		// the request set, whether or not an UPDATE was needed to store it.
		view := request.config
		// SQLAlchemy writes only the attributes whose value changed, and
		// stamps updated_at only when it writes: an update that sets every
		// field to its current value emits no UPDATE at all.
		var sets []string
		var args []any
		set := func(column, cast string, value any) {
			args = append(args, value)
			sets = append(sets, fmt.Sprintf("%s=$%d%s", column, len(args)+1, cast))
		}
		if request.name != nil && *request.name != current.Name {
			set("name", "", *request.name)
		}
		if request.credentialID != nil {
			credential, err := resolveCredential(r.Context(), tx, orgID, current.Provider, *request.credentialID)
			if err != nil {
				return err
			}
			if current.CredentialID == nil || *current.CredentialID != credential {
				set("credential_id", "", credential)
			}
		}
		if request.config != nil {
			stored, err := pyjson.DecodeString(current.Config)
			if err != nil {
				return fmt.Errorf("decode stored config: %w", err)
			}
			if !pyjson.Equal(stored, request.config) {
				text, err := dumps(request.config)
				if err != nil {
					return err
				}
				set("config", "::json", text)
			}
		}
		if request.isActive != nil && *request.isActive != current.IsActive {
			set("is_active", "", *request.isActive)
		}
		if request.scheduleCron != nil && (current.ScheduleCron == nil || *current.ScheduleCron != *request.scheduleCron) {
			set("schedule_cron", "", *request.scheduleCron)
		}
		if request.timezone != nil && (current.Timezone == nil || *current.Timezone != *request.timezone) {
			set("timezone", "", *request.timezone)
		}
		if request.name != nil {
			updated.Name = *request.name
		}
		if request.isActive != nil {
			updated.IsActive = *request.isActive
		}
		if request.scheduleCron != nil {
			updated.ScheduleCron = request.scheduleCron
		}
		if request.timezone != nil {
			updated.Timezone = request.timezone
		}
		if len(sets) == 0 {
			object, err = integrationObjectWith(updated, view)
			return err
		}
		args = append(args, h.Now().UTC())
		sets = append(sets, fmt.Sprintf("updated_at=$%d", len(args)+1))
		updated, err = scanIntegration(tx.QueryRow(r.Context(),
			`UPDATE public.integrations SET `+strings.Join(sets, ", ")+` WHERE id=$1 AND org_id=`+fmt.Sprintf("$%d", len(args)+2)+` RETURNING `+integrationColumns,
			append(append([]any{id}, args...), orgID)...))
		if err != nil {
			return fmt.Errorf("update integration: %w", err)
		}
		object, err = integrationObjectWith(updated, view)
		return err
	})
	if err != nil {
		h.fail(w, r, "update integration", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, object, nil)
}

func (h handlers) listSources(w http.ResponseWriter, r *http.Request) {
	orgID := orgOf(r)
	_, id, err := requireIntegration(r.Context(), h.Pool, orgID, r.PathValue("integration_id"))
	if err != nil {
		h.fail(w, r, "get integration", err)
		return
	}
	rows, err := listSources(r.Context(), h.Pool, orgID, id)
	if err != nil {
		h.fail(w, r, "list sources", err)
		return
	}
	out := make([]pyjson.Value, 0, len(rows))
	for _, row := range rows {
		object, err := sourceObject(row)
		if err != nil {
			h.fail(w, r, "render source", err)
			return
		}
		out = append(out, object)
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// sourceEnableBody is IntegrationSourceUpdate.
func parseSourceUpdate(body pybody.Body) (bool, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return false, problems
	}
	field := pybody.Get(pybody.Model{Errors: &problems, Object: object, Loc: []pyjson.Value{"body"}}, "is_enabled", pybody.Required, pybody.Bool)
	return field.Value, problems
}

// jiraKey is discovery/repos.py jira_key_norm: .strip().lower().
func jiraKey(value string) string { return pythonparity.Lower(pythonparity.Strip(value)) }

// systemMarkers are the discovery bookkeeping keys an explicit enable or
// disable supersedes.
var systemMarkers = []string{"capped_by_repo_limit", "superseded_by_scope_change"}

func (h handlers) updateSource(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	enabled, problems := parseSourceUpdate(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	orgID := orgOf(r)
	var object *pyjson.Object
	err := h.inTx(r.Context(), func(tx pgx.Tx) error {
		_, integrationID, err := requireIntegration(r.Context(), tx, orgID, r.PathValue("integration_id"))
		if err != nil {
			return err
		}
		sourceID, ok := pathID(r.PathValue("source_id"))
		if !ok {
			return refuse(http.StatusNotFound, "Source not found")
		}
		current, err := getSource(r.Context(), tx, orgID, integrationID, sourceID)
		if err != nil {
			return err
		}
		if current == nil {
			return refuse(http.StatusNotFound, "Source not found")
		}
		updated, err := h.setSourceEnabled(r.Context(), tx, orgID, *current, enabled)
		if err != nil {
			return err
		}
		object, err = sourceObject(updated)
		return err
	})
	if err != nil {
		h.fail(w, r, "update source", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, object, nil)
}

// setSourceEnabled is IntegrationSourceService.set_enabled.
func (h handlers) setSourceEnabled(ctx context.Context, tx pgx.Tx, orgID string, current source, enabled bool) (source, error) {
	if enabled && !current.IsEnabled && jiraKey(current.Provider) == "jira" {
		// The same org advisory lock create_sync_config's preflight and
		// discovery's rebalance hold, so concurrent enables cannot each pass
		// the limit check on the same pre-commit count.
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, synclimits.AdvisoryLockKey(orgID)); err != nil {
			return current, fmt.Errorf("take repo limit lock: %w", err)
		}
		orgUUID, err := pythonparity.ParseUUID(orgID)
		if err != nil {
			return current, fmt.Errorf("org id is not a uuid: %w", err)
		}
		inputs, err := licensing.LoadTierLimitInputs(ctx, tx, orgUUID)
		if err != nil {
			return current, err
		}
		limit, err := licensing.GetLimitFrom(inputs, "max_repos")
		if err != nil {
			return current, err
		}
		if limit != nil {
			maxRepos, err := limitInt(limit)
			if err != nil {
				return current, err
			}
			count, err := synclimits.ActiveRepoUsageCount(ctx, tx, orgID)
			if err != nil {
				return current, err
			}
			if int64(count)+1 > maxRepos {
				h.Logger.WarnContext(ctx, "jira_source_enable_rejected_repo_limit", slog.String("org_id", orgID),
					slog.String("source_id", current.ID.String()), slog.Int64("max_repos", maxRepos))
				return current, refuse(http.StatusForbidden, fmt.Sprintf("Enabling this source would exceed the org's repo limit (%s)", pyjson.Repr(limit)))
			}
		}
	}
	var sets []string
	var args []any
	if enabled != current.IsEnabled {
		args = append(args, enabled)
		sets = append(sets, fmt.Sprintf("is_enabled=$%d", len(args)+1))
	}
	// `metadata = source.metadata_ or {}` then metadata.get(...): a stored
	// value that is truthy and not an object has no .get (AttributeError).
	metadata, err := markerMetadata(current.Metadata)
	if err != nil {
		return current, err
	}
	if hasMarker(metadata) {
		// Any explicit operator enable or disable supersedes all automatic
		// discovery bookkeeping, in either direction.
		kept := pyjson.NewObject()
		for _, key := range metadata.Keys() {
			if key == systemMarkers[0] || key == systemMarkers[1] {
				continue
			}
			value, _ := metadata.Get(key)
			kept.Set(key, value)
		}
		text, err := dumps(kept)
		if err != nil {
			return current, err
		}
		args = append(args, text)
		sets = append(sets, fmt.Sprintf("metadata=$%d::json", len(args)+1))
	}
	if len(sets) == 0 {
		return current, nil
	}
	updated, err := scanSource(tx.QueryRow(ctx, `UPDATE public.integration_sources SET `+strings.Join(sets, ", ")+` WHERE id=$1 RETURNING `+sourceColumns,
		append([]any{current.ID}, args...)...))
	if err != nil {
		return current, fmt.Errorf("update source: %w", err)
	}
	return updated, nil
}

func markerMetadata(text string) (*pyjson.Object, error) {
	value, err := pyjson.DecodeString(text)
	if err != nil {
		return nil, err
	}
	if object, ok := value.(*pyjson.Object); ok {
		return object, nil
	}
	if !pyjson.Truthy(value) {
		return pyjson.NewObject(), nil
	}
	return nil, fmt.Errorf("stored metadata is not an object")
}

func hasMarker(metadata *pyjson.Object) bool {
	for _, key := range systemMarkers {
		if value, present := metadata.Get(key); present && pyjson.Truthy(value) {
			return true
		}
	}
	return false
}

// limitInt is int(max_repos) for the limit value the tier service returns.
func limitInt(limit pyjson.Value) (int64, error) {
	switch typed := limit.(type) {
	case pyjson.Int:
		if typed.Int != nil && typed.Int.IsInt64() {
			return typed.Int.Int64(), nil
		}
	case pyjson.Float:
		return int64(typed), nil
	}
	return 0, fmt.Errorf("max_repos limit %s is not an integer", pyjson.Repr(limit))
}

func (h handlers) listDatasets(w http.ResponseWriter, r *http.Request) {
	orgID := orgOf(r)
	_, id, err := requireIntegration(r.Context(), h.Pool, orgID, r.PathValue("integration_id"))
	if err != nil {
		h.fail(w, r, "get integration", err)
		return
	}
	rows, err := listDatasets(r.Context(), h.Pool, orgID, id)
	if err != nil {
		h.fail(w, r, "list datasets", err)
		return
	}
	out := make([]pyjson.Value, 0, len(rows))
	for _, row := range rows {
		object, err := datasetObject(row)
		if err != nil {
			h.fail(w, r, "render dataset", err)
			return
		}
		out = append(out, object)
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// datasetUpdate is IntegrationDatasetUpdate.
type datasetUpdate struct {
	key     string
	enabled bool
}

func parseDatasetUpdates(body pybody.Body) ([]datasetUpdate, pybody.Errors) {
	var problems pybody.Errors
	object, ok := problems.Object(body)
	if !ok {
		return nil, problems
	}
	item := func(model pybody.Model) (datasetUpdate, bool) {
		key := pybody.Get(model, "dataset_key", pybody.Required, pybody.Str)
		enabled := pybody.Get(model, "is_enabled", pybody.Required, pybody.Bool)
		return datasetUpdate{key: key.Value, enabled: enabled.Value}, key.OK && enabled.OK
	}
	field := pybody.Get(pybody.Model{Errors: &problems, Object: object, Loc: []pyjson.Value{"body"}}, "datasets", pybody.Required, pybody.ModelList(item))
	return field.Value, problems
}

func (h handlers) updateDatasets(w http.ResponseWriter, r *http.Request) {
	body, _ := policy.BodyFrom(r.Context())
	items, problems := parseDatasetUpdates(body)
	if len(problems) > 0 {
		policy.WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail(problems), nil)
		return
	}
	orgID := orgOf(r)
	var out []pyjson.Value
	err := h.inTx(r.Context(), func(tx pgx.Tx) error {
		current, integrationID, err := requireIntegration(r.Context(), tx, orgID, r.PathValue("integration_id"))
		if err != nil {
			return err
		}
		// One row object per key: a key repeated in the batch is the same ORM
		// row twice, and the response shows its final state both times.
		rows := map[string]*dataset{}
		var updated []*dataset
		for _, item := range items {
			row, err := h.applyDataset(r.Context(), tx, orgID, current.Provider, integrationID, item, rows)
			if err != nil {
				return err
			}
			updated = append(updated, row)
		}
		// Rendered before the commit, from each row's final state.
		out = make([]pyjson.Value, 0, len(updated))
		for _, row := range updated {
			object, err := datasetObject(*row)
			if err != nil {
				return err
			}
			out = append(out, object)
		}
		return nil
	})
	if err != nil {
		h.fail(w, r, "update datasets", err)
		return
	}
	policy.WriteModel(w, http.StatusOK, out, nil)
}

// applyDataset is one iteration of update_integration_datasets.
func (h handlers) applyDataset(ctx context.Context, tx pgx.Tx, orgID, provider string, integrationID uuid.UUID, item datasetUpdate, rows map[string]*dataset) (*dataset, error) {
	row := rows[item.key]
	if row == nil {
		existing, err := getDatasetByKey(ctx, tx, orgID, integrationID, item.key)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			if !schedsync.SupportsDataset(provider, item.key) {
				return nil, refuse(http.StatusNotFound, fmt.Sprintf("Dataset '%s' not found", item.key))
			}
			// IntegrationDatasetService.create: a concurrent insert of the
			// same key falls back to enabling the row that won.
			created, err := scanDataset(tx.QueryRow(ctx, `
INSERT INTO public.integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options)
VALUES ($1, $2, $3, $4, $5, '{}'::json)
ON CONFLICT (org_id, integration_id, dataset_key) DO NOTHING RETURNING `+datasetColumns,
				uuid.New(), orgID, integrationID, item.key, item.enabled))
			if err == nil {
				rows[item.key] = &created
				return &created, nil
			}
			if !errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("insert dataset: %w", err)
			}
			existing, err = getDatasetByKey(ctx, tx, orgID, integrationID, item.key)
			if err != nil {
				return nil, err
			}
			if existing == nil {
				return nil, fmt.Errorf("dataset %s vanished after a conflicting insert", item.key)
			}
		}
		row = existing
		rows[item.key] = row
	}
	if row.IsEnabled != item.enabled {
		updated, err := scanDataset(tx.QueryRow(ctx, `UPDATE public.integration_datasets SET is_enabled=$2 WHERE id=$1 RETURNING `+datasetColumns, row.ID, item.enabled))
		if err != nil {
			return nil, fmt.Errorf("update dataset: %w", err)
		}
		*row = updated
	}
	return row, nil
}
