// Package telemetry is plan area K's org telemetry settings
// (api/telemetry/router.py): GET /status, POST /opt-in, POST /opt-out.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// Settings keys (services/telemetry.py).
const (
	category         = "telemetry"
	optInKey         = "telemetry_opt_in"
	lastReportAtKey  = "telemetry_last_report_at"
	optInDescription = "Controls voluntary telemetry reporting."
)

// rejected counts org-id refusals by reason, as the Python api's
// record_telemetry_org_id_rejected does.
var rejected = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/telemetry").Int64Counter(
		"dev_health_api_telemetry_org_id_rejected_total",
		metric.WithDescription("Telemetry route requests refused for their org, by reason"))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("dev_health_api_telemetry_org_id_rejected_total")
	}
	return counter
}()

// Routes returns the area's routes. endpoint is TELEMETRY_ENDPOINT ("" =
// reports are not sent).
func Routes(pool *pgxpool.Pool, guard *policy.Guard, auth *policy.Authenticator, endpoint string, logger *slog.Logger) []httpapi.Route {
	h := handlers{pool: pool, auth: auth, logger: logger, now: time.Now, endpoint: endpoint,
		client: &http.Client{Timeout: sendTimeout}}
	wrap := func(next http.HandlerFunc) http.Handler {
		return guard.Wrap(policy.Authenticated, h.withOrg(next))
	}
	return []httpapi.Route{
		{Method: http.MethodGet, Pattern: "/api/v1/telemetry/status", Handler: wrap(h.status)},
		{Method: http.MethodPost, Pattern: "/api/v1/telemetry/opt-in", Handler: wrap(h.optIn)},
		{Method: http.MethodPost, Pattern: "/api/v1/telemetry/opt-out", Handler: wrap(h.optOut)},
		{Method: http.MethodPost, Pattern: "/api/v1/telemetry/report",
			Handler: guard.Wrap(policy.Authenticated, h.requirePlatformRole(h.withOrg(h.report)))},
	}
}

type handlers struct {
	pool     *pgxpool.Pool
	auth     *policy.Authenticator
	logger   *slog.Logger
	now      func() time.Time
	endpoint string
	client   *http.Client
}

type orgKey struct{}

func (h handlers) refuse(w http.ResponseWriter, r *http.Request, reason, detail string) {
	rejected.Add(r.Context(), 1, metric.WithAttributes(attribute.String("reason", reason)))
	h.logger.WarnContext(r.Context(), "telemetry org refused", slog.String("reason", reason), slog.String("path", r.URL.Path))
	policy.WriteDetail(w, http.StatusForbidden, detail, nil)
}

// withOrg is get_org_id: the acting org comes from authenticated state --
// while impersonating, only the target's org; otherwise the X-Org-Id
// header (raw, as FastAPI's Header reads it) or the caller's own org, which
// must be the caller's own, a membership, or any org for a superuser.
func (h handlers) withOrg(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := policy.UserFrom(r.Context())
		header := ""
		if values := r.Header.Values("X-Org-Id"); len(values) > 0 {
			header = policy.Latin1(values[0])
		}
		if session := policy.ImpersonationFrom(r.Context()); session != nil {
			target := session.TargetOrgID.String()
			requested := header
			if requested == "" {
				requested = target
			}
			if requested != target {
				h.refuse(w, r, "not_a_member", "X-Org-Id not permitted for this user")
				return
			}
			next(w, r.WithContext(context.WithValue(r.Context(), orgKey{}, requested)))
			return
		}
		requested := header
		if requested == "" {
			requested = user.OrgID
		}
		if requested == "" {
			h.refuse(w, r, "no_org_context", "Organization context required")
			return
		}
		if !user.IsSuperuser && requested != user.OrgID {
			member, err := h.auth.IsMember(r.Context(), user.UserID, requested)
			if err != nil {
				h.internal(w, r, "membership", err)
				return
			}
			if !member {
				h.refuse(w, r, "not_a_member", "X-Org-Id not permitted for this user")
				return
			}
		}
		next(w, r.WithContext(context.WithValue(r.Context(), orgKey{}, requested)))
	})
}

func (h handlers) internal(w http.ResponseWriter, r *http.Request, step string, err error) {
	h.logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
		slog.String("step", step), slog.String("error", err.Error()))
	policy.WriteInternal(w)
}

// setting is SettingsService.get: (value, found). An encrypted row's value
// needs SETTINGS_ENCRYPTION_KEY, which the api does not hold: it is an
// error (the telemetry keys are always written unencrypted).
func setting(ctx context.Context, q pgx.Tx, orgID, key string) (*string, bool, error) {
	var value *string
	var encrypted bool
	err := q.QueryRow(ctx, `SELECT value, is_encrypted FROM settings WHERE org_id = $1 AND category = $2 AND key = $3`,
		orgID, category, key).Scan(&value, &encrypted)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if encrypted && value != nil && *value != "" {
		return nil, false, errEncrypted
	}
	return value, true, nil
}

var errEncrypted = errors.New("telemetry: setting is encrypted")

// optedIn is get_opt_in_status: default "false"; str(value).strip().lower()
// in {"1", "true", "yes", "on"} (a NULL value reads as "None").
func optedIn(value *string, found bool) bool {
	text := "false"
	if found {
		text = "None"
		if value != nil {
			text = *value
		}
	}
	switch strings.ToLower(strings.TrimSpace(text)) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// lastReportAt is get_last_report_at: falsy → None; "Z" → "+00:00" then
// fromisoformat; ValueError → None.
func (h handlers) lastReportAt(ctx context.Context, value *string, found bool, orgID string) pyjson.Value {
	if !found || value == nil || *value == "" {
		return nil
	}
	parsed, ok := pytime.FromISOFormat(strings.ReplaceAll(*value, "Z", "+00:00"))
	if !ok {
		h.logger.WarnContext(ctx, "invalid telemetry_last_report_at value", slog.String("org_id", orgID))
		return nil
	}
	return pytime.Pydantic(parsed)
}

func (h handlers) respond(w http.ResponseWriter, r *http.Request, tx pgx.Tx, orgID string, opted *bool) {
	ctx := r.Context()
	if opted == nil {
		value, found, err := setting(ctx, tx, orgID, optInKey)
		if err != nil {
			h.internal(w, r, "read opt-in", err)
			return
		}
		status := optedIn(value, found)
		opted = &status
	}
	value, found, err := setting(ctx, tx, orgID, lastReportAtKey)
	if err != nil {
		h.internal(w, r, "read last report", err)
		return
	}
	if err := tx.Commit(ctx); err != nil {
		h.internal(w, r, "commit", err)
		return
	}
	out := pyjson.NewObject()
	out.Set("opted_in", *opted)
	out.Set("last_report_at", h.lastReportAt(ctx, value, found, orgID))
	policy.WriteModel(w, http.StatusOK, out, nil)
}

func (h handlers) status(w http.ResponseWriter, r *http.Request) {
	tx, err := h.pool.Begin(r.Context())
	if err != nil {
		h.internal(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(r.Context())) }()
	h.respond(w, r, tx, r.Context().Value(orgKey{}).(string), nil)
}

func (h handlers) optIn(w http.ResponseWriter, r *http.Request)  { h.setOptIn(w, r, true) }
func (h handlers) optOut(w http.ResponseWriter, r *http.Request) { h.setOptIn(w, r, false) }

// setOptIn is set_opt_in: SettingsService.set(key, "true"/"false",
// encrypt=False, description=...) -- update the row if it exists, else
// insert one.
func (h handlers) setOptIn(w http.ResponseWriter, r *http.Request, enabled bool) {
	ctx := r.Context()
	orgID := ctx.Value(orgKey{}).(string)
	value := "false"
	if enabled {
		value = "true"
	}
	tx, err := h.pool.Begin(ctx)
	if err != nil {
		h.internal(w, r, "begin", err)
		return
	}
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
	now := h.now().UTC()
	var current, description *string
	var encrypted bool
	err = tx.QueryRow(ctx, `SELECT value, is_encrypted, description FROM settings
		WHERE org_id = $1 AND category = $2 AND key = $3 FOR UPDATE`, orgID, category, optInKey).Scan(&current, &encrypted, &description)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, err := tx.Exec(ctx, `INSERT INTO settings (id, org_id, category, key, value, is_encrypted, description, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, false, $6, $7, $7)`, uuid.New(), orgID, category, optInKey, value, optInDescription, now); err != nil {
			h.internal(w, r, "insert opt-in", err)
			return
		}
	case err != nil:
		h.internal(w, r, "read opt-in", err)
		return
	case current == nil || *current != value || encrypted || description == nil || *description != optInDescription:
		// SQLAlchemy writes (and bumps updated_at) only when an attribute
		// actually changes.
		if _, err := tx.Exec(ctx, `UPDATE settings SET value = $4, is_encrypted = false, description = $5, updated_at = $6
			WHERE org_id = $1 AND category = $2 AND key = $3`, orgID, category, optInKey, value, optInDescription, now); err != nil {
			h.internal(w, r, "update opt-in", err)
			return
		}
	}
	h.respond(w, r, tx, orgID, &enabled)
}
