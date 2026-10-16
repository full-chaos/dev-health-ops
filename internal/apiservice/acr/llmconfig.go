package acr

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// LLMConfigScope is the credential scope the llm-config route requires. A
// svc_acr_ credential minted before this scope existed holds only
// entitlements:read and is refused with 403 until it is rotated.
const LLMConfigScope = "llm_config:read"

const llmConfigSchemaVersion = "acr_llm_config.v1"

// ErrCredentialInvalid is a missing, unknown, revoked or expired credential.
var ErrCredentialInvalid = errors.New("service credential invalid")

// ErrCredentialScope is a valid credential that lacks the required scope.
var ErrCredentialScope = errors.New("service credential lacks the required scope")

// CredentialVerifier checks an acr service bearer token for one scope and
// returns the credential's id, safe to log (never the token).
type CredentialVerifier interface {
	Verify(ctx context.Context, token, scope string) (credentialID string, err error)
}

// LLMConfigSource reads an org's stored BYO LLM config.
type LLMConfigSource interface {
	OrgConfig(ctx context.Context, orgID string) (llmorgsettings.OrgConfig, bool, error)
}

// llmConfigResponse is the 200 body. APIKey is a plain string on purpose: this
// is the one value the route exists to serve. It is built last, in
// llmConfigBody, and never reaches a logger.
type llmConfigResponse struct {
	SchemaVersion string `json:"schema_version"`
	OrgID         string `json:"org_id"`
	Provider      string `json:"provider"`
	Model         string `json:"model"`
	BaseURL       string `json:"base_url"`
	APIKey        string `json:"api_key"`
	Version       string `json:"version"`
}

func llmConfigBody(orgID string, cfg llmorgsettings.OrgConfig) llmConfigResponse {
	return llmConfigResponse{
		SchemaVersion: llmConfigSchemaVersion, OrgID: orgID,
		Provider: cfg.Provider, Model: cfg.Model, BaseURL: cfg.BaseURL,
		APIKey: cfg.APIKey.Reveal(), Version: cfg.Version,
	}
}

var llmConfigRequests = func() metric.Int64Counter {
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/acr").Int64Counter(
		"devhealth_acr_llm_config_requests_total",
		metric.WithDescription("Requests to the internal org llm-config route, by outcome."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter("devhealth_acr_llm_config_requests_total")
	}
	return counter
}()

func countLLMConfig(ctx context.Context, outcome string) {
	llmConfigRequests.Add(ctx, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
}

// llmConfigHandler serves GET .../orgs/{org_id}/llm-config. Order: bearer
// (401), scope (403), org id (404), store (503), config (200 or 404). No
// branch logs or returns the key, the token, or an error that could carry one.
func llmConfigHandler(verifier CredentialVerifier, source LLMConfigSource, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		w.Header().Set("Cache-Control", "no-store")
		if verifier == nil || source == nil {
			logger.ErrorContext(ctx, "acr llm-config: route not configured")
			countLLMConfig(ctx, "unconfigured")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		token, ok := bearerToken(r)
		if !ok {
			unauthorized(ctx, w, "missing")
			return
		}
		credentialID, err := verifier.Verify(ctx, token, LLMConfigScope)
		switch {
		case errors.Is(err, ErrCredentialInvalid):
			unauthorized(ctx, w, "invalid")
			return
		case errors.Is(err, ErrCredentialScope):
			logger.InfoContext(ctx, "acr llm-config: scope refused", slog.String("credential_id", credentialID))
			countLLMConfig(ctx, "forbidden")
			writeDetail(w, http.StatusForbidden, "Forbidden")
			return
		case err != nil:
			logger.ErrorContext(ctx, "acr llm-config: credential lookup failed", slog.String("error_type", errorKind(err)))
			countLLMConfig(ctx, "credential_unavailable")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		orgID := r.PathValue("org_id")
		if _, err := uuid.Parse(orgID); err != nil {
			countLLMConfig(ctx, "bad_org")
			writeDetail(w, http.StatusNotFound, "Not found")
			return
		}
		cfg, found, err := source.OrgConfig(ctx, orgID)
		if errors.Is(err, llmorgsettings.ErrStoredSettingUnreadable) {
			logger.ErrorContext(ctx, "acr llm-config: stored config unreadable", slog.String("org_id", orgID))
			countLLMConfig(ctx, "unreadable")
			writeDetail(w, http.StatusConflict, "Stored config unreadable")
			return
		}
		if err != nil {
			logger.ErrorContext(ctx, "acr llm-config: store unavailable",
				slog.String("org_id", orgID), slog.String("error_type", errorKind(err)))
			countLLMConfig(ctx, "store_unavailable")
			writeDetail(w, http.StatusServiceUnavailable, "Service unavailable")
			return
		}
		if !found {
			logger.InfoContext(ctx, "acr llm-config: none configured",
				slog.String("org_id", orgID), slog.String("credential_id", credentialID))
			countLLMConfig(ctx, "not_found")
			writeDetail(w, http.StatusNotFound, "Not found")
			return
		}
		body := llmConfigBody(orgID, cfg)
		logger.InfoContext(ctx, "acr llm-config: served",
			slog.String("org_id", orgID), slog.String("credential_id", credentialID), slog.String("version", cfg.Version))
		countLLMConfig(ctx, "served")
		out := pyjson.NewObject()
		out.Set("schema_version", body.SchemaVersion)
		out.Set("org_id", body.OrgID)
		out.Set("provider", body.Provider)
		out.Set("model", body.Model)
		out.Set("base_url", body.BaseURL)
		out.Set("api_key", body.APIKey)
		out.Set("version", body.Version)
		// A Go-only route has no response model: the JSONResponse writer, not WriteModel.
		policy.WriteJSON(w, http.StatusOK, out, http.Header{"X-Content-Type-Options": {"nosniff"}})
	})
}

func unauthorized(ctx context.Context, w http.ResponseWriter, outcome string) {
	countLLMConfig(ctx, "unauthorized_"+outcome)
	w.Header().Set("WWW-Authenticate", "Bearer")
	writeDetail(w, http.StatusUnauthorized, "Unauthorized")
}

func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	const prefix = "bearer "
	if len(header) <= len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return "", false
	}
	token := strings.TrimSpace(header[len(prefix):])
	return token, token != ""
}

// errorKind names an error's concrete type without its text, which may carry
// a connection string or row value.
func errorKind(err error) string {
	return fmt.Sprintf("%T", err)
}

// PostgresCredentialVerifier checks svc_acr_ tokens against
// internal_service_credentials, the table `service-credentials create` writes
// (admin/servicecredentials.go: token_hash is the hex SHA-256 of the token,
// token_prefix its first 16 characters). It is read-only; last_used_at is not
// touched.
type PostgresCredentialVerifier struct {
	Pool *pgxpool.Pool
	Now  func() time.Time
}

const acrTokenPrefix = "svc_acr_"

func (v PostgresCredentialVerifier) Verify(ctx context.Context, token, scope string) (string, error) {
	if !strings.HasPrefix(token, acrTokenPrefix) || len(token) < 16 {
		return "", ErrCredentialInvalid
	}
	now := time.Now().UTC()
	if v.Now != nil {
		now = v.Now()
	}
	sum := sha256.Sum256([]byte(token))
	presented := hex.EncodeToString(sum[:])
	rows, err := v.Pool.Query(ctx, `SELECT id::text, token_hash, scopes::text, expires_at, revoked_at
		FROM internal_service_credentials WHERE service_name = 'acr' AND token_prefix = $1`, token[:16])
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var matched struct {
		id      string
		scopes  string
		expires *time.Time
		revoked *time.Time
	}
	found := false
	for rows.Next() {
		var id, hash, scopes string
		var expires, revoked *time.Time
		if err := rows.Scan(&id, &hash, &scopes, &expires, &revoked); err != nil {
			return "", err
		}
		if subtle.ConstantTimeCompare([]byte(hash), []byte(presented)) == 1 {
			matched.id, matched.scopes, matched.expires, matched.revoked = id, scopes, expires, revoked
			found = true
		}
	}
	if err := rows.Err(); err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if !found || matched.revoked != nil || (matched.expires != nil && !matched.expires.After(now)) {
		return "", ErrCredentialInvalid
	}
	var held []string
	if err := json.Unmarshal([]byte(matched.scopes), &held); err != nil {
		return matched.id, ErrCredentialScope
	}
	for _, have := range held {
		if have == scope {
			return matched.id, nil
		}
	}
	return matched.id, ErrCredentialScope
}
