package sso

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// providerRow is one sso_providers row as _provider_to_response reads it.
// The JSON columns are kept as their stored text.
type providerRow struct {
	ID, OrgID                                    string
	Name, Protocol, Status, DefaultRole          string
	IsDefault, AllowIdpInitiated, AutoProvision  bool
	Config, AllowedDomains                       *string
	LastMetadataSyncAt, LastLoginAt, LastErrorAt *time.Time
	LastError                                    *string
	CreatedAt, UpdatedAt                         time.Time
}

// setStatus is POST /sso/providers/{provider_id}/activate and /deactivate
// (router.py activate_sso_provider, deactivate_sso_provider): an owner or
// admin sets the status of one of their organization's providers.
func (h handlers) setStatus(status string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		caller := policy.UserFrom(ctx)
		if caller.Role != "owner" && caller.Role != "admin" {
			policy.WriteDetail(w, http.StatusForbidden, "Admin access required", nil)
			return
		}
		// uuid.UUID(user.org_id), then uuid.UUID(provider_id): a ValueError
		// from either is not caught, the Python api's bare 500.
		orgID, err := pythonparity.ParseUUID(caller.OrgID)
		if err != nil {
			h.fail(w, r, "parse org id", err)
			return
		}
		providerID, err := pythonparity.ParseUUID(r.PathValue("provider_id"))
		if err != nil {
			h.fail(w, r, "parse provider id", err)
			return
		}
		tx, err := h.Pool.Begin(ctx)
		if err != nil {
			h.fail(w, r, "begin", err)
			return
		}
		defer func() { _ = tx.Rollback(context.Background()) }()
		row, err := scanProvider(tx.QueryRow(ctx, `UPDATE sso_providers SET status = $3, updated_at = $4
WHERE id = $1::uuid AND org_id = $2::uuid
RETURNING `+providerColumns, providerID, orgID, status, h.Now().UTC()))
		if errors.Is(err, pgx.ErrNoRows) {
			policy.WriteDetail(w, http.StatusNotFound, "SSO provider not found", nil)
			return
		}
		if err != nil {
			h.fail(w, r, "update provider", err)
			return
		}
		// The route commits before it renders the response, so a stored
		// config or allowed_domains of the wrong shape (_provider_to_response's
		// TypeError, a 500) still leaves the new status written.
		if err := tx.Commit(ctx); err != nil {
			h.fail(w, r, "commit", err)
			return
		}
		out, err := providerResponse(row)
		if err != nil {
			h.fail(w, r, "render provider", err)
			return
		}
		policy.WriteModel(w, http.StatusOK, out, nil)
	})
}

const providerColumns = `id::text, org_id::text, name, protocol, status, default_role, is_default, allow_idp_initiated,
	auto_provision_users, config::text, allowed_domains::text, last_metadata_sync_at, last_login_at, last_error_at,
	last_error, created_at, updated_at`

func scanProvider(row pgx.Row) (*providerRow, error) {
	var p providerRow
	err := row.Scan(&p.ID, &p.OrgID, &p.Name, &p.Protocol, &p.Status, &p.DefaultRole, &p.IsDefault,
		&p.AllowIdpInitiated, &p.AutoProvision, &p.Config, &p.AllowedDomains, &p.LastMetadataSyncAt,
		&p.LastLoginAt, &p.LastErrorAt, &p.LastError, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func optionalTime(at *time.Time) pyjson.Value {
	if at == nil {
		return nil
	}
	return pytime.Pydantic(pytime.UTC(*at))
}

// errShape is _string_dict / _string_list's TypeError on a stored value of
// the wrong shape, which the route does not catch.
var errShape = errors.New("sso: stored provider column has the wrong JSON shape")

// providerResponse is _provider_to_response: the row with config's
// client_secret masked and a certificate over 50 characters cut to 50 plus
// "...", allowed_domains without its nulls.
func providerResponse(p *providerRow) (*pyjson.Object, error) {
	config := pyjson.NewObject()
	if p.Config != nil {
		decoded, err := pyjson.DecodeString(*p.Config)
		if err != nil {
			return nil, err
		}
		switch value := decoded.(type) {
		case nil:
		case *pyjson.Object:
			config = value
		default:
			return nil, fmt.Errorf("%w: config", errShape)
		}
	}
	if _, present := config.Get("client_secret"); present {
		config.Set("client_secret", "********")
	}
	if certificate, present := config.Get("certificate"); present {
		if text, ok := certificate.(string); ok {
			if runes := pyjson.Runes(text); len(runes) > 50 {
				config.Set("certificate", pyjson.FromRunes(runes[:50])+"...")
			}
		}
	}
	domains := []pyjson.Value{}
	if p.AllowedDomains != nil {
		decoded, err := pyjson.DecodeString(*p.AllowedDomains)
		if err != nil {
			return nil, err
		}
		switch value := decoded.(type) {
		case nil:
		case []pyjson.Value:
			for _, item := range value {
				switch item.(type) {
				case string:
					domains = append(domains, item)
				case nil:
				default:
					return nil, fmt.Errorf("%w: allowed_domains item", errShape)
				}
			}
		default:
			return nil, fmt.Errorf("%w: allowed_domains", errShape)
		}
	}
	out := pyjson.NewObject()
	out.Set("id", p.ID)
	out.Set("org_id", p.OrgID)
	out.Set("name", p.Name)
	out.Set("protocol", p.Protocol)
	out.Set("status", p.Status)
	out.Set("is_default", p.IsDefault)
	out.Set("allow_idp_initiated", p.AllowIdpInitiated)
	out.Set("auto_provision_users", p.AutoProvision)
	out.Set("default_role", p.DefaultRole)
	out.Set("config", config)
	out.Set("allowed_domains", domains)
	out.Set("last_metadata_sync_at", optionalTime(p.LastMetadataSyncAt))
	out.Set("last_login_at", optionalTime(p.LastLoginAt))
	if p.LastError != nil {
		out.Set("last_error", *p.LastError)
	} else {
		out.Set("last_error", nil)
	}
	out.Set("last_error_at", optionalTime(p.LastErrorAt))
	out.Set("created_at", pytime.Pydantic(pytime.UTC(p.CreatedAt)))
	out.Set("updated_at", pytime.Pydantic(pytime.UTC(p.UpdatedAt)))
	return out, nil
}

// fail logs a failure with its step and writes the Python api's bare 500.
func (h handlers) fail(w http.ResponseWriter, r *http.Request, step string, err error) {
	h.Logger.ErrorContext(r.Context(), "api sso route failed", "path", r.URL.Path, "step", step, "error", err.Error())
	policy.WriteInternal(w)
}
