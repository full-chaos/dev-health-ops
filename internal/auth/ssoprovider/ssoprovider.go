// Package ssoprovider is auth-service's writer of public.sso_providers, the SSO provider management go-api does not serve
// (the provider CRUD there is a fail-loud stub). It keeps the legacy SSOService semantics (src/dev_health_ops/api/services/sso.py
// and api/auth/sso/router.py at 64efc37bb3^): a created provider is pending_setup; a default provider clears the default
// flag of the organization's other providers of the same protocol; an update touches only the fields it names and sets
// updated_at; activate and deactivate set the status; delete removes the row (users.sso_provider_id becomes NULL by its
// foreign key; replay and flow rows stay). The client secret is sealed with the SSO cipher and never leaves the store: a
// provider the store returns carries the masked config the legacy response showed, never the secret or its sealed form.
// Every write is one audit.Commit with its own audit row.
package ssoprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/audit"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// Statuses are the legacy SSOProviderStatus values.
const (
	StatusActive       = "active"
	StatusInactive     = "inactive"
	StatusPendingSetup = "pending_setup"
)

// Protocols are the two protocols a provider can be created with.
const (
	ProtocolSAML = "saml"
	ProtocolOIDC = "oidc"
)

// ErrNotFound is the provider that does not exist in the organization.
var ErrNotFound = errors.New("ssoprovider: provider not found")

// ErrStore is a store fault.
var ErrStore = errors.New("ssoprovider: store fault")

// Provider is one public.sso_providers row as the legacy response showed it: the config is the stored object in its stored key
// order, masked (a certificate is cut to 50 characters and "...", a client_secret key reads "********") and written as go-api
// writes a response model; the sealed secret is never part of it.
type Provider struct {
	ID, OrgID                                    string
	Name, Protocol, Status, DefaultRole          string
	IsDefault, AllowIdpInitiated, AutoProvision  bool
	Config                                       json.RawMessage
	AllowedDomains                               []string
	LastMetadataSyncAt, LastLoginAt, LastErrorAt *time.Time
	LastError                                    *string
	CreatedAt, UpdatedAt                         time.Time
}

// Actor names who a write acts for and the credential it came with; the audit row carries both.
type Actor struct {
	PrincipalID *uuid.UUID
	RequestID   *string
	Caller      audit.Caller
}

// Filter is the list query: legacy protocol and status filters, and the page.
type Filter struct {
	Protocol, Status string
	Limit, Offset    int64
}

// Config is the stored config of one create or update: the JSON the provider row keeps, and the sealed secret object (nil
// when the update names no oidc_config).
type Config struct {
	JSON    *pyjson.Object
	Secrets *string
}

// CreateInput is a create.
type CreateInput struct {
	Name, Protocol, DefaultRole                 string
	Config                                      Config
	IsDefault, AllowIdpInitiated, AutoProvision bool
	AllowedDomains                              []string
}

// UpdateInput is an update: a nil field is left alone (the legacy PATCH semantics).
type UpdateInput struct {
	Name                                        *string
	Config                                      *Config
	IsDefault, AllowIdpInitiated, AutoProvision *bool
	DefaultRole                                 *string
	AllowedDomains                              *[]string
}

// Store reads and writes public.sso_providers. Pool is the auth-service pool (the one audit.Commit runs on).
type Store struct {
	Pool   *pgxpool.Pool
	Schema authschema.ValidatedIdentifier
	Now    func() time.Time
}

func (s Store) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

const columns = `id::text, org_id::text, name, protocol, status, default_role, is_default, allow_idp_initiated,
	auto_provision_users, config::text, allowed_domains::text, last_metadata_sync_at, last_login_at, last_error_at,
	last_error, created_at, updated_at`

type scanner interface{ Scan(dest ...any) error }

func scan(row scanner) (*Provider, error) {
	var p Provider
	var config, domains *string
	if err := row.Scan(&p.ID, &p.OrgID, &p.Name, &p.Protocol, &p.Status, &p.DefaultRole, &p.IsDefault, &p.AllowIdpInitiated,
		&p.AutoProvision, &config, &domains, &p.LastMetadataSyncAt, &p.LastLoginAt, &p.LastErrorAt, &p.LastError, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return nil, err
	}
	var err error
	if p.Config, err = MaskedConfig(config); err != nil {
		return nil, err
	}
	p.AllowedDomains = []string{}
	if domains != nil {
		var list []*string
		if err := json.Unmarshal([]byte(*domains), &list); err != nil {
			return nil, fmt.Errorf("%w: the stored allowed_domains is not a list", ErrStore)
		}
		for _, item := range list {
			if item != nil {
				p.AllowedDomains = append(p.AllowedDomains, *item)
			}
		}
	}
	return &p, nil
}

// MaskedConfig is the stored config text as the legacy response showed it: the object's keys in the stored order, a null or
// absent config as {}, masked and written in the response-model form (compact, not ASCII-escaped).
func MaskedConfig(stored *string) (json.RawMessage, error) {
	config := pyjson.NewObject()
	if stored != nil {
		decoded, err := pyjson.DecodeString(*stored)
		if err != nil {
			return nil, fmt.Errorf("%w: the stored config is not JSON", ErrStore)
		}
		switch value := decoded.(type) {
		case nil:
		case *pyjson.Object:
			config = value
		default:
			return nil, fmt.Errorf("%w: the stored config is not a JSON object", ErrStore)
		}
	}
	mask(config)
	raw, err := pyjson.MarshalModel(config)
	if err != nil {
		return nil, fmt.Errorf("%w: the stored config cannot be written", ErrStore)
	}
	return raw, nil
}

// mask is the legacy _provider_to_response masking of the stored config.
func mask(config *pyjson.Object) {
	if _, ok := config.Get("client_secret"); ok {
		config.Set("client_secret", "********")
	}
	if value, ok := config.Get("certificate"); ok {
		if certificate, isText := value.(string); isText {
			if runes := pyjson.Runes(certificate); len(runes) > 50 {
				config.Set("certificate", pyjson.FromRunes(runes[:50])+"...")
			}
		}
	}
}

func fault(err error) error {
	if err == nil || errors.Is(err, ErrNotFound) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrStore, err)
}

// List is the legacy list_providers: the organization's providers, filtered, oldest first, with the unfiltered-by-page total.
func (s Store) List(ctx context.Context, org uuid.UUID, f Filter) ([]Provider, int64, error) {
	where, args := `org_id = $1`, []any{org}
	if f.Protocol != "" {
		args = append(args, f.Protocol)
		where += fmt.Sprintf(` AND protocol = $%d`, len(args))
	}
	if f.Status != "" {
		args = append(args, f.Status)
		where += fmt.Sprintf(` AND status = $%d`, len(args))
	}
	var total int64
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM public.sso_providers WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, fault(err)
	}
	args = append(args, f.Limit, f.Offset)
	rows, err := s.Pool.Query(ctx, fmt.Sprintf(`SELECT %s FROM public.sso_providers WHERE %s ORDER BY created_at, id LIMIT $%d OFFSET $%d`,
		columns, where, len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, fault(err)
	}
	defer rows.Close()
	items := []Provider{}
	for rows.Next() {
		p, err := scan(rows)
		if err != nil {
			return nil, 0, fault(err)
		}
		items = append(items, *p)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fault(err)
	}
	return items, total, nil
}

// Get is the legacy get_provider: the provider of this organization, or ErrNotFound.
func (s Store) Get(ctx context.Context, org, id uuid.UUID) (*Provider, error) {
	p, err := scan(s.Pool.QueryRow(ctx, `SELECT `+columns+` FROM public.sso_providers WHERE id = $1 AND org_id = $2`, id, org))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return p, fault(err)
}

func (s Store) auditEvent(eventType string, org, id uuid.UUID, a Actor, attributes map[string]any) audit.AuditEvent {
	attributes["target_kind"] = "sso_provider"
	attributes["target_id"] = id.String()
	a.Caller.Annotate(attributes)
	orgID := org.String()
	event := audit.AuditEvent{EventType: eventType, Outcome: audit.OutcomeAllowed, OrganizationID: &orgID, Attributes: attributes, RequestID: a.RequestID}
	if a.PrincipalID != nil {
		actor := a.PrincipalID.String()
		event.ActorPrincipalID = &actor
	}
	return event
}

// clearDefault is the legacy _clear_default_provider: no other provider of the organization and protocol stays default.
func clearDefault(ctx context.Context, tx audit.TxOps, org uuid.UUID, protocol string, except *uuid.UUID, now time.Time) error {
	if except == nil {
		_, err := tx.Exec(ctx, `UPDATE public.sso_providers SET is_default = false, updated_at = $3 WHERE org_id = $1 AND protocol = $2 AND is_default`, org, protocol, now)
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE public.sso_providers SET is_default = false, updated_at = $3 WHERE org_id = $1 AND protocol = $2 AND is_default AND id <> $4`, org, protocol, now, *except)
	return err
}

// marshalConfig is the stored text of a config: json.dumps of the model dump, as the legacy writer stored it.
func marshalConfig(config *pyjson.Object) (string, error) {
	return pyjson.Dumps(config)
}

// Create is the legacy create_provider: a pending_setup provider; a default one clears the others' flag first.
func (s Store) Create(ctx context.Context, org uuid.UUID, in CreateInput, a Actor) (*Provider, error) {
	id := uuid.New()
	now := s.now()
	config, err := marshalConfig(in.Config.JSON)
	if err != nil {
		return nil, fault(err)
	}
	domains, err := json.Marshal(in.AllowedDomains)
	if err != nil {
		return nil, fault(err)
	}
	err = audit.Commit(ctx, s.Pool, s.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			if in.IsDefault {
				if err := clearDefault(ctx, tx, org, in.Protocol, nil, now); err != nil {
					return err
				}
			}
			_, err := tx.Exec(ctx, `INSERT INTO public.sso_providers (id, org_id, name, protocol, status, default_role, is_default, allow_idp_initiated,
	auto_provision_users, config, encrypted_secrets, allowed_domains, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::json, $11::json, $12::json, $13, $13)`,
				id, org, in.Name, in.Protocol, StatusPendingSetup, in.DefaultRole, in.IsDefault, in.AllowIdpInitiated, in.AutoProvision,
				config, in.Config.Secrets, string(domains), now)
			return err
		},
		Audit: s.auditEvent("auth.sso_provider.created", org, id, a, map[string]any{"reason_code": "sso_provider_created", "provider": in.Protocol}),
	})
	if err != nil {
		return nil, fault(err)
	}
	return s.Get(ctx, org, id)
}

// Update is the legacy update_provider: only the named fields change, updated_at moves, a default flag clears the others'.
func (s Store) Update(ctx context.Context, org, id uuid.UUID, in UpdateInput, a Actor) (*Provider, error) {
	now := s.now()
	err := audit.Commit(ctx, s.Pool, s.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var protocol string
			if err := tx.QueryRow(ctx, `SELECT protocol FROM public.sso_providers WHERE id = $1 AND org_id = $2 FOR UPDATE`, id, org).Scan(&protocol); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return err
			}
			sets, args := []string{}, []any{id, org, now}
			set := func(column, cast string, value any) {
				args = append(args, value)
				sets = append(sets, fmt.Sprintf("%s = $%d%s", column, len(args), cast))
			}
			if in.Name != nil {
				set("name", "", *in.Name)
			}
			if in.Config != nil {
				config, err := marshalConfig(in.Config.JSON)
				if err != nil {
					return err
				}
				set("config", "::json", config)
				if in.Config.Secrets != nil {
					set("encrypted_secrets", "::json", *in.Config.Secrets)
				}
			}
			if in.IsDefault != nil {
				if *in.IsDefault {
					if err := clearDefault(ctx, tx, org, protocol, &id, now); err != nil {
						return err
					}
				}
				set("is_default", "", *in.IsDefault)
			}
			if in.AllowIdpInitiated != nil {
				set("allow_idp_initiated", "", *in.AllowIdpInitiated)
			}
			if in.AutoProvision != nil {
				set("auto_provision_users", "", *in.AutoProvision)
			}
			if in.DefaultRole != nil {
				set("default_role", "", *in.DefaultRole)
			}
			if in.AllowedDomains != nil {
				domains, err := json.Marshal(*in.AllowedDomains)
				if err != nil {
					return err
				}
				set("allowed_domains", "::json", string(domains))
			}
			statement := `UPDATE public.sso_providers SET updated_at = $3`
			for _, clause := range sets {
				statement += `, ` + clause
			}
			_, err := tx.Exec(ctx, statement+` WHERE id = $1 AND org_id = $2`, args...)
			return err
		},
		Audit:    s.auditEvent("auth.sso_provider.updated", org, id, a, map[string]any{"reason_code": "sso_provider_updated"}),
		Refusals: []error{ErrNotFound},
	})
	if err != nil {
		return nil, fault(err)
	}
	return s.Get(ctx, org, id)
}

// SetStatus is activate and deactivate: the legacy update_provider with only the status.
func (s Store) SetStatus(ctx context.Context, org, id uuid.UUID, status string, a Actor) (*Provider, error) {
	if status != StatusActive && status != StatusInactive {
		return nil, fmt.Errorf("%w: status %q", ErrStore, status)
	}
	now := s.now()
	reason, eventType := "sso_provider_activated", "auth.sso_provider.activated"
	if status == StatusInactive {
		reason, eventType = "sso_provider_deactivated", "auth.sso_provider.deactivated"
	}
	err := audit.Commit(ctx, s.Pool, s.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			var previous string
			if err := tx.QueryRow(ctx, `SELECT status FROM public.sso_providers WHERE id = $1 AND org_id = $2 FOR UPDATE`, id, org).Scan(&previous); err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrNotFound
				}
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE public.sso_providers SET status = $3, updated_at = $4 WHERE id = $1 AND org_id = $2`, id, org, status, now)
			return err
		},
		Audit:    s.auditEvent(eventType, org, id, a, map[string]any{"reason_code": reason, "new_status": statusAttribute(status)}),
		Refusals: []error{ErrNotFound},
	})
	if err != nil {
		return nil, fault(err)
	}
	return s.Get(ctx, org, id)
}

// statusAttribute maps a provider status onto the closed audit status vocabulary.
func statusAttribute(status string) string {
	if status == StatusActive {
		return "active"
	}
	return "disabled"
}

// Delete is the legacy delete_provider: the row goes; users.sso_provider_id becomes NULL through its foreign key.
func (s Store) Delete(ctx context.Context, org, id uuid.UUID, a Actor) error {
	err := audit.Commit(ctx, s.Pool, s.Schema, audit.Mutation{
		Apply: func(ctx context.Context, tx audit.TxOps) error {
			tag, err := tx.Exec(ctx, `DELETE FROM public.sso_providers WHERE id = $1 AND org_id = $2`, id, org)
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return ErrNotFound
			}
			return nil
		},
		Audit:    s.auditEvent("auth.sso_provider.deleted", org, id, a, map[string]any{"reason_code": "sso_provider_deleted"}),
		Refusals: []error{ErrNotFound},
	})
	return fault(err)
}
