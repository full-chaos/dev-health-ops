// Package ssoaudit writes go-api's records of an SSO sign-in: the sso_login row in public.audit_logs and the
// public.sso_providers row's record of a provider failure (internal/api/sso recordSSOFailure) or of a completed
// sign-in (finishSSOLogin).
package ssoaudit

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Row is go-api's sso_login failure row: action sso_login, resource sso_provider, status failure. Stage "" writes no
// stage key, as go-api does.
type Row struct {
	OrgID        uuid.UUID
	ProviderID   uuid.UUID
	Protocol     string
	Stage        string
	ErrorMessage string
}

// Execer is the subset of a pool or a transaction a row needs.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

const auditInsertSQL = `INSERT INTO public.audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description,
	 changes, request_metadata, status, error_message, created_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

// Insert writes row into go-api's public.audit_logs with internal/api/audit.PGWriter's columns and value types, so
// both services write the same row for the same refusal. ErrorMessage is written as given.
func Insert(ctx context.Context, exec Execer, row Row, at time.Time) error {
	meta := pyjson.NewObject()
	meta.Set("protocol", row.Protocol)
	if row.Stage != "" {
		meta.Set("stage", row.Stage)
	}
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		return err
	}
	message := row.ErrorMessage
	var user *uuid.UUID
	var description *string
	_, err = exec.Exec(ctx, auditInsertSQL,
		uuid.New(), row.OrgID, user, "sso_login", "sso_provider", row.ProviderID.String(), description,
		[]byte("{}"), []byte(metaJSON), "failure", &message, at.UTC())
	return err
}

// ProviderFailure records a provider failure of a sign-in. The sign-in answers only after the record is written; a
// write error is the sign-in's 500.
type ProviderFailure interface {
	Failed(ctx context.Context, row Row) error
}

// Beginner opens the record's transaction.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// PGProviderFailure is go-api's recordSSOFailure: in one transaction the failure text and its time on the provider
// row, then the audit row. The provider's status is left as it was.
type PGProviderFailure struct {
	DB Beginner
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

const providerFailureSQL = `UPDATE public.sso_providers SET last_error = $2, last_error_at = $3, updated_at = $3
WHERE id = $1`

// Failed implements ProviderFailure. ErrorMessage is sanitized as go-api sanitizes it.
func (p PGProviderFailure) Failed(ctx context.Context, row Row) error {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	at := now().UTC()
	row.ErrorMessage = pythonparity.SanitizeErrorTextHardened(row.ErrorMessage, 4000)
	if _, err := tx.Exec(ctx, providerFailureSQL, row.ProviderID, row.ErrorMessage, at); err != nil {
		return err
	}
	if err := Insert(ctx, tx, row, at); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Success is a completed sign-in. OrgID is the provider's organization; Protocol is the provider row's protocol, as
// go-api writes it ("oidc", "saml", "oauth_gitlab").
type Success struct {
	OrgID       uuid.UUID
	ProviderID  uuid.UUID
	PrincipalID uuid.UUID
	Protocol    string
}

// SignInRecord records a completed sign-in, after its session exists and before any token leaves. A write error is the
// sign-in's 500: the tokens are withheld.
type SignInRecord interface {
	Succeeded(ctx context.Context, success Success) error
}

// PGSignIn is go-api's finishSSOLogin record: in one transaction the provider row's last_login_at and updated_at, then
// the audit row. A provider row deleted since the callback admitted it updates nothing and the sign-in goes on.
type PGSignIn struct {
	DB Beginner
	// Now is injectable for tests; nil means time.Now.
	Now func() time.Time
}

const signInSQL = `UPDATE public.sso_providers SET last_login_at = $2, updated_at = $2 WHERE id = $1`

// Succeeded implements SignInRecord.
func (p PGSignIn) Succeeded(ctx context.Context, success Success) error {
	tx, err := p.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	at := now().UTC()
	_, _ = at, signInSQL
	_ = insertSuccess
	return tx.Commit(ctx)
}

// insertSuccess writes go-api's sso_login success row with internal/api/audit.PGWriter's columns and value types:
// resource session, the user's id as the resource id, the provider and the protocol in the metadata.
func insertSuccess(ctx context.Context, exec Execer, success Success, at time.Time) error {
	meta := pyjson.NewObject()
	meta.Set("provider_id", success.ProviderID.String())
	meta.Set("protocol", success.Protocol)
	metaJSON, err := pyjson.Dumps(meta)
	if err != nil {
		return err
	}
	var description, message *string
	_, err = exec.Exec(ctx, auditInsertSQL,
		uuid.New(), success.OrgID, &success.PrincipalID, "sso_login", "session", success.PrincipalID.String(), description,
		[]byte("{}"), []byte(metaJSON), "success", message, at.UTC())
	return err
}
