// Package entitlements is the one implementation of per-organization feature
// overrides (org_feature_overrides): grant, list and revoke. The superuser admin
// API (internal/apiservice/admin) and the dho admin verbs both call it.
package entitlements

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// DB is the part of a pgx pool or connection the operations use.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Override is an org_feature_overrides row with its flag's key. Config is the
// column's raw JSON text.
type Override struct {
	ID, OrgID, FeatureID uuid.UUID
	FeatureKey           string
	IsEnabled            bool
	ExpiresAt            *time.Time
	Config               []byte
	Reason               *string
	CreatedBy, UpdatedBy *uuid.UUID
	CreatedAt            time.Time
}

// Columns is the select list Scan reads (the table aliased o, the flag aliased f).
const Columns = `o.id, o.org_id, o.feature_id, f.key, o.is_enabled, o.expires_at, o.config, o.reason, o.created_by, o.updated_by, o.created_at`

// Scan reads one row of Columns. A missing row is (nil, nil).
func Scan(row pgx.Row) (*Override, error) {
	var o Override
	err := row.Scan(&o.ID, &o.OrgID, &o.FeatureID, &o.FeatureKey, &o.IsEnabled, &o.ExpiresAt, &o.Config,
		&o.Reason, &o.CreatedBy, &o.UpdatedBy, &o.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}

// FeatureIDByKey is the id of the flag with this key; ok is false when none exists.
func FeatureIDByKey(ctx context.Context, db DB, key string) (id uuid.UUID, ok bool, err error) {
	err = db.QueryRow(ctx, `SELECT id FROM feature_flags WHERE key = $1`, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, false, nil
	}
	return id, err == nil, err
}

// Exists reports whether the organization already has an override for the flag.
func Exists(ctx context.Context, db DB, orgID, featureID uuid.UUID) (bool, error) {
	var exists bool
	err := db.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM org_feature_overrides WHERE org_id = $1 AND feature_id = $2)`, orgID, featureID).Scan(&exists)
	return exists, err
}

// GrantParams is one override to insert. ConfigJSON "" stores the empty object.
type GrantParams struct {
	ID, OrgID, FeatureID uuid.UUID
	IsEnabled            bool
	ExpiresAt            *time.Time
	ConfigJSON           string
	Reason               *string
	CreatedBy            *uuid.UUID
	Now                  time.Time
}

// Grant inserts the override. The table's unique (org_id, feature_id) key makes a
// second grant of the same flag fail in the database.
func Grant(ctx context.Context, db DB, p GrantParams) error {
	config := p.ConfigJSON
	if config == "" {
		config = "{}"
	}
	_, err := db.Exec(ctx, `
INSERT INTO org_feature_overrides
	(id, org_id, feature_id, is_enabled, expires_at, config, reason, created_by, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6::json, $7, $8, $9, $9)`,
		p.ID, p.OrgID, p.FeatureID, p.IsEnabled, p.ExpiresAt, config, p.Reason, p.CreatedBy, p.Now.UTC())
	return err
}

// List is every override of the organization, newest first.
func List(ctx context.Context, db DB, orgID uuid.UUID) ([]*Override, error) {
	rows, err := db.Query(ctx, `SELECT `+Columns+`
FROM org_feature_overrides o JOIN feature_flags f ON o.feature_id = f.id
WHERE o.org_id = $1 ORDER BY o.created_at DESC, f.key ASC`, orgID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []*Override
	for rows.Next() {
		o, err := Scan(rows)
		if err != nil {
			return nil, err
		}
		list = append(list, o)
	}
	return list, rows.Err()
}

// Revoke deletes overrides of the organization and returns how many rows went.
// Exactly one of overrideID and featureKey selects them; the organization always
// bounds the delete, so another organization's override is never touched.
func Revoke(ctx context.Context, db DB, orgID uuid.UUID, overrideID *uuid.UUID, featureKey string) (int64, error) {
	if (overrideID == nil) == (featureKey == "") {
		return 0, errors.New("entitlements: revoke needs exactly one of an override id and a feature key")
	}
	var (
		tag pgconn.CommandTag
		err error
	)
	if overrideID != nil {
		tag, err = db.Exec(ctx, `DELETE FROM org_feature_overrides WHERE id = $1 AND org_id = $2`, *overrideID, orgID)
	} else {
		tag, err = db.Exec(ctx, `DELETE FROM org_feature_overrides WHERE org_id = $1
AND feature_id IN (SELECT id FROM feature_flags WHERE key = $2)`, orgID, featureKey)
	}
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
