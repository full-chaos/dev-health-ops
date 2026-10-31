// Package publicaudit is auth-service's one writer of go-api's audit row: public.audit_logs, the row
// internal/api/audit.PGWriter writes. Every auth-service flow that took a route over from go-api writes that route's
// row here, with PGWriter's columns and Go value types, so both services write the same row for the same event
// (D6509: the auth runtime role holds INSERT on exactly these columns, authschema.PublicAuditWriteColumns).
package publicaudit

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

// Execer is the subset of a pool or a transaction a row needs.
type Execer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

// Row is one audit_logs row in internal/api/audit.Entry's terms. Changes and RequestMetadata are pre-encoded JSON
// (pyjson, Python spacing and key order); nil is "{}", as AuditLog stores None. Status "" is "success".
type Row struct {
	OrgID           uuid.UUID
	UserID          *uuid.UUID
	Action          string
	ResourceType    string
	ResourceID      string
	Description     *string
	Changes         []byte
	RequestMetadata []byte
	Status          string
	ErrorMessage    *string
	CreatedAt       time.Time
}

const insertSQL = `INSERT INTO public.audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description,
	 changes, request_metadata, status, error_message, created_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`

// Insert writes row through exec with PGWriter's argument values and types: a new id, the JSON columns as []byte
// ("{}" when empty), status "success" when empty, created_at in UTC.
func Insert(ctx context.Context, exec Execer, row Row) error {
	status := row.Status
	if status == "" {
		status = "success"
	}
	_, err := exec.Exec(ctx, insertSQL,
		uuid.New(), row.OrgID, row.UserID, row.Action, row.ResourceType, row.ResourceID, row.Description,
		orEmptyObject(row.Changes), orEmptyObject(row.RequestMetadata), status, row.ErrorMessage, row.CreatedAt.UTC())
	return err
}

const insertTextJSONSQL = `INSERT INTO public.audit_logs
	(id, org_id, user_id, action, resource_type, resource_id, description,
	 changes, request_metadata, status, error_message, created_at)
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8::json, $9::json, $10, $11, $12)`

// InsertInAuditTx is Insert through an audited transaction (audit.TxOps). Its statements run in pgx's exec mode,
// which sends each argument with the type of its Go value, so a []byte would arrive as bytea: the two JSON columns
// go as text with an explicit ::json cast instead. The stored row is the same: json keeps the text verbatim.
func InsertInAuditTx(ctx context.Context, exec Execer, row Row) error {
	status := row.Status
	if status == "" {
		status = "success"
	}
	_, err := exec.Exec(ctx, insertTextJSONSQL,
		uuid.New(), row.OrgID, row.UserID, row.Action, row.ResourceType, row.ResourceID, row.Description,
		string(orEmptyObject(row.Changes)), string(orEmptyObject(row.RequestMetadata)), status, row.ErrorMessage, row.CreatedAt.UTC())
	return err
}

func orEmptyObject(raw []byte) []byte {
	if len(raw) == 0 {
		return []byte("{}")
	}
	return raw
}
