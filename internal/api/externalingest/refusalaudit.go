package externalingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/internal/api/audit"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The class-free reasons a known ingest token is refused for: error_message of the audit row and the label of
// the write-failure counter.
const (
	refusalTokenRevoked       = "token_revoked"
	refusalTokenExpired       = "token_expired"
	refusalInsufficientScope  = "insufficient_scope"
	refusalFeatureNotEnabled  = "feature_not_enabled"
	refusalOverFailureLimit   = "over_failure_limit"
	refusalAuditWriteDeadline = 5 * time.Second
	refusalAuditDescription   = "Ingest authentication refused"
)

// auditFailures counts the refusals whose audit row could not be written, by reason.
var auditFailures = func() metric.Int64Counter {
	const name = "dev_health_api_ingest_auth_audit_failures_total"
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/externalingest").Int64Counter(
		name, metric.WithDescription("External-ingest refusals of a known token whose audit row could not be written, by reason"))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}()

// recordKnownRefusal takes the failure-limiter hit of a refusal decided from a token row, then audits it. The hit
// comes first and decides: a hit the limiter does not admit is a refusal beyond the address's failure limit, so no
// row is written (the rows per address are bounded by that limit, also under concurrency) and the drop is counted
// under over_failure_limit; the answer is the same either way. A limiter store that cannot answer is the unhandled
// 500 and writes no row. It returns that 500, or nil when the caller answers its refusal.
func (d Deps) recordKnownRefusal(
	ctx context.Context, r *http.Request, ip, reason, tokenID, orgID string, sourceID *string, requiredScope string,
) *ingestError {
	admitted, err := d.limiters.failure.AllowCounted(ctx, ip, "")
	if err != nil {
		return unhandledError()
	}
	if !admitted {
		auditFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", refusalOverFailureLimit)))
		return nil
	}
	d.auditRefusal(ctx, r, reason, tokenID, orgID, sourceID, requiredScope)
	return nil
}

// auditRefusal records the refusal of a token whose row was read: the organisation and the token come from
// that row, never from the request. It runs before the answer is written and never changes it: a write that
// fails is logged (the reason, the token id and the SQLSTATE or the Go type of the error, never its text, which
// can hold row values) and counted. The write outlives a client that has gone.
func (d Deps) auditRefusal(ctx context.Context, r *http.Request, reason, tokenID, orgID string, sourceID *string, requiredScope string) {
	err := d.writeRefusalAudit(ctx, r, reason, tokenID, orgID, sourceID, requiredScope)
	if err == nil {
		return
	}
	attributes := []any{"reason", reason, "token_id", tokenID}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		attributes = append(attributes, "sqlstate", pgErr.Code)
	} else {
		attributes = append(attributes, "error_type", fmt.Sprintf("%T", err))
	}
	d.logger().WarnContext(ctx, "external-ingest refusal audit not written", attributes...)
	auditFailures.Add(ctx, 1, metric.WithAttributes(attribute.String("reason", reason)))
}

func (d Deps) writeRefusalAudit(ctx context.Context, r *http.Request, reason, tokenID, orgID string, sourceID *string, requiredScope string) error {
	org, err := uuid.Parse(orgID)
	if err != nil {
		return errors.New("token org id is not a uuid")
	}
	metadata := pyjson.NewObject()
	metadata.Set("method", r.Method)
	if route := routeTemplate(r); route != "" {
		metadata.Set("route", route)
	}
	if requiredScope != "" {
		metadata.Set("required_scope", requiredScope)
	}
	if sourceID != nil {
		metadata.Set("source_id", *sourceID)
	}
	encoded, err := pyjson.Dumps(metadata)
	if err != nil {
		return err
	}
	description := refusalAuditDescription
	entry := audit.Entry{
		OrgID:           org,
		Action:          audit.ActionIngestAuthFailed,
		ResourceType:    audit.ResourceExternalIngestToken,
		ResourceID:      tokenID,
		Description:     &description,
		RequestMetadata: []byte(encoded),
		Status:          "failure",
		ErrorMessage:    &reason,
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refusalAuditWriteDeadline)
	defer cancel()
	_, err = audit.PGWriter{Now: d.Now}.Write(writeCtx, d.Pool, entry)
	return err
}

// routeTemplate is the matched route pattern without its method ("/api/v1/external-ingest/batches/{ingestion_id}").
func routeTemplate(r *http.Request) string {
	_, route, found := strings.Cut(r.Pattern, " ")
	if !found {
		return r.Pattern
	}
	return route
}
