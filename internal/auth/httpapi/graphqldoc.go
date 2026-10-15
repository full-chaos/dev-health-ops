package httpapi

import (
	"context"
	"log/slog"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
)

// The access line and the server span of a GraphQL request name the document
// by its digest and registered operation, so a readback can count status per
// document. Never the query text, a variable, or a client-chosen operation
// name.
const (
	GraphQLDigestAttribute    = "dev_health.graphql.document_digest"
	GraphQLOperationAttribute = "dev_health.graphql.operation_name"
	GraphQLDigestReasonAttr   = "dev_health.graphql.digest_reason"
)

// GraphQLDigestReason says why a request carries (or lacks) a registered
// digest: a CLOSED enum, never free text.
type GraphQLDigestReason string

const (
	// GraphQLDigestRegistered: the digest maps to a registered operation.
	GraphQLDigestRegistered GraphQLDigestReason = "registered"
	// GraphQLDigestUnregistered: a document was read but its digest is not registered.
	GraphQLDigestUnregistered GraphQLDigestReason = "unregistered"
	// GraphQLDigestMissingDocument: the request carried no document text.
	GraphQLDigestMissingDocument GraphQLDigestReason = "missing_document"
	// GraphQLDigestNotResolved: the request was refused (method, size, identity,
	// body shape) before a document was resolved.
	GraphQLDigestNotResolved GraphQLDigestReason = "not_resolved"
)

type graphQLDocument struct {
	digest, operation string
	reason            GraphQLDigestReason
}

type graphQLDocumentKey struct{}

// graphQLDocumentSink is the per-request cell, embedded in the structs both
// observers already allocate per request.
type graphQLDocumentSink struct {
	doc atomic.Pointer[graphQLDocument]
}

func (s *graphQLDocumentSink) load() *graphQLDocument { return s.doc.Load() }

func sinkFrom(ctx context.Context) *graphQLDocumentSink {
	if match, ok := ctx.Value(observedRouteKey{}).(*observedRoute); ok {
		return &match.graphQL
	}
	if recorder, ok := ctx.Value(notFoundCauseKey{}).(*accessRecorder); ok {
		return &recorder.graphQL
	}
	return nil
}

// RecordGraphQLDocument records the document a /graphql request carried, for
// the access line and the server span. The last call wins. digest is kept only
// when it is 64 hex characters (anything else is recorded as empty), and an
// unknown reason is recorded as not_resolved, so no free text can reach a log
// line or an attribute. A no-op on a context without an observer.
func RecordGraphQLDocument(ctx context.Context, digest, operation string, reason GraphQLDigestReason) {
	sink := sinkFrom(ctx)
	if sink == nil {
		return
	}
	if len(digest) != 64 || !allHex(digest) {
		digest = ""
	}
	switch reason {
	case GraphQLDigestRegistered, GraphQLDigestUnregistered, GraphQLDigestMissingDocument, GraphQLDigestNotResolved:
	default:
		reason = GraphQLDigestNotResolved
	}
	sink.doc.Store(&graphQLDocument{digest: digest, operation: operation, reason: reason})
}

func (d *graphQLDocument) logAttrs() []slog.Attr {
	if d == nil {
		return nil
	}
	return []slog.Attr{
		slog.String("graphql_digest", d.digest),
		slog.String("graphql_operation", d.operation),
		slog.String("graphql_digest_reason", string(d.reason)),
	}
}

func (d *graphQLDocument) spanAttrs() []attribute.KeyValue {
	if d == nil {
		return nil
	}
	return []attribute.KeyValue{
		attribute.String(GraphQLDigestAttribute, d.digest),
		attribute.String(GraphQLOperationAttribute, d.operation),
		attribute.String(GraphQLDigestReasonAttr, string(d.reason)),
	}
}
