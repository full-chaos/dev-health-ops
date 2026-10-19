// Package queuecontrol holds the queue-control endpoint mode shared by
// internal/platform/config and internal/storage/postgres. It is a leaf so the
// auth binaries reach it without linking the platform config package.
package queuecontrol

// Mode describes the endpoint semantics promised by the operator.
// Direct PostgreSQL and PgBouncer session pooling preserve River's required
// session semantics. Transaction pooling cannot propagate cancellation to a
// running River worker.
type Mode string

const (
	Direct      Mode = "direct"
	Session     Mode = "session"
	Transaction Mode = "transaction"
)
