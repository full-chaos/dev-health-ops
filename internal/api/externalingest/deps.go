// Package externalingest is the `dho api` route area for
// /api/v1/external-ingest/*: the customer-push ingestion contract (Python
// src/dev_health_ops/api/external_ingest/). It owns its own bearer-token
// authentication (fcpush_... against external_ingest_tokens/_sources) --
// deliberately NOT the shared session/org middleware every other api area
// uses, since these callers are customer scripts and CI jobs, not logged-in
// users.
//
// Every port here states its scope explicitly where it narrows the Python
// original. ValidateRecords (validate.go, records.go) is exact: codes,
// messages and paths, pinned by a live oracle over every record kind.
// matchesInstance
// (ownership.go) covers the explicit-source-row precedence, the managed-
// sync string-match override, and the operational-GitHub/GitLab host match
// against an explicitly configured or default host, not the narrower
// sub-case of a self-hosted instance whose host is known only via a
// decrypted managed credential (see ownership.go's doc comment).
package externalingest

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// Limits mirrors router.py's _limits_payload(): the live, env-overridable
// ingest limits (EXTERNAL_INGEST_MAX_RECORDS / EXTERNAL_INGEST_MAX_BODY_BYTES).
type Limits struct {
	MaxRecords   int
	MaxBodyBytes int
}

// DefaultLimits are schemas.py's MAX_RECORDS_DEFAULT/MAX_BODY_BYTES_DEFAULT.
var DefaultLimits = Limits{MaxRecords: 1000, MaxBodyBytes: 10_000_000}

// envLimits reads EXTERNAL_INGEST_MAX_RECORDS/EXTERNAL_INGEST_MAX_BODY_BYTES,
// matching router.py's _max_records()/_max_body_bytes() (os.environ.get with
// the same defaults). An unset or unparseable value falls back to the
// default for that one field independently -- a malformed MAX_RECORDS must
// not also silently reset MAX_BODY_BYTES.
func envLimits() Limits {
	limits := DefaultLimits
	if raw := os.Getenv("EXTERNAL_INGEST_MAX_RECORDS"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limits.MaxRecords = parsed
		}
	}
	if raw := os.Getenv("EXTERNAL_INGEST_MAX_BODY_BYTES"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limits.MaxBodyBytes = parsed
		}
	}
	return limits
}

// Deps are this area's dependencies, handed down from apiservice (which
// owns the one shared Postgres pool and Valkey client every api area is
// built from -- see internal/apiservice/deps.go).
type Deps struct {
	Pool   *pgxpool.Pool
	Valkey valkeygo.Client
	// Limits overrides envLimits() for tests. Production callers leave this
	// zero and get the live env-read value, matching router.py reading
	// EXTERNAL_INGEST_MAX_RECORDS/_MAX_BODY_BYTES fresh rather than caching
	// it at startup.
	Limits Limits
	Now    func() time.Time
	Logger *slog.Logger
	// Counters is where the six route limits count: the api's shared counter
	// store (Valkey when configured, so a limit holds across replicas); nil
	// means an in-process one on Now.
	Counters httpapi.CounterStore

	limiters      *authLimiters
	routeLimiters *routeLimiters
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d Deps) limits() Limits {
	if d.Limits.MaxRecords != 0 || d.Limits.MaxBodyBytes != 0 {
		return d.Limits
	}
	return envLimits()
}

func (d Deps) logger() *slog.Logger {
	if d.Logger != nil {
		return d.Logger
	}
	return slog.Default()
}

// Routes builds the 7 /api/v1/external-ingest/* routes (router.py + status.py),
// mounted by internal/apiservice.Routes. Per-route/per-caller rate limiting
// (rate_limit.py's INGEST_READ_LIMIT/_VALIDATE_LIMIT/_BATCH_LIMIT) is
// enforced INSIDE each handler (ratelimit.go's routeLimiters), not via
// httpapi.Route's RateLimitPerSecond/Burst: that mechanism is one bucket
// shared by every caller of a route (httpapi.Bucket's own doc comment), the
// wrong shape for Python's per-IP (schema discovery, unauthenticated) or
// per-validated-token (everything else) keying -- and the token key is only
// known after requireIngestScope resolves, which a route-level, pre-auth
// bucket cannot see. GET /availability carries no rate limit in Python
// either (router.py has no @limiter.limit on it) and gets none here.
func Routes(deps Deps) []httpapi.Route {
	if deps.limiters == nil {
		deps.limiters = newAuthLimiters(deps.Counters, deps.Now)
	}
	if deps.routeLimiters == nil {
		deps.routeLimiters = newRouteLimiters(deps.Counters, deps.Now)
	}
	maxBodyBytes := int64(deps.limits().MaxBodyBytes)
	const prefix = "/api/v1/external-ingest"
	guarded := func(h http.HandlerFunc) http.HandlerFunc { return recoverToIngestError(deps.logger(), h) }
	return []httpapi.Route{
		{Method: "GET", Pattern: prefix + "/schemas", Handler: guarded(deps.handleListSchemas())},
		{Method: "GET", Pattern: prefix + "/schemas/{schema_version}", Handler: guarded(deps.handleGetSchema())},
		{Method: "GET", Pattern: prefix + "/availability", Handler: guarded(deps.handleAvailability())},
		{Method: "POST", Pattern: prefix + "/validate", Handler: guarded(deps.handleValidate()), MaxBodyBytes: maxBodyBytes},
		{Method: "POST", Pattern: prefix + "/batches", Handler: guarded(deps.handleAcceptBatch()), MaxBodyBytes: maxBodyBytes},
		{Method: "GET", Pattern: prefix + "/batches", Handler: guarded(deps.handleListBatches())},
		{Method: "GET", Pattern: prefix + "/batches/{ingestion_id}", Handler: guarded(deps.handleGetBatch())},
	}
}
