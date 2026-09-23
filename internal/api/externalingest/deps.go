// Package externalingest is the `dho api` route area for
// /api/v1/external-ingest/*: the customer-push ingestion contract (Python
// src/dev_health_ops/api/external_ingest/). It owns its own bearer-token
// authentication (fcpush_... against external_ingest_tokens/_sources) --
// deliberately NOT the shared session/org middleware every other api area
// uses, since these callers are customer scripts and CI jobs, not logged-in
// users.
//
// Every port here states its scope explicitly where it narrows the Python
// original: validateRecords (validate.go) matches Pydantic's error codes
// and paths but not always its exact message text; resolveEffectiveMode
// (ownership.go) covers the explicit-source-row precedence and the
// managed-sync string-match override, not the narrow self-hosted-GitHub/
// GitLab credential-host sub-branch (see ownership.go's doc comment).
package externalingest

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/jackc/pgx/v5/pgxpool"
	valkeygo "github.com/valkey-io/valkey-go"
)

// Limits mirrors router.py's _limits_payload(): the live, env-overridable
// ingest limits.
type Limits struct {
	MaxRecords   int
	MaxBodyBytes int
}

// DefaultLimits are schemas.py's MAX_RECORDS_DEFAULT/MAX_BODY_BYTES_DEFAULT.
var DefaultLimits = Limits{MaxRecords: 1000, MaxBodyBytes: 10_000_000}

// Deps are this area's dependencies, handed down from apiservice (which
// owns the one shared Postgres pool and Valkey client every api area is
// built from -- see internal/apiservice/deps.go).
type Deps struct {
	Pool   *pgxpool.Pool
	Valkey valkeygo.Client
	Limits Limits
	Now    func() time.Time

	limiters *authLimiters
}

func (d Deps) now() time.Time {
	if d.Now != nil {
		return d.Now()
	}
	return time.Now()
}

func (d Deps) limits() Limits {
	if d.Limits.MaxRecords == 0 && d.Limits.MaxBodyBytes == 0 {
		return DefaultLimits
	}
	return d.Limits
}

// Routes builds the 7 /api/v1/external-ingest/* routes (router.py + status.py),
// mounted by internal/apiservice.Routes.
func Routes(deps Deps) []httpapi.Route {
	if deps.limiters == nil {
		deps.limiters = newAuthLimiters(deps.Now)
	}
	const prefix = "/api/v1/external-ingest"
	return []httpapi.Route{
		{Method: "GET", Pattern: prefix + "/schemas", Handler: deps.handleListSchemas()},
		{Method: "GET", Pattern: prefix + "/schemas/{schema_version}", Handler: deps.handleGetSchema()},
		{Method: "GET", Pattern: prefix + "/availability", Handler: deps.handleAvailability()},
		{Method: "POST", Pattern: prefix + "/validate", Handler: deps.handleValidate()},
		{Method: "POST", Pattern: prefix + "/batches", Handler: deps.handleAcceptBatch()},
		{Method: "GET", Pattern: prefix + "/batches", Handler: deps.handleListBatches()},
		{Method: "GET", Pattern: prefix + "/batches/{ingestion_id}", Handler: deps.handleGetBatch()},
	}
}
