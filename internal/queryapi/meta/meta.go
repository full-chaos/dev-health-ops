// Package meta is the Go port of GET /api/v1/meta
// (src/dev_health_ops/api/main.py:424-466's meta handler; response shape
// from MetaResponse, src/dev_health_ops/api/models/schemas.py:214-222).
//
// # Auth: PUBLIC
//
// The route is unauthenticated: contracts/auth/v1/endpoint-profiles.query-api.json
// classifies "GET /api/v1/meta" as public with tenant_requirement "none".
// The handler adds no auth check.
//
// # No ClickHouse read, no server version
//
// The response is static. It does not read ClickHouse and does not name the
// database server build: the Python route's `version` field (from
// `SELECT version()`) is deliberately not served, because a public endpoint
// that names the exact server build helps fingerprinting.
//
// # Field notes
//
//   - backend: always "clickhouse", the only supported analytics backend.
//   - last_ingest_at / coverage: main.py hardcodes `None` / `{}`; same here.
//   - limits / supported_endpoints: literal values from main.py:452-463.
package meta

// Response ports MetaResponse (api/models/schemas.py:214-222). Plain field
// names: main.py's FastAPI app carries no alias generator, so Pydantic's
// wire form is the snake_case attribute name verbatim.
type Response struct {
	Backend            string         `json:"backend"`
	LastIngestAt       *string        `json:"last_ingest_at"`
	Coverage           map[string]any `json:"coverage"`
	Limits             map[string]int `json:"limits"`
	SupportedEndpoints []string       `json:"supported_endpoints"`
}

// supportedEndpoints ports main.py:453-463's literal list verbatim.
func supportedEndpoints() []string {
	return []string{
		"/api/v1/home",
		"/api/v1/quadrant",
		"/api/v1/flame",
		"/api/v1/heatmap",
		"/api/v1/work-units",
		"/api/v1/sankey",
		"/api/v1/investment",
		"/api/v1/opportunities",
		"/graphql",
	}
}

// BuildResponse ports main.py:424-466's meta() handler body. It reads no
// store and cannot fail.
func BuildResponse() Response {
	return Response{
		Backend:            "clickhouse",
		LastIngestAt:       nil,
		Coverage:           map[string]any{},
		Limits:             map[string]int{"max_days": 365, "max_repos": 1000},
		SupportedEndpoints: supportedEndpoints(),
	}
}
