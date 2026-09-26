package fixturesgen

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Product telemetry generator: a port of
// src/dev_health_ops/fixtures/generators/product_telemetry.py. The draw order
// of every random call is the Python one (arguments and payload dicts are
// evaluated before the event id is drawn, exactly as Python evaluates a call's
// arguments before its body), because the stream is the contract.

const (
	// ProductTelemetrySchemaVersion is the schemaVersion of every generated event.
	ProductTelemetrySchemaVersion = "2026-05-telemetry-v1"
	// ProductTelemetrySource is the source column every generated row carries.
	ProductTelemetrySource = "dev-health-web"
)

var (
	telemetryRoutePatterns = []string{
		"/dashboard", "/metrics", "/metrics/dora", "/investment", "/code", "/work", "/prs",
		"/issues", "/deployments", "/security", "/people/[person_id]/metrics/[metric]",
		"/reports/[id]", "/superadmin/product-telemetry",
	}
	telemetryFeatures = [][2]string{
		{"dashboard", "dashboard"}, {"investment", "metrics"}, {"dora", "metrics"}, {"code", "code"},
		{"work", "work"}, {"testops", "testops"}, {"feature-flags", "feature-flags"},
		{"security", "security"}, {"ai-workflow", "ai"},
	}
	telemetryFilterKeys    = []string{"scope", "date", "repo", "developer", "work", "flow", "artifact", "blocked", "issueType"}
	telemetryFilterViews   = []string{"dashboard", "metrics", "investment", "code", "work", "security"}
	telemetryChartKinds    = []string{"quadrant", "timeseries", "treemap", "flame", "sankey"}
	telemetryChartActions  = []string{"point_selected", "overlay_toggled", "overlay_ignored", "drilldown", "zoom"}
	telemetryNavGroups     = []string{"operate", "improve", "investigate", "admin"}
	telemetryNavItems      = []string{"dashboard", "metrics", "code", "work", "security", "ai", "settings"}
	telemetryGuides        = []string{"quadrant-guide", "investment-guide", "dora-guide", "compounding-risk-guide"}
	telemetryErrorBounds   = []string{"route", "global"}
	telemetryErrorClasses  = []string{"RenderError", "FetchError", "TypeError", "ChunkLoadError"}
	telemetryChartScopes   = []any{"org", "team", "repo", nil}
	telemetryNavActions    = []string{"group_expanded", "group_collapsed", "item_selected"}
	blockedTelemetryFields = map[string]struct{}{
		"email": {}, "name": {}, "userId": {}, "orgId": {}, "url": {}, "query": {}, "search": {},
		"stack": {}, "message": {}, "title": {}, "body": {},
	}
)

// ProductTelemetryCeilingDays is the generation ceiling for
// product_telemetry_events: the table's 180-day TTL minus the 30-day shelf-life
// margin minus 1 day of strict headroom (ttl_horizon.max_generated_age_days_for_table).
// A test derives the TTL from the ClickHouse migration so this cannot drift from
// the schema unnoticed.
const ProductTelemetryCeilingDays = 180 - 30 - 1

// ProductTelemetrySpec is one org's seeding run.
type ProductTelemetrySpec struct {
	OrgID          string
	Days           int
	SessionsPerDay int
	// Seed is mixed with the org id; nil is Python's None (mixed as 0).
	Seed    *int64
	EndTime time.Time
}

// ProductTelemetryRow is one product_telemetry_events row as
// persist_product_telemetry_events builds it, minus ingested_at (the time of the
// write, not of the generation).
type ProductTelemetryRow struct {
	OrgIDHash       string
	EventID         string
	Name            string
	SchemaVersion   string
	SessionID       string
	AnonymousUserID string
	RoutePattern    *string
	PayloadJSON     string
	OccurredAt      time.Time
	Source          string
}

// ProductTelemetryOrgHash is sha256(org_id) in hex, the recipe of the GraphQL resolvers.
func ProductTelemetryOrgHash(orgID string) string {
	sum := sha256.Sum256([]byte(orgID))
	return hex.EncodeToString(sum[:])
}

type telemetryGenerator struct {
	spec    ProductTelemetrySpec
	orgHash string
	rng     *Rand
	rows    []ProductTelemetryRow
	err     error
}

// GenerateProductTelemetry produces the full ordered event stream of one org.
func GenerateProductTelemetry(spec ProductTelemetrySpec) ([]ProductTelemetryRow, error) {
	seed := int64(0)
	if spec.Seed != nil {
		seed = *spec.Seed
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s", seed, spec.OrgID)))
	rngSeed := new(big.Int).SetBytes(sum[:8])
	generator := &telemetryGenerator{spec: spec, orgHash: ProductTelemetryOrgHash(spec.OrgID), rng: NewRand(rngSeed)}

	days := min(spec.Days, ProductTelemetryCeilingDays)
	endTime := spec.EndTime.UTC()
	startDay := endTime.Add(-time.Duration(days) * 24 * time.Hour)
	startDay = time.Date(startDay.Year(), startDay.Month(), startDay.Day(), 9, 0, 0, 0, time.UTC)
	for dayOffset := 0; dayOffset < days; dayOffset++ {
		dayStart := startDay.AddDate(0, 0, dayOffset)
		for sessionIndex := 0; sessionIndex < spec.SessionsPerDay; sessionIndex++ {
			offsetMinutes := generator.rng.RandInt(0, 9*60)
			jitterSeconds := generator.rng.RandInt(0, 59)
			generator.session(dayStart.Add(time.Duration(offsetMinutes)*time.Minute + time.Duration(jitterSeconds)*time.Second))
		}
	}
	if err := generator.rng.Err(); err != nil {
		return nil, err
	}
	if generator.err != nil {
		return nil, generator.err
	}
	return generator.rows, nil
}

func (g *telemetryGenerator) eventID() string {
	return fmt.Sprintf("evt_%016x", g.rng.GetRandBits(64))
}

func (g *telemetryGenerator) sessionID() string {
	return fmt.Sprintf("ses_%016x", g.rng.GetRandBits(64))
}

func (g *telemetryGenerator) anonUserID() string {
	bins := max(5, g.spec.SessionsPerDay/2)
	return fmt.Sprintf("anon_%04d", g.rng.RandRange(bins))
}

func (g *telemetryGenerator) pickRoute() string {
	return telemetryRoutePatterns[g.rng.Choice(len(telemetryRoutePatterns))]
}

func pick(g *telemetryGenerator, from []string) string { return from[g.rng.Choice(len(from))] }

// event appends one row. The payload was built (and its random draws made) by
// the caller before this draws the event id, as in the Python _event().
func (g *telemetryGenerator) event(name string, ts time.Time, sessionID, anonUserID string, route *string, payload map[string]any) {
	for key := range payload {
		if _, blocked := blockedTelemetryFields[key]; blocked {
			g.err = fmt.Errorf("payload contains blocked telemetry key %q", key)
		}
	}
	encoded, err := encodePayload(payload)
	if err != nil {
		g.err = err
	}
	g.rows = append(g.rows, ProductTelemetryRow{
		OrgIDHash:       g.orgHash,
		EventID:         g.eventID(),
		Name:            name,
		SchemaVersion:   ProductTelemetrySchemaVersion,
		SessionID:       sessionID,
		AnonymousUserID: anonUserID,
		RoutePattern:    route,
		PayloadJSON:     encoded,
		OccurredAt:      ts,
		Source:          ProductTelemetrySource,
	})
}

// encodePayload is json.dumps(payload, sort_keys=True, separators=(",", ":")):
// sorted keys, no spaces. The values are ASCII strings, integers, booleans and
// null, for which encoding/json and Python write the same bytes.
func encodePayload(payload map[string]any) (string, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return "", err
	}
	return strings.TrimSuffix(buffer.String(), "\n"), nil
}

func (g *telemetryGenerator) session(sessionStart time.Time) {
	rng := g.rng
	sessionID := g.sessionID()
	anonUserID := g.anonUserID()
	entryRoute := g.pickRoute()
	ts := sessionStart

	route := entryRoute
	g.event("session_started", ts, sessionID, anonUserID, &route, map[string]any{"entryRoutePattern": entryRoute})

	currentRoute := entryRoute
	var prevRoute *string
	pagesViewed := 0
	for i, n := 0, rng.RandInt(3, 8); i < n; i++ {
		ts = ts.Add(time.Duration(rng.RandInt(5, 90)) * time.Second)
		previous := currentRoute
		prevRoute = &previous
		currentRoute = g.pickRoute()
		pagesViewed++
		var referrer any
		if prevRoute != nil {
			referrer = *prevRoute
		}
		page := strings.Split(strings.Trim(currentRoute, "/"), "/")[0]
		if page == "" {
			page = "root"
		}
		current := currentRoute
		g.event("page_viewed", ts, sessionID, anonUserID, &current, map[string]any{
			"routePattern": currentRoute, "page": page, "referrerRoutePattern": referrer,
		})
	}

	interactions := 0
	at := func() *string { route := currentRoute; return &route }

	for i, n := 0, rng.RandInt(1, 3); i < n; i++ {
		ts = ts.Add(time.Duration(rng.RandInt(2, 30)) * time.Second)
		feature := telemetryFeatures[rng.Choice(len(telemetryFeatures))]
		interactions++
		g.event("feature_viewed", ts, sessionID, anonUserID, at(), map[string]any{
			"feature": feature[0], "surface": feature[1], "routePattern": currentRoute,
		})
	}

	for i, n := 0, rng.RandInt(0, 3); i < n; i++ {
		ts = ts.Add(time.Duration(rng.RandInt(2, 30)) * time.Second)
		interactions++
		view := pick(g, telemetryFilterViews)
		filterKey := pick(g, telemetryFilterKeys)
		valueCount := rng.RandInt(1, 5)
		custom := rng.Random() < 0.2
		g.event("filter_changed", ts, sessionID, anonUserID, at(), map[string]any{
			"view": view, "filterKey": filterKey, "valueCount": valueCount, "isCustomDateRange": custom,
		})
	}

	for i, n := 0, rng.RandInt(0, 3); i < n; i++ {
		ts = ts.Add(time.Duration(rng.RandInt(2, 30)) * time.Second)
		interactions++
		chart := pick(g, telemetryChartKinds)
		action := pick(g, telemetryChartActions)
		surface := pick(g, telemetryNavGroups)
		scope := telemetryChartScopes[rng.Choice(len(telemetryChartScopes))]
		g.event("chart_interacted", ts, sessionID, anonUserID, at(), map[string]any{
			"chart": chart, "action": action, "surface": surface, "scope": scope,
		})
	}

	for i, n := 0, rng.RandInt(0, 2); i < n; i++ {
		ts = ts.Add(time.Duration(rng.RandInt(2, 20)) * time.Second)
		interactions++
		group := pick(g, telemetryNavGroups)
		item := pick(g, telemetryNavItems)
		action := pick(g, telemetryNavActions)
		g.event("navigation_interacted", ts, sessionID, anonUserID, at(), map[string]any{
			"group": group, "item": item, "action": action,
		})
	}

	if rng.Random() < 0.4 {
		ts = ts.Add(time.Duration(rng.RandInt(2, 20)) * time.Second)
		interactions++
		guide := pick(g, telemetryGuides)
		surface := pick(g, telemetryNavGroups)
		g.event("guide_opened", ts, sessionID, anonUserID, at(), map[string]any{"guide": guide, "surface": surface})
	}

	if rng.Random() < 0.05 {
		ts = ts.Add(time.Duration(rng.RandInt(1, 10)) * time.Second)
		boundary := pick(g, telemetryErrorBounds)
		digest := fmt.Sprintf("d_%08x", rng.GetRandBits(32))
		errorClass := pick(g, telemetryErrorClasses)
		g.event("client_error", ts, sessionID, anonUserID, at(), map[string]any{
			"boundary": boundary, "digest": digest, "errorClass": errorClass, "routePattern": currentRoute,
		})
	}

	ts = ts.Add(time.Duration(rng.RandInt(5, 60)) * time.Second)
	durationMs := int64(ts.Sub(sessionStart) / time.Millisecond)
	g.event("session_ended", ts, sessionID, anonUserID, at(), map[string]any{
		"durationMs": durationMs, "pagesViewed": pagesViewed, "interactions": interactions,
	})
}

// SyntheticOrgIDs are the ids the Python verb seeds when Postgres cannot be read:
// uuid5(NAMESPACE_URL, "seed-org-<i>") for i in range(count).
func SyntheticOrgIDs(count int) []string {
	ids := make([]string, 0, max(count, 0))
	for index := 0; index < count; index++ {
		ids = append(ids, uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("seed-org-%d", index))).String())
	}
	return ids
}
