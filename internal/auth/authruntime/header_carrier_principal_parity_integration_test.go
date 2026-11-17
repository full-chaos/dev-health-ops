//go:build integration

package authruntime

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	stdclickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workloadinput"
	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/mcpclass"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/producttelemetry"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	riverstore "github.com/full-chaos/dev-health-ops/internal/storage/river"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/chschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// CHAOS-13945 RC10: the X-DH-Internal-* header carrier that names a principal (X-DH-Internal-Principal-Id) is decided
// by the real auth-service decision point as that principal, and gets the decision the same user's own access token
// gets on /graphql. The cases are every registered root in the flipped namespaces (hpHeaderCells), which
// /query/run-operation serves: per caller and per root, served / refused / unavailable and the served bytes agree
// between /graphql (user token) and /query/run-operation (header carrier + principal). An MCP root also agrees on the
// MCP listener; any other root is refused there with its own reason, before and after the flip. The one exception is
// the platform role (acp Q5): the named principal is decided without it, so a platform admin who is not a member is
// refused on the carrier. The venue holds a test-only seed (hpSeed), so a served cell compares rows, not empty shapes.

const hpHeaderCells = "analytics,metrics,git"

// hpCase is one RC10 root. mcpReason is the MCP listener's refusal reason (and its status) for a root outside the MCP
// allowlist; it is "" for an MCP root, whose MCP leg must answer as the user token does. variables (test-only)
// replace generated variables so the root reads the seeded rows.
type hpCase struct {
	mcpReason string
	mcpStatus int
	variables map[string]any
	// seeded, when set, is what a served body holds only from seeded rows, for a root that answers derived text
	// rather than the rows themselves.
	seeded *regexp.Regexp
}

var (
	hpMCP          = hpCase{}
	hpNotAllowed   = hpCase{mcpReason: "root_field_not_allowed", mcpStatus: http.StatusForbidden}
	hpTooComplex   = hpCase{mcpReason: "complexity_limit", mcpStatus: http.StatusBadRequest}
	hpCases        = map[string]hpCase{}
	hpCaseBaseline = map[string]hpCase{
		"Query.analytics":            hpMCP,
		"Query.capacityForecast":     hpMCP,
		"Query.capacityForecasts":    hpMCP,
		"Query.catalog":              hpMCP,
		"Query.cognitiveLoad":        hpMCP,
		"Query.complexityTimeseries": hpMCP,
		"Query.compoundingRisk":      hpMCP,
		"Query.hotspots":             hpMCP,
		"Query.securityAlerts":       hpMCP,
		"Query.securityOverview":     hpMCP,
		"Query.sourceHealth":         hpMCP,
		"Query.throughputForecast":   hpMCP,
		"Query.workGraphArtifacts":   hpMCP,
		"Query.workGraphEdges":       hpMCP,
		"Query.workGraphFlow":        hpMCP,

		"Query.aiAttributedPrs":           hpNotAllowed,
		"Query.aiAttributionOverview":     hpNotAllowed,
		"Query.aiComparison":              hpNotAllowed,
		"Query.aiGovernanceSummary":       hpNotAllowed,
		"Query.aiImpactSummary":           hpNotAllowed,
		"Query.aiOpportunities":           hpNotAllowed,
		"Query.aiReviewLoad":              hpNotAllowed,
		"Query.aiRiskBreakdown":           hpNotAllowed,
		"Query.aiWorkflowDrilldown":       hpNotAllowed,
		"Query.busFactor":                 hpNotAllowed,
		"Query.coverageBaselines":         hpNotAllowed,
		"Query.coverageScopeBaseline":     hpNotAllowed,
		"Query.experiments":               hpNotAllowed,
		"Query.featureFlagEvents":         hpNotAllowed,
		"Query.featureFlags":              hpNotAllowed,
		"Query.home":                      hpTooComplex,
		"Query.improveOpportunities":      hpNotAllowed,
		"Query.operatingReview":           hpNotAllowed,
		"Query.pr":                        hpNotAllowed,
		"Query.productTelemetryDashboard": hpNotAllowed,
		"Query.recommendations":           hpNotAllowed,
		"Query.reportRuns":                hpNotAllowed,
		"Query.reviewEdges":               hpNotAllowed,
		"Query.savedReport":               hpNotAllowed,
		"Query.savedReports":              hpNotAllowed,
		"Query.testopsJobFailures":        hpNotAllowed,
		"Query.testopsRisk":               hpNotAllowed,
		"Query.testopsTestWindow":         hpNotAllowed,
	}
)

func init() {
	// A root whose body holds values that do not come from rows: its census is a pattern only seeded rows produce.
	// experiments answers only suggestion text: a "Reduce <metric>" card (metric not empty) comes from seeded deltas
	// that got worse; with no rows the one card is the metric-less default.
	seeded := map[string]string{
		"Query.experiments": `"metric":"[a-z_]+"`,
		// The root id is the request's own variable and comes back without any row: only an edge is seeded.
		"Query.aiWorkflowDrilldown": `"edges":\[\{`,
		// The window lengths and thresholds are non-zero without any row: only a backlog or throughput is seeded.
		"Query.throughputForecast": `"(backlogSize|meanWeeklyThroughput|sampleCount)":[1-9]`,
	}
	for key, pattern := range seeded {
		c := hpCaseBaseline[key]
		c.seeded = regexp.MustCompile(pattern)
		hpCaseBaseline[key] = c
	}
	for key, c := range hpCaseBaseline {
		if v, ok := hpCaseVariables[key]; ok {
			c.variables = v
		}
		hpCases[key] = c
	}
}

// hpRange is a test-only date range over the seeded days 2026-09-01 and 2026-09-04.
var hpRange = map[string]any{"startDate": "2026-09-01", "endDate": "2026-09-05"}

// hpCaseVariables are the test-only variable overrides: each names seeded data (team, ids, window) so the root reads
// seeded rows.
var hpCaseVariables = map[string]map[string]any{
	"Query.savedReport":               {"reportId": hpReportID.String()},
	"Query.reportRuns":                {"reportId": hpReportID.String()},
	"Query.pr":                        {"id": hpRepos[0].id.String() + "#3"},
	"Query.aiAttributedPrs":           {"dateRange": hpRange},
	"Query.aiAttributionOverview":     {"dateRange": hpRange},
	"Query.aiComparison":              {"dateRange": hpRange},
	"Query.aiWorkflowDrilldown":       {"rootType": "ISSUE", "rootId": hpSeedMark + "-issue_id-1"},
	"Query.coverageBaselines":         {"endDate": "2026-09-05"},
	"Query.coverageScopeBaseline":     {"endDate": "2026-09-05"},
	"Query.featureFlags":              {"limit": 50},
	"Query.productTelemetryDashboard": {"input": hpRange},
	"Query.testopsJobFailures":        {"input": map[string]any{"sinceDate": "2026-09-01", "untilDate": "2026-09-05"}},
	"Query.analytics": {"batch": map[string]any{"timeseries": []any{
		map[string]any{"dimension": "REPO", "measure": "COUNT", "interval": "DAY",
			"dateRange": map[string]any{"startDate": "2026-09-01", "endDate": "2026-09-08"}},
		map[string]any{"dimension": "REPO", "measure": "PIPELINE_SUCCESS_RATE", "interval": "DAY",
			"dateRange": map[string]any{"startDate": "2026-09-01", "endDate": "2026-09-08"}},
	}}},
}

type hpCaller struct {
	name      string
	user      uuid.UUID
	userToken func(*http.Request)
	// platformNonMember: a platform admin with no membership in org A; the carrier must refuse it (acp Q5).
	platformNonMember bool
}

type hpRoot struct {
	op qpOperation
	hpCase
}

// hpRoots are the RC10 operations: the registered document of each case root, with the case's variables.
func hpRoots(t *testing.T, org string) []hpRoot {
	t.Helper()
	var roots []hpRoot
	for _, op := range qpGraphQLOperations(t, org).roots {
		c, ok := hpCases[op.key]
		if !ok {
			continue
		}
		if !op.registered {
			t.Fatalf("%s has an RC10 case but no registered document", op.key)
		}
		if op.variables == nil {
			op.variables = map[string]any{}
		}
		for name, value := range c.variables {
			op.variables[name] = value
		}
		op.bodyBytes = op.body(t)
		head, _, _ := strings.Cut(op.document, "{")
		t.Logf("ROOT %-34s %s variables=%s", op.key, strings.TrimSpace(head), op.variablesJSON(t))
		roots = append(roots, hpRoot{op: op, hpCase: c})
	}
	if len(roots) != len(hpCases) {
		t.Fatalf("%d RC10 cases, %d roots found: TestHeaderCarrierRootCases names the gap", len(hpCases), len(roots))
	}
	return roots
}

// TestHeaderCarrierRootCases is the census: every registered root in the flipped namespaces (the documents
// /query/run-operation serves) has an RC10 case, every case is such a root, and a case's MCP form matches the MCP
// allowlist.
func TestHeaderCarrierRootCases(t *testing.T) {
	allowed := mcpclass.AllowedRoots()
	flipped := map[string]bool{}
	for _, op := range qpGraphQLOperations(t, uuid.NewString()).roots {
		if op.registered && strings.Contains(","+hpHeaderCells+",", ","+op.namespace+",") {
			flipped[op.key] = true
		}
	}
	if len(flipped) == 0 {
		t.Fatal("no registered root in the flipped namespaces: the census would measure nothing")
	}
	for key := range flipped {
		c, ok := hpCases[key]
		if !ok {
			t.Errorf("%s is a registered root in a flipped namespace with no RC10 case", key)
			continue
		}
		_, field, _ := strings.Cut(key, ".")
		if allowed[field] != (c.mcpReason == "") {
			t.Errorf("%s: MCP allowlist %v, but the case says mcpReason %q", key, allowed[field], c.mcpReason)
		}
	}
	for key := range hpCases {
		if !flipped[key] {
			t.Errorf("RC10 case %s is not a registered root in a flipped namespace", key)
		}
	}
	t.Logf("%d registered roots in %s, %d RC10 cases", len(flipped), hpHeaderCells, len(hpCases))
}

func TestHeaderCarrierPrincipalParity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	v, b, pre := newHPVenue(t, ctx)
	roots := hpRoots(t, v.orgA.String())
	callers := v.hpCallers(t, ctx)
	cells, carrierAsks, servedSame, seeded := 0, 0, 0, 0
	viewer := map[string]qpAnswer{}
	for _, c := range callers {
		carrier := hpCarrier(v.orgA, c.user)
		for _, root := range roots {
			op := root.op
			user := v.send(t, b, qpClass{name: c.name, carry: c.userToken},
				qpRow{listener: "public", method: http.MethodPost, target: "/graphql", body: op.bodyBytes}, uuid.NewString())
			before := b.pdp.count()
			mcp := v.send(t, b, qpClass{name: c.name, carry: carrier},
				qpRow{listener: "mcp", method: http.MethodPost, target: "/query", body: op.bodyBytes}, uuid.NewString())
			mcpAsks := b.pdp.count() - before
			runOp := v.send(t, b, qpClass{name: c.name, carry: carrier},
				qpRow{listener: "internal", method: http.MethodPost, target: "/query/run-operation", body: op.bodyBytes}, uuid.NewString())
			carrierAsks += b.pdp.count() - before
			cells++
			uv, mv, rv := hpVerdict(user), hpVerdict(mcp), hpVerdict(runOp)
			t.Logf("%-25s %-34s user=%-28s mcp=%-28s run-operation=%s", c.name, op.key, uv, mv, rv)
			if root.mcpReason != "" {
				// Outside the MCP allowlist: refused on the MCP listener before any decision, after the flip and before it.
				preBefore := pre.pdp.count()
				preMCP := hpSendBefore(t, v, pre, c.name, carrier, op)
				preAsks := pre.pdp.count() - preBefore
				for _, leg := range []struct {
					build  string
					answer qpAnswer
					asks   int
				}{{"after the flip", mcp, mcpAsks}, {"before the flip", preMCP, preAsks}} {
					if got := hpRefusal(leg.answer); got != fmt.Sprintf("%d %s", root.mcpStatus, root.mcpReason) || leg.asks != 0 {
						t.Errorf("%s %s MCP %s: %s with %d asks, want %d %s with 0 asks\n %s",
							c.name, op.key, leg.build, got, leg.asks, root.mcpStatus, root.mcpReason, leg.answer)
					}
				}
			}
			switch {
			case c.platformNonMember:
				if !strings.HasPrefix(rv, "refused") || (root.mcpReason == "" && !strings.HasPrefix(mv, "refused")) {
					t.Errorf("%s %s: the carrier answered mcp=%s run-operation=%s, want refused (no platform bypass, acp Q5)\n mcp: %s\n run-operation: %s",
						c.name, op.key, mv, rv, mcp, runOp)
				}
			case uv != rv || (root.mcpReason == "" && uv != mv):
				t.Errorf("%s %s: user=%s mcp=%s run-operation=%s, want the same verdict\n user: %s\n mcp: %s\n run-operation: %s",
					c.name, op.key, uv, mv, rv, user, mcp, runOp)
			case uv == "served":
				// A served cell answers the same rows. The MCP listener ends its body with a newline; that framing byte is
				// not compared.
				u, r := hpBody(user), hpBody(runOp)
				same := u.equal(r)
				if root.mcpReason == "" {
					same = same && u.equal(hpBody(mcp))
				}
				if !same {
					t.Errorf("%s %s: served, bodies differ\n user: %s\n mcp: %s\n run-operation: %s", c.name, op.key, u, hpBody(mcp), r)
				} else {
					servedSame++
				}
			}
			for leg, answer := range map[string]qpAnswer{"user": user, "mcp": mcp, "run-operation": runOp} {
				if hpDecoyEvidence(answer.body) {
					t.Errorf("%s %s %s: a body holds org B's decoy rows\n %s", c.name, op.key, leg, answer)
				}
			}
			if uv != "served" {
				if c.name == "member_viewer" {
					// The census needs every root served to a member: a root that is not cannot show it reads rows.
					t.Errorf("%s: not served to member_viewer (%s), so the census measures nothing\n %s", op.key, uv, user)
				}
				continue
			}
			switch c.name {
			case "member_viewer":
				viewer[op.key] = hpBody(user)
				marks, numbers := hpSeedEvidence(user.body)
				if root.seeded != nil {
					marks, numbers = len(root.seeded.FindAll(user.body, -1)), 0
				}
				t.Logf("CENSUS %-34s seed_marks=%d nonzero_numbers=%d", op.key, marks, numbers)
				if marks == 0 && numbers == 0 {
					t.Errorf("%s: served cell measures nothing (0 seeded rows in the body)\n %s", op.key, user)
				} else {
					seeded++
				}
			case "member_repo_grant":
				// Repository grants do not narrow these reads (their actions are repository_scope none): the grant
				// member answers as a viewer.
				if want, ok := viewer[op.key]; ok && !want.equal(hpBody(user)) {
					t.Errorf("%s: member_repo_grant answers other rows than member_viewer\n viewer: %s\n grant: %s", op.key, want, hpBody(user))
				}
			}
		}
	}
	if carrierAsks == 0 {
		t.Fatal("the header carrier never asked the decision point: the cells are not decided")
	}
	t.Logf("%d cells over %d callers and %d roots; the carrier asked the decision point %d times; %d served cells byte-equal; %d roots served with seeded rows",
		cells, len(callers), len(roots), carrierAsks, servedSame, seeded)
}

// hpSendBefore sends op on the MCP listener of the build before the flip (no header cells).
func hpSendBefore(t *testing.T, v *qpVenue, pre *qpBuild, name string, carrier func(*http.Request), op qpOperation) qpAnswer {
	t.Helper()
	return v.send(t, pre, qpClass{name: name, carry: carrier},
		qpRow{listener: "mcp", method: http.MethodPost, target: "/query", body: op.bodyBytes}, uuid.NewString())
}

// hpRefusal is "status reason" of an MCP refusal.
func hpRefusal(a qpAnswer) string {
	reason := ""
	if m := hpReason.FindSubmatch(a.body); m != nil {
		reason = string(m[1])
	}
	return fmt.Sprintf("%d %s", a.status, reason)
}

// hpDecoyEvidence reports whether a body holds org B's rows: a decoy string, or a number only a decoy row reaches.
func hpDecoyEvidence(raw []byte) bool {
	if bytes.Contains(raw, []byte(hpDecoyMark)) {
		return true
	}
	var parsed map[string]any
	if json.Unmarshal(raw, &parsed) != nil {
		return false
	}
	found := false
	var walk func(any)
	walk = func(x any) {
		switch y := x.(type) {
		case []any:
			for _, e := range y {
				walk(e)
			}
		case map[string]any:
			for _, e := range y {
				walk(e)
			}
		case float64:
			if y >= hpDecoyFloor || y <= -hpDecoyFloor {
				found = true
			}
		}
	}
	walk(parsed["data"])
	return found
}

// hpSeedEvidence counts what in a body can only come from the seed: seed marks (seeded strings and repository
// names) and non-zero numbers in the data member.
func hpSeedEvidence(raw []byte) (marks, numbers int) {
	marks = bytes.Count(raw, []byte(hpSeedMark)) + bytes.Count(raw, []byte("acme/")) + bytes.Count(raw, []byte(hpSeedClock))
	var parsed map[string]any
	if json.Unmarshal(raw, &parsed) != nil {
		return marks, 0
	}
	var walk func(any)
	walk = func(x any) {
		switch y := x.(type) {
		case []any:
			for _, e := range y {
				walk(e)
			}
		case map[string]any:
			for _, e := range y {
				walk(e)
			}
		case float64:
			if y != 0 {
				numbers++
			}
		}
	}
	walk(parsed["data"])
	return marks, numbers
}

// hpCarrier is acr's real header set (role viewer, no elevated flag) for org, naming user.
func hpCarrier(org, user uuid.UUID) func(*http.Request) {
	return func(r *http.Request) {
		r.Header.Set(internalidentity.HeaderOrgID, org.String())
		r.Header.Set(internalidentity.HeaderRole, "viewer")
		r.Header.Set(internalidentity.HeaderSuperuser, "false")
		r.Header.Set(internalidentity.HeaderImpersonationActive, "false")
		r.Header.Set(internalidentity.HeaderPrincipalID, session.WirePrincipalID(user))
	}
}

func hpBody(a qpAnswer) qpAnswer {
	a = a.masked()
	a.body = bytes.TrimRight(a.body, "\n")
	return a
}

var hpReason = regexp.MustCompile(`"reason":"([a-z_]+)"`)

// hpVerdict is the decision an answer carries: served, refused (401, 403, FORBIDDEN, a legacy AUTHORIZATION_ERROR) or
// unavailable with its reason (503, SERVICE_UNAVAILABLE).
func hpVerdict(a qpAnswer) string {
	body := string(a.body)
	reason := ""
	if m := hpReason.FindStringSubmatch(body); m != nil {
		reason = ":" + m[1]
	}
	switch {
	case a.status == http.StatusServiceUnavailable || strings.Contains(body, `"SERVICE_UNAVAILABLE"`):
		return "unavailable" + reason
	case a.status == http.StatusUnauthorized || a.status == http.StatusForbidden || strings.Contains(body, `"code":"FORBIDDEN"`):
		return "refused"
	case strings.Contains(body, "AUTHORIZATION_ERROR"):
		return "refused:legacy"
	case a.status == http.StatusOK && !strings.Contains(body, `"errors"`):
		return "served"
	}
	var parsed map[string]any
	if json.Unmarshal(a.body, &parsed) == nil && parsed["data"] != nil {
		return "served:field-errors"
	}
	return "other:" + http.StatusText(a.status)
}

// hpUser is a go-api users row (email at example.com) projected into auth.
func (v *qpVenue) hpUser(t *testing.T, ctx context.Context, superuser bool) uuid.UUID {
	t.Helper()
	id := uuid.New()
	v.az.exec(t, ctx, `INSERT INTO public.users
	(id, email, password_hash, auth_provider, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, NULL, 'local', true, true, $3, 0, now(), now())`, id, "hp-"+id.String()[:8]+"@example.com", superuser)
	if err := v.az.identity.Project(ctx, id); err != nil {
		t.Fatalf("project the principal: %v", err)
	}
	return id
}

func (v *qpVenue) hpMember(t *testing.T, ctx context.Context, user uuid.UUID, role string) {
	t.Helper()
	v.az.armed(t, ctx, `INSERT INTO public.memberships (id, user_id, org_id, role, joined_at, created_at) VALUES (gen_random_uuid(), $1, $2, $3, now(), now())`,
		user, v.orgA, role)
}

func (v *qpVenue) hpCallers(t *testing.T, ctx context.Context) []hpCaller {
	t.Helper()
	org := v.orgA
	v.az.exec(t, ctx, `INSERT INTO auth.roles (role_key, name, scope) VALUES ('platform_admin', 'platform_admin', 'platform') ON CONFLICT (role_key) DO NOTHING`)
	var callers []hpCaller
	add := func(name string, user uuid.UUID, token func(*http.Request), platformNonMember bool) {
		callers = append(callers, hpCaller{name: name, user: user, userToken: token, platformNonMember: platformNonMember})
	}
	for _, role := range []string{"viewer", "member", "admin", "owner"} {
		user := v.hpUser(t, ctx, false)
		v.hpMember(t, ctx, user, role)
		add("member_"+role, user, v.bearer(t, ctx, user, &org), false)
	}
	// A member whose reads are narrowed to two repositories by resource grants.
	granted := v.hpUser(t, ctx, false)
	v.hpMember(t, ctx, granted, "member")
	for _, key := range []string{"acme/web", "acme/api"} {
		v.az.armed(t, ctx, `INSERT INTO auth.resource_grants (principal_id, organization_id, role_key, resource_kind, resource_id)
VALUES ($1, $2, 'member', 'repository', $3)`, granted, org, key)
	}
	add("member_repo_grant", granted, v.bearer(t, ctx, granted, &org), false)
	// Not a member of org A: the forged-org case (the carrier names org A for a principal that has no row there).
	outsider := v.hpUser(t, ctx, false)
	add("outsider_forged_org", outsider, v.bearer(t, ctx, outsider, &org), false)
	// Deactivated after its session started.
	deactivated := v.hpUser(t, ctx, false)
	v.hpMember(t, ctx, deactivated, "member")
	deactivatedToken := v.bearer(t, ctx, deactivated, &org)
	v.az.exec(t, ctx, `UPDATE public.users SET is_active = false WHERE id = $1`, deactivated)
	if err := v.az.identity.Project(ctx, deactivated); err != nil {
		t.Fatalf("project the deactivated principal: %v", err)
	}
	add("deactivated", deactivated, deactivatedToken, false)
	// Membership removed after its session started.
	removed := v.hpUser(t, ctx, false)
	v.hpMember(t, ctx, removed, "member")
	removedToken := v.bearer(t, ctx, removed, &org)
	v.az.armed(t, ctx, `DELETE FROM public.memberships WHERE user_id = $1 AND org_id = $2`, removed, org)
	add("membership_removed", removed, removedToken, false)
	// Platform admins (is_superuser): one a viewer of org A, one with no membership there.
	adminMember := v.hpUser(t, ctx, true)
	v.hpMember(t, ctx, adminMember, "viewer")
	add("platform_admin_member", adminMember, v.bearer(t, ctx, adminMember, &org), false)
	adminOutsider := v.hpUser(t, ctx, true)
	add("platform_admin_non_member", adminOutsider, v.bearer(t, ctx, adminOutsider, &org), true)
	return callers
}

// newHPVenue is newQPVenue with the named principal kind on, the test-only seed written, and two builds: the enforce
// build with the header cells on (after the flip) and the same build with no header cells (before the flip).
func newHPVenue(t *testing.T, ctx context.Context) (*qpVenue, *qpBuild, *qpBuild) {
	t.Helper()
	qpQuietLogs(t)
	issuerFront := httptest.NewUnstartedServer(nil)
	issuer := "https://" + issuerFront.Listener.Addr().String()
	workloadID := uuid.New()
	caller := session.WirePrincipalID(workloadID)
	az := newAuthorizeRouterEnv(t, ctx, map[string]string{
		authconfig.EnvTokenIssuerURL:                issuer,
		authconfig.EnvAuthorizeCallerSubjects:       caller,
		authconfig.EnvAuthorizeNamedPrincipalCaller: caller,
		authconfig.EnvWorkloadBootstrap:             "true",
	})
	az.caller = caller
	api := az.front.Config.Handler
	issuerFront.Config.Handler = api
	issuerFront.TLS = &tls.Config{Certificates: []tls.Certificate{authruntimeTestIssuerTLS}}
	issuerFront.StartTLS()
	t.Cleanup(issuerFront.Close)
	qpTrustGuard(t)

	v := &qpVenue{az: az, issuer: issuer, client: &http.Client{Timeout: 60 * time.Second}}
	v.orgA = az.azOrg(t, ctx, true, true)
	bootstrap := v.seedWorkload(t, ctx, workloadID)
	for _, root := range mcpclass.SortedRoots() {
		pgseed.ClassDecision(ctx, t, az.admin, mcpclass.Operation(root), "primary")
	}
	jwksPath := v.envelopeJWKS(t)
	ch, err := containers.StartClickHouse(ctx)
	if err != nil {
		t.Fatalf("start ClickHouse: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = ch.Close(closeCtx)
	})
	chschema.Apply(ctx, t, ch)
	hpSeed(t, ctx, ch.URI, v.orgA, uuid.New())
	hpSeedPostgres(t, ctx, v)
	settings := map[string]string{
		"CLICKHOUSE_URI":               ch.URI,
		"GO_API_ENVELOPE_JWKS_PATH":    jwksPath,
		"GO_API_ENVELOPE_ISSUER":       qpEnvelopeIssuer,
		"GO_API_ENVELOPE_AUDIENCE":     qpEnvelopeAudience,
		"DEV_HEALTH_ENV":               "ci",
		"AUTH_ACCESS_TOKEN_ISSUER":     issuer,
		workloadinput.EnvIssuer:        issuer,
		workloadinput.EnvBootstrapFile: bootstrap,
	}
	for _, name := range qpRouteSwitches(t) {
		settings[name] = "true"
	}
	// Production query-api runs with auth-service owning sessions (deploy values.prod.yaml): its access-token path
	// reads the auth sessions table, and its role holds that read.
	settings["AUTH_PROXY_SESSIONS"] = "true"
	settings["GO_API_REGISTRY_POSTGRES_URI"] = hpQueryAPIRole(t, ctx, az, settings)
	build := func(name, headerCells string) *qpBuild {
		counter := &qpCounter{next: api, revisions: map[int]int{}}
		proxy := httptest.NewServer(counter)
		t.Cleanup(proxy.Close)
		return v.build(t, ctx, name, qpMerge(settings, map[string]string{
			pdp.EnvMode:                        pdp.ModeEnforce,
			pdp.EnvAuthProxyURL:                proxy.URL,
			authzenforce.QueryAPI.Env():        qpEnforceCells,
			authzenforce.QueryAPIHeaders.Env(): headerCells,
		}), counter)
	}
	return v, build("H", hpHeaderCells), build("H0", "")
}

// hpQueryAPIRole is query-api's production role shape on the venue database: a LOGIN NOSUPERUSER role with exactly
// the grants of postgresstore.QueryAPIPosture, applied through the pinned river migrations as the deploy hook does.
// While auth-service owns sessions (AUTH_PROXY_SESSIONS) the role also holds the auth session read, and the platform
// role read while AUTH_PLATFORM_ROLES_OWNED, as rivermigrate grants them. It returns that role's DSN after the posture
// check passes on it.
func hpQueryAPIRole(t *testing.T, ctx context.Context, az *azRouterEnv, settings map[string]string) string {
	t.Helper()
	suffix := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	domain, queue, role := "hp_domain_"+suffix, "hp_queue_"+suffix, "hp_query_api_"+suffix
	password := testRolePassword(t)
	database := az.admin.Config().ConnConfig.Database
	statements := []string{"REVOKE TEMPORARY ON DATABASE " + database + " FROM PUBLIC"}
	for _, name := range []string{domain, queue, role} {
		statements = append(statements,
			fmt.Sprintf("CREATE ROLE %s LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '%s'", name, password),
			"GRANT CONNECT ON DATABASE "+database+" TO "+name)
	}
	for _, statement := range statements {
		if _, err := az.admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", strings.ReplaceAll(statement, password, "<password>"), err)
		}
	}
	var grants []riverstore.TableGrant
	for _, table := range postgresstore.QueryAPIPosture().RequiredTables {
		grants = append(grants, riverstore.TableGrant{TableName: table.TableName,
			AllowInsert: table.AllowInsert, AllowUpdate: table.AllowUpdate, AllowDelete: table.AllowDelete})
	}
	var columns []riverstore.ColumnGrant
	for _, column := range postgresstore.QueryAPIPosture().ColumnScoped {
		columns = append(columns, riverstore.ColumnGrant{TableName: column.TableName, ColumnName: column.ColumnName, Privilege: column.Privilege})
	}
	var sessions *postgresstore.QueryAPISessionRead
	var sessionGrant *riverstore.SessionReadGrant
	if settings["AUTH_PROXY_SESSIONS"] == "true" {
		read := postgresstore.QueryAPISessionReadFor("auth", settings["AUTH_PLATFORM_ROLES_OWNED"] == "true")
		sessions = &read
		sessionGrant = &riverstore.SessionReadGrant{Schema: read.Schema, Table: read.Table, Columns: read.Columns}
		for _, table := range read.Also {
			sessionGrant.Also = append(sessionGrant.Also, riverstore.SessionReadTable{Table: table.Table, Columns: table.Columns})
		}
	}
	if _, err := riverstore.ApplyPinnedMigrations(ctx, az.admin, riverstore.MigrationOptions{
		Schema: "river", DomainRole: domain, QueueRole: queue,
		QueryAPIRole: role, QueryAPIGrants: grants, QueryAPIColumnGrants: columns,
		QueryAPISessionRead: sessionGrant, AuthSchema: "auth",
	}); err != nil {
		t.Fatalf("apply the pinned river migrations with the query-api leg: %v", err)
	}
	dsn, err := url.Parse(az.admin.Config().ConnString())
	if err != nil {
		t.Fatal(err)
	}
	dsn.User = url.UserPassword(role, password)
	pool, err := pgxpool.New(ctx, dsn.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	var superuser bool
	if err := pool.QueryRow(ctx, `SELECT rolsuper FROM pg_roles WHERE rolname = current_user`).Scan(&superuser); err != nil || superuser {
		t.Fatalf("the query-api login must be a NOSUPERUSER role (superuser=%v, err=%v)", superuser, err)
	}
	t.Logf("query-api login %s: NOSUPERUSER, posture grants, auth session read %v (AUTH_PROXY_SESSIONS=%q, AUTH_PLATFORM_ROLES_OWNED=%q)",
		"hp_query_api_<id>", sessions != nil, settings["AUTH_PROXY_SESSIONS"], settings["AUTH_PLATFORM_ROLES_OWNED"])
	if err := postgresstore.CheckQueryAPIAuthorizationWithSessions(ctx, pool, role, "river", sessions); err != nil {
		t.Fatalf("the query-api role must satisfy its posture: %v", err)
	}
	return dsn.String()
}

// hpRepo is one seeded repository: two inside member_repo_grant's grants, one outside them.
type hpRepo struct {
	name string
	id   uuid.UUID
}

// hpRepos are the test-only repositories of the RC10 seed. Their ids follow the provider rule (sha of the lowered
// full name), so org B's decoy rows share an id with org A's, as two orgs that sync one repository do.
var hpRepos = func() []hpRepo {
	var out []hpRepo
	for _, name := range []string{"acme/web", "acme/api", "acme/ops"} {
		out = append(out, hpRepo{name: name, id: uuid.NewSHA1(uuid.NameSpaceURL, []byte(name))})
	}
	return out
}()

const (
	// hpSeedMark is in every seeded org A string; hpDecoyMark in every org B decoy string. A served body must hold
	// seeded data and must never hold a decoy.
	hpSeedMark  = "hpseed"
	hpDecoyMark = "hpdecoy"
	hpTeam      = "parity"
	// hpDecoyNumber is the base of org B's wide decoy numbers; hpDecoyFloor is the least value in a body that only
	// a decoy row can produce (org A's rows are 2..50; a mean over every seeded row with one decoy stays above it).
	hpDecoyNumber = 70000000
	hpDecoyFloor  = 1000000
	// hpSeedClock is the time of day of every seeded timestamp: a body that shows it shows a seeded row.
	hpSeedClock = "12:34:56"
)

// hpSeedTables are the ClickHouse tables the RC10 roots read: the executed query_log census of PLAN-13945-RC10x-2,
// seeded test-only.
var hpSeedTables = []string{
	"ai_attribution",
	"ai_governance_coverage_daily",
	"ai_impact_metrics_daily",
	"ai_policy_events",
	"ai_workflow_artifact_edges",
	"ai_workflow_issue_edges",
	"capacity_forecasts",
	"ci_job_runs",
	"ci_pipeline_runs",
	"compounding_risk_daily",
	"coverage_snapshots",
	"deploy_metrics_daily",
	"estimate_coverage_metrics_daily",
	"feature_flag",
	"feature_flag_event",
	"file_complexity_snapshots",
	"file_hotspot_daily",
	"git_commits",
	"git_commit_stats",
	"git_pull_requests",
	"incident_metrics_daily",
	"product_telemetry_events",
	"projects",
	"recommendations_daily",
	"repo_change_failure_daily",
	"repo_complexity_daily",
	"repo_metrics_daily",
	"repos",
	"review_edges_daily",
	"security_alerts",
	"team_metrics_daily",
	"team_project_ownership",
	"teams",
	"testops_coverage_metrics_daily",
	"testops_pipeline_metrics_daily",
	"testops_pipeline_stability",
	"testops_quality_drag",
	"testops_release_confidence",
	"testops_test_metrics_daily",
	"test_suite_results",
	"user_metrics_daily",
	"work_graph_deployment_incident_edges",
	"work_graph_edges",
	"work_graph_issue_pr",
	"work_graph_pr_commit",
	"work_graph_pr_deployment_edges",
	"work_graph_pr_review_outcome_edges",
	"work_item_cycle_times",
	"work_item_metrics_daily",
	"work_items",
	"work_item_state_durations_daily",
	"work_unit_investments",
	"work_unit_issue_size",
	"work_unit_membership_runs",
	"work_unit_supersessions",
}

// hpSeedDays are the seeded days: the generated variables' dates (2026-09-01; an end date is generated as the start)
// and 2026-09-04, days of the prior windows (20 and 24 days back for a 14-day window, 35 and 40 for a 30-day one) and
// five days of a default window that ends now (a detector wants 5 data days). A later day gets larger values (hpLiteral), so the recent window reads worse than the
// prior one.
func hpSeedDays(now time.Time) []time.Time {
	days := []time.Time{
		time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC),
	}
	for _, back := range []int{40, 35, 24, 20, 12, 9, 6, 4, 2} {
		days = append(days, now.Add(-time.Duration(back)*24*time.Hour).Truncate(24*time.Hour))
	}
	return days
}

// hpSeed writes the test-only RC10 seed: for each table, org A rows over the three repositories and seeded days,
// and one org B decoy row under acme/web's id. Values come from each column's name and type (system.columns);
// a column the generator has no value for keeps its default.
func hpSeed(t *testing.T, ctx context.Context, uri string, orgA, orgB uuid.UUID) {
	t.Helper()
	options, err := stdclickhouse.ParseDSN(uri)
	if err != nil {
		t.Fatalf("seed: parse the ClickHouse DSN: %v", err)
	}
	conn, err := stdclickhouse.Open(options)
	if err != nil {
		t.Fatalf("seed: open ClickHouse: %v", err)
	}
	defer conn.Close()
	days := hpSeedDays(time.Now().UTC())
	for _, table := range hpSeedTables {
		columns := hpColumns(t, ctx, conn, table)
		var names []string
		for _, c := range columns {
			names = append(names, c.name)
		}
		var rows []string
		n := 0
		for _, repo := range hpRepos {
			for _, day := range days {
				n++
				rows = append(rows, hpRow(table, columns, orgA, repo, day, n, false))
			}
		}
		// Org B's decoy: one row on every seeded day, so it is inside every root's read window.
		for _, day := range days {
			n++
			rows = append(rows, hpRow(table, columns, orgB, hpRepo{name: hpDecoyMark + "/web", id: hpRepos[0].id}, day, n, true))
		}
		statement := "INSERT INTO " + table + " (" + strings.Join(names, ", ") + ") VALUES " + strings.Join(rows, ", ")
		if err := conn.Exec(ctx, statement); err != nil {
			t.Fatalf("seed %s: %v", table, err)
		}
	}
}

type hpColumn struct{ name, typ string }

func hpColumns(t *testing.T, ctx context.Context, conn stdclickhouse.Conn, table string) []hpColumn {
	t.Helper()
	rows, err := conn.Query(ctx, `SELECT name, type FROM system.columns WHERE database = currentDatabase() AND table = ? AND default_kind NOT IN ('MATERIALIZED', 'ALIAS', 'EPHEMERAL') ORDER BY position`, table)
	if err != nil {
		t.Fatalf("seed: columns of %s: %v", table, err)
	}
	defer rows.Close()
	var out []hpColumn
	for rows.Next() {
		var c hpColumn
		if err := rows.Scan(&c.name, &c.typ); err != nil {
			t.Fatal(err)
		}
		if hpLiteral(table, c, uuid.Nil, hpRepos[0], time.Time{}, 1, false) != "" {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		t.Fatalf("seed: table %s has no column the seed can write (missing table?)", table)
	}
	return out
}

func hpRow(table string, columns []hpColumn, org uuid.UUID, repo hpRepo, day time.Time, n int, decoy bool) string {
	values := make([]string, len(columns))
	for i, c := range columns {
		values[i] = hpLiteral(table, c, org, repo, day, n, decoy)
	}
	return "(" + strings.Join(values, ", ") + ")"
}

var (
	hpWrapped  = regexp.MustCompile(`^(?:LowCardinality|Nullable)\((.*)\)$`)
	hpEnumHead = regexp.MustCompile(`^Enum(?:8|16)\('((?:[^'\\]|\\.)*)'`)
	hpOff      = regexp.MustCompile(`deleted|retract|tombstone|supersed|suppress|exclud|is_bot|revoked|archived|stale|hidden`)
)

// hpLiteral is the SQL literal of column c in one seeded row, or "" when the generator has none (the column keeps
// its default).
func hpLiteral(table string, c hpColumn, org uuid.UUID, repo hpRepo, day time.Time, n int, decoy bool) string {
	typ := c.typ
	nullable := strings.Contains(typ, "Nullable(")
	for {
		m := hpWrapped.FindStringSubmatch(typ)
		if m == nil {
			break
		}
		typ = m[1]
	}
	mark := hpSeedMark
	if decoy {
		mark = hpDecoyMark
	}
	name := strings.ToLower(c.name)
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
	switch {
	case nullable && hpOff.MatchString(name):
		return "NULL"
	case name == "row_view":
		return "''"
	case name == "hash" || name == "commit_hash" || name == "sha":
		return quote(fmt.Sprintf("%s-sha-%d", mark, n))
	case table == "ci_job_runs" && name == "status":
		return quote("failure")
	case table == "ci_pipeline_runs" && name == "status":
		return quote("success")
	case table == "git_pull_requests" && name == "state":
		return quote("merged")
	case table == "ai_impact_metrics_daily" && name == "attribution_bucket":
		if n%2 == 1 {
			return quote("ai_assisted")
		}
		return quote("human")
	case table == "work_item_metrics_daily" && !decoy && !hpOff.MatchString(name) && hpNumeric(typ):
		// One value on every row: the capacity forecast's Monte Carlo draws from equal days, so its answer is
		// the same on every request.
		return "5"
	case table == "work_graph_edges" && name == "source_type":
		return quote("issue")
	case table == "work_graph_edges" && name == "target_type":
		return quote("pr")
	case table == "work_graph_edges" && name == "edge_type":
		return quote("implements")
	case table == "work_graph_edges" && name == "provenance":
		return quote("native")
	case table == "ai_attribution" && name == "subject_type":
		return quote("pull_request")
	case table == "ai_attribution" && name == "subject_id":
		return quote(fmt.Sprintf("%s#%d", repo.id, 2+n))
	case table == "ai_attribution" && name == "kind":
		return quote("ai_assisted")
	case table == "ai_attribution" && name == "source":
		return quote("pr_label")
	case table == "product_telemetry_events" && name == "org_id_hash":
		return quote(producttelemetry.OrgHash(org.String()))
	case table == "product_telemetry_events" && name == "name":
		return quote("page_viewed")
	case table == "repos" && name == "id":
		return quote(repo.id.String())
	case table == "teams" && name == "id":
		return quote(hpTeam)
	case table == "teams" && name == "name":
		return quote(mark + "-team")
	case typ == "UUID":
		switch {
		case name == "org_id" || name == "organization_id":
			return quote(org.String())
		case strings.Contains(name, "repo"):
			return quote(repo.id.String())
		case strings.Contains(name, "team"):
			return quote(uuid.NewSHA1(uuid.NameSpaceURL, []byte("team:"+hpTeam)).String())
		}
		return quote(uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("%s:%s:%d", mark, name, n))).String())
	case typ == "String" || strings.HasPrefix(typ, "FixedString"):
		switch {
		case name == "org_id" || name == "organization_id":
			return quote(org.String())
		case name == "repo_id":
			return quote(repo.id.String())
		case name == "repo" || name == "repo_name" || name == "full_name" || name == "repository" || name == "repo_full_name" || name == "canonical_repo":
			return quote(repo.name)
		case name == "team_id":
			return quote(hpTeam)
		case name == "provider":
			return quote("github")
		}
		if strings.HasPrefix(typ, "FixedString") {
			return ""
		}
		return quote(fmt.Sprintf("%s-%s-%d", mark, name, n))
	case typ == "Date" || typ == "Date32":
		return quote(day.Format("2006-01-02"))
	case strings.HasPrefix(typ, "DateTime"):
		return quote(day.Format("2006-01-02") + " " + hpSeedClock)
	case typ == "Bool":
		if hpOff.MatchString(name) {
			return "false"
		}
		return "true"
	case hpNumeric(typ):
		if hpOff.MatchString(name) || strings.HasPrefix(name, "is_") || strings.HasPrefix(name, "has_") {
			if hpOff.MatchString(name) {
				return "0"
			}
			return "1"
		}
		if decoy {
			// A decoy number is large enough that any sum or mean that takes it in stays at or above hpDecoyFloor.
			if strings.HasSuffix(typ, "8") || strings.HasSuffix(typ, "16") || strings.HasPrefix(typ, "Decimal") {
				return fmt.Sprint(90 + n)
			}
			return fmt.Sprint(hpDecoyNumber + n)
		}
		return fmt.Sprint(2 + n)
	case strings.HasPrefix(typ, "Enum"):
		if m := hpEnumHead.FindStringSubmatch(typ); m != nil {
			return "'" + m[1] + "'"
		}
	case strings.HasPrefix(typ, "Array("):
		return "[]"
	case strings.HasPrefix(typ, "Map("):
		return "{}"
	}
	return ""
}

// hpReportID is the test-only saved report of the seed.
var hpReportID = uuid.NewSHA1(uuid.NameSpaceURL, []byte(hpSeedMark+"-report"))

// hpSeedPostgres writes the test-only PostgreSQL rows the RC10 roots read (sourceHealth, savedReport(s), reportRuns),
// with one org B decoy of each.
func hpSeedPostgres(t *testing.T, ctx context.Context, v *qpVenue) {
	t.Helper()
	now := time.Now().UTC().Add(-24 * time.Hour).Truncate(24 * time.Hour).Add(12*time.Hour + 34*time.Minute + 56*time.Second)
	for _, row := range []struct {
		org, mark string
		report    uuid.UUID
	}{{v.orgA.String(), hpSeedMark, hpReportID}, {uuid.NewString(), hpDecoyMark, uuid.New()}} {
		v.az.exec(t, ctx, `INSERT INTO public.sync_configurations (id, org_id, name, provider, created_at, updated_at, last_sync_at, last_sync_success)
VALUES ($1, $2, $3, $3, $4, $4, $4, true)`, uuid.New(), row.org, row.mark+"-sync", now)
		v.az.exec(t, ctx, `INSERT INTO public.saved_reports (id, org_id, name, description, created_at, updated_at) VALUES ($1, $2, $3, $4, $5, $5)`,
			row.report, row.org, row.mark+"-report", row.mark+"-report-description", now)
		v.az.exec(t, ctx, `INSERT INTO public.report_runs (id, report_id, status, started_at, completed_at, duration_seconds, rendered_markdown, created_at)
VALUES ($1, $2, 'completed', $3, $3, 2.5, $4, $3)`, uuid.New(), row.report, now, row.mark+"-run")
	}
}

func hpNumeric(typ string) bool {
	for _, prefix := range []string{"Int", "UInt", "Float", "Decimal"} {
		if strings.HasPrefix(typ, prefix) {
			return true
		}
	}
	return false
}
