//go:build integration

package main

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
)

// The saved-report reads, exercised through the real /query handler against a
// real Postgres: a signed envelope is verified by the real principal.Verifier,
// its claims reach the resolvers through the real middleware, and every
// statement runs on the production pgx pool.

const savedReportsPostgresDDL = `
CREATE TABLE saved_reports (
    id uuid PRIMARY KEY,
    org_id text NOT NULL DEFAULT '',
    name text NOT NULL,
    description text,
    report_plan json NOT NULL DEFAULT '{}',
    is_template boolean NOT NULL DEFAULT false,
    template_source_id uuid REFERENCES saved_reports(id) ON DELETE SET NULL,
    parameters json,
    schedule_id uuid,
    is_active boolean NOT NULL DEFAULT true,
    last_run_at timestamptz,
    last_run_status text,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    created_by text
);
CREATE TABLE report_runs (
    id uuid PRIMARY KEY,
    report_id uuid NOT NULL REFERENCES saved_reports(id) ON DELETE CASCADE,
    status text NOT NULL,
    started_at timestamptz,
    completed_at timestamptz,
    duration_seconds double precision,
    rendered_markdown text,
    artifact_url text,
    provenance_records json,
    error text,
    triggered_by text NOT NULL DEFAULT 'manual',
    created_at timestamptz NOT NULL
);
INSERT INTO saved_reports (id, org_id, name, description, report_plan, parameters, created_at, updated_at, created_by, last_run_at, last_run_status) VALUES
 ('aaaaaaaa-0000-0000-0000-00000000000a', 'org-1', 'Weekly', 'first', '{"b": 1, "a": [1, 2.5, null], "b": 3}', NULL, '2026-01-01T00:00:00Z', '2026-01-03T00:00:00Z', 'ABC-123', '2026-01-02T03:04:05Z', 'success'),
 ('aaaaaaaa-0000-0000-0000-00000000000b', 'org-1', 'Monthly', NULL, '{}', '{"team": "t-1"}', '2026-01-01T00:00:00Z', '2026-01-05T00:00:00Z', NULL, NULL, NULL),
 ('cccccccc-0000-0000-0000-00000000000c', 'org-2', 'Weekly', 'foreign', '{"foreign": true}', NULL, '2026-01-01T00:00:00Z', '2026-01-04T00:00:00Z', NULL, NULL, NULL);
UPDATE saved_reports SET template_source_id = 'aaaaaaaa-0000-0000-0000-00000000000a' WHERE id = 'aaaaaaaa-0000-0000-0000-00000000000b';
INSERT INTO report_runs (id, report_id, status, started_at, completed_at, duration_seconds, rendered_markdown, provenance_records, error, triggered_by, created_at) VALUES
 ('bbbbbbbb-0000-0000-0000-000000000001', 'aaaaaaaa-0000-0000-0000-00000000000a', 'success', '2026-01-02T03:00:00Z', '2026-01-02T03:04:05Z', 245.5, '# done', '[{"k": 1, "k": 2}]', NULL, 'api', '2026-01-02T03:00:00Z'),
 ('bbbbbbbb-0000-0000-0000-000000000002', 'aaaaaaaa-0000-0000-0000-00000000000a', 'failed', NULL, NULL, NULL, NULL, NULL, 'boom', 'scheduler', '2026-01-03T03:00:00Z'),
 ('bbbbbbbb-0000-0000-0000-000000000003', 'aaaaaaaa-0000-0000-0000-00000000000a', 'pending', NULL, NULL, NULL, NULL, NULL, NULL, 'api', '2026-01-04T03:00:00Z'),
 ('dddddddd-0000-0000-0000-000000000001', 'cccccccc-0000-0000-0000-00000000000c', 'success', NULL, NULL, NULL, NULL, NULL, NULL, 'api', '2026-01-02T03:00:00Z');
`

const (
	reportA = "aaaaaaaa-0000-0000-0000-00000000000a"
	reportB = "aaaaaaaa-0000-0000-0000-00000000000b"
	reportC = "cccccccc-0000-0000-0000-00000000000c"
)

func TestSavedReportRoutes_ThroughTheSignedEnvelope(t *testing.T) {
	pool := startTestRegistryPostgres(t)
	if _, err := pool.Exec(t.Context(), savedReportsPostgresDDL); err != nil {
		t.Fatal(err)
	}
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := principal.NewVerifier(writeTestJWKS(t, pub), itTestIssuer, itTestAudience)
	if err != nil {
		t.Fatal(err)
	}
	handler, _, _ := newQueryHandler(emptyCHClient{}, pool, verifier, itTestSchemaDigest)
	for doc, op := range map[string]string{
		registeredSavedReportsDocument: "savedReports",
		registeredSavedReportDocument:  "savedReport",
		registeredReportRunsDocument:   "reportRuns",
		registeredBusFactorDocument:    "busFactor",
	} {
		setRoutingMode(t, pool, digestHex(doc), op, "canary")
	}

	token := func(org string) string {
		return signDataHealthEnvelope(t, priv, principal.Claims{OrgID: org, Role: "member"}, time.Now().Add(time.Hour))
	}
	run := func(doc, org string, vars map[string]any) (int, map[string]any) {
		rec := postGraphQLWithVariables(t, handler, doc, token(org), vars)
		var body map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("response is not JSON: %v: %s", err, rec.Body.String())
		}
		return rec.Code, body
	}
	data := func(body map[string]any, root string) map[string]any {
		d, _ := body["data"].(map[string]any)
		out, _ := d[root].(map[string]any)
		return out
	}
	items := func(conn map[string]any) []map[string]any {
		var out []map[string]any
		list, _ := conn["items"].([]any)
		for _, it := range list {
			out = append(out, it.(map[string]any))
		}
		return out
	}
	names := func(conn map[string]any) string {
		var n []string
		for _, it := range items(conn) {
			n = append(n, it["name"].(string))
		}
		return strings.Join(n, ",")
	}
	denied := func(body map[string]any) bool {
		b, _ := json.Marshal(body["errors"])
		return strings.Contains(string(b), "AUTHORIZATION_ERROR")
	}

	t.Run("list is the caller's org only, newest update first", func(t *testing.T) {
		_, body := run(registeredSavedReportsDocument, "org-1", map[string]any{"orgId": "org-1", "limit": 50, "offset": 0})
		conn := data(body, "savedReports")
		if conn["total"] != float64(2) || names(conn) != "Monthly,Weekly" {
			t.Fatalf("org-1 list = %v", body)
		}
		_, body = run(registeredSavedReportsDocument, "org-2", map[string]any{"orgId": "org-2", "limit": 50, "offset": 0})
		if conn = data(body, "savedReports"); conn["total"] != float64(1) || names(conn) != "Weekly" || items(conn)[0]["description"] != "foreign" {
			t.Fatalf("org-2 list = %v", body)
		}
	})

	t.Run("paging", func(t *testing.T) {
		for _, tc := range []struct {
			limit, offset int
			want          string
		}{{1, 0, "Monthly"}, {1, 1, "Weekly"}, {1, 2, ""}, {0, 0, ""}, {50, 100000, ""}} {
			_, body := run(registeredSavedReportsDocument, "org-1", map[string]any{"orgId": "org-1", "limit": tc.limit, "offset": tc.offset})
			conn := data(body, "savedReports")
			if names(conn) != tc.want || conn["total"] != float64(2) {
				t.Errorf("limit=%d offset=%d: %v", tc.limit, tc.offset, body)
			}
			if _, isList := conn["items"].([]any); !isList {
				t.Errorf("limit=%d offset=%d: items is not a list: %v", tc.limit, tc.offset, body)
			}
		}
		_, body := run(registeredSavedReportsDocument, "org-1", map[string]any{"orgId": "org-1", "limit": -1, "offset": 0})
		if body["errors"] == nil || data(body, "savedReports") != nil {
			t.Errorf("a negative limit must be an error: %v", body)
		}
	})

	t.Run("explicit null pagination is refused, omitted takes the default", func(t *testing.T) {
		for _, vars := range []map[string]any{
			{"orgId": "org-1", "limit": nil, "offset": 0},
			{"orgId": "org-1", "limit": 50, "offset": nil},
		} {
			_, body := run(registeredSavedReportsDocument, "org-1", vars)
			if body["errors"] == nil || !strings.Contains(toJSON(body["errors"]), "must not be null") || data(body, "savedReports") != nil {
				t.Errorf("explicit null served: %v", body)
			}
		}
		_, body := run(registeredReportRunsDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": reportA, "limit": nil})
		if body["errors"] == nil || !strings.Contains(toJSON(body["errors"]), "must not be null") {
			t.Errorf("explicit null runs limit served: %v", body)
		}
		// A nullable argument still accepts an explicit null.
		_, body = run(registeredBusFactorDocument, "org-1", map[string]any{"orgId": "org-1", "scope": nil})
		if strings.Contains(toJSON(body), "must not be null") {
			t.Errorf("explicit null for a nullable argument refused: %v", body)
		}
		_, body = run(registeredSavedReportsDocument, "org-1", map[string]any{"orgId": "org-1"})
		if conn := data(body, "savedReports"); body["errors"] != nil || conn["total"] != float64(2) || len(items(conn)) != 2 {
			t.Errorf("omitted limit/offset = %v", body)
		}
		_, body = run(registeredReportRunsDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": reportA})
		if conn := data(body, "reportRuns"); body["errors"] != nil || conn["total"] != float64(3) {
			t.Errorf("omitted runs limit = %v", body)
		}
	})

	t.Run("every argument across absent, null, zero, empty, wrong type and bounds", func(t *testing.T) {
		type cell struct {
			name    string
			doc     string
			vars    map[string]any
			refused bool
			total   float64
		}
		list := func(v map[string]any) map[string]any { v["orgId"] = "org-1"; return v }
		cells := []cell{
			{"list limit omitted", registeredSavedReportsDocument, list(map[string]any{"offset": 0}), false, 2},
			{"list offset omitted", registeredSavedReportsDocument, list(map[string]any{"limit": 50}), false, 2},
			{"list limit null", registeredSavedReportsDocument, list(map[string]any{"limit": nil}), true, 0},
			{"list offset null", registeredSavedReportsDocument, list(map[string]any{"offset": nil}), true, 0},
			{"list limit zero", registeredSavedReportsDocument, list(map[string]any{"limit": 0}), false, 2},
			{"list offset zero", registeredSavedReportsDocument, list(map[string]any{"offset": 0}), false, 2},
			{"list limit negative", registeredSavedReportsDocument, list(map[string]any{"limit": -1}), true, 0},
			{"list offset negative", registeredSavedReportsDocument, list(map[string]any{"offset": -1}), true, 0},
			{"list limit int32 max", registeredSavedReportsDocument, list(map[string]any{"limit": 2147483647}), false, 2},
			{"list limit int32 max+1", registeredSavedReportsDocument, list(map[string]any{"limit": 2147483648}), true, 0},
			{"list offset int32 max+1", registeredSavedReportsDocument, list(map[string]any{"offset": 2147483648}), true, 0},
			// Named limit: gqlgen's Int scalar reads a numeric string for every
			// Go-served Int argument, where graphql-core refuses it; the cell pins
			// today's behaviour.
			{"list limit numeric string", registeredSavedReportsDocument, list(map[string]any{"limit": "5"}), false, 2},
			{"list limit fractional", registeredSavedReportsDocument, list(map[string]any{"limit": 1.5}), true, 0},
			{"list limit bool", registeredSavedReportsDocument, list(map[string]any{"limit": true}), true, 0},
			{"list orgId omitted", registeredSavedReportsDocument, map[string]any{"limit": 5}, true, 0},
			{"list orgId null", registeredSavedReportsDocument, map[string]any{"orgId": nil}, true, 0},
			{"list orgId empty", registeredSavedReportsDocument, map[string]any{"orgId": ""}, true, 0},
			{"get reportId omitted", registeredSavedReportDocument, map[string]any{"orgId": "org-1"}, true, 0},
			{"get reportId null", registeredSavedReportDocument, map[string]any{"orgId": "org-1", "reportId": nil}, true, 0},
			{"get reportId empty", registeredSavedReportDocument, map[string]any{"orgId": "org-1", "reportId": ""}, true, 0},
			{"get reportId number", registeredSavedReportDocument, map[string]any{"orgId": "org-1", "reportId": 12}, true, 0},
			{"runs reportId omitted", registeredReportRunsDocument, map[string]any{"orgId": "org-1"}, true, 0},
			{"runs reportId null", registeredReportRunsDocument, map[string]any{"orgId": "org-1", "reportId": nil}, true, 0},
			{"runs reportId empty", registeredReportRunsDocument, map[string]any{"orgId": "org-1", "reportId": ""}, true, 0},
			{"runs limit zero", registeredReportRunsDocument, map[string]any{"orgId": "org-1", "reportId": reportA, "limit": 0}, false, 3},
			{"runs limit negative", registeredReportRunsDocument, map[string]any{"orgId": "org-1", "reportId": reportA, "limit": -1}, true, 0},
			{"runs limit int32 max+1", registeredReportRunsDocument, map[string]any{"orgId": "org-1", "reportId": reportA, "limit": 2147483648}, true, 0},
			{"runs limit numeric string", registeredReportRunsDocument, map[string]any{"orgId": "org-1", "reportId": reportA, "limit": "5"}, false, 3},
			{"runs limit non-numeric string", registeredReportRunsDocument, map[string]any{"orgId": "org-1", "reportId": reportA, "limit": "five"}, true, 0},
		}
		for _, c := range cells {
			_, body := run(c.doc, "org-1", c.vars)
			raw := toJSON(body)
			if c.refused {
				if body["errors"] == nil || strings.Contains(raw, "Weekly") || strings.Contains(raw, "Monthly") {
					t.Errorf("%s: not refused: %s", c.name, raw)
				}
				continue
			}
			root := "savedReports"
			if c.doc == registeredReportRunsDocument {
				root = "reportRuns"
			}
			if body["errors"] != nil || data(body, root)["total"] != c.total {
				t.Errorf("%s: %s", c.name, raw)
			}
		}
	})

	t.Run("one report by id", func(t *testing.T) {
		_, body := run(registeredSavedReportDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": reportA})
		got := data(body, "savedReport")
		if got["id"] != reportA || got["orgId"] != "org-1" || got["name"] != "Weekly" || got["createdBy"] != "ABC-123" ||
			got["lastRunStatus"] != "success" || got["lastRunAt"] != "2026-01-02T03:04:05Z" {
			t.Fatalf("report = %v", body)
		}
		if plan, _ := json.Marshal(got["reportPlan"]); string(plan) != `{"a":[1,2.5,null],"b":3}` {
			t.Errorf("reportPlan = %s", plan)
		}
		_, body = run(registeredSavedReportDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": reportB})
		got = data(body, "savedReport")
		if got["templateSourceId"] != reportA || got["parameters"].(map[string]any)["team"] != "t-1" || got["description"] != nil {
			t.Errorf("clone report = %v", body)
		}
		// the same id in other spellings names the same report
		for _, spelling := range []string{strings.ToUpper(reportA), strings.ReplaceAll(reportA, "-", ""), "{" + reportA + "}", "urn:uuid:" + reportA} {
			_, body = run(registeredSavedReportDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": spelling})
			if data(body, "savedReport")["id"] != reportA {
				t.Errorf("spelling %q: %v", spelling, body)
			}
		}
	})

	t.Run("another org's report is null, never served", func(t *testing.T) {
		_, body := run(registeredSavedReportDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": reportC})
		if body["errors"] != nil || data(body, "savedReport") != nil || strings.Contains(toJSON(body), "foreign") {
			t.Fatalf("org-1 read org-2's report: %v", body)
		}
		_, body = run(registeredSavedReportDocument, "org-2", map[string]any{"orgId": "org-2", "reportId": reportC})
		if data(body, "savedReport")["id"] != reportC {
			t.Fatalf("org-2 cannot read its own report: %v", body)
		}
		_, body = run(registeredSavedReportDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": "00000000-0000-0000-0000-000000000001"})
		if body["errors"] != nil || data(body, "savedReport") != nil {
			t.Fatalf("unknown id = %v", body)
		}
	})

	t.Run("malformed id is an error", func(t *testing.T) {
		_, body := run(registeredSavedReportDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": "not-a-uuid"})
		if body["errors"] == nil || data(body, "savedReport") != nil {
			t.Fatalf("malformed id = %v", body)
		}
	})

	t.Run("runs newest first, paged, scoped by the report's org", func(t *testing.T) {
		_, body := run(registeredReportRunsDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": reportA, "limit": 50})
		conn := data(body, "reportRuns")
		var statuses []string
		for _, it := range items(conn) {
			statuses = append(statuses, it["status"].(string))
		}
		if conn["total"] != float64(3) || strings.Join(statuses, ",") != "pending,failed,success" {
			t.Fatalf("runs = %v", body)
		}
		last := items(conn)[2]
		if last["durationSeconds"] != 245.5 || last["renderedMarkdown"] != "# done" || last["triggeredBy"] != "api" || last["startedAt"] != "2026-01-02T03:00:00Z" {
			t.Errorf("run = %v", last)
		}
		if p, _ := json.Marshal(last["provenanceRecords"]); string(p) != `[{"k":2}]` {
			t.Errorf("provenanceRecords = %s", p)
		}
		first := items(conn)[0]
		if first["error"] != nil || first["startedAt"] != nil || first["provenanceRecords"] != nil {
			t.Errorf("pending run = %v", first)
		}
		_, body = run(registeredReportRunsDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": reportA, "limit": 1})
		if conn = data(body, "reportRuns"); len(items(conn)) != 1 || conn["total"] != float64(3) {
			t.Errorf("limit 1 = %v", body)
		}
		for _, id := range []string{reportC, "00000000-0000-0000-0000-000000000001"} {
			_, body = run(registeredReportRunsDocument, "org-1", map[string]any{"orgId": "org-1", "reportId": id, "limit": 50})
			if conn = data(body, "reportRuns"); body["errors"] != nil || len(items(conn)) != 0 || conn["total"] != float64(0) {
				t.Errorf("runs of %s read by org-1 = %v", id, body)
			}
		}
		_, body = run(registeredReportRunsDocument, "org-2", map[string]any{"orgId": "org-2", "reportId": reportC, "limit": 50})
		if conn = data(body, "reportRuns"); conn["total"] != float64(1) {
			t.Errorf("org-2 runs = %v", body)
		}
	})

	t.Run("an orgId naming another org is denied on every field", func(t *testing.T) {
		for doc, vars := range map[string]map[string]any{
			registeredSavedReportsDocument: {"orgId": "org-2", "limit": 50, "offset": 0},
			registeredSavedReportDocument:  {"orgId": "org-2", "reportId": reportC},
			registeredReportRunsDocument:   {"orgId": "org-2", "reportId": reportC, "limit": 50},
		} {
			code, body := run(doc, "org-1", vars)
			if code != http.StatusOK || !denied(body) || strings.Contains(toJSON(body), "foreign") {
				t.Errorf("foreign orgId served: %d %v", code, body)
			}
		}
	})

	t.Run("no envelope is refused before any read", func(t *testing.T) {
		rec := postGraphQLWithVariables(t, handler, registeredSavedReportsDocument, "", map[string]any{"orgId": "org-1", "limit": 50, "offset": 0})
		if rec.Code != http.StatusUnauthorized || strings.Contains(rec.Body.String(), "Weekly") {
			t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
		}
	})
}

func toJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
