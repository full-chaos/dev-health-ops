//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/principal"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The saved-report mutations, differentially: the REAL Python resolvers (the
// real /graphql app, through TestClient) and the Go mutations over the real
// query-api dispatch pipeline answer the same requests against two copies of
// one Postgres database that the real Alembic chain built and one seed filled.
// Go writes through a login that holds ONLY the additive write manifest
// (postgres.QueryAPIWritePosture), so a privilege a mutation needs and the
// manifest lacks fails here, on the real schema.
//
// Compared: the response (status, and the body decoded and re-encoded the way
// Python's json does, so key order, int-versus-float and escapes all count),
// then every row the mutations touched, column by column.

const (
	oracleJWTKey = "venue-oracle-saved-report-mutations-secret-key-32b!"
	oracleOrgA   = "0a0a0a0a-0a0a-4a0a-8a0a-0a0a0a0a0a0a"
	oracleOrgB   = "0b0b0b0b-0b0b-4b0b-8b0b-0b0b0b0b0b0b"
)

// The seeded reports, fixed ids so both planes name the same rows.
const (
	reportPlain     = "a0000000-0000-4000-8000-000000000001" // org A, active, no schedule
	reportScheduled = "a0000000-0000-4000-8000-000000000002" // org A, active, scheduled
	reportInactive  = "a0000000-0000-4000-8000-000000000003" // org A, inactive
	reportOther     = "b0000000-0000-4000-8000-000000000001" // org B
	reportClone     = "a0000000-0000-4000-8000-000000000004" // org A, plan + parameters to clone
	reportDelete    = "a0000000-0000-4000-8000-000000000005" // org A, with a run to cascade
	reportMissing   = "c0000000-0000-4000-8000-00000000dead" // names nothing
	// The DST reports: a fixed last_run_at just before a zone's transition, so a
	// schedule's next run is a function of (cron, zone, base) alone.
	reportDST1 = "a0000000-0000-4000-8000-000000000011" // 2026-03-08T06:30Z, before New York springs forward
	reportDST2 = "a0000000-0000-4000-8000-000000000012" // 2026-11-01T05:30Z, before New York falls back
	reportDST3 = "a0000000-0000-4000-8000-000000000013" // 2026-10-03T14:30Z, before Lord Howe springs forward
	reportDST4 = "a0000000-0000-4000-8000-000000000014" // 2026-10-25T00:30Z, before London falls back
)

// A fixed instant well in the past: a timestamp equal to it is compared as
// text (it is data), a timestamp near "now" is a per-plane clock reading.
const seededAt = "2026-01-02T03:04:05+00:00"

type oracleCase struct {
	name string
	op   string // operation: createSavedReport ...
	org  string // the caller's org: A or B
	vars string // the variables object, raw JSON text (never re-encoded)
	// expect is what the Python plane must answer (see expectation).
	expect expectation
	// messageDiffers names a case whose error MESSAGE is a declared divergence
	// (the reason); both planes must still answer an error, and every other part
	// of the answer is still compared.
	messageDiffers string
}

func oracleDocuments() map[string]string {
	return map[string]string{
		"createSavedReport": registeredCreateSavedReportDocument,
		"updateSavedReport": registeredUpdateSavedReportDocument,
		"deleteSavedReport": registeredDeleteSavedReportDocument,
		"cloneSavedReport":  registeredCloneSavedReportDocument,
		"triggerReport":     registeredTriggerReportDocument,
	}
}

func repoRootFromHere(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve package path")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
}

func TestSavedReportMutationsVenueOracle(t *testing.T) {
	runSavedReportVenue(t, oracleCases(), nil)
}

// runSavedReportVenue sends every case to both planes. A case named in
// goRefuses is a declared divergence in which Python accepts what Go refuses:
// each plane's answer is asserted as that, the rows are not compared (they
// differ by construction), and the count of rows each plane wrote is.
func runSavedReportVenue(t *testing.T, cases []oracleCase, goRefuses map[string]string) {
	ctx := context.Background()
	root := repoRootFromHere(t)
	userA, userB := uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root, JWTKey: oracleJWTKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for i, org := range []struct {
				id, slug string
				user     uuid.UUID
			}{{oracleOrgA, "venue-a", userA}, {oracleOrgB, "venue-b", userB}} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $3, 'community', 'stripe', true, now(), now())`, org.id, org.slug, fmt.Sprintf("Venue %d", i))
				exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, $2, true, true, false, 0, now(), now())`, org.user, fmt.Sprintf("venue-%d@example.com", i))
				exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, uuid.New(), org.id, org.user)
			}
			job := uuid.MustParse("d0000000-0000-4000-8000-000000000001")
			exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, status, is_running, next_run_at, run_count, failure_count, created_at, updated_at)
VALUES ($1, $2, 'report:scheduled', 'report', '', '0 6 * * *', 'UTC', $3::json, 0, false, $4, 0, 0, $4, $4)`,
				job, oracleOrgA, `{"report_id": "`+reportScheduled+`"}`, seededAt)
			for _, r := range []struct {
				id, org, name, plan, params string
				schedule                    *uuid.UUID
				active                      bool
			}{
				{reportPlain, oracleOrgA, "plain", `{"sections": [1, 2]}`, `{"team": "core"}`, nil, true},
				{reportScheduled, oracleOrgA, "scheduled", `{}`, `{}`, &job, true},
				{reportInactive, oracleOrgA, "inactive", `{}`, `{}`, nil, false},
				{reportOther, oracleOrgB, "other", `{}`, `{}`, nil, true},
				{reportClone, oracleOrgA, "clone-source", `{"z": 1, "a": {"y": [1.5, "é"]}}`, `{"b": 1, "a": 2}`, nil, true},
				{reportDelete, oracleOrgA, "to-delete", `{}`, `{}`, nil, true},
				{reportDST1, oracleOrgA, "dst-1", `{}`, `{}`, nil, true},
				{reportDST2, oracleOrgA, "dst-2", `{}`, `{}`, nil, true},
				{reportDST3, oracleOrgA, "dst-3", `{}`, `{}`, nil, true},
				{reportDST4, oracleOrgA, "dst-4", `{}`, `{}`, nil, true},
			} {
				at := seededAt
				// created_at differs from last_run_at, so a schedule's base
				// (last_run_at when there is one, else created_at) is decided.
				createdAt := "2025-12-01T00:00:00+00:00"
				switch r.id {
				case reportDST1:
					at = "2026-03-08T06:30:00+00:00"
				case reportDST2:
					at = "2026-11-01T05:30:00+00:00"
				case reportDST3:
					at = "2026-10-03T14:30:00+00:00"
				case reportDST4:
					at = "2026-10-25T00:30:00+00:00"
				}
				exec(`INSERT INTO saved_reports (id, org_id, name, description, report_plan, is_template, parameters, schedule_id, is_active, last_run_at, last_run_status, created_by, created_at, updated_at)
VALUES ($1, $2, $3, 'seeded', $4::json, false, $5::json, $6, $7, $8, 'success', 'seed', $9, $8)`,
					r.id, r.org, r.name, r.plan, r.params, r.schedule, r.active, at, createdAt)
			}
			exec(`INSERT INTO report_runs (id, report_id, status, triggered_by, provenance_records, attempt_count, execution_reclaim_count, notification_status, created_at)
VALUES ('e0000000-0000-4000-8000-000000000001', $1, 'success', 'seed', '[]'::json, 0, 0, 'pending', $2)`, reportDelete, seededAt)
			return map[string]map[string]any{
				"a": {"user_id": userA.String(), "email": "venue-0@example.com", "org_id": oracleOrgA, "role": "admin"},
				"b": {"user_id": userB.String(), "email": "venue-1@example.com", "org_id": oracleOrgB, "role": "admin"},
			}
		},
	})

	docs := oracleDocuments()
	requests := make([]venueoracle.Request, len(cases))
	for i, c := range cases {
		token := venue.Tokens["a"]
		if c.org == oracleOrgB {
			token = venue.Tokens["b"]
		}
		body := `{"query": ` + jsonString(docs[c.op]) + `, "variables": ` + c.vars + `}`
		requests[i] = venueoracle.Request{
			Name: c.name, Method: "POST", Path: "/graphql",
			Headers: map[string]string{"Authorization": "Bearer " + token, "Content-Type": "application/json"},
			Body:    venueoracle.B64(body),
		}
	}
	python := venue.ServePython(t, requests)

	goHandler, closeGo := startGoMutationServer(t, ctx, venue)
	defer closeGo()
	failures := 0
	var receipt strings.Builder
	for i, c := range cases {
		goResponse := doGo(t, goHandler, c, docs[c.op], requests[i])
		if reason, declared := goRefuses[c.name]; declared {
			pyBody, _ := normalizeMutationBody(python[i].Body, "python")
			goBody, _ := normalizeMutationBody(goResponse.Body, "go")
			ok := python[i].Status == 200 && !errorsPresent(pyBody) && errorsPresent(goBody)
			fmt.Fprintf(&receipt, "%-58s python=%d go=%d DECLARED (%s) holds=%v\n", c.name, python[i].Status, goResponse.Status, reason, ok)
			if !ok {
				failures++
				t.Errorf("%s: declared divergence (%s) does not hold\n python: %s\n go:     %s", c.name, reason, truncateOracleBody(python[i].Body), truncateOracleBody(goResponse.Body))
			}
			continue
		}
		if why := unmetExpectation(c, python[i]); why != "" {
			failures++
			t.Errorf("%s: the case is malformed, the Python plane did not answer as expected: %s\n python: %s", c.name, why, truncateOracleBody(python[i].Body))
			continue
		}
		same, why := compareMutationResponse(c, python[i], goResponse)
		fmt.Fprintf(&receipt, "%-58s python=%d go=%d %s\n", c.name, python[i].Status, goResponse.Status, venueoracle.Mark(same))
		if !same {
			failures++
			t.Errorf("%s: %s\n python: %s\n go:     %s", c.name, why, truncateOracleBody(python[i].Body), truncateOracleBody(goResponse.Body))
		}
	}
	t.Log("\n" + receipt.String())

	if len(goRefuses) == 0 {
		compareRows(t, ctx, venue)
	} else {
		compareRowCounts(t, ctx, venue, len(goRefuses))
	}
	if failures == 0 {
		t.Logf("%d requests compared", len(cases))
	}
	venueoracle.WriteProof(t)
}

func jsonString(text string) string {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(text); err != nil {
		panic(err)
	}
	return strings.TrimSpace(out.String())
}

// startGoMutationServer builds the Go side: the real dispatch pipeline
// (digest -> registered operation, principal envelope, org context, raw body)
// over the real gqlgen executor and the real Writer, with the pool logged in
// as a role holding only the additive write manifest.
func startGoMutationServer(t *testing.T, ctx context.Context, venue *venueoracle.Venue) (http.HandlerFunc, func()) {
	t.Helper()
	admin, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(admin.Close)
	const role = "venue_qapi_writer"
	const password = "venue_qapi_password"
	for _, statement := range []string{
		"CREATE ROLE " + role + " LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD '" + password + "'",
		"GRANT USAGE ON SCHEMA public TO " + role,
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}
	for _, table := range postgresstore.QueryAPIWritePosture().RequiredTables {
		privileges := "SELECT"
		if table.AllowInsert {
			privileges += ", INSERT"
		}
		if table.AllowUpdate {
			privileges += ", UPDATE"
		}
		if table.AllowDelete {
			privileges += ", DELETE"
		}
		if _, err := admin.Exec(ctx, "GRANT "+privileges+" ON public."+table.TableName+" TO "+role); err != nil {
			t.Fatalf("grant %s: %v", table.TableName, err)
		}
	}
	t.Cleanup(func() { containers.DropRole(admin, role, t.Logf) })

	pool, err := pgxpool.New(ctx, withRole(t, venue.AdminURI(t, venue.GoDB), role, password))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := postgresstore.CheckQueryAPIWriteGrants(ctx, pool, role); err != nil {
		t.Fatalf("the venue role must satisfy the write manifest it was granted: %v", err)
	}

	writer := newReportWriter(pool, filepath.Join(repoRootFromHere(t), "contracts", "jobs", "v1"))
	if writer == nil || writer.Outbox == nil {
		t.Fatal("the writer has no outbox: triggerReport would not be measured")
	}
	gql := newGraphQLServer(&graph.Resolver{Postgres: pool, ReportWriter: writer})

	verifier, priv := iaVerifier(t)
	byDigest := map[string]string{}
	static := routeswitch.StaticSwitch{}
	for op, doc := range oracleDocuments() {
		byDigest[digestHex(doc)] = op
		static[op] = true
	}
	mux := routeswitch.NewMux(static)
	for op := range oracleDocuments() {
		mux.Register(op, gql)
	}
	handler := newDocumentDispatchHandler(os.Getenv, mux, byDigest, verifier, true)
	envelopes = func(org string) string {
		return iaEnvelope(t, priv, principal.Claims{OrgID: org, Role: "admin"})
	}
	return handler, func() {}
}

var envelopes func(org string) string

func withRole(t *testing.T, raw, role, password string) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed.User = url.UserPassword(role, password)
	return parsed.String()
}

func doGo(t *testing.T, handler http.HandlerFunc, c oracleCase, doc string, request venueoracle.Request) venueoracle.Response {
	t.Helper()
	raw, err := base64.StdEncoding.DecodeString(*request.Body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+envelopes(c.org))
	rec := httptest.NewRecorder()
	handler(rec, req)
	body, _ := io.ReadAll(rec.Result().Body)
	return venueoracle.Response{Status: rec.Code, Headers: map[string]string{"content-type": rec.Header().Get("Content-Type")}, Body: string(body)}
}

var (
	oracleUUID = regexp.MustCompile(`[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}`)
	oracleTime = regexp.MustCompile(`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})`)
)

var seededIDs = map[string]bool{
	reportPlain: true, reportScheduled: true, reportInactive: true, reportOther: true, reportClone: true,
	reportDelete: true, reportMissing: true, oracleOrgA: true, oracleOrgB: true,
	"d0000000-0000-4000-8000-000000000001": true, "e0000000-0000-4000-8000-000000000001": true,
}

// normalizeMutationBody decodes the response the way Python's json does and
// re-encodes it the way json.dumps does (so key order, int versus float and
// escapes count), then masks what is a per-plane reading: a generated id is
// numbered by first appearance, a timestamp near the present keeps only its
// shape (whether it has a fractional part). A seeded id stays, and a seeded
// timestamp is compared as an instant.
//
// Two named divergences are normalized, each asserted rather than ignored:
//   - the DateTime wire text: Python writes "+00:00", Go "Z" for the same
//     instant (the divergence already declared for the saved-report reads).
//     Each plane must use ITS form on every timestamp.
//   - the order of the top-level "data" and "errors" members of an error
//     response (Python writes data first, Go errors first).
func normalizeMutationBody(body, plane string) (string, error) {
	canonical, err := reports.PythonDumps([]byte(body))
	if err != nil {
		return "", err
	}
	numbered := map[string]int{}
	canonical = oracleUUID.ReplaceAllStringFunc(canonical, func(id string) string {
		if seededIDs[id] {
			return id
		}
		if _, ok := numbered[id]; !ok {
			numbered[id] = len(numbered) + 1
		}
		return fmt.Sprintf("<uuid#%d>", numbered[id])
	})
	now := time.Now()
	wantSuffix := map[string]string{"python": "+00:00", "go": "Z"}[plane]
	canonical = oracleTime.ReplaceAllStringFunc(canonical, func(stamp string) string {
		if !strings.HasSuffix(stamp, wantSuffix) {
			return "<ts with the wrong offset form: " + stamp + ">"
		}
		parsed, err := time.Parse(time.RFC3339Nano, stamp)
		if err != nil {
			return stamp
		}
		if parsed.Before(now.Add(-24*time.Hour)) || parsed.After(now.Add(24*time.Hour)) {
			return "<instant " + parsed.UTC().Format(time.RFC3339Nano) + ">"
		}
		if strings.Contains(stamp, ".") {
			return "<ts:frac>"
		}
		return "<ts:nofrac>"
	})
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(canonical), &top); err != nil {
		return canonical, nil
	}
	if raw, ok := top["errors"]; ok {
		top["errors"] = json.RawMessage(orderErrorMembers(string(raw)))
	}
	var members []string
	for _, key := range []string{"data", "errors", "extensions"} {
		if raw, ok := top[key]; ok {
			members = append(members, jsonString(key)+": "+string(raw))
			delete(top, key)
		}
	}
	for key, raw := range top {
		members = append(members, "?"+jsonString(key)+": "+string(raw))
	}
	return "{" + strings.Join(members, ", ") + "}", nil
}

// orderErrorMembers writes each error object with its members in one fixed
// order (message, locations, path, extensions, then any other): the order of
// the members of an error is a named divergence (Python writes locations
// before path, Go the reverse), the members themselves are still compared.
func orderErrorMembers(errorsJSON string) string {
	var list []json.RawMessage
	if err := json.Unmarshal([]byte(errorsJSON), &list); err != nil {
		return errorsJSON
	}
	out := make([]string, 0, len(list))
	for _, item := range list {
		var member map[string]json.RawMessage
		if err := json.Unmarshal(item, &member); err != nil {
			out = append(out, string(item))
			continue
		}
		var parts []string
		for _, key := range []string{"message", "locations", "path", "extensions"} {
			if raw, ok := member[key]; ok {
				parts = append(parts, jsonString(key)+": "+string(raw))
				delete(member, key)
			}
		}
		rest := make([]string, 0, len(member))
		for key := range member {
			rest = append(rest, key)
		}
		sort.Strings(rest)
		for _, key := range rest {
			parts = append(parts, "?"+jsonString(key)+": "+string(member[key]))
		}
		out = append(out, "{"+strings.Join(parts, ", ")+"}")
	}
	return "[" + strings.Join(out, ", ") + "]"
}

func compareMutationResponse(c oracleCase, python, goResponse venueoracle.Response) (bool, string) {
	// A request the executor rejects before it runs (a variable that does not
	// coerce) is HTTP 200 with errors from Strawberry and 422 with errors from
	// gqlgen: the GraphQL-over-HTTP status the read operations already carry. A
	// case names it as a divergence (messageDiffers); no other pair is accepted.
	declaredStatus := c.messageDiffers != "" && python.Status == http.StatusOK && goResponse.Status == http.StatusUnprocessableEntity
	if python.Status != goResponse.Status && !declaredStatus {
		return false, fmt.Sprintf("status python=%d go=%d", python.Status, goResponse.Status)
	}
	pyBody, err := normalizeMutationBody(python.Body, "python")
	if err != nil {
		return false, "python body does not decode: " + err.Error()
	}
	goBody, err := normalizeMutationBody(goResponse.Body, "go")
	if err != nil {
		return false, "go body does not decode: " + err.Error()
	}
	if c.messageDiffers != "" {
		return errorsPresent(pyBody) && errorsPresent(goBody) && dataOf(pyBody) == dataOf(goBody), "both planes must answer an error with the same data (" + c.messageDiffers + ")"
	}
	if pyBody != goBody {
		return false, "body differs"
	}
	return true, ""
}

func errorsPresent(body string) bool { return strings.Contains(body, `"errors": [`) }

func dataOf(body string) string {
	var top map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &top); err != nil {
		return body
	}
	return string(top["data"])
}

// compareRows compares every table the mutations touched, ids and clock
// readings masked in SQL so what remains is data.
func compareRows(t *testing.T, ctx context.Context, venue *venueoracle.Venue) {
	t.Helper()
	queries := map[string]string{
		"saved_reports": `SELECT r.org_id, r.name, r.description, r.report_plan::text, r.is_template,
  (SELECT s.name FROM saved_reports s WHERE s.id = r.template_source_id), r.parameters::text,
  (SELECT j.name FROM scheduled_jobs j WHERE j.id = r.schedule_id), r.is_active, r.last_run_at, r.last_run_status,
  r.created_by, r.created_at < now() - interval '1 hour', r.updated_at >= r.created_at
FROM saved_reports r ORDER BY r.org_id, r.name, r.description`,
		"scheduled_jobs": `SELECT j.org_id, j.name, j.job_type, j.provider, j.schedule_cron, j.timezone,
  regexp_replace(j.job_config::text, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<id>'),
  j.status, j.is_running, j.run_count, j.failure_count, j.sync_config_id IS NULL, j.next_run_at
FROM scheduled_jobs j ORDER BY j.org_id, j.name`,
		"report_runs": `SELECT s.name, r.status, r.scheduled_occurrence_id, r.started_at, r.completed_at, r.duration_seconds,
  r.rendered_markdown, r.artifact_url, r.provenance_records::text, r.error, r.attempt_count, r.execution_reclaim_count,
  r.notification_status, r.triggered_by, r.created_at < now() - interval '1 hour'
FROM report_runs r JOIN saved_reports s ON s.id = r.report_id ORDER BY s.name, r.triggered_by`,
		"worker_job_outbox": `SELECT job_kind, contract_version, queue, priority, max_attempts, status, attempt_count,
  regexp_replace(dedupe_key, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<run>'),
  regexp_replace(args::text, '(report-run:|report\.run:|"id": ")[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '\1<run>', 'g'),
  payload_hash ~ '^sha256:[0-9a-f]{64}$', prerequisite_completion_key IS NULL
FROM worker_job_outbox ORDER BY regexp_replace(args::text, '(report-run:|report\.run:|"id": ")[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '\1<run>', 'g')`,
	}
	names := make([]string, 0, len(queries))
	for name := range queries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		query := "SELECT row_to_json(t)::text FROM (" + queries[name] + ") t"
		py := strings.Split(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query), " | ")
		gr := strings.Split(venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query), " | ")
		if len(py) != len(gr) {
			t.Errorf("%s: python has %d rows, go has %d", name, len(py), len(gr))
		}
		for i := 0; i < len(py) && i < len(gr); i++ {
			if py[i] != gr[i] {
				t.Errorf("%s row %d differs\n python: %s\n go:     %s", name, i, py[i], gr[i])
			}
		}
	}
}

// unmetExpectation is why the Python answer is not the one the case was written
// for, or "".
func unmetExpectation(c oracleCase, python venueoracle.Response) string {
	if c.expect == expectAny {
		return ""
	}
	if python.Status != 200 {
		return fmt.Sprintf("status %d", python.Status)
	}
	body, err := normalizeMutationBody(python.Body, "python")
	if err != nil {
		return err.Error()
	}
	hasErrors := errorsPresent(body)
	dataNull := dataOf(body) == "null" || strings.HasPrefix(dataOf(body), `{"`+c.op+`": null`)
	switch c.expect {
	case expectData:
		if hasErrors || dataNull {
			return "want data and no error"
		}
	case expectNull:
		if hasErrors || !dataNull {
			return "want a null answer and no error"
		}
	case expectError:
		if !hasErrors {
			return "want an error"
		}
	}
	return ""
}

// compareRowCounts is the comparison of a run with declared divergences: Go wrote
// exactly `declared` fewer saved reports than Python (the ones it refused).
func compareRowCounts(t *testing.T, ctx context.Context, venue *venueoracle.Venue, declared int) {
	t.Helper()
	count := func(uri string) string {
		return venueoracle.TableRows(t, ctx, uri, "SELECT count(*)::text FROM saved_reports")
	}
	py, gr := count(venue.AdminURI(t, venue.SourceDB)), count(venue.AdminURI(t, venue.GoDB))
	var pn, gn int
	fmt.Sscan(py, &pn)
	fmt.Sscan(gr, &gn)
	if pn-gn != declared {
		t.Errorf("python wrote %d saved reports, go %d: want exactly %d fewer from Go (the declared refusals)", pn, gn, declared)
	}
}

// cronDialectGoRefuses names the five-field expressions croniter accepts and the
// scheduler's cron subset refuses (internal/scheduler/sync.NextOccurrence: random
// R fields, mixed day-of-month/weekday auxiliaries, and the like). On these Go
// answers an error where Python creates the schedule.
var cronDialectGoRefuses = map[string]string{
	"\u0660 \u0660 \u0661 \u0661 *": "croniter reads Unicode decimal digits as numbers, the scheduler's cron parser reads ASCII digits only",
	"R R * * *":                     "a random (R) field: croniter picks a value, the scheduler's cron subset is deterministic and refuses it",
}

// TestSavedReportMutationsCronDialectVenueOracle measures where the two cron
// dialects part on the create path. It is the whole list, not a sample: every
// expression either answers alike on both planes or is named in
// cronDialectGoRefuses with the reason.
func TestSavedReportMutationsCronDialectVenueOracle(t *testing.T) {
	runSavedReportVenue(t, cronDialectCases(), cronDialectGoRefusesByCase())
}

func cronDialectGoRefusesByCase() map[string]string {
	out := map[string]string{}
	for _, c := range cronDialectCases() {
		for expression, reason := range cronDialectGoRefuses {
			if strings.HasSuffix(c.name, fmt.Sprintf("%q", expression)) {
				out[c.name] = reason
			}
		}
	}
	return out
}
