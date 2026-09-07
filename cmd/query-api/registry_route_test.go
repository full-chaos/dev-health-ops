package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// registryPayload mirrors registryResponse, declared separately on purpose:
// unmarshalling into the production struct would let a field RENAME pass
// (both sides move together). Parsing into an independent shape means these
// tests assert the wire contract `dev-hops go-api routing` actually reads.
type registryPayload struct {
	SchemaDigest string `json:"schema_digest"`
	Operations   []struct {
		Operation      string `json:"operation"`
		DocumentDigest string `json:"document_digest"`
	} `json:"operations"`
}

func fetchRegistryPayload(t *testing.T, handler http.HandlerFunc) registryPayload {
	t.Helper()
	rec := httptest.NewRecorder()
	handler(rec, httptest.NewRequest(http.MethodGet, "/registry", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /registry: status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("GET /registry: Content-Type = %q, want application/json", got)
	}
	var payload registryPayload
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("GET /registry: body is not JSON: %v (body=%q)", err, rec.Body.String())
	}
	return payload
}

// The contract `dev-hops go-api routing enable` preflight 3 depends on: the
// response describes EXACTLY what was registered, no more and no less. An
// operation missing here makes the CLI refuse to enable something that
// actually works; an extra one lets it enable something unreachable.
// Asserted by set-equality against the map that was passed in, never
// against a hand-typed list (the same discipline
// TestMountedRouteLogMessage_ListsExactlyRegisteredOperations uses -- a
// hand-typed expectation is what went stale for three waves).
func TestNewRegistryHandler_ReportsExactlyTheRegisteredMap(t *testing.T) {
	registered := map[string]string{
		"featureFlags":  "digest-feature-flags",
		"reviewEdges":   "digest-review-edges",
		"cognitiveLoad": "digest-cognitive-load",
	}
	payload := fetchRegistryPayload(t, newRegistryHandler("sha256:test-digest", registered))

	if payload.SchemaDigest != "sha256:test-digest" {
		t.Fatalf("schema_digest = %q, want %q", payload.SchemaDigest, "sha256:test-digest")
	}
	got := map[string]string{}
	for _, entry := range payload.Operations {
		if _, dup := got[entry.Operation]; dup {
			t.Fatalf("operation %q appears twice in the response", entry.Operation)
		}
		got[entry.Operation] = entry.DocumentDigest
	}
	if len(got) != len(registered) {
		t.Fatalf("response lists %d operations, registered map has %d: got=%v want=%v", len(got), len(registered), got, registered)
	}
	for operation, digest := range registered {
		if got[operation] != digest {
			t.Fatalf("operation %q: document_digest = %q, want %q", operation, got[operation], digest)
		}
	}
}

// Go randomises map iteration; an unsorted body would differ between two
// requests to the same process, which makes diffing two /registry responses
// (the obvious way to compare two deployments) useless.
func TestNewRegistryHandler_OperationsAreSortedAndStable(t *testing.T) {
	handler := newRegistryHandler("sha256:test-digest", map[string]string{
		"zebra": "z", "alpha": "a", "mike": "m", "bravo": "b",
	})
	first := fetchRegistryPayload(t, handler)

	names := make([]string, 0, len(first.Operations))
	for _, entry := range first.Operations {
		names = append(names, entry.Operation)
	}
	want := []string{"alpha", "bravo", "mike", "zebra"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("operations = %v, want sorted %v", names, want)
	}

	for i := 0; i < 5; i++ {
		next := fetchRegistryPayload(t, handler)
		nextNames := make([]string, 0, len(next.Operations))
		for _, entry := range next.Operations {
			nextNames = append(nextNames, entry.Operation)
		}
		if strings.Join(nextNames, ",") != strings.Join(names, ",") {
			t.Fatalf("repeat request %d returned a different order: %v vs %v", i, nextNames, names)
		}
	}
}

// /registry is a read surface. A POST must not be treated as a GET.
func TestNewRegistryHandler_RejectsNonGET(t *testing.T) {
	handler := newRegistryHandler("sha256:test-digest", map[string]string{"featureFlags": "d"})
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		rec := httptest.NewRecorder()
		handler(rec, httptest.NewRequest(method, "/registry", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s /registry: status = %d, want 405", method, rec.Code)
		}
	}
}

func TestNewRegistryHandler_EmptyRegistrationSetIsAnEmptyList(t *testing.T) {
	// Not an error and not a null: the CLI distinguishes "reachable, serves
	// nothing" (refuse preflight 3, naming the operations) from "unreachable"
	// (refuse preflight 1). A null here would decode to a nil slice and blur
	// the two.
	payload := fetchRegistryPayload(t, newRegistryHandler("sha256:d", map[string]string{}))
	if payload.Operations == nil {
		t.Fatalf("operations decoded as nil; want an empty list so the CLI can tell 'serves nothing' from 'no answer'")
	}
	if len(payload.Operations) != 0 {
		t.Fatalf("operations = %v, want empty", payload.Operations)
	}
}

// captureLog runs fn with the standard logger redirected, returning what it
// wrote.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf strings.Builder
	prevOutput := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevOutput)
		log.SetFlags(prevFlags)
	}()
	fn()
	return buf.String()
}

// A nil pool must not panic: newQueryHandler is called in tests and in
// configurations where the pool may not have been built.
func TestLogRoutingStateDrift_NilPoolIsSilentAndSafe(t *testing.T) {
	logged := captureLog(t, func() { logRoutingStateDrift(nil, "sha256:d") })
	if logged != "" {
		t.Fatalf("nil pool logged %q, want nothing", logged)
	}
}

// An unreachable registry must be reported as a FAILED CHECK, never as
// "nothing enabled" -- conflating the two is the same class of defect this
// whole file exists to prevent. pgxpool.New does not dial eagerly, so a
// pool pointed at a closed port fails on first query, which is exactly the
// path under test.
func TestLogRoutingStateDrift_UnreachableRegistryReportsFailureNotEmptiness(t *testing.T) {
	pool, err := pgxpool.New(context.Background(), "postgres://nobody:nobody@127.0.0.1:1/nonexistent")
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	logged := captureLog(t, func() { logRoutingStateDrift(pool, "sha256:live") })

	if !strings.Contains(logged, "drift check failed") {
		t.Fatalf("unreachable registry did not log a check failure; got %q", logged)
	}
	if strings.Contains(logged, "table is empty") {
		t.Fatalf("unreachable registry was reported as an empty table -- these must never be conflated; got %q", logged)
	}
	if strings.Contains(logged, "ROUTING ROWS STALE") {
		t.Fatalf("unreachable registry was reported as stale rows; got %q", logged)
	}
}

// classifyRoutingDrift's three outcomes, at the UNIT tier.
//
// codex r1 (P3) mutated `live > 0` to `live == 0` and the unit tests stayed
// green -- only the Postgres-testcontainer tests under the integration tag
// caught it, so a plain `go test ./...` could not. The decision is the whole
// point of this file; these drive it directly so the cheap tier catches an
// inversion.
func TestClassifyRoutingDrift_EmptyTableIsNotAnIncident(t *testing.T) {
	lines := classifyRoutingDrift(map[string]int64{}, "sha256:live")
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d: %v", len(lines), lines)
	}
	if !strings.Contains(lines[0], "table is empty") {
		t.Fatalf("empty table not reported as empty: %q", lines[0])
	}
	if strings.Contains(lines[0], "STALE") {
		t.Fatalf("empty table reported as stale: %q", lines[0])
	}
}

func TestClassifyRoutingDrift_LiveRowsAreNotStale(t *testing.T) {
	lines := classifyRoutingDrift(
		map[string]int64{"sha256:live": 15, "sha256:old": 12}, "sha256:live")
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d: %v", len(lines), lines)
	}
	if strings.Contains(lines[0], "STALE") {
		t.Fatalf("rows exist at the live digest but were reported STALE: %q", lines[0])
	}
	if !strings.Contains(lines[0], "15 at live schema digest sha256:live") {
		t.Fatalf("live count not reported: %q", lines[0])
	}
	if !strings.Contains(lines[0], "12 at other digests") {
		t.Fatalf("superseded rows not counted separately: %q", lines[0])
	}
}

func TestClassifyRoutingDrift_RowsOnlyAtASupersededDigestAreStale(t *testing.T) {
	// The exact 2026-09-01 census.
	lines := classifyRoutingDrift(map[string]int64{"sha256:67b87d38": 12}, "sha256:29d509cd")
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d: %v", len(lines), lines)
	}
	for _, want := range []string{
		"ROUTING ROWS STALE", "12 rows at sha256:67b87d38",
		"0 at sha256:29d509cd", "dev-hops go-api routing enable",
	} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("stale line missing %q: %q", want, lines[0])
		}
	}
}

// Multiple superseded digests each get their own line, in sorted order --
// Go randomises map iteration, and a log an operator cannot diff between
// restarts is not a log they will trust.
func TestClassifyRoutingDrift_StaleDigestsAreSortedAndComplete(t *testing.T) {
	lines := classifyRoutingDrift(
		map[string]int64{"sha256:ccc": 1, "sha256:aaa": 2, "sha256:bbb": 3},
		"sha256:live")
	if len(lines) != 3 {
		t.Fatalf("want one line per stale digest, got %d: %v", len(lines), lines)
	}
	for i, want := range []string{"sha256:aaa", "sha256:bbb", "sha256:ccc"} {
		if !strings.Contains(lines[i], want) {
			t.Fatalf("line %d = %q, want it to name %s (sorted order)", i, lines[i], want)
		}
	}
}
