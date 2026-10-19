//go:build integration

package admin_test

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-10082, a named divergence from Python (ruling 2026-10-18, record
// integrity over parity): created_by_id comes from the verified principal and
// the X-User-Id request header is ignored. Python trusted the header, so
// these requests, which Python rejected (422/400/500) and created no row for,
// are answered differently by Go. Their Python answers stay in the frozen
// golden and are consumed unread; the Go side is checked by hand after the
// parity run and its row comparisons, so the two planes' tables were still
// equal when compared.
//
// The exemption is keyed to the exact request (name, method, path) and is
// closed both ways: splitXUserIDRequests fails when a listed request is no
// longer in the list, and when a request that carries an X-User-Id header is in
// neither list (a new header-decided request must be decided, not slip into the
// parity run or past it).
type requestID struct{ name, method, path string }

const (
	ipPath        = "/api/v1/admin/ip-allowlist"
	retentionPath = "/api/v1/admin/retention-policies"
)

// xUserIDDivergent: Python rejected the header (no row); Go ignores it.
var xUserIDDivergent = map[requestID]bool{
	{"W ip create with malformed X-User-Id", "POST", ipPath}:           true,
	{"W ip create with 31-char X-User-Id", "POST", ipPath}:             true,
	{"W ip create with non-hex 32-char X-User-Id", "POST", ipPath}:     true,
	{"W ip create with unknown X-User-Id", "POST", ipPath}:             true,
	{"W create with malformed X-User-Id", "POST", retentionPath}:       true,
	{"W create with non-hex 32-char X-User-Id", "POST", retentionPath}: true,
	{"W create with unknown X-User-Id", "POST", retentionPath}:         true,
}

// xUserIDParityEqual: the header names the token's own user, so the principal
// and Python's header give the same created_by_id and the parity run compares
// them.
var xUserIDParityEqual = map[requestID]bool{
	{"W ip create with existing X-User-Id", "POST", ipPath}:     true,
	{"W ip create with braced X-User-Id", "POST", ipPath}:       true,
	{"W create with existing X-User-Id", "POST", retentionPath}: true,
}

func idOf(r venueoracle.Request) requestID { return requestID{r.Name, r.Method, r.Path} }

// splitXUserIDRequests returns the requests and answers Diff compares and the
// answers of the divergent requests, which the caller consumes. matches picks
// the listed requests that belong to this test's route.
func splitXUserIDRequests(t *testing.T, path string, requests []venueoracle.Request, answers []venueoracle.Response) (compared []venueoracle.Request, comparedAnswers, divergentAnswers []venueoracle.Response) {
	t.Helper()
	seen := map[requestID]bool{}
	for i, request := range requests {
		id := idOf(request)
		seen[id] = true
		_, carriesHeader := request.Headers["X-User-Id"]
		if (xUserIDDivergent[id] || xUserIDParityEqual[id]) && !carriesHeader {
			t.Errorf("request %q is listed in the X-User-Id exemption but no longer sends X-User-Id (CHAOS-10151)", request.Name)
		}
		switch {
		case xUserIDDivergent[id]:
			divergentAnswers = append(divergentAnswers, answers[i])
			continue
		case carriesHeader && !xUserIDParityEqual[id]:
			t.Errorf("request %q sends X-User-Id and is in neither the divergent nor the parity-equal list (CHAOS-10082)", request.Name)
		}
		compared = append(compared, request)
		comparedAnswers = append(comparedAnswers, answers[i])
	}
	var stale []string
	for _, set := range []map[requestID]bool{xUserIDDivergent, xUserIDParityEqual} {
		for id := range set {
			if len(id.path) >= len(path) && id.path[:len(path)] == path && !seen[id] {
				stale = append(stale, id.name)
			}
		}
	}
	sort.Strings(stale)
	for _, name := range stale {
		t.Errorf("the X-User-Id exemption lists %q, which is no longer among the requests (CHAOS-10082)", name)
	}
	return compared, comparedAnswers, divergentAnswers
}

// The same ruling changes a second thing the frozen Python answers record:
// Python stored NULL in created_by_id for every create that carried no
// X-User-Id (the web client never sends one) and Go now stores the creating
// admin. Response bodies are compared with created_by_id blanked on both
// planes; each Go create is checked to carry its own caller (checkCreatedBy);
// and before the row comparison the Go rows created by the run are set back to
// NULL (resetCreatedBy) so every OTHER column of those rows is still compared
// against the frozen Python rows. A create whose header named the token's own
// user (xUserIDParityEqual) stored that id on both planes and is left alone.
var createdByJSON = regexp.MustCompile(`"created_by_id":"[0-9a-fA-F-]{36}"`)

func blankCreatedBy(body string) string {
	return createdByJSON.ReplaceAllString(body, `"created_by_id":null`)
}

// createdByLedger records, per Go create, the caller it must carry. Every
// create is also checked in the database (persisted), not only in the response.
type createdByLedger struct {
	callers map[uuid.UUID]uuid.UUID // created row id -> verified caller
	reset   []uuid.UUID             // rows set back to NULL before the row comparison
}

// principalOf names the user id behind a request's bearer token.
func principalOf(request venueoracle.Request, tokens map[string]string, users map[string]uuid.UUID) (uuid.UUID, bool) {
	bearer := strings.TrimPrefix(request.Headers["Authorization"], "Bearer ")
	for name, token := range tokens {
		if token == bearer {
			id, ok := users[name]
			return id, ok
		}
	}
	return uuid.Nil, false
}

// note records a created row and checks the response names its caller.
func (l *createdByLedger) note(t *testing.T, request venueoracle.Request, createdID uuid.UUID, gotCreatedBy any, want uuid.UUID, known bool) {
	t.Helper()
	if !known {
		t.Errorf("%s: cannot tell which user the bearer token belongs to", request.Name)
		return
	}
	if got, _ := gotCreatedBy.(string); got != want.String() {
		t.Errorf("%s: created_by_id = %v, want the verified caller %s", request.Name, gotCreatedBy, want)
	}
	if l.callers == nil {
		l.callers = map[uuid.UUID]uuid.UUID{}
	}
	l.callers[createdID] = want
	if !xUserIDParityEqual[idOf(request)] {
		l.reset = append(l.reset, createdID)
	}
}

// verifyThenReset reads the PERSISTED created_by_id of every row the run
// created and requires the caller (the response alone can show the actor while
// the row stores NULL), then sets the rows to NULL, the value the frozen Python
// rows hold for them, except the creates whose header named the token's own
// user (both planes stored that id).
func (l *createdByLedger) verifyThenReset(t *testing.T, ctx context.Context, adminURI, table string) {
	t.Helper()
	pool, err := pgxpool.New(ctx, adminURI)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if len(l.callers) == 0 || len(l.reset) == 0 {
		t.Fatalf("%s: the run recorded no created rows; the created_by_id check proves nothing", table)
	}
	ids := make([]uuid.UUID, 0, len(l.callers))
	for id := range l.callers {
		ids = append(ids, id)
	}
	rows, err := pool.Query(ctx, "SELECT id, created_by_id FROM "+table+" WHERE id = ANY($1)", ids)
	if err != nil {
		t.Fatalf("%s: reading created_by_id: %v", table, err)
	}
	found := 0
	for rows.Next() {
		var id uuid.UUID
		var createdBy *uuid.UUID
		if err := rows.Scan(&id, &createdBy); err != nil {
			t.Fatal(err)
		}
		found++
		if createdBy == nil || *createdBy != l.callers[id] {
			t.Errorf("%s row %s: persisted created_by_id = %v, want the verified caller %s", table, id, createdBy, l.callers[id])
		}
	}
	rows.Close()
	if found != len(l.callers) {
		t.Errorf("%s: %d of %d created rows were found in the database", table, found, len(l.callers))
	}
	if _, err := pool.Exec(ctx, "UPDATE "+table+" SET created_by_id = NULL WHERE id = ANY($1)", l.reset); err != nil {
		t.Fatalf("%s: resetting created_by_id: %v", table, err)
	}
}
