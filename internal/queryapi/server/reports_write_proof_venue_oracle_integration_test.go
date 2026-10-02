//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof/writeproof"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports/writeproofcases"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// The WRITE-PROOF baselines (CHAOS-6098, on CHAOS-6810's single-plane form).
//
// `goapi prove-write` runs one registered case ONCE, on the deployed Go build,
// and compares the digest of what the mutation persisted with a committed
// baseline; it never runs the Python resolver. This oracle is where that
// baseline comes from and stays true: each of the five cases runs through the
// SAME writeproof.Execute the live verb uses, once against the real Python
// resolver (real /graphql app) and once against the real Go dispatch pipeline,
// each on its own copy of one real-Alembic database. Both digests must be equal
// to each other and to the committed digest, so a case cannot drift from either
// plane, and the committed digest is what a real run produces, not what was
// written down.
//
// The golden holds, per case, the digest the Python plane's write persisted ("write proof <case>"), and one
// warm-up request. The BODY of the recorded Python answer is not in the golden: a red frozen run names the
// case whose digest differs, and seeing the difference needs a re-record at the pinned build.
//
// The two planes use different run tags on purpose: the digest must not depend
// on the tag (the Normalizer masks it), and equal digests under different tags
// prove that it does not.
func TestSavedReportWriteProofBaselinesVenueOracle(t *testing.T) {
	ctx := context.Background()
	root := repoRootFromHere(t)
	golden := venueoracle.OpenGolden(t, venueGolden("saved-report-write-proof", t.Name(), "527a21c98dc994c6d1821cc0b50f8bbc358908918651dbe5c725535974e9306d"))
	user := stableVenueID("saved-report-write-proof/user")
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Golden: golden, Root: golden.PythonRoot(t, root), JWTKey: oracleJWTKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			for _, statement := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, 'venue-a', 'Venue A', 'community', 'stripe', true, now(), now())`, []any{oracleOrgA}},
				{`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-0@example.com', true, true, false, 0, now(), now())`, []any{user}},
				{`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'admin', now(), now(), now())`, []any{uuid.New(), oracleOrgA, user}},
			} {
				if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			return map[string]map[string]any{
				"a": {"user_id": user.String(), "email": "venue-0@example.com", "org_id": oracleOrgA, "role": "admin"},
			}
		},
	})

	goDB, err := pgxpool.New(ctx, venue.AdminURI(t, venue.GoDB))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(goDB.Close)
	// The Python plane exists only while recording: its database, its answers and the effects of
	// each case are produced then and frozen (the golden's rows hold them per case).
	var pythonDB *pgxpool.Pool
	if golden.Recording() {
		var err error
		pythonDB, err = pgxpool.New(ctx, venue.AdminURI(t, venue.SourceDB))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(pythonDB.Close)
	}
	goHandler, closeGo := startGoMutationServer(t, ctx, venue)
	defer closeGo()

	// One real request puts the golden in its Python state: the per-case rows below are compared
	// after it, and the file's order is OpenGolden, Python, comparisons, Finish.
	warmUp := golden.Python(t, venue, []venueoracle.Request{{
		Name: "write proof warm-up", Method: "POST", Path: "/graphql",
		Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["a"], "Content-Type": "application/json"},
		Body:    venueoracle.B64(`{"query": "{ __typename }"}`),
	}})
	golden.Consumed(t, warmUp...)

	docs := oracleDocuments()
	pythonPoster := func(name string) writeproof.Poster {
		return func(_ context.Context, document, variables string) (writeproof.Response, error) {
			body := `{"query": ` + jsonString(document) + `, "variables": ` + variables + `}`
			// While recording only: the case's digest is computed on the Python plane's own answer, as the
			// plane gave it; the golden freezes the digest (CompareRows below), not the HTTP answer.
			answer := venue.ServePython(t, []venueoracle.Request{{
				Name: name, Method: "POST", Path: "/graphql",
				Headers: map[string]string{"Authorization": "Bearer " + venue.Tokens["a"], "Content-Type": "application/json"},
				Body:    venueoracle.B64(body),
			}})[0]
			return writeproof.Response{Status: answer.Status, Body: []byte(answer.Body), Build: "python", WireAttempts: 1}, nil
		}
	}
	goPoster := func(_ context.Context, document, variables string) (writeproof.Response, error) {
		body := `{"query": ` + jsonString(document) + `, "variables": ` + variables + `}`
		request := httptest.NewRequest(http.MethodPost, "/query", bytes.NewReader([]byte(body)))
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Authorization", "Bearer "+envelopes(oracleOrgA))
		recorder := httptest.NewRecorder()
		goHandler(recorder, request)
		answer, _ := io.ReadAll(recorder.Result().Body)
		return writeproof.Response{Status: recorder.Code, Body: answer, Build: "go", WireAttempts: 1}, nil
	}

	committed := writeproofcases.Baselines()
	update := os.Getenv(writeproofcases.UpdateEnv) == "1"
	// A case is built with the committed digest, or a placeholder that names
	// something so Validate accepts it when one is being produced.
	placeholders := map[string]string{}
	for _, name := range writeproofcases.Names() {
		placeholders[name] = "sha256:not-yet-produced"
		if digest := committed[name]; digest != "" && !update {
			placeholders[name] = digest
		}
	}
	produced := map[string]string{}
	var receipt strings.Builder
	for _, c := range writeproofcases.Build(placeholders) {
		goRun := writeproof.RunTag("wporaclego" + strings.ReplaceAll(c.Name, "saved-report-", ""))
		goResult, goErr := writeproof.Execute(ctx, goDB, oracleOrgA, c, goRun, docs[c.Operation], goPoster)
		if goErr != nil {
			t.Errorf("%s: execute failed on the Go plane: %v", c.Name, goErr)
			continue
		}
		if goResult.TerminalState == writeproof.StateProofFailed {
			t.Errorf("%s: the go plane did not produce a usable observation: %s", c.Name, goResult.Detail)
		}
		if problem := answerProblem(c, goResult); problem != "" {
			t.Errorf("%s: the go plane's answer is not the one the case was written for: %s", c.Name, problem)
		}
		goEffects, _ := goResult.Effects.Canonical()
		// The Python plane's digest and effects of this case: executed while recording (the same
		// writeproof.Execute on its own database, under its own run tag, with its checks), frozen after.
		// The digest is what the case's baseline holds, and what the planes are required to agree on.
		pythonSummary := golden.CompareRows(t, "write proof "+c.Name, func() string {
			pythonRun := writeproof.RunTag("wporaclepy" + strings.ReplaceAll(c.Name, "saved-report-", ""))
			pythonResult, pythonErr := writeproof.Execute(ctx, pythonDB, oracleOrgA, c, pythonRun, docs[c.Operation], pythonPoster(c.Name))
			if pythonErr != nil {
				t.Fatalf("%s: execute failed on the Python plane: %v", c.Name, pythonErr)
			}
			if pythonResult.TerminalState == writeproof.StateProofFailed {
				t.Errorf("%s: the python plane did not produce a usable observation: %s", c.Name, pythonResult.Detail)
			}
			if problem := answerProblem(c, pythonResult); problem != "" {
				t.Errorf("%s: the python plane's answer is not the one the case was written for: %s", c.Name, problem)
			}
			return pythonResult.Digest
		}, goResult.Digest)
		if pythonSummary != goResult.Digest {
			// The planes persisted different effects; CompareRows has failed the test with both digests.
			pythonEffects := "(the Python plane's effects are not frozen: only its digest is)"
			t.Logf("%s: python digest %s, go digest %s, go effects: %s %s", c.Name, pythonSummary, goResult.Digest, goEffects, pythonEffects)
			continue
		}
		produced[c.Name] = goResult.Digest
		effects, _ := goResult.Effects.Canonical()
		t.Logf("%s effects: %s", c.Name, effects)
		state := "MATCH"
		if update {
			state = "PRODUCED"
		} else if goResult.TerminalState != writeproof.StateMatch {
			state = "DIFFERS FROM THE COMMITTED BASELINE"
			goEffects, _ := goResult.Effects.Canonical()
			t.Errorf("%s: both planes agree (%s) but not with the committed baseline %s\n effects: %s", c.Name, goResult.Digest, c.BaselineDigest, goEffects)
		}
		fmt.Fprintf(&receipt, "%-24s python=%s go=%s %s rows=%d\n", c.Name, pythonSummary[:19], goResult.Digest[:19], state, goResult.Effects.Rows())
		if goResult.Kept != nil && goResult.TeardownErr != nil {
			t.Errorf("%s: teardown failed on the Go plane: %v", c.Name, goResult.TeardownErr)
		}
	}
	t.Log("\n" + receipt.String())

	if update && !t.Failed() {
		path := filepath.Join(root, "internal", "queryapi", "reports", "writeproofcases", "baselines.json")
		if err := writeproofcases.WriteBaselines(path, produced); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", path)
	}
	if !update && len(produced) != len(committed) {
		names := make([]string, 0, len(committed))
		for name := range committed {
			names = append(names, name)
		}
		sort.Strings(names)
		t.Errorf("the committed baselines name %v but %d cases were measured", names, len(produced))
	}
	golden.SkipDiff(t)
	golden.Finish(t)
	venueoracle.WriteGoOnlyProof(t, "Go's saved-report write effects against the frozen digests of the Python resolvers' writes, and the committed baselines")
}

// answerProblem is why a plane's response is not the mutation succeeding, or
// "": a case whose mutation answered null (nothing found) would still leave
// seeded rows to digest, and the digest would prove nothing about the write.
func answerProblem(c writeproof.Case, result writeproof.Result) string {
	response, ok := result.Effects.Response.(map[string]any)
	if !ok {
		return fmt.Sprintf("the answer is not an object: %v", result.Effects.Response)
	}
	data, ok := response["data"].(map[string]any)
	if !ok {
		return fmt.Sprintf("the answer has no data object: %v", response)
	}
	value, present := data[c.Operation]
	switch {
	case !present || value == nil:
		return fmt.Sprintf("data.%s is null or absent", c.Operation)
	case c.Operation == "deleteSavedReport" && value != true:
		return fmt.Sprintf("data.deleteSavedReport is %v, want true", value)
	}
	return ""
}

// TestWriteProofGoldenHoldsOneRequestAndOneDigestPerCase pins the shape of the frozen file: one Python
// request (the warm-up) and one row comparison per registered case. A frozen run that asked the golden for
// more or fewer answers than it holds is refused by the harness; this names what the file must hold.
func TestWriteProofGoldenHoldsOneRequestAndOneDigestPerCase(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "venue", "saved-report-write-proof.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Requests []json.RawMessage          `json:"requests"`
		Rows     map[string]json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Requests) != 1 {
		t.Errorf("the golden holds %d Python requests, want exactly the warm-up", len(file.Requests))
	}
	for _, name := range writeproofcases.Names() {
		if _, ok := file.Rows["write proof "+name]; !ok {
			t.Errorf("the golden has no recorded digest for case %s", name)
		}
	}
	if len(file.Rows) != len(writeproofcases.Names()) {
		t.Errorf("the golden holds %d row comparisons for %d cases", len(file.Rows), len(writeproofcases.Names()))
	}
}
