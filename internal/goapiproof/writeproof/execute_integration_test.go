//go:build integration

package writeproof

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
)

// The synthetic case: one mutation that "creates a report" (a row in a scratch
// table, with a generated id, a timestamp it stamps itself and a jsonb payload).
// Its baseline digest is committed in testdata/synthetic_baseline.json and is
// checked against a real run every time (freshness by EXECUTION, not by
// digesting source).

const syntheticDDL = `
CREATE TABLE IF NOT EXISTS wp_synthetic (
	id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
	org TEXT NOT NULL,
	run TEXT NOT NULL,
	name TEXT NOT NULL,
	made_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	payload JSONB NOT NULL DEFAULT '{}'::jsonb
)`

type syntheticSeeder struct{ failSeed bool }

func (s syntheticSeeder) Seed(ctx context.Context, db goapiproof.Querier, org string, run RunTag) error {
	if s.failSeed {
		return errors.New("seed refused")
	}
	// A fixed-id seeded row that stays visible, dated far from now.
	_, err := db.Exec(ctx, `INSERT INTO wp_synthetic (id, org, run, name, made_at, payload)
		VALUES ('0000feed-0000-4000-8000-000000000001', $1, $2, 'seed', '2026-01-02T03:04:05Z', '{"seeded": true}')`, org, string(run))
	return err
}

func (syntheticSeeder) Teardown(ctx context.Context, db goapiproof.Querier, org string, run RunTag) error {
	_, err := db.Exec(ctx, `DELETE FROM wp_synthetic WHERE org = $1 AND run = $2`, org, string(run))
	return err
}

func syntheticCase(baseline string) Case {
	return Case{
		Name:          "synthetic-create",
		Operation:     "syntheticCreate",
		VariablesJSON: `{"name":"made by the mutation","n":9007199254740993}`,
		Seeder:        syntheticSeeder{},
		Tables: []Table{{
			Label: "wp_synthetic",
			SQL:   `SELECT id, name, made_at, payload FROM wp_synthetic WHERE org = $1 AND run = $2 ORDER BY name`,
		}},
		KeepIDs:        []string{"0000feed-0000-4000-8000-000000000001"},
		BaselineDigest: baseline,
	}
}

// posterInserting is the "deployed build": it performs the mutation's effect for
// the case's variables, returns a GraphQL data answer, and counts its calls.
func posterInserting(t *testing.T, pool *pgxpool.Pool, org string, run RunTag, name string, calls *int) Poster {
	return func(ctx context.Context, _ string, variables string) (Response, error) {
		*calls++
		var v struct {
			Name string          `json:"name"`
			N    json.RawMessage `json:"n"`
		}
		if err := json.Unmarshal([]byte(variables), &v); err != nil {
			t.Errorf("variables were not sent verbatim JSON: %v", err)
		}
		if name == "" {
			name = v.Name
		}
		var id string
		if err := pool.QueryRow(ctx, `INSERT INTO wp_synthetic (org, run, name, payload) VALUES ($1,$2,$3,$4::jsonb) RETURNING id::text`,
			org, string(run), name, fmt.Sprintf(`{"n": %s, "run": %q}`, v.N, run)).Scan(&id); err != nil {
			t.Fatalf("mutation effect: %v", err)
		}
		return Response{Status: 200, Body: []byte(fmt.Sprintf(`{"data":{"syntheticCreate":{"id":%q,"name":%q}}}`, id, name))}, nil
	}
}

func startPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate Postgres: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, syntheticDDL); err != nil {
		t.Fatalf("create synthetic table: %v", err)
	}
	if _, err := pool.Exec(ctx, registryschema.DDL); err != nil {
		t.Fatalf("create registry schema: %v", err)
	}
	return pool
}

func rowsFor(t *testing.T, pool *pgxpool.Pool, org string, run RunTag) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM wp_synthetic WHERE org=$1 AND run=$2`, org, string(run)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// The committed baseline must equal what a REAL run produces. When this fails
// after an intended change, the message carries the new digest to commit.
func TestTheCommittedBaselineIsWhatARealRunProduces(t *testing.T) {
	pool := startPool(t)
	raw, err := os.ReadFile("testdata/synthetic_baseline.json")
	if err != nil {
		t.Fatal(err)
	}
	var committed struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(raw, &committed); err != nil {
		t.Fatal(err)
	}
	calls := 0
	run := RunTag("gwc-wp-baseline")
	result, err := Execute(context.Background(), pool, "org-fixture", syntheticCase("sha256:placeholder-not-the-baseline"), run, "mutation M { x }", posterInserting(t, pool, "org-fixture", run, "", &calls))
	if err != nil {
		t.Fatal(err)
	}
	if result.Digest != committed.Digest {
		t.Fatalf("the committed baseline is stale: a real run produced %s, testdata/synthetic_baseline.json says %s", result.Digest, committed.Digest)
	}
}

func TestAMatchProvesAndCleansUpAndAdmitsTheMutation(t *testing.T) {
	ctx := context.Background()
	pool := startPool(t)
	raw, _ := os.ReadFile("testdata/synthetic_baseline.json")
	var committed struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(raw, &committed); err != nil {
		t.Fatal(err)
	}
	run := RunTag("gwc-wp-match")
	calls := 0
	result, err := Execute(ctx, pool, "org-fixture", syntheticCase(committed.Digest), run, "mutation M { x }", posterInserting(t, pool, "org-fixture", run, "", &calls))
	if err != nil {
		t.Fatal(err)
	}
	if result.TerminalState != StateMatch || result.Kept != nil || result.TeardownErr != nil {
		t.Fatalf("want a clean match, got %+v", result)
	}
	if calls != 1 || result.Posts != 1 {
		t.Fatalf("the mutation must be posted exactly once, got calls=%d posts=%d", calls, result.Posts)
	}
	if n := rowsFor(t, pool, "org-fixture", run); n != 0 {
		t.Fatalf("a match must tear the dataset down, %d rows remain", n)
	}

	// The state the system exists to reach: the receipt this run yields is
	// ADMITTED by the enablement predicate for the mutation, and only for it.
	key := goapiproof.Receipt{SchemaDigest: "sha256:s", DocumentDigest: "sha256:d", CandidateBuild: "b1"}
	receipt, err := result.Receipt(ReceiptInput{
		SchemaDigest: key.SchemaDigest, DocumentDigest: key.DocumentDigest, CandidateBuild: key.CandidateBuild,
		RequestIdentity: "sha256:req", Org: "org-fixture", RecordedBy: "test", ReviewEvidence: "synthetic",
		Route: goapiproof.RouteProof, BuildBinding: goapiproof.EdgeBuildPresent, ObservedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := goapiproof.WriteAtomic(ctx, pool, receipt); err != nil {
		t.Fatalf("the database refused the write receipt: %v", err)
	}
	docs := map[string]string{"syntheticCreate": "sha256:d"}
	asMutation, err := goapiproof.OperationsWithEnablementProofByKind(ctx, pool, "sha256:s", "b1", goapiproof.TargetModeCanary, docs, map[string]string{"syntheticCreate": goapiproof.OperationKindMutation})
	if err != nil || !asMutation["syntheticCreate"] {
		t.Fatalf("canary must admit the mutation on a proof-route write receipt: %v %v", asMutation, err)
	}
	asPrimary, err := goapiproof.OperationsWithEnablementProofByKind(ctx, pool, "sha256:s", "b1", goapiproof.TargetModePrimary, docs, map[string]string{"syntheticCreate": goapiproof.OperationKindMutation})
	if err != nil || asPrimary["syntheticCreate"] {
		t.Fatalf("primary must NOT admit a proof-route write receipt: %v %v", asPrimary, err)
	}
	asQuery, err := goapiproof.OperationsWithEnablementProofByKind(ctx, pool, "sha256:s", "b1", goapiproof.TargetModeCanary, docs, map[string]string{"syntheticCreate": goapiproof.OperationKindQuery})
	if err != nil || asQuery["syntheticCreate"] {
		t.Fatalf("a write receipt must never admit the operation as a query: %v %v", asQuery, err)
	}
}

func TestAMismatchIsKeptAndNamed(t *testing.T) {
	pool := startPool(t)
	raw, _ := os.ReadFile("testdata/synthetic_baseline.json")
	var committed struct {
		Digest string `json:"digest"`
	}
	_ = json.Unmarshal(raw, &committed)
	run := RunTag("gwc-wp-mismatch")
	calls := 0
	// A build that persists a DIFFERENT name than the baseline's.
	result, err := Execute(context.Background(), pool, "org-fixture", syntheticCase(committed.Digest), run, "mutation M { x }", posterInserting(t, pool, "org-fixture", run, "a divergent write", &calls))
	if err != nil {
		t.Fatal(err)
	}
	if result.TerminalState != StateMismatch {
		t.Fatalf("want mismatch, got %s (%s)", result.TerminalState, result.Detail)
	}
	if result.Kept == nil || result.Kept.Run != run || rowsFor(t, pool, "org-fixture", run) == 0 {
		t.Fatalf("a mismatch must keep and name the dataset: %+v", result.Kept)
	}
	receipt, err := result.Receipt(ReceiptInput{SchemaDigest: "s", DocumentDigest: "d", CandidateBuild: "b", RequestIdentity: "r", Org: "org-fixture", RecordedBy: "t", ReviewEvidence: "e", Route: goapiproof.RouteEdge, BuildBinding: goapiproof.EdgeBuildPresent, ObservedAt: time.Now().UTC()})
	if err != nil || receipt.TerminalState != StateMismatch || receipt.SideEffectDigest != result.Digest {
		t.Fatalf("the mismatch receipt must carry the observed digest: %+v %v", receipt, err)
	}
}

func TestAnErrorsMemberATransportFailureAndAnEmptyObservationAreNeverAMatch(t *testing.T) {
	raw, _ := os.ReadFile("testdata/synthetic_baseline.json")
	var committed struct {
		Digest string `json:"digest"`
	}
	_ = json.Unmarshal(raw, &committed)

	for name, post := range map[string]func(*pgxpool.Pool, RunTag, *int) Poster{
		"errors member": func(pool *pgxpool.Pool, run RunTag, calls *int) Poster {
			inner := posterInserting(t, pool, "org-fixture", run, "", calls)
			return func(ctx context.Context, d, v string) (Response, error) {
				resp, err := inner(ctx, d, v)
				resp.Body = []byte(`{"data":null,"errors":[{"message":"boom"}]}`)
				return resp, err
			}
		},
		"transport failure": func(pool *pgxpool.Pool, run RunTag, calls *int) Poster {
			inner := posterInserting(t, pool, "org-fixture", run, "", calls)
			return func(ctx context.Context, d, v string) (Response, error) {
				_, _ = inner(ctx, d, v) // the write DID run, then the answer was lost
				return Response{}, errors.New("connection reset")
			}
		},
		"nothing persisted": func(pool *pgxpool.Pool, run RunTag, calls *int) Poster {
			return func(context.Context, string, string) (Response, error) {
				*calls++
				return Response{Status: 200, Body: []byte(`{"data":{"syntheticCreate":null}}`)}, nil
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			pool := startPool(t)
			run := RunTag("gwc-wp-" + fmt.Sprint(len(name)))
			calls := 0
			c := syntheticCase(committed.Digest)
			if name == "nothing persisted" {
				// A case whose seeder persists nothing AND whose mutation persists
				// nothing: the empty set must not match an empty baseline.
				c.Seeder = noSeeder{}
			}
			result, err := Execute(context.Background(), pool, "org-fixture", c, run, "mutation M { x }", post(pool, run, &calls))
			if err != nil {
				t.Fatal(err)
			}
			if result.TerminalState != StateProofFailed {
				t.Fatalf("want proof_failed, got %s", result.TerminalState)
			}
			if calls != 1 {
				t.Fatalf("the mutation must not be retried: %d posts", calls)
			}
			if result.Kept == nil {
				t.Fatal("the dataset must be kept after a failure")
			}
		})
	}
}

func TestASeedFailureNeverPostsTheMutation(t *testing.T) {
	pool := startPool(t)
	calls := 0
	c := syntheticCase("sha256:x")
	c.Seeder = syntheticSeeder{failSeed: true}
	_, err := Execute(context.Background(), pool, "org-fixture", c, "gwc-wp-seedfail", "mutation M { x }", posterInserting(t, pool, "org-fixture", "gwc-wp-seedfail", "", &calls))
	if !errors.Is(err, ErrNotExecuted) || calls != 0 {
		t.Fatalf("a failed seed must refuse before any post: err=%v calls=%d", err, calls)
	}
}

// A transport that used more than one connection for the single post may have
// sent the mutation twice: the run is never a match, whatever the effects say.
func TestAPostTheTransportMayHaveSentTwiceIsNeverAMatch(t *testing.T) {
	pool := startPool(t)
	raw, _ := os.ReadFile("testdata/synthetic_baseline.json")
	var committed struct {
		Digest string `json:"digest"`
	}
	_ = json.Unmarshal(raw, &committed)
	run := RunTag("gwc-wp-twice")
	calls := 0
	inner := posterInserting(t, pool, "org-fixture", run, "", &calls)
	post := func(ctx context.Context, d, v string) (Response, error) {
		resp, err := inner(ctx, d, v)
		resp.WireAttempts = 2
		return resp, err
	}
	result, err := Execute(context.Background(), pool, "org-fixture", syntheticCase(committed.Digest), run, "mutation M { x }", post)
	if err != nil {
		t.Fatal(err)
	}
	if result.TerminalState != StateProofFailed || result.Kept == nil {
		t.Fatalf("want proof_failed with the dataset kept, got %s kept=%v (%s)", result.TerminalState, result.Kept, result.Detail)
	}
	// With one attempt the very same effects ARE the baseline: the refusal is
	// about the wire, not about what was persisted.
	if result.Digest != committed.Digest {
		t.Fatalf("the effects should equal the baseline (only the wire count differs): %s vs %s", result.Digest, committed.Digest)
	}
}

// r1 P1: a response that does not carry the candidate build establishes nothing
// about which build wrote: never a match, and the dataset (the only evidence of
// what ran) is kept.
func TestAResponseWithoutTheCandidateBuildIsNeverAMatchAndKeepsTheDataset(t *testing.T) {
	raw, _ := os.ReadFile("testdata/synthetic_baseline.json")
	var committed struct {
		Digest string `json:"digest"`
	}
	_ = json.Unmarshal(raw, &committed)

	for name, build := range map[string]string{"absent": "", "another build": "0000other"} {
		t.Run(name, func(t *testing.T) {
			pool := startPool(t) // a kept dataset holds the synthetic seed's fixed id
			run := RunTag("gwc-wp-build-" + fmt.Sprint(len(name)))
			calls := 0
			inner := posterInserting(t, pool, "org-fixture", run, "", &calls)
			post := func(ctx context.Context, d, v string) (Response, error) {
				resp, err := inner(ctx, d, v)
				resp.Build = build
				return resp, err
			}
			result, err := Execute(context.Background(), pool, "org-fixture", syntheticCase(committed.Digest), run, "mutation M { x }", post, WithRequiredBuild("abc123"))
			if err != nil {
				t.Fatal(err)
			}
			if result.TerminalState != StateProofFailed || result.Kept == nil || rowsFor(t, pool, "org-fixture", run) == 0 {
				t.Fatalf("want proof_failed with the dataset kept, got %s kept=%v (%s)", result.TerminalState, result.Kept, result.Detail)
			}
			if result.Digest != committed.Digest {
				t.Fatalf("the effects equal the baseline (only the build tie is missing): %s vs %s", result.Digest, committed.Digest)
			}
		})
	}

	// And with the right build the same run IS a match.
	pool := startPool(t)
	run := RunTag("gwc-wp-build-ok")
	calls := 0
	inner := posterInserting(t, pool, "org-fixture", run, "", &calls)
	post := func(ctx context.Context, d, v string) (Response, error) {
		resp, err := inner(ctx, d, v)
		resp.Build = "abc123"
		return resp, err
	}
	result, err := Execute(context.Background(), pool, "org-fixture", syntheticCase(committed.Digest), run, "mutation M { x }", post, WithRequiredBuild("abc123"))
	if err != nil || result.TerminalState != StateMatch {
		t.Fatalf("the control must match: %v %+v", err, result)
	}
}
