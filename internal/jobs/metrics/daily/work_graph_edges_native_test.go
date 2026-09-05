package daily

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workgraphedges"
	"github.com/google/uuid"
)

// providerRowsStub feeds LoadWorkGraphEdgeRepoProviders rows whose provider
// COLUMN is deliberately populated, so a test can prove the column is ignored.
type providerRowsStub struct {
	ids       []uuid.UUID
	providers []string
	position  int
}

func (rows *providerRowsStub) Next() bool { return rows.position < len(rows.ids) }
func (rows *providerRowsStub) Scan(destinations ...any) error {
	if len(destinations) != 2 {
		return fmt.Errorf("expected 2 scan destinations, got %d", len(destinations))
	}
	id, ok := destinations[0].(*uuid.UUID)
	if !ok {
		return fmt.Errorf("destination 0 is %T, want *uuid.UUID", destinations[0])
	}
	provider, ok := destinations[1].(*string)
	if !ok {
		return fmt.Errorf("destination 1 is %T, want *string", destinations[1])
	}
	*id = rows.ids[rows.position]
	*provider = rows.providers[rows.position]
	rows.position++
	return nil
}
func (rows *providerRowsStub) Err() error                         { return nil }
func (rows *providerRowsStub) Close() error                       { return nil }
func (rows *providerRowsStub) ScanStruct(any) error               { return errors.New("unused") }
func (rows *providerRowsStub) ColumnTypes() []chdriver.ColumnType { return nil }
func (rows *providerRowsStub) Totals(...any) error                { return nil }
func (rows *providerRowsStub) Columns() []string                  { return []string{"id", "provider"} }
func (rows *providerRowsStub) HasData() bool                      { return len(rows.ids) > 0 }

type providerConnStub struct{ rows *providerRowsStub }

func (conn *providerConnStub) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	return conn.rows, nil
}

// TestRepoProvidersAreTheJobProviderNotTheColumn pins the rule codex r1 (P1)
// on #2240 was right about and this branch originally got wrong.
//
// On the daily worker path discover_repos NEVER reads the repos table:
//
//	worker_metrics.py:1729  one repo_id per run_daily_metrics_job call
//	job_daily.py:1198       discover_repos(repo_id=repo_id, ...) with NO provider=
//	job_daily.py:126        so the parameter default provider="auto" applies
//	job_daily.py:129-136    if repo_id: return [DiscoveredRepo(source=provider)]
//
// so repo_provider_by_id is {repo: "auto"} for every repo, on every run.
//
// The stub supplies "github" and "gitlab" in the provider COLUMN. If the loader
// ever reads that column again this test fails, which is the point: the column
// is present, populated, and must be ignored.
func TestRepoProvidersAreTheJobProviderNotTheColumn(t *testing.T) {
	repoA := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	repoB := uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	conn := &providerConnStub{rows: &providerRowsStub{
		ids:       []uuid.UUID{repoA, repoB},
		providers: []string{"github", "gitlab"},
	}}

	providers, err := LoadWorkGraphEdgeRepoProviders(
		context.Background(), conn, "70d529e0-3c06-4597-8480-794fd02328b6", "auto", nil)
	if err != nil {
		t.Fatalf("load providers: %v", err)
	}
	for _, repo := range []uuid.UUID{repoA, repoB} {
		if got := providers[repo.String()]; got != "auto" {
			t.Errorf("repo %s: got provider %q, want \"auto\" -- the JOB provider, not "+
				"the repos.provider column. Reading the column writes a provider Python "+
				"never writes, and splits these repos across several extractPerProvider "+
				"passes where Python makes one (which changes emitted ORDER, though "+
				"measurably NOT which edges exist).", repo, got)
		}
	}
	if len(providers) != 2 {
		t.Errorf("expected 2 mapped repos, got %d", len(providers))
	}
}

// TestUnmappedRepositoryFallsBackToUnknownNotTheJobProvider pins the OTHER
// fallback, which is a different one.
//
// _by_provider does `repo_provider_by_id.get(str(repo_id), "unknown")`: a repo
// that discover_repos never returned is labelled "unknown", NOT the job
// provider. The job-provider fallback happens inside the map's construction
// and a repo absent from the map was never subject to it. Collapsing these two
// fallbacks into one is the natural simplification and it is wrong.
func TestUnmappedRepositoryFallsBackToUnknownNotTheJobProvider(t *testing.T) {
	known := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	unmapped := uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	providers := map[string]string{known.String(): "github"}

	if got := workGraphEdgesProviderFor(providers, known); got != "github" {
		t.Errorf("mapped repo: got %q, want github", got)
	}
	if got := workGraphEdgesProviderFor(providers, unmapped); got != "unknown" {
		t.Errorf("unmapped repo: got %q, want unknown (NOT the job provider)", got)
	}
}

// TestPartitionRepoIsSeededEvenWhenTheReposQueryMissesIt is codex round
// chaos-4286a-r2's finding 2. On the production path discover_repos
// short-circuits on repo_id with NO `repos` read at all (see the doc comment
// on LoadWorkGraphEdgeRepoProviders and on this test's sibling above), so
// Python's map is NEVER missing the partition's own repo. The Go query can
// miss it (a stale/absent `repos` row), and before the fix that meant the
// repo read as "unknown" here -- this pins that it now reads jobProvider
// instead, while a repo genuinely OUTSIDE the partition still falls back to
// "unknown" exactly as TestUnmappedRepositoryFallsBackToUnknownNotTheJobProvider
// pins.
func TestPartitionRepoIsSeededEvenWhenTheReposQueryMissesIt(t *testing.T) {
	partitionRepo := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	outsidePartition := uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")
	// The query returns NOTHING for partitionRepo -- simulating a stale or
	// absent `repos` row -- so only the seeding step can supply it.
	conn := &providerConnStub{rows: &providerRowsStub{}}

	providers, err := LoadWorkGraphEdgeRepoProviders(
		context.Background(), conn, "70d529e0-3c06-4597-8480-794fd02328b6", "auto",
		[]uuid.UUID{partitionRepo})
	if err != nil {
		t.Fatalf("load providers: %v", err)
	}
	if got := workGraphEdgesProviderFor(providers, partitionRepo); got != "auto" {
		t.Errorf("partition repo missing from the query: got %q, want \"auto\" -- "+
			"the partition's own repo must be seeded with the job provider even when "+
			"the repos-table query misses it, matching Python's short-circuit guarantee", got)
	}
	// Negative control: a repo the partition never named must still fall back
	// to "unknown", so the fix cannot have degenerated into "seed everything".
	if got := workGraphEdgesProviderFor(providers, outsidePartition); got != "unknown" {
		t.Errorf("repo outside the partition: got %q, want \"unknown\"", got)
	}
}

// TestExtractPerProviderSplitsAndSortsLikePython pins two things Python does
// that a single flattened call would not.
//
//  1. The extractor runs once per provider, so the per-repo deployment index a
//     heuristic incident walks is rebuilt per provider -- an incident can
//     never link to a deployment from a different provider.
//  2. Providers are visited in SORTED order (`for wf_provider in
//     sorted(edge_providers)`), which is what makes the emitted row order
//     deterministic.
func TestExtractPerProviderSplitsAndSortsLikePython(t *testing.T) {
	const org = "70d529e0-3c06-4597-8480-794fd02328b6"
	repoGitHub := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	repoGitLab := uuid.MustParse("0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d")

	providers := map[string]string{
		repoGitHub.String(): "github",
		repoGitLab.String(): "gitlab",
	}

	deployments := []workgraphedges.DeploymentRow{
		{RepoID: repoGitLab, DeploymentID: "gitlab-dep"},
		{RepoID: repoGitHub, DeploymentID: "github-dep"},
	}
	incidents := []workgraphedges.IncidentRow{
		{RepoID: repoGitHub, IncidentID: "inc-github"},
		{RepoID: repoGitLab, IncidentID: "inc-gitlab"},
	}

	result, err := extractPerProvider(
		providers, nil, deployments, incidents, org, time.Time{},
	)
	if err != nil {
		t.Fatalf("extractPerProvider: %v", err)
	}

	if len(result.DeploymentIncidentEdges) != 2 {
		t.Fatalf("expected one edge per provider, got %d", len(result.DeploymentIncidentEdges))
	}
	// "github" sorts before "gitlab", so github's edge is emitted first.
	first, second := result.DeploymentIncidentEdges[0], result.DeploymentIncidentEdges[1]
	if first.Provider != "github" || second.Provider != "gitlab" {
		t.Errorf("providers must be visited in sorted order: got %q then %q",
			first.Provider, second.Provider)
	}
	// Each incident linked ONLY to its own provider's deployment.
	if first.DeploymentID != "github-dep" || first.IncidentID != "inc-github" {
		t.Errorf("github edge crossed providers: %s -> %s", first.IncidentID, first.DeploymentID)
	}
	if second.DeploymentID != "gitlab-dep" || second.IncidentID != "inc-gitlab" {
		t.Errorf("gitlab edge crossed providers: %s -> %s", second.IncidentID, second.DeploymentID)
	}
}

// TestIncidentAdapterLeavesDeploymentIDEmpty pins CHAOS-5110 as a DELIBERATE
// reproduction rather than an oversight a future reader might "fix".
//
// active_incidents_query selects no deployment_id, so every incident reaching
// the extractor from the daily job takes the heuristic branch. If this ever
// starts returning a non-empty DeploymentID, the rows change from
// source="heuristic" to source="native" -- and since `source` is IN the
// sorting key, the new rows will sit BESIDE the old ones rather than replacing
// them.
func TestIncidentAdapterLeavesDeploymentIDEmpty(t *testing.T) {
	repoID := uuid.MustParse("d4f322ad-2102-1fbf-8425-7400573194f7")
	adapted := workGraphEdgeIncidents([]IncidentRow{
		{RepoID: repoID, IncidentID: "inc-1"},
	})
	if len(adapted) != 1 {
		t.Fatalf("expected 1 adapted incident, got %d", len(adapted))
	}
	if adapted[0].DeploymentID != "" {
		t.Errorf("the daily loader cannot supply a deployment_id (CHAOS-5110); "+
			"got %q -- if this is now intended, the source column moves from "+
			"heuristic to native and that is a key change, not a value change",
			adapted[0].DeploymentID)
	}
	if adapted[0].StartedAt == nil {
		t.Error("started_at must be carried through; the query's own WHERE guarantees it is non-NULL")
	}
	if adapted[0].LastSynced != nil {
		t.Error("last_synced is unreachable in the _dt chain here and must stay nil")
	}
}

// TestWorkGraphEdgesPartialWriteGuardPinsBothDirections pins the partial-write
// decision in BOTH directions, which is the only shape that actually defends
// it.
//
// A test asserting only the ErrPartialWrite path would still pass if ordinary
// failures had quietly stopped failing open — and that regression is invisible
// in production, because the family simply disappears from the partition
// instead of erroring. The two mistakes are symmetric:
//
//   - wrap when nothing was written -> suppresses the compatibility bridge's
//     LEGITIMATE fallback, losing the family for that partition;
//   - do not wrap when something was -> the bridge adds a SECOND copy of rows
//     that already landed.
//
// Shape borrowed from #2235's TestPartialWriteIsSkippedNotFailedOpen, on
// lane-port-review-bench's advice.
func TestWorkGraphEdgesPartialWriteGuardPinsBothDirections(t *testing.T) {
	cause := errors.New("simulated ClickHouse send failure")

	t.Run("failure AFTER a write is a partial write", func(t *testing.T) {
		rows, err := wrapWorkGraphEdgesPartialWrite(7, 2, cause)
		if !errors.Is(err, ErrPartialWrite) {
			t.Errorf("a failure after 7 rows landed must wrap ErrPartialWrite so the bridge is "+
				"skipped; got %v", err)
		}
		if !errors.Is(err, cause) {
			t.Errorf("the original cause must survive wrapping; got %v", err)
		}
		if rows != 7 {
			t.Errorf("the TRUE rows-written count must be reported, got %d, want 7 — "+
				"reporting 0 here tells an operator the opposite of what happened and "+
				"misinforms the re-drive decision", rows)
		}
	})

	t.Run("failure BEFORE any write is an ordinary failure", func(t *testing.T) {
		rows, err := wrapWorkGraphEdgesPartialWrite(0, 1, cause)
		if errors.Is(err, ErrPartialWrite) {
			t.Error("a failure with nothing written must NOT wrap ErrPartialWrite: doing so " +
				"suppresses the bridge's legitimate fallback and loses the family for this " +
				"partition (partialwrite.go:29)")
		}
		if !errors.Is(err, cause) {
			t.Errorf("the original cause must be returned unchanged; got %v", err)
		}
		if rows != 0 {
			t.Errorf("nothing was written, so the count must be 0, got %d", rows)
		}
	})
}

// TestWorkGraphEdgesPartialWriteNamesEachTableByPosition pins the operator-facing
// message at EVERY write position, not just the one the ClickHouse test happens
// to exercise.
//
// It exists because the index is positional: a fourth write inserted in the
// middle renumbers the ones after it, and the ONLY thing that still needs a
// human is adding the new table's name to workGraphEdgesWriteOrder. If someone
// inserts a write and forgets the name, this test fails here rather than
// shipping a message that names the wrong table -- which would be worse than no
// name at all, since a wrong table sends an operator to the wrong data.
func TestWorkGraphEdgesPartialWriteNamesEachTableByPosition(t *testing.T) {
	cause := errors.New("clickhouse: connection reset")
	total := len(workGraphEdgesWriteOrder)

	for step, want := range []string{
		"work_graph_pr_review_outcome_edges",
		"work_graph_pr_deployment_edges",
		"work_graph_deployment_incident_edges",
	} {
		position := step + 1
		_, err := wrapWorkGraphEdgesPartialWrite(11, position, cause)
		msg := err.Error()
		if !strings.Contains(msg, want) {
			t.Errorf("write %d: message does not name %q: %s", position, want, msg)
		}
		if fragment := fmt.Sprintf("write %d of %d", position, total); !strings.Contains(msg, fragment) {
			t.Errorf("write %d: message does not carry %q: %s", position, fragment, msg)
		}
		if !strings.Contains(msg, "11 row(s) landed") {
			t.Errorf("write %d: message drops the true row count: %s", position, msg)
		}
	}

	// NEGATIVE CONTROL. A step past the end means a write was added without its
	// name. The message must SAY so rather than silently naming the last table
	// or panicking, so this asserts the loud marker and asserts the absence of
	// any real table name.
	_, err := wrapWorkGraphEdgesPartialWrite(11, total+1, cause)
	if !strings.Contains(err.Error(), "UNREGISTERED TABLE") {
		t.Errorf("an out-of-range write position must be called out loudly, got: %v", err)
	}
	for _, name := range workGraphEdgesWriteOrder {
		if strings.Contains(err.Error(), name) {
			t.Errorf("out-of-range position named a real table (%q), which would mislead an operator: %v", name, err)
		}
	}
}

// sendErrorBatch's Append succeeds (nothing crossed the network yet) but Send
// fails -- the ambiguous shape a real ack-loss produces: ClickHouse may have
// committed the batch server-side, ack-loss is a transport failure, not a
// database rejection.
type sendErrorBatch struct{ sendErr error }

func (batch *sendErrorBatch) Append(...any) error             { return nil }
func (batch *sendErrorBatch) Send() error                     { return batch.sendErr }
func (batch *sendErrorBatch) Abort() error                    { return nil }
func (batch *sendErrorBatch) AppendStruct(any) error          { return errors.New("unused") }
func (batch *sendErrorBatch) Close() error                    { return nil }
func (batch *sendErrorBatch) Column(int) chdriver.BatchColumn { return nil }
func (batch *sendErrorBatch) Flush() error                    { return nil }
func (batch *sendErrorBatch) IsSent() bool                    { return false }
func (batch *sendErrorBatch) Rows() int                       { return 0 }
func (batch *sendErrorBatch) Columns() []column.Interface     { return nil }

type sendErrorBatchConn struct{ batch *sendErrorBatch }

func (conn *sendErrorBatchConn) PrepareBatch(
	context.Context, string, ...chdriver.PrepareBatchOption,
) (chdriver.Batch, error) {
	return conn.batch, nil
}

// TestWorkGraphEdgesWriteReportsRowsOnSendAckLoss is codex round
// chaos-4286a-r2's finding 1. Before the fix, each WriteWorkGraph*Edges
// function returned (0, err) on ANY Send failure, including one where
// ClickHouse actually committed the batch and only the acknowledgement was
// lost in transit -- understating `written` in the caller and letting
// wrapWorkGraphEdgesPartialWrite's `written > 0` guard stay CLOSED on a batch
// that may already be sitting in the table, so the Python bridge would
// rewrite it too. The fix reports len(rows) on a Send error specifically
// (PrepareBatch/Append errors, which never crossed the network, still
// correctly report 0).
func TestWorkGraphEdgesWriteReportsRowsOnSendAckLoss(t *testing.T) {
	sendErr := errors.New("simulated ack loss: connection reset after write")
	conn := &sendErrorBatchConn{batch: &sendErrorBatch{sendErr: sendErr}}
	rows := []workgraphedges.PRReviewOutcomeEdge{
		{EdgeID: "e1", OrgID: uuid.New(), PRID: "pr-1", ReviewOutcomeID: "ro-1", Provider: "github", Source: "native"},
		{EdgeID: "e2", OrgID: uuid.New(), PRID: "pr-2", ReviewOutcomeID: "ro-2", Provider: "github", Source: "native"},
	}

	written, err := WriteWorkGraphPRReviewOutcomeEdges(context.Background(), conn, rows, time.Now())
	if !errors.Is(err, sendErr) {
		t.Fatalf("err = %v, want it to wrap the Send error", err)
	}
	if written != len(rows) {
		t.Fatalf("written = %d, want %d (the ambiguous-ack case must report the rows as landed, "+
			"not 0 -- a caller that trusts 0 here can fail OPEN onto a batch ClickHouse already has)",
			written, len(rows))
	}
}
