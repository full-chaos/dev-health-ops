package providerunit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/connerr"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

// realDroppedConnFirstBlockError builds the driver error of a server-side
// close seen by the first read after an INSERT was sent:
//   - the inner error is REAL: ch-go proto.Reader.ReadByte over a loopback TCP
//     connection whose peer closed (FIN), so io.ReadFull gets io.EOF and ch-go
//     wraps it as "read: EOF";
//   - the outer wrap is the clickhouse-go v2 conn_process.go first-block line,
//     copied verbatim (the connection type is unexported).
func realDroppedConnFirstBlockError(t *testing.T) error {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		server, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = server.Close()
		}
	}()
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, readErr := chproto.NewReader(conn).ReadByte()
	if readErr == nil {
		t.Fatal("ReadByte on a peer-closed connection returned no error")
	}
	const connID = 1
	driverErr := fmt.Errorf("query processing: failed to read first block packet from %s (conn_id=%d): %w",
		conn.RemoteAddr(), connID, readErr)
	if !errors.Is(driverErr, io.EOF) || !strings.HasSuffix(driverErr.Error(), "read: EOF") {
		t.Fatalf("driver error %q is not the dropped-connection shape (read: EOF, errors.Is io.EOF)", driverErr)
	}
	return driverErr
}

// droppingConn is a driver.Conn whose PrepareBatch fails the way the service
// conn does when the INSERT's first server packet never comes: the driver
// error marked connerr.AtWrite (internal/storage/clickhouse
// TestTheServiceConnMarksOnlyItsInsertCalls). With onSend, PrepareBatch
// succeeds and the batch's Send fails so.
type droppingConn struct {
	driver.Conn
	err    error
	onSend bool
}

func (c droppingConn) PrepareBatch(context.Context, string, ...driver.PrepareBatchOption) (driver.Batch, error) {
	if c.onSend {
		return droppingSendBatch{err: connerr.AtWrite(c.err)}, nil
	}
	return nil, connerr.AtWrite(c.err)
}

type droppingSendBatch struct {
	driver.Batch
	err error
}

func (droppingSendBatch) Append(...any) error { return nil }
func (batch droppingSendBatch) Send() error   { return batch.err }
func (droppingSendBatch) Abort() error        { return nil }

func sinkDropClaim(now time.Time) providersync.Claim {
	capability, _ := providersync.Capability("github", "cicd")
	return providersync.Claim{
		Unit: providersync.Unit{
			ID: "11111111-1111-4111-8111-111111111111", SyncRunID: "22222222-2222-4222-8222-222222222222",
			OrgID: "org-acme", IntegrationID: "33333333-3333-4333-8333-333333333333",
			SourceID: "44444444-4444-4444-8444-444444444444", SourceExternalID: "acme/api", SourceName: "acme/api",
			Provider: "github", Dataset: "cicd", CostClass: capability.CostClass, Mode: "incremental",
			CredentialID: "66666666-6666-4666-8666-666666666666", AuthSource: "integration_credential",
		},
		Owner: "55555555-5555-4555-8555-555555555555", Attempt: 1, LeaseExpiresAt: now.Add(time.Hour),
	}
}

// droppingTestOpsSink is the REAL GitHub tests ClickHouse sink over a
// connection that drops under the INSERT.
func droppingTestOpsSink(driverErr error) providersync.TestOpsClickHouseEffects {
	return droppingTestOpsSinkAt(droppingConn{err: driverErr})
}

func droppingTestOpsSinkAt(conn droppingConn) providersync.TestOpsClickHouseEffects {
	return providersync.TestOpsClickHouseEffects{
		Conn:  conn,
		Lease: providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	}
}

func testSuiteResultsEffect(t *testing.T, now time.Time) providersync.EffectBatch {
	t.Helper()
	row := fmt.Sprintf(`{"org_id":"org-acme","repo_id":"repo-1","run_id":"9001","suite_id":"s1","suite_name":"pytest","total_count":1,"last_synced":%q}`,
		now.Format(time.RFC3339Nano))
	effect, err := providersync.BuildEffectBatch("test_suite_results", providersync.EffectReadbackRequired,
		[]json.RawMessage{json.RawMessage(row)})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

// realSinkDropError is the error the REAL TestOps sink returns for a dropped
// INSERT: the service conn's marked driver error, unchanged.
func realSinkDropError(t *testing.T) error {
	t.Helper()
	now := time.Date(2026, 11, 22, 17, 27, 12, 0, time.UTC)
	driverErr := realDroppedConnFirstBlockError(t)
	sinkErr := droppingTestOpsSink(driverErr).WriteEffect(context.Background(), sinkDropClaim(now), testSuiteResultsEffect(t, now))
	if !connerr.IsDroppedWrite(sinkErr) || !errors.Is(sinkErr, driverErr) || sinkErr.Error() != driverErr.Error() {
		t.Fatalf("the TestOps sink changed the dropped INSERT error: %v", sinkErr)
	}
	return sinkErr
}

// committedSinkDropError is the error a chunk commit returns when the REAL
// TestOps sink's INSERT drops under the REAL effect committer.
func committedSinkDropError(t *testing.T) error {
	t.Helper()
	now := time.Date(2026, 11, 22, 17, 27, 12, 0, time.UTC)
	committer := providersync.EffectCommitter{
		Ledger:   &testEffectLedger{},
		Sink:     droppingTestOpsSink(realDroppedConnFirstBlockError(t)),
		Readback: testEffectReadback{},
		Now:      func() time.Time { return now },
	}
	_, err := committer.Commit(context.Background(), sinkDropClaim(now),
		[]providersync.EffectBatch{testSuiteResultsEffect(t, now)}, now)
	if err == nil || !errors.Is(err, io.EOF) || !strings.HasSuffix(err.Error(), "read: EOF") {
		t.Fatalf("committer error = %v, want the dropped-connection driver error", err)
	}
	return err
}

// droppingSink fails chosen WriteEffect calls (1-based) with err and accepts
// the rest; it counts calls so a cell knows chunks were written between.
type droppingSink struct {
	mu     sync.Mutex
	calls  int
	failOn map[int]bool
	err    error
}

func (sink *droppingSink) WriteEffect(context.Context, providersync.Claim, providersync.EffectBatch) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.calls++
	if sink.failOn[sink.calls] {
		return sink.err
	}
	return nil
}

type okWalkDoer struct{}

func (okWalkDoer) Do(request *http.Request) (*http.Response, error) {
	if request.URL.Path == "/repos/acme/api" {
		return jsonResponse(request, http.StatusOK, strings.NewReader(`{"full_name":"acme/api","default_branch":"main"}`)), nil
	}
	return jsonResponse(request, http.StatusOK, strings.NewReader(`{}`)), nil
}

type dropWalkRepository struct{ *classRepository }

func (repository dropWalkRepository) Claim(ctx context.Context, request providersync.ClaimRequest) (providersync.Claim, error) {
	request.Owner = fixedWalkOwner
	return repository.classRepository.Claim(ctx, request)
}

type dropWalk struct {
	repository     *classRepository
	ledger         *providersync.InProcessChunkLedger
	ledgerClaim    providersync.Claim
	sink           *droppingSink
	attempt        int // River job.Attempt as the adapter passes it
	snoozes        int
	failures       int // ordinary (attempt-consuming) returns + the terminal one
	ordinalAtFail  []int
	classAtFail    []string
	retryCapAtFail []int
	lastErr        error
	terminal       bool
}

// driveDroppedWriteWalk runs the REAL Handler.Work over the REAL chunked
// streaming executor (InProcessChunkLedger, github/cicd descriptor, real
// EffectCommitter) with River's attempt rule: a snooze keeps job.Attempt, an
// ordinary retryable return consumes one (jobruntime/errors.go).
func driveDroppedWriteWalk(t *testing.T, pages int, failOn map[int]bool, writeErr error) dropWalk {
	t.Helper()
	now := time.Date(2026, 11, 22, 17, 0, 0, 0, time.UTC)
	unit := providerUnit()
	capability, ok := providersync.Capability("github", "cicd")
	if !ok {
		t.Fatal("github/cicd capability missing")
	}
	descriptor, ok := providersync.Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked || !descriptor.Plannable {
		t.Fatalf("github/cicd descriptor chunked=%v plannable=%v", descriptor.Chunked, descriptor.Plannable)
	}
	unit.SourceExternalID, unit.SourceName = "acme/api", "acme/api"
	unit.Provider, unit.Dataset, unit.CostClass = "github", "cicd", capability.CostClass
	walk := dropWalk{
		repository: &classRepository{memoryUnitRepository: newMemoryUnitRepository(unit)},
		sink:       &droppingSink{failOn: failOn, err: writeErr},
		attempt:    1,
	}
	handler := &Handler{
		Repository:    dropWalkRepository{walk.repository},
		LeaseDuration: time.Minute,
		Heartbeat:     10 * time.Second,
		Now:           func() time.Time { return now },
		BuildExecutor: func(session *providersync.LeaseSession) (providersync.CompleteRouteExecutor, error) {
			if walk.ledger == nil {
				walk.ledger, walk.ledgerClaim = providersync.NewInProcessChunkLedger(session.Claim), session.Claim
			}
			return providersync.CompleteRouteExecutor{
				Credentials: providerfoundation.CredentialResolver{
					Repository: githubCredentialRepository{unit: session.Claim.Unit},
					Decryptor:  githubCredentialDecryptor{},
				},
				Doer:         fakehttp.Client(okWalkDoer{}),
				Retry:        providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
				Budget:       testBudgetStore{},
				BudgetLimits: map[providersync.CostClass]int{capability.CostClass: 1},
				BudgetTTL:    time.Minute,
				Gate: func(providersync.Claim, *providerfoundation.HTTPClient) providerfoundation.BackoffGate {
					return testBackoffGate{}
				},
				Handler:    pagedWalkHandler{pages: pages, destinations: descriptor.Destinations},
				Comparator: matchingComparator{},
				Committer: providersync.EffectCommitter{
					Ledger: walk.ledger, Sink: walk.sink, Readback: testEffectReadback{}, Now: func() time.Time { return now },
				},
				HeartbeatInterval: 10 * time.Second,
				Now:               func() time.Time { return now },
			}, nil
		},
	}
	for step := 0; step < 20*pages; step++ {
		execution := providerExecution(unit, now, walk.attempt)
		execution.Definition.MaxAttempts = 5
		err := handler.Work(context.Background(), execution)
		walk.lastErr = err
		if err == nil {
			return walk
		}
		if _, snoozed := jobruntime.SnoozeDelay(err); snoozed {
			walk.snoozes++
			now = walk.repository.availableAt
			continue
		}
		walk.failures++
		walk.ordinalAtFail = append(walk.ordinalAtFail, walk.nextOrdinal(now))
		walk.classAtFail = append(walk.classAtFail, causeClass(err, false))
		walk.retryCapAtFail = append(walk.retryCapAtFail, retryMaxAttempts(err))
		if isPermanentReturn(err) || walk.repository.status == "failed" {
			walk.terminal = true
			return walk
		}
		walk.attempt++ // an ordinary retry consumes one River attempt
		now = now.Add(30 * time.Minute)
	}
	t.Fatalf("walk did not end in %d steps", 20*pages)
	return walk
}

func (walk dropWalk) nextOrdinal(now time.Time) int {
	if walk.ledger == nil {
		return 0
	}
	checkpoint, err := walk.ledger.LoadChunkCheckpoint(context.Background(), walk.ledgerClaim, now)
	if err != nil {
		return -1
	}
	return checkpoint.NextOrdinal
}

// dropWritesInChunks fails the first write of each named chunk ordinal (one
// chunk per page, one write per destination).
func dropWritesInChunks(t *testing.T, chunks ...int) map[int]bool {
	t.Helper()
	descriptor, _ := providersync.Descriptor("github", "cicd")
	perChunk := len(descriptor.Destinations)
	failOn := make(map[int]bool, len(chunks))
	for _, chunk := range chunks {
		failOn[chunk*perChunk+1] = true
	}
	return failOn
}

const dropWalkPages = 200

// Two dropped ClickHouse chunk writes, ~100 committed chunks apart, must not
// end the unit: each retry resumes at the checkpoint, and the drop has its
// own cause class with the registry ceiling (5), not the unclassified cap (2).
func TestTwoDroppedChunkWritesWithProgressDoNotExhaustTheUnit(t *testing.T) {
	sinkErr := realSinkDropError(t)
	walk := driveDroppedWriteWalk(t, dropWalkPages, dropWritesInChunks(t, 2, 100), sinkErr)
	if walk.terminal || walk.repository.status != "success" {
		t.Fatalf("unit ended %q category=%q cause_class=%q at attempt %d of 5 after %d dropped-write failures "+
			"(class at each failure %v); want it to finish",
			walk.repository.status, walk.repository.lastFailCategory, walk.repository.causeClass,
			walk.attempt, walk.failures, walk.classAtFail)
	}
	if walk.failures != 2 || walk.attempt != 3 {
		t.Fatalf("failures=%d attempt=%d, want 2 dropped-write retries then success on attempt 3", walk.failures, walk.attempt)
	}
	for index, class := range walk.classAtFail {
		if class != causeClassSinkConnectionDropped || walk.retryCapAtFail[index] != 0 {
			t.Errorf("retry %d: cause_class=%q cap=%d, want %q cap 0 (registry ceiling)",
				index+1, class, walk.retryCapAtFail[index], causeClassSinkConnectionDropped)
		}
	}
	if len(walk.ordinalAtFail) != 2 || walk.ordinalAtFail[1]-walk.ordinalAtFail[0] < 50 || walk.snoozes < 1 {
		t.Fatalf("premise: checkpoint at each failure=%v snoozes=%d, want progress between the drops",
			walk.ordinalAtFail, walk.snoozes)
	}
	checkpoint, err := walk.ledger.LoadChunkCheckpoint(context.Background(), walk.ledgerClaim, time.Now())
	if err == nil && checkpoint.NextOrdinal != dropWalkPages {
		t.Fatalf("checkpoint next_ordinal=%d, want %d", checkpoint.NextOrdinal, dropWalkPages)
	}
}

// The class is not progress-aware: five dropped writes over the unit's life
// still end it at the registry ceiling, under the dropped-connection class.
func TestFiveDroppedChunkWritesEndAtTheRegistryCeilingAsSinkConnectionDropped(t *testing.T) {
	sinkErr := realSinkDropError(t)
	walk := driveDroppedWriteWalk(t, dropWalkPages, dropWritesInChunks(t, 2, 40, 80, 120, 160), sinkErr)
	if !walk.terminal || walk.attempt != 5 || walk.failures != 5 ||
		walk.repository.status != "failed" || walk.repository.lastFailCategory != "provider_unit_exhausted" ||
		walk.repository.causeClass != causeClassSinkConnectionDropped {
		t.Fatalf("terminal=%v attempt=%d failures=%d status=%q category=%q cause_class=%q; "+
			"want failed provider_unit_exhausted %q at attempt 5 of 5",
			walk.terminal, walk.attempt, walk.failures, walk.repository.status,
			walk.repository.lastFailCategory, walk.repository.causeClass, causeClassSinkConnectionDropped)
	}
}
