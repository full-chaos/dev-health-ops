package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// replanMemoryLedger adds the discard to memoryEffectLedger: a reset removes
// the ledger and its snapshot together, as the Postgres ledger does.
type replanMemoryLedger struct {
	*memoryEffectLedger
	resets int
}

func (ledger *replanMemoryLedger) ResetPreparedEffectsForReplan(
	_ context.Context, claim Claim, expected EffectLedgerState, _ time.Time,
) error {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if !isSafeReplanState(claim, expected) {
		return ErrInvalidConfiguration
	}
	ledger.resets++
	ledger.state = EffectLedgerState{}
	ledger.preparedSnapshot = nil
	return nil
}

type discardReadbackResult struct {
	inspection EffectInspection
	err        error
}

// tableReadback answers per destination and counts every readback.
type tableReadback struct {
	mu      sync.Mutex
	results map[string]discardReadbackResult
	calls   int
}

func (readback *tableReadback) InspectEffect(_ context.Context, _ Claim, batch EffectBatch) (EffectInspection, error) {
	readback.mu.Lock()
	defer readback.mu.Unlock()
	readback.calls++
	result := readback.results[batch.Destination]
	return result.inspection, result.err
}

const (
	supersededFirstDestination  = "git_pull_request_comments"
	supersededSecondDestination = "git_pull_requests"
)

type supersededEffectSeed struct {
	status   GenerationBlockStatus
	blocked  bool
	readback discardReadbackResult
}

// seedSupersededPullRequestSnapshot stores a github/prs ledger and snapshot as
// an earlier binary would have written them for a destination set the route
// no longer emits (git_pull_request_comments, git_pull_requests).
func seedSupersededPullRequestSnapshot(
	t *testing.T, claim Claim, now time.Time, seeds map[string]supersededEffectSeed,
) *replanMemoryLedger {
	t.Helper()
	effects := make([]EffectBatch, 0, 2)
	for _, destination := range []string{supersededFirstDestination, supersededSecondDestination} {
		recovery := EffectReadbackRequired
		if seeds[destination].blocked {
			recovery = EffectRecoveryBlocked
		}
		effect, err := effectBatchFromValues(destination, recovery, []map[string]string{{"key": destination + "-42"}})
		if err != nil {
			t.Fatal(err)
		}
		effects = append(effects, effect)
	}
	sortEffectBatches(effects)
	stored := make([]storedPreparedEffect, 0, len(effects))
	for _, effect := range effects {
		stored = append(stored, storedPreparedEffect{
			Destination: effect.Destination, ContentDigest: effect.ContentDigest,
			Recovery: effect.Recovery, Rows: effect.Rows, PayloadBytes: effect.PayloadBytes,
		})
	}
	payload, err := json.Marshal(storedPreparedRouteSnapshot{
		SchemaVersion: preparedRouteSnapshotSchemaVersion, Generation: claim.GenerationKey(),
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset, NormalizedAt: now,
		Effects: stored, Result: json.RawMessage(`{"prs_synced":1}`),
		Evidence:   FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Records: 2},
		Comparison: ShadowComparison{Match: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256Hex(string(payload))
	state, err := NewEffectLedgerState(claim, effects, now)
	if err != nil {
		t.Fatal(err)
	}
	state.SchemaVersion = "v2"
	state.PreparedSnapshot = &PreparedRouteSnapshotReference{
		SchemaVersion: preparedRouteSnapshotSchemaVersion, ContentDigest: digest, PayloadBytes: len(payload),
	}
	started, committed := now.Add(time.Second), now.Add(2*time.Second)
	for index := range state.Effects {
		switch seeds[state.Effects[index].Destination].status {
		case GenerationBlockWriting:
			state.Effects[index].Status, state.Effects[index].StartedAt = GenerationBlockWriting, &started
		case GenerationBlockCommitted:
			state.Effects[index].Status = GenerationBlockCommitted
			state.Effects[index].StartedAt, state.Effects[index].CommittedAt = &started, &committed
		}
	}
	if err := state.validate(); err != nil {
		t.Fatalf("seeded ledger is invalid: %v", err)
	}
	return &replanMemoryLedger{memoryEffectLedger: &memoryEffectLedger{state: state, preparedSnapshot: payload}}
}

func currentPullRequestSocialBatch(t *testing.T, claim Claim) CompleteRouteBatch {
	t.Helper()
	pulls, err := effectBatchFromValues("git_pull_requests", EffectReadbackRequired, []map[string]string{{"key": "fresh-42"}})
	if err != nil {
		t.Fatal(err)
	}
	reviews, err := effectBatchFromValues("git_pull_request_reviews", EffectReadbackRequired, []map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{pulls, reviews}, Result: map[string]any{"prs_synced": 1},
		Watermark: claim.BeforeAt, Evidence: FetchEvidence{Provider: claim.Provider, Dataset: claim.Dataset, Records: 1},
	}
}

// TestSupersededSnapshotDiscardDecisionTable enumerates the discard decision:
// a discard never erases the evidence of a write that may have landed, and a
// read error is never treated as absent.
// discardCell is one row of the discard decision table.
type discardCell struct {
	name          string
	first, second supersededEffectSeed
	noReadback    bool
	wantReason    string
	wantReadbacks int
}

// supersededDiscardCells is the decision table: every ledger effect state
// crossed with every readback answer the sink can give, plus the no-readback
// case, each with the outcome it must produce.
func supersededDiscardCells() []discardCell {
	pending := supersededEffectSeed{status: GenerationBlockPending}
	writing := func(inspection EffectInspection, err error) supersededEffectSeed {
		return supersededEffectSeed{status: GenerationBlockWriting, readback: discardReadbackResult{inspection: inspection, err: err}}
	}
	committed := supersededEffectSeed{status: GenerationBlockCommitted}
	blocked := supersededEffectSeed{status: GenerationBlockPending, blocked: true}
	readError := errors.New("readback unavailable: clickhouse timeout")
	return []discardCell{
		{"pending + pending", pending, pending, false, "manifest_mismatch", 0},
		{"pending + writing absent", pending, writing(EffectAbsent, nil), false, "manifest_mismatch", 1},
		{"writing absent + writing absent", writing(EffectAbsent, nil), writing(EffectAbsent, nil), false, "manifest_mismatch", 2},
		{"pending + writing exact", pending, writing(EffectExact, nil), false, "manifest_mismatch_write_landed", 1},
		{"pending + writing conflict", pending, writing(EffectConflict, nil), false, "manifest_mismatch_write_landed", 1},
		{"pending + writing read error", pending, writing(EffectAbsent, readError), false, "manifest_mismatch_readback_failed", 1},
		{"pending + writing unknown inspection", pending, writing("", nil), false, "manifest_mismatch_readback_failed", 1},
		{"pending + writing with no readback bound", pending, writing(EffectAbsent, nil), true, "manifest_mismatch_readback_unavailable", 0},
		{"writing absent + writing exact", writing(EffectAbsent, nil), writing(EffectExact, nil), false, "manifest_mismatch_write_landed", 2},
		{"writing exact + writing absent", writing(EffectExact, nil), writing(EffectAbsent, nil), false, "manifest_mismatch_write_landed", 1},
		{"writing absent + writing read error", writing(EffectAbsent, nil), writing(EffectAbsent, readError), false, "manifest_mismatch_readback_failed", 2},
		{"committed + writing absent", committed, writing(EffectAbsent, nil), false, "manifest_mismatch_partially_committed", 0},
		{"committed + pending", committed, pending, false, "manifest_mismatch_partially_committed", 0},
		{"blocked + committed", blocked, committed, false, "manifest_mismatch_unreplayable", 0},
		{"blocked + writing exact", blocked, writing(EffectExact, nil), false, "manifest_mismatch_unreplayable", 0},
	}
}

func TestSupersededSnapshotDiscardDecisionTable(t *testing.T) {
	for _, cell := range supersededDiscardCells() {
		t.Run(cell.name, func(t *testing.T) {
			log := captureSlog(t)
			now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
			claim, session := preparedWorkItemsSession(t, now, "github", "prs")
			ledger := seedSupersededPullRequestSnapshot(t, claim, now, map[string]supersededEffectSeed{
				supersededFirstDestination: cell.first, supersededSecondDestination: cell.second,
			})
			readback := &tableReadback{results: map[string]discardReadbackResult{
				supersededFirstDestination: cell.first.readback, supersededSecondDestination: cell.second.readback,
			}}
			handler := &staticCompleteRouteHandler{batch: currentPullRequestSocialBatch(t, claim)}
			executor := preparedGitHubExecutor(now.Add(time.Hour), handler, ledger, &memoryEffectSink{})
			if !cell.noReadback {
				executor.Committer.Readback = readback
			}
			descriptor, _ := Descriptor("github", "prs")
			_, err := executor.Execute(context.Background(), session, descriptor)
			if !regexp.MustCompile(`reason=` + regexp.QuoteMeta(cell.wantReason) + `(\s|$)`).MatchString(log.String()) {
				t.Fatalf("log lacks reason=%s: %s", cell.wantReason, log.String())
			}
			if readback.calls != cell.wantReadbacks {
				t.Fatalf("readbacks=%d want %d", readback.calls, cell.wantReadbacks)
			}
			if cell.wantReason == "manifest_mismatch" {
				if err != nil || ledger.resets != 1 || handler.normalizedAt.IsZero() ||
					!strings.Contains(log.String(), "recovery=snapshot_discarded") {
					t.Fatalf("discard err=%v resets=%d recollected=%v, want the ledger discarded and the route re-collected",
						err, ledger.resets, !handler.normalizedAt.IsZero())
				}
				return
			}
			if !errors.Is(err, ErrEffectRecoveryUnsafe) || ledger.resets != 0 || !handler.normalizedAt.IsZero() ||
				ledger.state.SchemaVersion != "v2" || len(ledger.preparedSnapshot) == 0 {
				t.Fatalf("refusal err=%v resets=%d recollected=%v ledger=%q snapshot_kept=%v, want the ledger and snapshot kept as evidence",
					err, ledger.resets, !handler.normalizedAt.IsZero(), ledger.state.SchemaVersion, len(ledger.preparedSnapshot) != 0)
			}
		})
	}
}

// declaredConstants parses every non-test source file of the package and
// returns each string value declared as typeName, in either form a Go
// declaration can take -- `x typeName = "v"` or `x = typeName("v")` -- so an
// axis of the decision table below is the set the code actually declares
// rather than a list kept by hand. A value computed at run time is not a
// declaration and is outside what this can see.
func declaredConstants(t *testing.T, typeName string) map[string]bool {
	t.Helper()
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]bool{}
	fileSet := token.NewFileSet()
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(fileSet, file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, declaration := range parsed.Decls {
			general, ok := declaration.(*ast.GenDecl)
			if !ok || (general.Tok != token.CONST && general.Tok != token.VAR) {
				continue
			}
			for _, spec := range general.Specs {
				valueSpec, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				typed := false
				if ident, ok := valueSpec.Type.(*ast.Ident); ok && ident.Name == typeName {
					typed = true
				}
				for _, expression := range valueSpec.Values {
					literal, _ := expression.(*ast.BasicLit)
					if call, ok := expression.(*ast.CallExpr); ok && len(call.Args) == 1 {
						if ident, ok := call.Fun.(*ast.Ident); ok && ident.Name == typeName {
							literal, _ = call.Args[0].(*ast.BasicLit)
							typed = true
						}
					}
					if !typed || literal == nil || literal.Kind != token.STRING {
						continue
					}
					value, err := strconv.Unquote(literal.Value)
					if err != nil {
						t.Fatal(err)
					}
					values[value] = true
				}
			}
		}
	}
	if len(values) == 0 {
		t.Fatalf("no %s values declared in the package", typeName)
	}
	return values
}

// TestDiscardDecisionTableEnumeratesEveryDeclaredState pins the table's axes
// to the code: every ledger effect status, every readback answer a sink can
// return, and the recovery-blocked policy must each appear in a cell, so a
// new state added to the package goes red here instead of silently taking
// some default branch of the discard.
func TestDiscardDecisionTableEnumeratesEveryDeclaredState(t *testing.T) {
	statuses := declaredConstants(t, "GenerationBlockStatus")
	inspections := declaredConstants(t, "EffectInspection")
	coveredStatuses := map[string]bool{}
	coveredInspections := map[string]bool{}
	blockedCovered, readErrorCovered, unknownAnswerCovered, noReadbackCovered := false, false, false, false
	for _, cell := range supersededDiscardCells() {
		for _, seed := range []supersededEffectSeed{cell.first, cell.second} {
			coveredStatuses[string(seed.status)] = true
			if seed.blocked {
				blockedCovered = true
			}
			if seed.status != GenerationBlockWriting {
				continue
			}
			switch {
			case seed.readback.err != nil:
				readErrorCovered = true
			case !inspections[string(seed.readback.inspection)]:
				unknownAnswerCovered = true
			default:
				coveredInspections[string(seed.readback.inspection)] = true
			}
		}
		if cell.noReadback {
			noReadbackCovered = true
		}
	}
	for status := range statuses {
		if !coveredStatuses[status] {
			t.Errorf("ledger effect status %q is in no cell of the discard decision table", status)
		}
	}
	for inspection := range inspections {
		if !coveredInspections[inspection] {
			t.Errorf("readback answer %q is in no cell of the discard decision table", inspection)
		}
	}
	if !blockedCovered || !readErrorCovered || !unknownAnswerCovered || !noReadbackCovered {
		t.Fatalf("uncovered axes: blocked=%v read error=%v answer outside the declared set=%v no readback bound=%v",
			blockedCovered, readErrorCovered, unknownAnswerCovered, noReadbackCovered)
	}
}
