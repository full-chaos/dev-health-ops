package investment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/chwrite"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

// latchStore is a budget store with the admission rule of the Postgres store: a
// reservation settled with no cost (usage_unavailable) makes every later Admit
// of the org refuse.
type latchStore struct {
	mu      sync.Mutex
	admits  []llmbudget.AdmitRequest
	settles []llmbudget.SettleRequest
	latched bool
	refused int
}

func (store *latchStore) Admit(_ context.Context, request llmbudget.AdmitRequest) (llmbudget.Admission, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.latched {
		store.refused++
		return llmbudget.Admission{}, &llmbudget.AccountingError{Message: "BYO LLM usage is unavailable for the configured budget."}
	}
	store.admits = append(store.admits, request)
	return llmbudget.Admission{ID: fmt.Sprintf("r%d", len(store.admits)), BudgetConfigured: true}, nil
}

func (store *latchStore) Settle(_ context.Context, request llmbudget.SettleRequest) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.settles = append(store.settles, request)
	if request.Cost == nil && request.Outcome != llmbudget.OutcomeVoided {
		store.latched = true
	}
	return nil
}

// clefReplyWith is clefAnswers with its fields changed: edit gets the decoded
// answer and returns the body to send.
func clefReplyWith(edit func(map[string]any) map[string]any) func(int, []byte) (int, []byte) {
	return func(_ int, body []byte) (int, []byte) {
		var answer map[string]any
		if err := json.Unmarshal(jevAnswerBody(body, jevReply{supported: map[string]int{"quality.bugfix": 3}, inputTokens: 2644, model: "clef"}), &answer); err != nil {
			panic(err)
		}
		encoded, err := json.Marshal(edit(answer))
		if err != nil {
			panic(err)
		}
		return http.StatusOK, encoded
	}
}

func dropFields(names ...string) func(map[string]any) map[string]any {
	return func(answer map[string]any) map[string]any {
		for _, name := range names {
			delete(answer, name)
		}
		return answer
	}
}

func wrapInResult(answer map[string]any) map[string]any {
	return map[string]any{"result": answer, "success": true, "errors": []any{}, "messages": []any{}}
}

// servedClefRun serves two units through the production clef path over a store
// and returns the attempt rows of the run and the error of each unit.
func servedClefRun(t *testing.T, reply func(int, []byte) (int, []byte), store llmbudget.Store) ([]chwrite.AttemptRecord, []error) {
	t.Helper()
	stub := newClefBYO(reply)
	stub.store = store
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = served.Close() }()
	m := &Materializer{logger: testLogger()}
	m.SetServed(served)
	var errs []error
	for index, unit := range []string{"u1", "u2"} {
		_, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(index, unit, shadowTestBundle(t, unit, 6)))
		errs = append(errs, err)
	}
	conn := &attemptCaptureConn{}
	writer, err := chwrite.NewWriter(conn)
	if err != nil {
		t.Fatal(err)
	}
	served.finish(context.Background(), writer, shadowTestConfig())
	return conn.rows, errs
}

// One call whose answer has no usage must not block the next one: it settles at
// the reserved amount as a success, never as usage_unavailable.
func TestAClefAnswerWithNoUsageSettlesAtTheReservedAmountAndDoesNotLatch(t *testing.T) {
	store := &latchStore{}
	_, errs := servedClefRun(t, clefReplyWith(dropFields("usage")), store)
	for index, err := range errs {
		if err != nil {
			t.Fatalf("unit %d: %v", index+1, err)
		}
	}
	if store.refused != 0 || len(store.admits) != 2 || len(store.settles) != 2 {
		t.Fatalf("admits %d refused %d settles %d, want 2/0/2", len(store.admits), store.refused, len(store.settles))
	}
	for index, settle := range store.settles {
		if settle.Outcome != llmbudget.OutcomeSucceeded || settle.Cost == nil || store.admits[index].MaxCost == nil || *settle.Cost != *store.admits[index].MaxCost {
			t.Fatalf("settle %d = %+v, want a success at the reserved %v", index+1, settle, store.admits[index].MaxCost)
		}
	}
}

// The real answer of run 1 carried no model and no usage: it is accepted, the
// model of the row is the requested one and the row says both facts.
func TestAClefAnswerWithNoModelAndNoUsageIsAcceptedAndTheRowSaysSo(t *testing.T) {
	rows, errs := servedClefRun(t, clefReplyWith(dropFields("usage", "model")), &latchStore{})
	if errs[0] != nil || errs[1] != nil || len(rows) != 2 {
		t.Fatalf("errs %v rows %d, want two answered units", errs, len(rows))
	}
	for _, row := range rows {
		if row.HTTPStatus != 200 || row.State != "ok" || row.ModelReturned != "clef" || !row.ModelFromRequest ||
			row.UsageReason != "unavailable" || row.InputTokens != 0 || row.OutputTokens != 0 {
			t.Fatalf("row = %+v", row)
		}
	}
}

// A model the answer names is the row's model; no usage still says so.
func TestAClefAnswerWithNoUsageKeepsItsModelAndNamesTheReason(t *testing.T) {
	rows, errs := servedClefRun(t, clefReplyWith(dropFields("usage")), &latchStore{})
	if errs[0] != nil || len(rows) != 2 || rows[0].ModelFromRequest || rows[0].ModelReturned != "clef" || rows[0].UsageReason != "unavailable" {
		t.Fatalf("errs %v rows %+v", errs, rows)
	}
}

// With usage the reason is empty.
func TestAClefAnswerWithUsageHasNoUsageReason(t *testing.T) {
	rows, errs := servedClefRun(t, clefAnswers("clef"), &latchStore{})
	if errs[0] != nil || len(rows) != 2 || rows[0].UsageReason != "" || rows[0].ModelFromRequest || rows[0].InputTokens != 2644 {
		t.Fatalf("errs %v rows %+v", errs, rows)
	}
}

// The REST envelope is served like the plain shape, usage included.
func TestAWrappedClefAnswerIsServed(t *testing.T) {
	rows, errs := servedClefRun(t, clefReplyWith(wrapInResult), &latchStore{})
	if errs[0] != nil || errs[1] != nil || len(rows) != 2 {
		t.Fatalf("errs %v rows %d", errs, len(rows))
	}
	if rows[0].ModelReturned != "clef" || rows[0].ModelFromRequest || rows[0].InputTokens != 2644 || rows[0].UsageReason != "" {
		t.Fatalf("row = %+v", rows[0])
	}
}

// A model that is named and is another model is refused, with or without usage.
func TestAClefAnswerNamingAnotherModelIsRefusedWhenUsageIsMissing(t *testing.T) {
	reply := clefReplyWith(func(answer map[string]any) map[string]any {
		delete(answer, "usage")
		answer["model"] = "clef-flash"
		return answer
	})
	_, errs := servedClefRun(t, reply, &latchStore{})
	if errs[0] == nil {
		t.Fatal("an answer from another model was accepted")
	}
}

// A refusal of the budget (the store could not admit the call) is its own
// outcome: no call was sent, and it is not a transport failure.
func TestABudgetAccountingRefusalOfAClefCallIsItsOwnOutcome(t *testing.T) {
	store := &latchStore{latched: true}
	stub := newClefBYO(clefAnswers("clef"))
	stub.store = store
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = served.Close() }()
	_, err = served.categorize(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
	var failure *servedFailure
	if !errors.As(err, &failure) || failure.class != "budget_accounting" || failure.deterministic || stub.network.count() != 0 {
		t.Fatalf("err = %v, requests %d, want the budget_accounting class and no request", err, stub.network.count())
	}
	if served.outcomes["budget_accounting"] != 1 || served.outcomes["transport_other"] != 0 {
		t.Fatalf("outcomes = %v", served.outcomes)
	}
}

// settleFailStore admits like latchStore and fails every Settle: the call was
// sent and answered, then its accounting could not be written.
type settleFailStore struct{ latchStore }

func (store *settleFailStore) Settle(context.Context, llmbudget.SettleRequest) error {
	return errors.New("settle is down")
}

// A Settle failure after a good 200 means the call WAS sent and paid: one
// request left, the attempt row carries the usage the provider returned, and the
// outcome is budget_settle_failed, not budget_accounting (which is a refusal
// before any send).
func TestASettleFailureAfterAGoodClefAnswerKeepsItsAttemptAndHasItsOwnOutcome(t *testing.T) {
	for name, tc := range map[string]struct {
		reply      func(int, []byte) (int, []byte)
		wantInput  uint32
		wantReason string
	}{
		"with usage": {clefAnswers("clef"), 2644, ""},
		"no usage":   {clefReplyWith(dropFields("usage")), 0, "unavailable"},
	} {
		t.Run(name, func(t *testing.T) {
			stub := newClefBYO(tc.reply)
			stub.store = &settleFailStore{}
			served, err := stub.build().Build(context.Background(), "org-1")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = served.Close() }()
			_, err = served.categorize(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
			var failure *servedFailure
			if !errors.As(err, &failure) || failure.class != "budget_settle_failed" || failure.deterministic || failure.calls != 1 {
				t.Fatalf("err = %v (%+v), want the budget_settle_failed class, not deterministic, one call", err, failure)
			}
			conn := &attemptCaptureConn{}
			writer, werr := chwrite.NewWriter(conn)
			if werr != nil {
				t.Fatal(werr)
			}
			served.finish(context.Background(), writer, shadowTestConfig())
			if stub.network.count() != 1 || len(conn.rows) != 1 {
				t.Fatalf("requests_sent=%d attempt_rows=%d, want 1 and 1", stub.network.count(), len(conn.rows))
			}
			row := conn.rows[0]
			if row.HTTPStatus != 200 || row.InputTokens != tc.wantInput || row.UsageReason != tc.wantReason {
				t.Fatalf("row = %+v", row)
			}
			if served.outcomes["budget_settle_failed"] != 1 || served.outcomes["budget_accounting"] != 0 || served.outcomes["transport_other"] != 0 {
				t.Fatalf("outcomes = %v", served.outcomes)
			}
		})
	}
}
