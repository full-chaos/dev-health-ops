package investment

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// A clef answer that reports input_tokens and no output_tokens (the real clef
// account answers this way): the attempt row and the usage figure of the call
// carry a request id and a non-zero output count.
func TestAClefCallThatReportsNoOutputTokensStillRecordsRequestIDAndOutputTokens(t *testing.T) {
	inner := clefAnswers("clef")
	stub := newClefBYO(func(call int, body []byte) (int, []byte) {
		status, payload := inner(call, body)
		var answer map[string]any
		if err := json.Unmarshal(payload, &answer); err != nil {
			t.Fatal(err)
		}
		answer["usage"] = map[string]any{"input_tokens": 17132}
		out, err := json.Marshal(answer)
		if err != nil {
			t.Fatal(err)
		}
		return status, out
	})
	served, err := stub.build().Build(context.Background(), "org-1")
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = served.Close() }()
	m := &Materializer{}
	m.SetServed(served)
	outcome, err := m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
	if err != nil {
		t.Fatalf("categorize: %v", err)
	}
	rows := servedAttempts(t, served)
	if len(rows) != 1 || rows[0].HTTPStatus != http.StatusOK {
		t.Fatalf("attempt rows = %+v", rows)
	}
	if rows[0].RequestID == "" {
		t.Error("the attempt row has no request_id")
	}
	if rows[0].OutputTokens <= 0 {
		t.Errorf("the attempt row output_tokens = %d, want > 0", rows[0].OutputTokens)
	}
	if outcome.InputTokens != 17132 || outcome.OutputTokens <= 0 {
		t.Errorf("usage figure input %d output %d, want 17132 and > 0", outcome.InputTokens, outcome.OutputTokens)
	}
}
