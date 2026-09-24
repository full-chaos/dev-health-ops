package investmentexplain

import (
	"context"
	"errors"
	"testing"
)

// TestExplainInvestmentMixWindowErrAfterTheLLMCheck: Python meets a bad
// report window inside build_investment_response, after the LLM check and
// the cache read, so an unavailable LLM still answers llm_unavailable and an
// available one fails with the window's error before any read.
func TestExplainInvestmentMixWindowErrAfterTheLLMCheck(t *testing.T) {
	windowErr := errors.New("window overflow")
	opts := ExplainInvestmentMixOptions{OrgID: "org-1", LLMProvider: "auto", ForceRefresh: true, WindowErr: windowErr}
	var reader *Reader
	unavailable := func(context.Context, string, string) bool { return false }
	explanation, err := reader.ExplainInvestmentMix(context.Background(), nil, unavailable, nil, opts)
	if err != nil || explanation.Status == nil || *explanation.Status != "llm_unavailable" {
		t.Fatalf("LLM unavailable: got %+v, %v; want the llm_unavailable explanation", explanation, err)
	}
	available := func(context.Context, string, string) bool { return true }
	if _, err := reader.ExplainInvestmentMix(context.Background(), nil, available, nil, opts); !errors.Is(err, windowErr) {
		t.Fatalf("LLM available: err = %v, want the window error", err)
	}
}
