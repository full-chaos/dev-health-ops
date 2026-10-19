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

// TestExplainInvestmentMixWindowErrReadsNoCache: a request whose window the report cannot hold has no mix to key
// the cache on, so it must not read a row stored under its raw filters: such a row never passed the parser version.
func TestExplainInvestmentMixWindowErrReadsNoCache(t *testing.T) {
	windowErr := errors.New("window overflow")
	client := &capturingClient{}
	reader, err := NewReader(client)
	if err != nil {
		t.Fatal(err)
	}
	opts := ExplainInvestmentMixOptions{OrgID: "org-1", LLMProvider: "auto", WindowErr: windowErr}
	available := func(context.Context, string, string) bool { return true }
	if _, err := reader.ExplainInvestmentMix(context.Background(), nil, available, nil, opts); !errors.Is(err, windowErr) {
		t.Fatalf("err = %v, want the window error", err)
	}
	if client.calls != 0 {
		t.Fatalf("the window error path ran %d queries, want none (no cache read)", client.calls)
	}
}
