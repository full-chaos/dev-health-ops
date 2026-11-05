package llmbudget

import (
	"reflect"
	"testing"
)

func TestUnsupportedRequestParams(t *testing.T) {
	want := []string{"temperature"}
	if got := UnsupportedRequestParams(" OpenAI ", "GPT-6-luna"); !reflect.DeepEqual(got, want) {
		t.Fatalf("gpt-6-luna = %v, want %v", got, want)
	}
	for _, c := range [][2]string{{"openai", "gpt-5-mini"}, {"anthropic", "gpt-6-luna"}, {"openai", ""}} {
		if got := UnsupportedRequestParams(c[0], c[1]); len(got) != 0 {
			t.Errorf("%v = %v, want none", c, got)
		}
	}
}

func TestOnlyReasoningEffort(t *testing.T) {
	if got := OnlyReasoningEffort(" OpenAI ", "GPT-6-luna"); got != "none" {
		t.Fatalf("gpt-6-luna = %q, want none", got)
	}
	for _, c := range [][2]string{{"openai", "gpt-5-mini"}, {"anthropic", "gpt-6-luna"}, {"openai", ""}} {
		if got := OnlyReasoningEffort(c[0], c[1]); got != "" {
			t.Errorf("%v = %q, want none known", c, got)
		}
	}
}
