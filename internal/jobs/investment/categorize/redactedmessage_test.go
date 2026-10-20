package categorize

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
)

func TestRedactedMessageUsesOnlyTheMatchedErrorsOwnText(t *testing.T) {
	cases := map[string]struct {
		err  error
		want string
	}{
		"wrapped deadline": {&url.Error{Op: "Post", URL: "https://llm.example/g?prompt=PROMPT_SENTINEL", Err: context.DeadlineExceeded}, "context deadline exceeded"},
		"wrapped canceled": {fmt.Errorf("prompt PROMPT_SENTINEL: %w", context.Canceled), "context canceled"},
		"wrapped status":   {fmt.Errorf("prompt PROMPT_SENTINEL: %w", &httpStatusError{statusCode: 503, body: "BODY_SENTINEL"}), "http 503"},
		"unknown wrapper":  {fmt.Errorf("prompt PROMPT_SENTINEL: %w", errors.New("x")), "error of type *fmt.wrapError (text withheld)"},
	}
	for name, tc := range cases {
		got := RedactedMessage(tc.err)
		if strings.Contains(got, "SENTINEL") || got != tc.want {
			t.Errorf("%s: %q, want %q", name, got, tc.want)
		}
	}
	typed := &llmError{kind: llmErrorRateLimit, message: "Transient LLM rate limit from provider.", provider: "openai", model: "m"}
	got := RedactedMessage(fmt.Errorf("prompt PROMPT_SENTINEL: %w", typed))
	if strings.Contains(got, "SENTINEL") || !strings.Contains(got, "Transient LLM rate limit") {
		t.Errorf("wrapped typed error: %q", got)
	}
}
