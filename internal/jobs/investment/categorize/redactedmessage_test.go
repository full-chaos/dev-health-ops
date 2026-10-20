package categorize

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"
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

func TestRedactedMessageCutsTypedTextToTheRuneCap(t *testing.T) {
	body := strings.Repeat("é", 300)
	typed := &llmError{kind: llmErrorGeneric, message: body}
	got := RedactedMessage(typed)
	if n := utf8.RuneCountInString(got); n != redactedMessageLimit {
		t.Fatalf("rune count = %d, want %d", n, redactedMessageLimit)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("cut split a rune: %q", got)
	}
	if got != strings.Repeat("é", redactedMessageLimit) {
		t.Fatalf("cut text is not the first %d runes of the message", redactedMessageLimit)
	}
	short := RedactedMessage(&llmError{kind: llmErrorGeneric, message: "short"})
	if short != "short" {
		t.Fatalf("a short message must pass whole: %q", short)
	}
}
