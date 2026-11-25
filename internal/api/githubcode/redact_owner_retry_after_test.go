package githubcode

import (
	"strings"
	"testing"
	"time"
)

func TestRedactOwnerKeepsTheRefusedWaitAndRedactsTheText(t *testing.T) {
	refused := &Error{Class: "RateLimitException", Message: "github rate limit exceeded on org:acme", RetryAfter: 90 * time.Second}
	redacted, ok := RedactOwner(refused, "acme").(*Error)
	if !ok {
		t.Fatal("RedactOwner did not return an *Error")
	}
	if redacted.RetryAfter != 90*time.Second {
		t.Fatalf("RetryAfter %s, WANT the refused wait 1m30s", redacted.RetryAfter)
	}
	if redacted.Class != "RateLimitException" || redacted.Message != "github rate limit exceeded on org:{login}" {
		t.Fatalf("got %q %q", redacted.Class, redacted.Message)
	}
	if strings.Contains(redacted.Message, "acme") {
		t.Fatalf("the owner login is still in the text: %q", redacted.Message)
	}
	if refused.Message != "github rate limit exceeded on org:acme" {
		t.Fatalf("the input error was changed: %q", refused.Message)
	}
}
