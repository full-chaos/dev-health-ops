package useragent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func bound(t *testing.T, values ...string) *string {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", nil)
	for _, value := range values {
		r.Header.Add("User-Agent", value)
	}
	var got *string
	Middleware(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { got = Text(r.Context()) })).
		ServeHTTP(httptest.NewRecorder(), r)
	return got
}

func TestTheFirstUserAgentValueIsBoundDecodedAsLatin1(t *testing.T) {
	if got := bound(t, "Mozilla/5.0 (X11)", "second/1.0"); got == nil || *got != "Mozilla/5.0 (X11)" {
		t.Fatalf("Text = %v, want the first value", got)
	}
	// go-api's userAgent decodes each byte as one latin-1 code point.
	if got := bound(t, "agent \xe9\xff"); got == nil || *got != "agent éÿ" {
		t.Fatalf("Text = %q, want each byte as its latin-1 code point", deref(got))
	}
	long := strings.Repeat("a", 64<<10)
	if got := bound(t, long); got == nil || *got != long {
		t.Fatal("a long user agent is cut: go-api keeps it whole")
	}
}

func TestNoUserAgentIsNil(t *testing.T) {
	if got := bound(t); got != nil {
		t.Fatalf("Text = %q without a User-Agent, want nil", *got)
	}
	if Text(context.Background()) != nil {
		t.Fatal("Text of an empty context is not nil")
	}
}

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}
