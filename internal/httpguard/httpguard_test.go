package httpguard

import (
	"net/http"
	"testing"
	"time"
)

func TestNoRedirectsKeepsTheClientAndRefusesTheRedirect(t *testing.T) {
	if NoRedirects(nil) != nil {
		t.Fatal("a nil client must stay nil")
	}
	original := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
	guarded := NoRedirects(original)
	if guarded == original || original.CheckRedirect(nil, nil) != nil {
		t.Fatal("the supplied client must be copied, not changed")
	}
	if err := guarded.CheckRedirect(nil, nil); err != http.ErrUseLastResponse {
		t.Fatalf("CheckRedirect = %v, want http.ErrUseLastResponse", err)
	}
}

type decorator struct{ http.Client }

func TestNoRedirectsDoerWrapsAClientAndLeavesOtherDoersAlone(t *testing.T) {
	type doer interface {
		Do(*http.Request) (*http.Response, error)
	}
	var supplied doer = &http.Client{}
	guarded := NoRedirectsDoer(supplied)
	if client, ok := guarded.(*http.Client); !ok || client == supplied || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatalf("an *http.Client doer must come back as a no-redirect copy, got %T", guarded)
	}
	var other doer = &decorator{}
	if NoRedirectsDoer(other) != other {
		t.Fatal("a doer that is not an *http.Client must be returned as it is")
	}
}

func TestNewClientRefusesRedirectsAndKeepsTheTimeout(t *testing.T) {
	client := NewClient(7 * time.Second)
	if client.Timeout != 7*time.Second {
		t.Fatalf("timeout %v", client.Timeout)
	}
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("NewClient follows redirects")
	}
}
