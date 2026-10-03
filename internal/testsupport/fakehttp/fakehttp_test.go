package fakehttp

import (
	"net/http"
	"testing"
)

type leaf struct{}

func (leaf) Do(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: 302, Header: http.Header{"Location": {"https://elsewhere.example/x"}}, Body: http.NoBody}, nil
}

// A fake's 3xx is answered to the caller, not followed (CheckRedirect refuses), as the bare fake did; an *http.Client stays as it is.
func TestClientRefusesRedirectsAndKeepsAClientAsItIs(t *testing.T) {
	client := Client(leaf{}).(*http.Client)
	if client.CheckRedirect == nil || client.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
		t.Fatal("the adapter client follows redirects")
	}
	response, err := client.Get("https://base.example/x")
	if err != nil || response.StatusCode != 302 {
		t.Fatalf("want the 302 itself, got %v %v", response, err)
	}
	own := &http.Client{}
	if Client(own) != Doer(own) {
		t.Fatal("an *http.Client must stay as it is")
	}
	if Client(nil) != nil {
		t.Fatal("nil stays nil")
	}
}
