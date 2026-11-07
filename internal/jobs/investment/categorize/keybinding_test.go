package categorize

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// The head form of the q6 client cell of CHAOS-13093 (red on main 46530638,
// lane-scratch/cag-13093/logs/q6-main-13093.log sha256 3461dce9...). After a
// provider switch with no new key the org reader serves an empty key
// (llmorgsettings TestQ6HeadAProviderSwitchWithNoNewKeyServesNoKey), so the
// anthropic client is built from "" and no request reaches the vendor.
func TestQ6HeadTheAnthropicClientSendsNoRequestWithoutABoundKey(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	const servedKey = ""
	provider, err := newProviderFromCredentialsWithTransport(ProviderKindAnthropic, servedKey, server.URL, "claude-haiku-5-5", http.DefaultTransport)
	if err == nil {
		_, err = provider.Complete(context.Background(), CategorizationRequest("p"))
	}
	t.Logf("build_or_call_err=%v requests_at_vendor=%d", err != nil, requests.Load())
	if err == nil || requests.Load() != 0 {
		t.Errorf("an anthropic client with no bound key reached the vendor")
	}
}

// The head form of the Q3 client cell (red on main 2ed1bd5d, lane-scratch/
// cag-13096/logs/q3-main-13096.log): after a base_url change with no new key the
// reader serves no key (llmorgsettings TestQ3HeadSameProviderNewBaseURLNoKeyServesNoKey),
// so the openai client built for the new host sends nothing.
func TestQ3HeadTheOpenAIClientSendsNoRequestWithoutABoundKey(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()
	const servedKey = ""
	provider, err := newProviderFromCredentialsWithTransport(ProviderKindOpenAI, servedKey, server.URL, "gpt-5", http.DefaultTransport)
	if err == nil {
		_, err = provider.Complete(context.Background(), CategorizationRequest("p"))
	}
	t.Logf("build_or_call_err=%v requests_at_new_host=%d", err != nil, requests.Load())
	if err == nil || requests.Load() != 0 {
		t.Errorf("an openai client with no bound key reached the new host")
	}
}
