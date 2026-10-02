package gitlabcode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

// GitLab's next page is the integer in X-Next-Page, requested from the base URL: no header can send a request, or
// the token, to another host (measured on a stand-in server, CHAOS-7816).
func TestNextPageHeadersNeverLeaveTheBase(t *testing.T) {
	var mu sync.Mutex
	var otherHits []string
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		otherHits = append(otherHits, r.Header.Get("PRIVATE-TOKEN")+r.Header.Get("Authorization"))
		mu.Unlock()
		_, _ = w.Write([]byte(`[]`))
	}))
	defer other.Close()
	for name, headers := range map[string]map[string]string{
		"X-Next-Page is a URL":    {"X-Next-Page": other.URL + "/api/v4/projects?page=2"},
		"Link names another host": {"Link": "<" + other.URL + `/api/v4/projects?page=2>; rel="next"`, "X-Next-Page": "2"},
		"X-Next-Page host-ish":    {"X-Next-Page": "//" + other.Listener.Addr().String() + "/x"},
	} {
		t.Run(name, func(t *testing.T) {
			mu.Lock()
			otherHits = nil
			mu.Unlock()
			base := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for key, value := range headers {
					w.Header().Set(key, value)
				}
				_, _ = w.Write([]byte(`[{"id": 1, "name": "a"}]`))
			}))
			defer base.Close()
			client := Client{BaseURL: base.URL, Token: "SECRET-TOKEN"}
			_, _ = client.ListProjects(context.Background(), ListOptions{})
			mu.Lock()
			defer mu.Unlock()
			if len(otherHits) != 0 {
				t.Fatalf("the other host received %d request(s): %q", len(otherHits), otherHits)
			}
		})
	}
}
