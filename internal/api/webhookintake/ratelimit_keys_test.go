package webhookintake

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// hitRecorder is a memory store that records every counted (limit, path).
type hitRecorder struct {
	*httpapi.MemoryCounters
	mu   sync.Mutex
	hits []httpapi.Hit
}

func newHitRecorder() *hitRecorder {
	now := time.Now()
	return &hitRecorder{MemoryCounters: httpapi.NewMemoryCounters(func() time.Time { return now })}
}

func (h *hitRecorder) Increment(ctx context.Context, hit httpapi.Hit) (int64, error) {
	h.mu.Lock()
	h.hits = append(h.hits, hit)
	h.mu.Unlock()
	return h.MemoryCounters.Increment(ctx, hit)
}

func (h *hitRecorder) exactPaths() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []string
	for _, hit := range h.hits {
		if hit.Limit.ID == pagerdutyLimit.ID {
			out = append(out, hit.Path)
		}
	}
	return out
}

func pdRequest(path, peer string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.URL.Path = path
	request.RemoteAddr = peer + ":4242"
	return request
}

func spellingsOf(id uuid.UUID) []string {
	canonical := id.String()
	return []string{
		canonical,
		strings.ToUpper(canonical),
		"urn:uuid:" + canonical,
		"{" + canonical + "}",
		strings.ReplaceAll(canonical, "-", ""),
		strings.ToUpper(strings.ReplaceAll(canonical, "-", "")),
		strings.ToUpper(canonical[:18]) + canonical[18:],
	}
}

func TestSpellingsOfOneBindingIdShareOneExactBucket(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	recorder := newHitRecorder()
	gate := &Gate{limiters: newRateLimiters(recorder, nil), logger: quietTestLogger()}
	spellings := spellingsOf(uuid.New())
	for i := 0; i < pagerdutyLimitPerMinute; i++ {
		path := routePrefix + "/pagerduty/" + spellings[i%len(spellings)]
		if refuse := gate.Admit(pdRequest(path, "192.0.2.1"), providerPagerDuty); refuse != nil {
			t.Fatalf("delivery %d (%s) throttled early", i+1, spellings[i%len(spellings)])
		}
	}
	for _, spelling := range spellings {
		if refuse := gate.Admit(pdRequest(routePrefix+"/pagerduty/"+spelling, "192.0.2.1"), providerPagerDuty); refuse == nil {
			t.Errorf("spelling %q past the per-binding budget was admitted", spelling)
		}
	}
}

func TestDistinctBindingsKeepDistinctExactBuckets(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	gate := &Gate{limiters: newRateLimiters(newHitRecorder(), nil), logger: quietTestLogger()}
	first, second := uuid.New(), uuid.New()
	for i := 0; i < pagerdutyLimitPerMinute; i++ {
		if gate.Admit(pdRequest(routePrefix+"/pagerduty/"+first.String(), "192.0.2.1"), providerPagerDuty) != nil {
			t.Fatalf("delivery %d throttled early", i+1)
		}
	}
	if gate.Admit(pdRequest(routePrefix+"/pagerduty/"+first.String(), "192.0.2.1"), providerPagerDuty) == nil {
		t.Fatal("the first binding was not throttled at its budget")
	}
	if gate.Admit(pdRequest(routePrefix+"/pagerduty/"+second.String(), "192.0.2.1"), providerPagerDuty) != nil {
		t.Fatal("another binding shares the first one's bucket")
	}
}

func TestLowercaseCanonicalIdsKeepTheirCounterKeys(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	recorder := newHitRecorder()
	gate := &Gate{limiters: newRateLimiters(recorder, nil), logger: quietTestLogger()}
	path := routePrefix + "/pagerduty/" + uuid.NewString()
	gate.Admit(pdRequest(path, "192.0.2.1"), providerPagerDuty)
	if got := recorder.exactPaths(); len(got) != 1 || got[0] != path {
		t.Fatalf("the exact bucket key is %v, want the request path %q (counter keys continue across a roll)", got, path)
	}
}

func junkIds(id uuid.UUID) []string {
	canonical := id.String()
	return []string{
		"", "x", "not-a-uuid", strings.Repeat("a", 4000), ".", "..", "%31" + canonical[2:], canonical + "/", canonical + "/x",
		"x/" + canonical, "/" + canonical, canonical + "\n", " " + canonical, canonical + "0", canonical[:35], "{" + canonical, canonical + "}",
		"urn:uuid:" + canonical + "/", "a/b/c/d",
	}
}

func TestEveryIdTheHelperDoesNotParseSharesOneBucketAndNoCallerTextIsAKey(t *testing.T) {
	t.Setenv("TRUSTED_PROXIES", "")
	recorder := newHitRecorder()
	gate := &Gate{limiters: newRateLimiters(recorder, nil), logger: quietTestLogger()}
	id := uuid.New()
	ids := junkIds(id)
	for _, junk := range ids {
		gate.Admit(pdRequest(routePrefix+"/pagerduty/"+junk, "192.0.2.1"), providerPagerDuty)
	}
	want := routePrefix + "/pagerduty/" + malformedBucket
	paths := recorder.exactPaths()
	if len(paths) != len(ids) {
		t.Fatalf("%d exact-bucket hits for %d requests", len(paths), len(ids))
	}
	for index, got := range paths {
		if got != want {
			t.Errorf("id %q was keyed %q, want the shared bucket %q", ids[index], got, want)
		}
	}
}

func TestAMalformedOrExtraSegmentPathNeverProducesAnotherIdsKey(t *testing.T) {
	id := uuid.New()
	canonicalKey := routePrefix + "/pagerduty/" + id.String()
	for _, junk := range junkIds(id) {
		if got := exactBucketPath(routePrefix + "/pagerduty/" + junk); got == canonicalKey {
			// The exact canonical spelling alone may produce its own key.
			if junk != id.String() {
				t.Errorf("%q produced the canonical key of a valid id", junk)
			}
		}
	}
	for _, spelling := range spellingsOf(id) {
		if got := exactBucketPath(routePrefix + "/pagerduty/" + spelling); got != canonicalKey {
			t.Errorf("spelling %q keyed %q, want %q", spelling, got, canonicalKey)
		}
	}
}

func TestProviderCountsEveryPathUnderThePagerDutyPrefix(t *testing.T) {
	for _, tail := range append(junkIds(uuid.New()), uuid.NewString()) {
		if provider, ok := Provider(http.MethodPost, routePrefix+"/pagerduty/"+tail); !ok || provider != providerPagerDuty {
			t.Errorf("tail %q: provider %q %v, want pagerduty", tail, provider, ok)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut} {
		if _, ok := Provider(method, routePrefix+"/pagerduty/"+uuid.NewString()); ok {
			t.Errorf("%s was counted as an intake delivery", method)
		}
	}
}

func quietTestLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
