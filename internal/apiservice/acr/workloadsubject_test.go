package acr

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"
)

type fakeSubjectSource struct {
	mu    sync.Mutex
	value string
	err   error
	calls int
}

func (f *fakeSubjectSource) Subject(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.value, f.err
}

func (f *fakeSubjectSource) set(value string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.value, f.err = value, err
}

// All four internal acr routes answer the registry's workload: the acr token is accepted, a token of another
// workload is the 403, an unreadable registry is the 503 and a registry with no single acr-api is the 401, and none
// of it needs a restart.
func TestInternalRoutesFollowTheRegistrySubject(t *testing.T) {
	signer := newWLSigner(t)
	token := signer.sign(wlHeader(), wlClaims())
	for _, route := range wlRoutesUnderTest {
		t.Run(route.key, func(t *testing.T) {
			source := &fakeSubjectSource{value: wlSubject}
			auth := wlAuth(t, signer, "dev-health-api")
			auth.Source = source
			h := newWLHarness(t, auth)

			if rec := do(h.handler, route.path, "Bearer "+token); rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden || rec.Code == http.StatusServiceUnavailable {
				t.Fatalf("the registry's workload: status %d, want it accepted", rec.Code)
			}
			source.set(wlOther, nil)
			if rec := do(h.handler, route.path, "Bearer "+token); rec.Code != http.StatusForbidden {
				t.Fatalf("a token of another principal than the registry's: status %d, want 403", rec.Code)
			}
			source.set("", errors.New("registry unreadable"))
			if rec := do(h.handler, route.path, "Bearer "+token); rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("an unreadable registry: status %d, want 503", rec.Code)
			}
			source.set("", ErrSubjectNotConfigured)
			if rec := do(h.handler, route.path, "Bearer "+token); rec.Code != http.StatusUnauthorized {
				t.Fatalf("no single acr-api workload: status %d, want 401", rec.Code)
			}
			source.set(wlSubject, nil)
			if rec := do(h.handler, route.path, "Bearer "+token); rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden || rec.Code == http.StatusServiceUnavailable {
				t.Fatalf("the registry recovered: status %d, want it accepted", rec.Code)
			}
		})
	}
}

// A token that does not verify never reads the registry: a foreign token costs no database read.
func TestAForeignTokenDoesNotReadTheRegistry(t *testing.T) {
	signer := newWLSigner(t)
	source := &fakeSubjectSource{value: wlSubject}
	auth := wlAuth(t, signer, "dev-health-api")
	auth.Source = source
	h := newWLHarness(t, auth)
	foreign := newWLSigner(t).sign(wlHeader(), wlClaims())
	for _, route := range wlRoutesUnderTest {
		if rec := do(h.handler, route.path, "Bearer "+foreign); rec.Code != http.StatusUnauthorized {
			t.Fatalf("%s: a foreign token: status %d, want 401", route.key, rec.Code)
		}
	}
	if source.calls != 0 {
		t.Fatalf("the registry was read %d times for tokens that do not verify", source.calls)
	}
}

func TestCachedSubjectKeepsAnAnswerForTheTTLAndFollowsARotation(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	reads, current := 0, wlSubject
	cache := &CachedSubject{TTL: 30 * time.Second, Now: func() time.Time { return now }, Read: func(context.Context) (string, error) {
		reads++
		return current, nil
	}}
	for i := 0; i < 3; i++ {
		if got, err := cache.Subject(t.Context()); err != nil || got != wlSubject {
			t.Fatalf("read %d: %q %v", i, got, err)
		}
	}
	if reads != 1 {
		t.Fatalf("%d registry reads inside the TTL, want 1", reads)
	}
	current = wlOther
	now = now.Add(29 * time.Second)
	if got, _ := cache.Subject(t.Context()); got != wlSubject {
		t.Fatalf("inside the TTL the answer changed to %q", got)
	}
	now = now.Add(2 * time.Second)
	if got, err := cache.Subject(t.Context()); err != nil || got != wlOther {
		t.Fatalf("after the TTL the rotation is not followed: %q %v", got, err)
	}
}

func TestCachedSubjectFailsClosedWhenTheRegistryStaysUnreadable(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	failing := false
	cache := &CachedSubject{TTL: 30 * time.Second, Now: func() time.Time { return now }, Read: func(context.Context) (string, error) {
		if failing {
			return "", errors.New("down")
		}
		return wlSubject, nil
	}}
	if _, err := cache.Subject(t.Context()); err != nil {
		t.Fatal(err)
	}
	failing = true
	now = now.Add(45 * time.Second)
	if got, err := cache.Subject(t.Context()); err != nil || got != wlSubject {
		t.Fatalf("one failed refresh inside twice the TTL must serve the last good answer: %q %v", got, err)
	}
	now = now.Add(20 * time.Second)
	if got, err := cache.Subject(t.Context()); err == nil || got != "" {
		t.Fatalf("past twice the TTL an unreadable registry must be an error, got %q %v", got, err)
	}
	failing = false
	if got, err := cache.Subject(t.Context()); err != nil || got != wlSubject {
		t.Fatalf("recovery: %q %v", got, err)
	}
}

func TestCachedSubjectCachesNotConfiguredAndNeverPassesAnError(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	reads := 0
	cache := &CachedSubject{TTL: 30 * time.Second, Now: func() time.Time { return now }, Read: func(context.Context) (string, error) {
		reads++
		return "", ErrSubjectNotConfigured
	}}
	for i := 0; i < 2; i++ {
		if got, err := cache.Subject(t.Context()); !errors.Is(err, ErrSubjectNotConfigured) || got != "" {
			t.Fatalf("%q %v", got, err)
		}
	}
	if reads != 1 {
		t.Fatalf("%d reads, want 1", reads)
	}
	first := &CachedSubject{TTL: time.Second, Now: func() time.Time { return now }, Read: func(context.Context) (string, error) {
		return wlSubject, errors.New("down")
	}}
	if got, err := first.Subject(t.Context()); err == nil || got != "" {
		t.Fatalf("a first read that fails must be an error with no value, got %q %v", got, err)
	}
}
