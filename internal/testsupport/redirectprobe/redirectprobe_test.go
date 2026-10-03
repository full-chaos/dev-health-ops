package redirectprobe

import (
	"net/http"
	"testing"
)

type recorder struct {
	testing.TB
	failed bool
}

func (r *recorder) Helper()               {}
func (r *recorder) Fatal(args ...any)     { r.failed = true }
func (r *recorder) Fatalf(string, ...any) { r.failed = true }
func (r *recorder) Cleanup(func())        {}

func TestAssertFailsWhenTheBaseWasNeverReached(t *testing.T) {
	probe := New(t)
	r := &recorder{TB: t}
	probe.Assert(r)
	if !r.failed {
		t.Fatal("a probe whose base saw no request must fail")
	}
}

func TestAssertFailsWhenTheRedirectWasFollowedAndPassesWhenItWasNot(t *testing.T) {
	followed := New(t)
	if response, err := followed.Client().Get(followed.Base.URL + "/x"); err == nil {
		response.Body.Close()
	}
	r := &recorder{TB: t}
	followed.Assert(r)
	if !r.failed {
		t.Fatal("a followed redirect must fail")
	}
	refused := New(t)
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if response, err := client.Get(refused.Base.URL + "/x"); err == nil {
		response.Body.Close()
	}
	r = &recorder{TB: t}
	refused.Assert(r)
	if r.failed {
		t.Fatal("a refused redirect with the base reached must pass")
	}
}
