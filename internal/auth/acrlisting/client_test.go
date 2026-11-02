package acrlisting

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func list(t *testing.T, c *Client) ([]Row, error) {
	t.Helper()
	return c.List(context.Background())
}

func wantKind(t *testing.T, err error, kind Kind) *Error {
	t.Helper()
	var e *Error
	if !errors.As(err, &e) || e.Kind != kind {
		t.Fatalf("err = %v, want kind %q", err, kind)
	}
	return e
}

func TestWalkReturnsEveryRowInOrderAcrossPages(t *testing.T) {
	f := newFake(t, row("c", 1, "active"), row("a", 3, "revoked"), row("e", 2, "expired"), row("b", 1, "active"), row("d", 7, "active"))
	got, err := list(t, newClient(t, f, func(c *Config) { c.PageLimit = 2 }))
	if err != nil {
		t.Fatal(err)
	}
	want := []Row{row("a", 3, "revoked"), row("b", 1, "active"), row("c", 1, "active"), row("d", 7, "active"), row("e", 2, "expired")}
	if !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	calls := f.snapshotCalls()
	if len(calls) != 3 {
		t.Fatalf("calls = %v, want 3 pages", calls)
	}
	for i, q := range []string{"limit=2", "after=b&limit=2", "after=d&limit=2"} {
		if calls[i].query != q {
			t.Fatalf("page %d query = %q, want %q", i, calls[i].query, q)
		}
	}
}

func TestEmptyOrganizationIsNoRowsAndNoError(t *testing.T) {
	f := newFake(t)
	got, err := list(t, newClient(t, f, nil))
	if err != nil || len(got) != 0 {
		t.Fatalf("rows = %v, err = %v", got, err)
	}
}

func TestDefaultPageLimitIsTheContractMaximum(t *testing.T) {
	f := newFake(t, row("a", 1, "active"))
	if _, err := list(t, newClient(t, f, nil)); err != nil {
		t.Fatal(err)
	}
	if q := f.snapshotCalls()[0].query; q != "limit=500" {
		t.Fatalf("query = %q, want limit=500", q)
	}
}

func TestOnlyTheExchangePostsAndTheListingGets(t *testing.T) {
	f := newFake(t, row("a", 1, "active"), row("b", 1, "active"))
	if _, err := list(t, newClient(t, f, func(c *Config) { c.PageLimit = 1 })); err != nil {
		t.Fatal(err)
	}
	want := []string{"POST " + exchangePath, "GET " + listPath, "GET " + listPath}
	if !slices.Equal(f.methods, want) {
		t.Fatalf("requests = %v, want %v", f.methods, want)
	}
}

func TestExchangeFormIsTheRFC8693WorkloadGrant(t *testing.T) {
	f := newFake(t)
	if _, err := list(t, newClient(t, f, nil)); err != nil {
		t.Fatal(err)
	}
	form := f.exchanges[0]
	for k, v := range map[string]string{
		"grant_type":         "urn:ietf:params:oauth:grant-type:token-exchange",
		"subject_token":      subjectJWT,
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
	} {
		if form.Get(k) != v {
			t.Fatalf("form %s = %q, want %q", k, form.Get(k), v)
		}
	}
	if len(form) != 3 {
		t.Fatalf("form has %d fields, want 3: no scope and no requested type", len(form))
	}
	if f.exchangeCT[0] != "application/x-www-form-urlencoded" {
		t.Fatalf("content type = %q", f.exchangeCT[0])
	}
}

func TestListingCarriesTheAccessTokenAndNeverTheSubjectToken(t *testing.T) {
	f := newFake(t, row("a", 1, "active"))
	if _, err := list(t, newClient(t, f, nil)); err != nil {
		t.Fatal(err)
	}
	for _, c := range f.snapshotCalls() {
		if c.auth != "Bearer "+accessOne {
			t.Fatalf("authorization = %q", c.auth)
		}
		if strings.Contains(c.auth+c.query, subjectJWT) {
			t.Fatal("the subject token reached the listing")
		}
	}
}

func TestAccessTokenIsCachedUntilItsMargin(t *testing.T) {
	f := newFake(t)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	c := newClient(t, f, func(c *Config) { c.Now = func() time.Time { return now } })
	for i := 0; i < 3; i++ {
		if _, err := list(t, c); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.exchanges) != 1 {
		t.Fatalf("exchanges = %d, want 1", len(f.exchanges))
	}
	now = now.Add(3600*time.Second - 29*time.Second)
	_, _ = list(t, c)
	if len(f.exchanges) != 2 {
		t.Fatalf("exchanges = %d, want a refresh inside the 30 s margin", len(f.exchanges))
	}
}

func TestSubjectTokenFileIsReadAgainOnEachExchange(t *testing.T) {
	f := newFake(t)
	now := time.Date(2026, 11, 2, 12, 0, 0, 0, time.UTC)
	file := subjectFile(t, "subject-jwt-old")
	c := newClient(t, f, func(c *Config) { c.SubjectTokenFile = file; c.Now = func() time.Time { return now } })
	_, _ = list(t, c)
	if err := os.WriteFile(file, []byte("subject-jwt-new"), 0o600); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	_, _ = list(t, c)
	if got := f.exchanges[1].Get("subject_token"); got != "subject-jwt-new" {
		t.Fatalf("second exchange subject = %q", got)
	}
}

func TestShortLivedAccessTokenIsNeverCached(t *testing.T) {
	f := newFake(t)
	f.expiresIn = 10
	c := newClient(t, f, nil)
	_, _ = list(t, c)
	_, _ = list(t, c)
	if len(f.exchanges) != 2 {
		t.Fatalf("exchanges = %d, want 2", len(f.exchanges))
	}
}

func TestExchangeFailuresAreKindExchange(t *testing.T) {
	for name, set := range map[string]func(*fakeACR){
		"status 400":      func(f *fakeACR) { f.exchangeRC = 400 },
		"status 401":      func(f *fakeACR) { f.exchangeRC = 401 },
		"status 503":      func(f *fakeACR) { f.exchangeRC = 503 },
		"no access token": func(f *fakeACR) { f.exchangeRaw = `{"access_token":"","expires_in":60}` },
		"no expiry":       func(f *fakeACR) { f.exchangeRaw = `{"access_token":"x","expires_in":0}` },
		"not json":        func(f *fakeACR) { f.exchangeRaw = `nope` },
		"space in token":  func(f *fakeACR) { f.exchangeRaw = `{"access_token":"a b","expires_in":60}` },
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t, row("a", 1, "active"))
			set(f)
			_, err := list(t, newClient(t, f, nil))
			wantKind(t, err, KindExchange)
			if len(f.snapshotCalls()) != 0 {
				t.Fatal("the listing was called after a failed exchange")
			}
		})
	}
}

func TestSubjectTokenFileProblemsAreKindExchangeAndNeverReachTheNetwork(t *testing.T) {
	f := newFake(t)
	empty := subjectFile(t, " \n")
	big := subjectFile(t, strings.Repeat("x", 17<<10))
	for name, path := range map[string]string{"empty": empty, "oversized": big, "missing": empty + ".gone"} {
		t.Run(name, func(t *testing.T) {
			_, err := list(t, newClient(t, f, func(c *Config) { c.SubjectTokenFile = path }))
			wantKind(t, err, KindExchange)
		})
	}
	if len(f.exchanges) != 0 {
		t.Fatal("an exchange was sent")
	}
}

func TestListingStatusesMapToClosedKinds(t *testing.T) {
	for status, kind := range map[int]Kind{
		400: KindRejected, 401: KindUnauthorized, 403: KindForbidden, 404: KindRejected,
		429: KindRateLimited, 500: KindUnavailable, 502: KindUnavailable, 503: KindUnavailable,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			f := newFake(t, row("a", 1, "active"))
			f.listRC = status
			_, err := list(t, newClient(t, f, nil))
			e := wantKind(t, err, kind)
			if e.Status != status {
				t.Fatalf("status = %d, want %d", e.Status, status)
			}
			if n := len(f.snapshotCalls()); n != 1 {
				t.Fatalf("listing calls = %d, want 1: no retry in the client", n)
			}
		})
	}
}

func TestRetryAfterIsReadAsSecondsAndCappedAtFiveMinutes(t *testing.T) {
	for header, want := range map[string]time.Duration{"7": 7 * time.Second, "100000": 5 * time.Minute, "-3": 0, "soon": 0, "": 0} {
		f := newFake(t)
		f.listRC, f.retryAfter = 503, header
		_, err := list(t, newClient(t, f, nil))
		if got := wantKind(t, err, KindUnavailable).RetryAfter; got != want {
			t.Fatalf("Retry-After %q = %v, want %v", header, got, want)
		}
	}
	f := newFake(t)
	f.listRC, f.retryAfter = 429, "30"
	_, err := list(t, newClient(t, f, nil))
	if got := wantKind(t, err, KindRateLimited).RetryAfter; got != 30*time.Second {
		t.Fatalf("429 Retry-After = %v", got)
	}
}

func TestWrongShapesAreKindSchemaNeverNoRows(t *testing.T) {
	ok := func(items, next string) string {
		return `{"schema_version":"credential_lifecycle_list.v1","items":[` + items + `],"next_after":` + next + `}`
	}
	item := func(id, rev, st string) string {
		return `{"credential_id":"` + id + `","revision":` + rev + `,"status":"` + st + `"}`
	}
	for name, body := range map[string]string{
		"wrong version":        `{"schema_version":"credential_lifecycle_list.v2","items":[],"next_after":null}`,
		"no version":           `{"items":[],"next_after":null}`,
		"unknown status":       ok(item("a", "1", "frozen"), "null"),
		"revision zero":        ok(item("a", "0", "active"), "null"),
		"revision text":        ok(`{"credential_id":"a","revision":"1","status":"active"}`, "null"),
		"revision decimal":     ok(`{"credential_id":"a","revision":1.5,"status":"active"}`, "null"),
		"bad id":               ok(item("a b", "1", "active"), "null"),
		"empty id":             ok(item("", "1", "active"), "null"),
		"long id":              ok(item(strings.Repeat("a", 129), "1", "active"), "null"),
		"extra field":          ok(`{"credential_id":"a","revision":1,"status":"active","name":"x"}`, "null"),
		"extra top field":      `{"schema_version":"credential_lifecycle_list.v1","items":[],"next_after":null,"total":0}`,
		"missing next":         `{"schema_version":"credential_lifecycle_list.v1","items":[]}`,
		"items null":           `{"schema_version":"credential_lifecycle_list.v1","items":null,"next_after":null}`,
		"unsorted":             ok(item("b", "1", "active")+","+item("a", "1", "active"), "null"),
		"duplicate in page":    ok(item("a", "1", "active")+","+item("a", "1", "active"), "null"),
		"over the limit":       ok(item("a", "1", "active")+","+item("b", "1", "active")+","+item("c", "1", "active"), "null"),
		"bad next":             ok(item("a", "1", "active"), `"a b"`),
		"trailing data":        ok("", "null") + `{}`,
		"not json":             `<html>`,
		"next with no items":   ok("", `"a"`),
		"next below last item": ok(item("b", "1", "active")+","+item("c", "1", "active"), `"a"`),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.listRaw = func(string, int) (int, string) { return 200, body }
			_, err := list(t, newClient(t, f, func(c *Config) { c.PageLimit = 2 }))
			wantKind(t, err, KindSchema)
		})
	}
}

func TestPaginationMustProgressAndNeverRepeat(t *testing.T) {
	item := func(id string) string {
		return `{"credential_id":"` + id + `","revision":1,"status":"active"}`
	}
	cases := map[string]func(after string, limit int) (int, string){
		"cursor does not advance": func(after string, limit int) (int, string) {
			return 200, `{"schema_version":"credential_lifecycle_list.v1","items":[` + item("b") + `],"next_after":"b"}`
		},
		"item at or below the cursor": func(after string, limit int) (int, string) {
			if after == "" {
				return 200, `{"schema_version":"credential_lifecycle_list.v1","items":[` + item("b") + `],"next_after":"b"}`
			}
			return 200, `{"schema_version":"credential_lifecycle_list.v1","items":[` + item("a") + `],"next_after":null}`
		},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			f.listRaw = h
			_, err := list(t, newClient(t, f, func(c *Config) { c.PageLimit = 1 }))
			wantKind(t, err, KindSchema)
		})
	}
}

func TestWalkIsBoundedByMaxPages(t *testing.T) {
	f := newFake(t)
	n := 0
	f.listRaw = func(string, int) (int, string) {
		n++
		id := string(rune('a'+n/26)) + string(rune('a'+n%26))
		return 200, `{"schema_version":"credential_lifecycle_list.v1","items":[{"credential_id":"` + id + `","revision":1,"status":"active"}],"next_after":"` + id + `"}`
	}
	_, err := list(t, newClient(t, f, func(c *Config) { c.PageLimit = 1; c.MaxPages = 5 }))
	wantKind(t, err, KindWalk)
	if n != 5 {
		t.Fatalf("pages = %d, want 5", n)
	}
}

func TestOversizedBodyIsKindSchema(t *testing.T) {
	f := newFake(t)
	f.listRaw = func(string, int) (int, string) { return 200, strings.Repeat(" ", 2<<20) }
	_, err := list(t, newClient(t, f, nil))
	wantKind(t, err, KindSchema)
}

func TestRedirectIsRefusedAndNeverFollowed(t *testing.T) {
	var hit int
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit++ }))
	defer other.Close()
	f := newFake(t)
	f.listRaw = nil
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == exchangePath {
			_, _ = w.Write([]byte(`{"access_token":"a","expires_in":3600}`))
			return
		}
		http.Redirect(w, r, other.URL+"/x", http.StatusFound)
	})
	_, err := list(t, newClient(t, f, nil))
	wantKind(t, err, KindRejected)
	if hit != 0 {
		t.Fatal("the redirect was followed")
	}
}

func TestExchangeRedirectIsRefusedAndTheSubjectTokenIsNotForwarded(t *testing.T) {
	var seen string
	other := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		seen = r.PostForm.Get("subject_token")
	}))
	defer other.Close()
	f := newFake(t)
	f.srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/x", http.StatusTemporaryRedirect)
	})
	_, err := list(t, newClient(t, f, nil))
	wantKind(t, err, KindExchange)
	if seen != "" {
		t.Fatal("the subject token was forwarded by a redirect")
	}
}

func TestErrorsNameNoTokenBodyOrCredential(t *testing.T) {
	f := newFake(t)
	f.listRaw = func(string, int) (int, string) {
		return 200, `{"schema_version":"credential_lifecycle_list.v1","items":[{"credential_id":"cred-secret-id","revision":1,"status":"nope"}],"next_after":null}`
	}
	_, err := list(t, newClient(t, f, nil))
	for _, bad := range []string{subjectJWT, accessOne, "cred-secret-id", "nope"} {
		if strings.Contains(err.Error(), bad) {
			t.Fatalf("error %q names %q", err, bad)
		}
	}
	f2 := newFake(t)
	f2.exchangeRaw = `{"access_token":"leaky token","expires_in":60}`
	_, err = list(t, newClient(t, f2, nil))
	if strings.Contains(err.Error(), "leaky") || strings.Contains(err.Error(), subjectJWT) {
		t.Fatalf("exchange error %q names a token", err)
	}
}

func TestContextCancelStopsTheWalk(t *testing.T) {
	f := newFake(t, row("a", 1, "active"), row("b", 1, "active"))
	ctx, cancel := context.WithCancel(context.Background())
	f.listRaw = func(string, int) (int, string) {
		cancel()
		return 200, `{"schema_version":"credential_lifecycle_list.v1","items":[{"credential_id":"a","revision":1,"status":"active"}],"next_after":"a"}`
	}
	_, err := newClient(t, f, func(c *Config) { c.PageLimit = 1 }).List(ctx)
	wantKind(t, err, KindTransport)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want it to wrap context.Canceled", err)
	}
}

func TestUnreachableTokenEndpointIsKindExchange(t *testing.T) {
	f := newFake(t)
	c := newClient(t, f, nil)
	f.srv.Close()
	_, err := list(t, c)
	wantKind(t, err, KindExchange)
}

func TestNewRefusesPlainHTTPToANonLoopbackOrigin(t *testing.T) {
	loop, _ := url.Parse("http://127.0.0.1:1")
	remote, _ := url.Parse("http://acr.example.test")
	secure, _ := url.Parse("https://acr.example.test")
	file := subjectFile(t, "x")
	for name, cfg := range map[string]Config{
		"plain base":     {BaseURL: remote, TokenEndpoint: secure, SubjectTokenFile: file},
		"plain endpoint": {BaseURL: secure, TokenEndpoint: remote, SubjectTokenFile: file},
		"no base":        {TokenEndpoint: secure, SubjectTokenFile: file},
		"no endpoint":    {BaseURL: secure, SubjectTokenFile: file},
		"no subject":     {BaseURL: secure, TokenEndpoint: secure},
		"limit 501":      {BaseURL: secure, TokenEndpoint: secure, SubjectTokenFile: file, PageLimit: 501},
		"limit negative": {BaseURL: secure, TokenEndpoint: secure, SubjectTokenFile: file, PageLimit: -1},
		"base userinfo":  {BaseURL: mustURL("https://u:p@acr.example.test"), TokenEndpoint: secure, SubjectTokenFile: file},
		"base query":     {BaseURL: mustURL("https://acr.example.test/?a=b"), TokenEndpoint: secure, SubjectTokenFile: file},
	} {
		if _, err := New(cfg); err == nil {
			t.Fatalf("%s: New accepted the config", name)
		}
	}
	if _, err := New(Config{BaseURL: loop, TokenEndpoint: loop, SubjectTokenFile: file}); err != nil {
		t.Fatalf("loopback http refused: %v", err)
	}
	if _, err := New(Config{BaseURL: secure, TokenEndpoint: secure, SubjectTokenFile: file}); err != nil {
		t.Fatalf("https refused: %v", err)
	}
}

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

func TestKindsAreClosedMetricLabels(t *testing.T) {
	want := map[Kind]string{
		KindExchange: "exchange", KindUnauthorized: "unauthorized", KindForbidden: "forbidden", KindRateLimited: "rate_limited",
		KindUnavailable: "unavailable", KindRejected: "rejected", KindSchema: "schema", KindWalk: "walk", KindTransport: "transport",
	}
	for k, name := range want {
		if string(k) != name {
			t.Fatalf("kind %q, want %q", k, name)
		}
	}
}
