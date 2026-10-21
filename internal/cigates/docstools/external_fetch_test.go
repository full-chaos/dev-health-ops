package docstools_test

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/cigates/docstools"
)

// The urllib fetcher of check-external-links over a fake round tripper: no test opens a socket.

type rtFunc func(*http.Request) (*http.Response, error)

func (f rtFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type seen struct{ method, uri, host, agent string }

// fakeSite answers each request with the next status (and Location, when given) and records what it was asked.
func fakeSite(t *testing.T, answers ...string) (http.RoundTripper, *[]seen) {
	var got []seen
	return rtFunc(func(r *http.Request) (*http.Response, error) {
		got = append(got, seen{r.Method, r.URL.RequestURI(), r.Host, r.Header.Get("User-Agent")})
		if len(answers) == 0 {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL)
		}
		a := answers[0]
		if len(answers) > 1 {
			answers = answers[1:]
		}
		if a == "error" {
			return nil, errors.New("connection refused")
		}
		code, location, _ := strings.Cut(a, " ")
		resp := &http.Response{StatusCode: atoi(t, code), Header: http.Header{}, Body: io.NopCloser(strings.NewReader(""))}
		if location != "" {
			resp.Header.Set("Location", location)
		}
		return resp, nil
	}), &got
}

func atoi(t *testing.T, s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("bad status %q", s)
		}
		n = n*10 + int(c-'0')
	}
	return n
}

func fetchOnce(t *testing.T, url string, answers ...string) (bool, string, error, []seen) {
	t.Helper()
	rt, got := fakeSite(t, answers...)
	ok, detail, err := docstools.HTTPFetcherForTest(rt)(url)
	return ok, detail, err, *got
}

func TestDefaultFetcherRejectsNonHttpUrlsBeforeOpeningThem(t *testing.T) {
	for _, u := range []string{"file:///etc/passwd", "https:///nohost", "ftp://example.com/"} {
		ok, detail, err, got := fetchOnce(t, u)
		if ok || detail != "unsupported URL scheme" || err != nil || len(got) != 0 {
			t.Errorf("%s: ok %v detail %q err %v requests %v", u, ok, detail, err, got)
		}
	}
}

func TestFetcherHeadThenGet(t *testing.T) {
	ok, detail, _, got := fetchOnce(t, "https://example.com/a", "200")
	if !ok || detail != "status 200 (HEAD)" || len(got) != 1 {
		t.Errorf("HEAD 200: ok %v detail %q requests %v", ok, detail, got)
	}
	ok, detail, _, got = fetchOnce(t, "https://example.com/a", "405", "204")
	if !ok || detail != "status 204 (GET)" || len(got) != 2 || got[1].method != "GET" {
		t.Errorf("HEAD 405 then GET: ok %v detail %q requests %v", ok, detail, got)
	}
	ok, detail, _, _ = fetchOnce(t, "https://example.com/a", "404", "500")
	if ok || detail != "status 500 (GET)" {
		t.Errorf("both fail: ok %v detail %q", ok, detail)
	}
	ok, detail, _, _ = fetchOnce(t, "https://example.com/a", "error")
	if ok || !strings.HasPrefix(detail, "<urlopen error ") || !strings.HasSuffix(detail, "> (GET)") {
		t.Errorf("transport error: ok %v detail %q", ok, detail)
	}
}

func TestFetcherSendsTheRequestTargetAsUrllibDoes(t *testing.T) {
	_, _, _, got := fetchOnce(t, "https://Example.com:443/a%zz/b?q=1#frag#more", "200")
	want := seen{"HEAD", "/a%zz/b?q=1#frag", "Example.com:443", "Mozilla/5.0 (compatible; dev-health-docs-link-check/1.0; +https://github.com/full-chaos/dev-health-ops)"}
	if len(got) != 1 || got[0] != want {
		t.Errorf("requests %v, want %v", got, want)
	}
	_, _, _, got = fetchOnce(t, "http://example.com?x", "200")
	if len(got) != 1 || got[0].uri != "/?x" || got[0].host != "example.com" {
		t.Errorf("requests %v", got)
	}
}

func TestFetcherFollowsRedirectsOnlyToHttp(t *testing.T) {
	ok, detail, _, got := fetchOnce(t, "https://example.com/a", "301 /b", "302 https://other.example/c", "200")
	if !ok || detail != "status 200 (HEAD)" || len(got) != 3 || got[1].uri != "/b" || got[2].host != "other.example" || got[2].method != "HEAD" {
		t.Errorf("chain: ok %v detail %q requests %v", ok, detail, got)
	}
	// urllib refuses a redirect to a scheme it does not know with an HTTPError carrying the 3xx code, which the script
	// counts as reachable (code < 400).
	ok, detail, _, got = fetchOnce(t, "https://example.com/a", "302 javascript:alert(1)")
	if !ok || detail != "status 302 (HEAD)" || len(got) != 1 {
		t.Errorf("javascript redirect: ok %v detail %q requests %v", ok, detail, got)
	}
	ok, detail, _, _ = fetchOnce(t, "https://example.com/a", "301")
	if !ok || detail != "status 301 (HEAD)" {
		t.Errorf("redirect without Location: ok %v detail %q", ok, detail)
	}
	ok, detail, _, _ = fetchOnce(t, "https://example.com/a", "304")
	if !ok || detail != "status 304 (HEAD)" {
		t.Errorf("304: ok %v detail %q", ok, detail)
	}
}

func TestFetcherStopsARedirectLoopLikeUrllib(t *testing.T) {
	ok, detail, _, got := fetchOnce(t, "https://example.com/a", "302 /a")
	// The first request, then max_repeats (4) visits of the same target; the HTTPError carries 302, which is < 400.
	if !ok || detail != "status 302 (HEAD)" || len(got) != 5 {
		t.Errorf("loop: ok %v detail %q, %d requests", ok, detail, len(got))
	}
	answers := []string{}
	for i := 0; i < 20; i++ {
		answers = append(answers, "302 /p"+string(rune('a'+i)))
	}
	_, _, _, got = fetchOnce(t, "https://example.com/start", answers...)
	if len(got) != 11 {
		t.Errorf("chain of distinct targets: %d requests, want 11 (max_redirections 10)", len(got))
	}
}

func TestFetcherRefusesWhatHttpClientRefuses(t *testing.T) {
	for _, c := range []struct{ url, kind string }{
		{"https://example.com/a b", "http.client.InvalidURL: URL can't contain control characters. '/a b' (found at least ' ')"},
		{"https://example.com:abc/", "http.client.InvalidURL: nonnumeric port: 'abc'"},
		{"https://exa%20mple.com/", "http.client.InvalidURL: URL can't contain control characters. 'exa mple.com' (found at least ' ')"},
		{"https://example.com/café", "UnicodeEncodeError: 'ascii' codec can't encode character '\\xe9' in position 9: ordinal not in range(128)"},
		{"https://example.com/€€", "UnicodeEncodeError: 'ascii' codec can't encode characters in position 6-7: ordinal not in range(128)"},
		{"https://ΑΣ.example/", "UnicodeEncodeError: 'latin-1' codec can't encode characters in position 0-1: ordinal not in range(256)"},
	} {
		ok, _, err, got := fetchOnce(t, c.url)
		if ok || err == nil || err.Error() != c.kind || len(got) != 0 {
			t.Errorf("%s: ok %v err %v requests %v; want %s", c.url, ok, err, got, c.kind)
		}
	}
}

func TestFetcherReportsWhatHttpClientDoesNotRefuse(t *testing.T) {
	ok, detail, err, got := fetchOnce(t, "https://example.com:99999/")
	if ok || err != nil || !strings.Contains(detail, "out of range") || len(got) != 0 {
		t.Errorf("port 99999: ok %v detail %q err %v requests %v", ok, detail, err, got)
	}
	ok, _, err, got = fetchOnce(t, "https://example.com: 8_0/", "200")
	if !ok || err != nil || len(got) != 1 {
		t.Errorf("int(' 8_0') is 80 to http.client: ok %v err %v requests %v", ok, err, got)
	}
	ok, _, err, got = fetchOnce(t, "https://café.example/", "200")
	if !ok || err != nil || len(got) != 1 || got[0].host != "café.example" {
		t.Errorf("a Latin-1 host is no refusal: ok %v err %v requests %v", ok, err, got)
	}
}

// The script's own redirect handler refuses a target that urllib lets through (ftp, or http without a host); urllib
// itself answers a file: or javascript: target with an HTTPError carrying the 3xx code before the handler runs.
func TestHttpRedirectHandlerRejectsNonHttpRedirectTargets(t *testing.T) {
	for _, target := range []string{"ftp://example.com/x", "http:///nohost"} {
		ok, detail, _, got := fetchOnce(t, "https://example.com/a", "302 "+target)
		if ok || detail != "<urlopen error unsupported redirect URL scheme> (GET)" || len(got) != 2 {
			t.Errorf("%s: ok %v detail %q requests %v", target, ok, detail, got)
		}
	}
	ok, detail, _, _ := fetchOnce(t, "https://example.com/a", "302 file:///etc/passwd")
	if !ok || detail != "status 302 (HEAD)" {
		t.Errorf("file: redirect: ok %v detail %q; want the HTTPError 302 the script counts as reachable", ok, detail)
	}
}

// urllib keeps HEAD on a redirect (Python 3.14 Lib/urllib/request.py redirect_request: method="HEAD" if m == "HEAD"
// else "GET"), and the GET retry starts again at the first URL. Recorded once with the frozen script and a local
// http.server on Python 3.14.7: (False, 'status 404 (GET)'), requests HEAD /start, HEAD /target, GET /start.
func TestFetcherKeepsHeadAcrossARedirectAsUrllibDoes(t *testing.T) {
	ok, detail, _, got := fetchOnce(t, "https://example.com/start", "302 /target", "405", "404")
	want := []string{"HEAD /start", "HEAD /target", "GET /start"}
	var gotReq []string
	for _, s := range got {
		gotReq = append(gotReq, s.method+" "+s.uri)
	}
	if ok || detail != "status 404 (GET)" || strings.Join(gotReq, ",") != strings.Join(want, ",") {
		t.Errorf("ok %v detail %q requests %v; Python (False, 'status 404 (GET)') %v", ok, detail, gotReq, want)
	}
}
