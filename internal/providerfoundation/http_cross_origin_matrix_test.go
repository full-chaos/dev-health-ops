package providerfoundation

import (
	"context"
	"crypto/tls"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A server that answers with a redirect or a next-page URL must never receive the credential of the origin it was
// asked: tested with two real servers, over every status a client follows, and over the shapes of "another origin"
// (other host, other port, https to http, same origin). The credential is a synthetic value.

const matrixToken = "synthetic-matrix-credential"

type recordedRequest struct {
	header http.Header
	path   string
}

type recorder struct {
	mu   sync.Mutex
	seen []recordedRequest
}

func (r *recorder) add(req *http.Request) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, recordedRequest{header: req.Header.Clone(), path: req.URL.Path})
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.seen)
}

func (r *recorder) credentialSeen() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, req := range r.seen {
		for _, name := range []string{"Authorization", "Private-Token", "X-Api-Key"} {
			if req.header.Get(name) != "" {
				return true
			}
		}
	}
	return false
}

func tlsClientOf(server *httptest.Server) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: server.Client().Transport.(*http.Transport).TLSClientConfig.InsecureSkipVerify, RootCAs: server.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs}}}
}

func stampCredential(r *http.Request) error {
	r.Header.Set("Authorization", "Bearer "+matrixToken)
	r.Header.Set("PRIVATE-TOKEN", matrixToken)
	r.Header.Set("X-Api-Key", matrixToken)
	return nil
}

func TestACredentialNeverLeavesItsOriginOnAServerSuppliedURL(t *testing.T) {
	type target struct {
		name string
		// destination builds the server the origin points to; leaves reports whether it is another origin.
		build func(t *testing.T, seen *recorder) (url string, leaves bool)
	}
	handlerFor := func(seen *recorder) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			seen.add(r)
			_, _ = io.WriteString(w, `[]`)
		})
	}
	targets := []target{
		{"other host", func(t *testing.T, seen *recorder) (string, bool) {
			s := httptest.NewServer(handlerFor(seen))
			t.Cleanup(s.Close)
			u, _ := url.Parse(s.URL)
			// 127.0.0.1 versus localhost: same machine, another host name.
			return "http://localhost:" + u.Port(), true
		}},
		{"same host other port", func(t *testing.T, seen *recorder) (string, bool) {
			s := httptest.NewServer(handlerFor(seen))
			t.Cleanup(s.Close)
			return s.URL, true
		}},
	}
	statuses := []int{http.StatusMovedPermanently, http.StatusFound, http.StatusTemporaryRedirect, http.StatusPermanentRedirect}

	for _, tg := range targets {
		for _, status := range statuses {
			t.Run("refuse/"+tg.name+"/"+strconv.Itoa(status), func(t *testing.T) {
				var dest recorder
				destURL, _ := tg.build(t, &dest)
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, destURL+"/stolen", status)
				}))
				t.Cleanup(origin.Close)
				follower := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
				client, err := NewHTTPClient("gitlab", origin.URL, follower, stampCredential, DefaultRetryPolicy(), LeaseGuardFunc(func(context.Context) error { return nil }))
				if err != nil {
					t.Fatal(err)
				}
				response, err := client.Do(t.Context(), "GET", "/start", nil)
				if err == nil && response != nil {
					_ = response.Body.Close()
				}
				if dest.count() != 0 || dest.credentialSeen() {
					t.Fatalf("the other origin received %d request(s), credential seen: %v", dest.count(), dest.credentialSeen())
				}
			})
			t.Run("strip/"+tg.name+"/"+strconv.Itoa(status), func(t *testing.T) {
				var dest recorder
				destURL, _ := tg.build(t, &dest)
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					http.Redirect(w, r, destURL+"/followed", status)
				}))
				t.Cleanup(origin.Close)
				client := &http.Client{CheckRedirect: DropCredentialsOnHostChange}
				request, _ := http.NewRequest("GET", origin.URL+"/start", nil)
				_ = stampCredential(request)
				response, err := client.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if dest.count() != 1 {
					t.Fatalf("the redirect must be followed once, got %d", dest.count())
				}
				if dest.credentialSeen() {
					t.Fatal("a credential header reached the other origin")
				}
			})
		}
	}
}

func TestASchemeDowngradeStripsTheCredential(t *testing.T) {
	var dest recorder
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dest.add(r); _, _ = io.WriteString(w, `[]`) }))
	t.Cleanup(plain.Close)
	secure := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, plain.URL+"/downgraded", http.StatusFound)
	}))
	t.Cleanup(secure.Close)
	client := secure.Client()
	client.CheckRedirect = DropCredentialsOnHostChange
	request, _ := http.NewRequest("GET", secure.URL+"/start", nil)
	_ = stampCredential(request)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if dest.count() != 1 || dest.credentialSeen() {
		t.Fatalf("downgrade hop: %d request(s), credential seen %v", dest.count(), dest.credentialSeen())
	}
}

func TestASameOriginRedirectKeepsTheCredential(t *testing.T) {
	var seen recorder
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r)
		if r.URL.Path == "/start" {
			http.Redirect(w, r, "/moved", http.StatusTemporaryRedirect)
			return
		}
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(server.Close)
	client := &http.Client{CheckRedirect: DropCredentialsOnHostChange}
	request, _ := http.NewRequest("GET", server.URL+"/start", nil)
	_ = stampCredential(request)
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if seen.count() != 2 || seen.seen[1].header.Get("PRIVATE-TOKEN") != matrixToken || seen.seen[1].header.Get("Authorization") == "" {
		t.Fatalf("same-origin hop must keep the credential: %+v", seen.seen)
	}
}

func TestALinkNextURLOnAnotherOriginIsRefusedWithoutARequest(t *testing.T) {
	var dest recorder
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dest.add(r); _, _ = io.WriteString(w, `[]`) }))
	t.Cleanup(other.Close)
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(plain.Close)
	tlsOrigin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(tlsOrigin.Close)
	otherURL, _ := url.Parse(other.URL)
	cases := map[string]struct {
		base, next string
		wantRefuse bool
	}{
		"other host":           {plain.URL, "http://localhost:" + otherURL.Port() + "/p2", true},
		"same host other port": {plain.URL, other.URL + "/p2", true},
		"scheme downgrade":     {tlsOrigin.URL, "http://" + tlsOrigin.Listener.Addr().String() + "/p2", true},
		"same origin":          {plain.URL, plain.URL + "/p2", false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			client, err := NewHTTPClient("gitlab", c.base, tlsClientOf(tlsOrigin), stampCredential, DefaultRetryPolicy(), LeaseGuardFunc(func(context.Context) error { return nil }))
			if err != nil {
				t.Fatal(err)
			}
			before := dest.count()
			response, err := client.Do(t.Context(), "GET", c.next, nil)
			if response != nil {
				_ = response.Body.Close()
			}
			if c.wantRefuse {
				if err == nil {
					t.Fatal("a next URL on another origin must be refused")
				}
				if dest.count() != before || dest.credentialSeen() {
					t.Fatal("the other origin received a request")
				}
				return
			}
			if err != nil {
				t.Fatalf("same origin must be followed: %v", err)
			}
		})
	}
}

type lineCapture struct{ lines *[]string }

func (h lineCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h lineCapture) Handle(_ context.Context, r slog.Record) error {
	line := r.Level.String() + " " + r.Message
	r.Attrs(func(a slog.Attr) bool { line += " " + a.Key + "=" + a.Value.String(); return true })
	*h.lines = append(*h.lines, line)
	return nil
}
func (h lineCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h lineCapture) WithGroup(string) slog.Handler      { return h }

func TestEveryDepartureIsLoggedAtWarnWithoutTheCredential(t *testing.T) {
	var lines []string
	previous := slog.Default()
	slog.SetDefault(slog.New(lineCapture{&lines}))
	t.Cleanup(func() { slog.SetDefault(previous) })

	var dest recorder
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { dest.add(r); _, _ = io.WriteString(w, `[]`) }))
	t.Cleanup(other.Close)
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/x?access_token="+matrixToken, http.StatusFound)
	}))
	t.Cleanup(origin.Close)

	strip := &http.Client{CheckRedirect: DropCredentialsOnHostChange}
	request, _ := http.NewRequest("GET", origin.URL, nil)
	_ = stampCredential(request)
	if response, err := strip.Do(request); err == nil {
		_ = response.Body.Close()
	}
	client, err := NewHTTPClient("gitlab", origin.URL, &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}, stampCredential, DefaultRetryPolicy(), LeaseGuardFunc(func(context.Context) error { return nil }))
	if err != nil {
		t.Fatal(err)
	}
	if response, err := client.Do(t.Context(), "GET", "/start", nil); err == nil && response != nil {
		_ = response.Body.Close()
	}
	if _, err := client.Do(t.Context(), "GET", other.URL+"/next", nil); err == nil {
		t.Fatal("an absolute URL on another origin must be refused")
	}
	var stripped, refused int
	for _, line := range lines {
		if !strings.HasPrefix(line, "WARN credential origin departure") {
			continue
		}
		if strings.Contains(line, matrixToken) {
			t.Fatalf("a log line holds the credential: %s", line)
		}
		if strings.Contains(line, "action=stripped") {
			stripped++
		}
		if strings.Contains(line, "action=refused") {
			refused++
		}
	}
	if stripped < 1 || refused < 2 {
		t.Fatalf("want a stripped line and two refused lines (redirect, follow URL), got stripped=%d refused=%d in %v", stripped, refused, lines)
	}
}
