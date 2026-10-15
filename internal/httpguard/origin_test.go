package httpguard

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"log/slog"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/redirectprobe"
)

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

func TestSameOriginTable(t *testing.T) {
	cases := []struct {
		name, a, b string
		same       bool
	}{
		{"identical", "https://api.example.test/a", "https://api.example.test/b?x=1", true},
		{"case and default port", "https://API.example.test:443/a", "https://api.example.test/b", true},
		{"http default port", "http://h.test:80/", "http://h.test/", true},
		{"other host", "https://api.example.test/", "https://evil.example.test/", false},
		{"subdomain", "https://example.test/", "https://api.example.test/", false},
		{"same host other port", "https://h.test:8443/", "https://h.test:9443/", false},
		{"explicit port vs default", "https://h.test:8443/", "https://h.test/", false},
		{"scheme downgrade", "https://h.test/", "http://h.test/", false},
		{"userinfo is not the origin", "https://user:pw@h.test/", "https://h.test/", true},
		{"no host", "/relative", "/relative", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := SameOrigin(mustURL(t, c.a), mustURL(t, c.b)); got != c.same {
				t.Fatalf("SameOrigin(%s, %s) = %v, want %v", c.a, c.b, got, c.same)
			}
		})
	}
	if SameOrigin(nil, nil) {
		t.Fatal("two nil URLs must not be the same origin")
	}
}

func TestStripCredentialsRemovesEveryCredentialHeader(t *testing.T) {
	request, _ := http.NewRequest("GET", "https://h.test/", nil)
	for _, name := range CredentialHeaders {
		request.Header.Set(name, "synthetic")
	}
	request.Header.Set("Accept", "application/json")
	StripCredentials(request)
	for _, name := range CredentialHeaders {
		if request.Header.Get(name) != "" {
			t.Fatalf("%s survived", name)
		}
	}
	if request.Header.Get("Accept") == "" {
		t.Fatal("a non-credential header was removed")
	}
}

type captureHandler struct{ lines *[]string }

func (h captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h captureHandler) Handle(_ context.Context, r slog.Record) error {
	line := r.Level.String() + " " + r.Message
	r.Attrs(func(a slog.Attr) bool { line += " " + a.Key + "=" + a.Value.String(); return true })
	*h.lines = append(*h.lines, line)
	return nil
}
func (h captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h captureHandler) WithGroup(string) slog.Handler      { return h }

func TestRecordLeftOriginLogsOriginsOnlyAtWarn(t *testing.T) {
	var lines []string
	previous := slog.Default()
	slog.SetDefault(slog.New(captureHandler{&lines}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	RecordLeftOrigin(context.Background(), ActionRefused, "test.site",
		mustURL(t, "https://user:secretpw@a.test/path?token=synthetic"), mustURL(t, "http://b.test:8080/other?token=synthetic"))
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "WARN ") {
		t.Fatalf("want one warn line, got %v", lines)
	}
	for _, leak := range []string{"secretpw", "synthetic", "/path", "/other", "user"} {
		if strings.Contains(lines[0], leak) {
			t.Fatalf("log line leaks %q: %s", leak, lines[0])
		}
	}
	for _, want := range []string{"action=refused", "site=test.site", "from_origin=https://a.test", "to_origin=http://b.test:8080", "scheme_downgrade=true"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("missing %q in %s", want, lines[0])
		}
	}
}

func TestNoRedirectsRecordsACrossOriginRefusal(t *testing.T) {
	var lines []string
	previous := slog.Default()
	slog.SetDefault(slog.New(captureHandler{&lines}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	probe := redirectprobe.New(t)
	response, err := NoRedirects(probe.Client()).Get(probe.Base.URL + "/x")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	probe.Assert(t)
	if len(lines) != 1 || !strings.Contains(lines[0], "action=refused") || !strings.Contains(lines[0], "site=httpguard.no_redirects") {
		t.Fatalf("want one refused line, got %v", lines)
	}
}

func TestALogLabelNeverCarriesALineBreakOrSpace(t *testing.T) {
	hostile := &url.URL{Scheme: "ht\ntp", Host: "evil.test\nWARN forged line=1"}
	label := OriginOf(hostile).Label()
	if strings.ContainsAny(label, "\n\r \t") || !strings.HasPrefix(label, "other://evil.test_warn_forged_line_1") {
		t.Fatalf("label %q is not sanitized", label)
	}
	var lines []string
	previous := slog.Default()
	slog.SetDefault(slog.New(captureHandler{&lines}))
	t.Cleanup(func() { slog.SetDefault(previous) })
	RecordLeftOrigin(context.Background(), ActionRefused, "test.site", mustURL(t, "https://a.test/"), hostile)
	if len(lines) != 1 || strings.ContainsAny(lines[0], "\n\r") {
		t.Fatalf("log line is not one clean line: %q", lines)
	}
}
