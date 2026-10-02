package restcore

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// The core follows no redirect: a 3xx is an error whose text names where the server wanted to send the request. That text
// reaches logs and API answers, and a Location can hold userinfo, a query with a token, a path with an id, a fragment: it
// must carry the scheme and the host only (CHAOS-7927).

const plantedLocationSecret = "sk-planted-location-7927"

func redirectError(t *testing.T, location string, setLocation bool) (class, message string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if setLocation {
			w.Header().Set("Location", location)
		}
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(server.Close)
	_, err := Core{Provider: "github", RetryAfter: func(Response) time.Duration { return 0 }}.Get(context.Background(), server.URL, "GET /probe")
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want an *Error", err)
	}
	return apiErr.Class, apiErr.Message
}

func TestRedirectErrorCarriesTheSchemeAndHostOnly(t *testing.T) {
	for _, row := range []struct {
		name, location, want string
	}{
		{"userinfo, port, path, query and fragment", "https://admin:" + plantedLocationSecret + "@files.example.test:8443/orgs/42/repos?access_token=" + plantedLocationSecret + "#frag", "https://files.example.test:8443"},
		{"a plain absolute URL", "https://api.example.test/next", "https://api.example.test"},
		{"a scheme-relative URL", "//cdn.example.test/x?token=" + plantedLocationSecret, "//cdn.example.test"},
		{"an IPv6 host", "http://[2001:db8::1]:9/x?k=" + plantedLocationSecret, "http://[2001:db8::1]:9"},
	} {
		t.Run(row.name, func(t *testing.T) {
			class, message := redirectError(t, row.location, true)
			if class != "APIException" {
				t.Fatalf("the error class is %q, want APIException (a redirect is an API exception, not a transport error)", class)
			}
			for _, leak := range []string{plantedLocationSecret, "admin", "orgs/42", "access_token", "frag", "token=", "k="} {
				if strings.Contains(message, leak) {
					t.Fatalf("the error text carries %q: %s", leak, message)
				}
			}
			if !strings.Contains(message, "HTTP 302 -> "+row.want+";") {
				t.Fatalf("the error text does not name %q as the target: %s", row.want, message)
			}
		})
	}
}

func TestRedirectErrorNamesNoTargetForARelativeUnparsableOrMissingLocation(t *testing.T) {
	for _, row := range []struct {
		name, location string
		set            bool
		want           string
	}{
		{"a relative path with a query", "/orgs/42/repos?token=" + plantedLocationSecret, true, "<relative Location>"},
		{"a bare name", "next-page?token=" + plantedLocationSecret, true, "<relative Location>"},
		{"unparsable (net/http refuses it and quotes it)", "https://exa%zzmple.test/x?token=" + plantedLocationSecret, true, "<unparsable Location>"},
		{"a scheme with no host", "mailto:" + plantedLocationSecret, true, "<relative Location>"},
		{"no Location header", "", false, "<no Location header>"},
	} {
		t.Run(row.name, func(t *testing.T) {
			class, message := redirectError(t, row.location, row.set)
			if class != "APIException" {
				t.Fatalf("the error class is %q, want APIException: net/http's own refusal of an unparsable Location must not change the class", class)
			}
			if strings.Contains(message, plantedLocationSecret) || strings.Contains(message, "orgs/42") {
				t.Fatalf("the error text carries part of the Location: %s", message)
			}
			if !strings.Contains(message, row.want) {
				t.Fatalf("the error text = %s, want the target %q", message, row.want)
			}
		})
	}
}

func TestRedirectTargetIsTheSchemeAndHostOfALocation(t *testing.T) {
	for location, want := range map[string]string{
		"https://u:p@h.example.test/a?b#c": "https://h.example.test",
		"http://h.example.test":            "http://h.example.test",
		"//h.example.test/p":               "//h.example.test",
		"/only/a/path":                     "<relative Location>",
		"":                                 "<relative Location>",
		"%zz":                              "<unparsable Location>",
	} {
		if got := RedirectTarget(location); got != want {
			t.Errorf("RedirectTarget(%q) = %q, want %q", location, got, want)
		}
	}
}

// A transport error that is not a redirect keeps its class and its text: only net/http's refusal of a Location is reworded.
type failingTransport struct{ err error }

func (transport failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, transport.err
}

func TestAnOrdinaryTransportErrorKeepsItsClassAndText(t *testing.T) {
	core := Core{
		Provider:   "github",
		RetryAfter: func(Response) time.Duration { return 0 },
		HTTP:       &http.Client{Transport: failingTransport{err: errors.New("tls: handshake exploded")}},
	}
	_, err := core.Get(context.Background(), "https://api.example.test/x", "GET /probe")
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Class != "TransportError" || !strings.Contains(apiErr.Message, "tls: handshake exploded") {
		t.Fatalf("err = %v, want a TransportError that keeps the transport's text", err)
	}
	if strings.Contains(apiErr.Message, "unexpected redirect") {
		t.Fatalf("an ordinary transport error was reworded as a redirect: %s", apiErr.Message)
	}
}
