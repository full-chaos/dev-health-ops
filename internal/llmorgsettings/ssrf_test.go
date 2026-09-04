package llmorgsettings

import (
	"context"
	"errors"
	"net"
	"testing"
)

// hermeticAnswers ports tests/test_byo_base_url_ssrf.py's `hermetic_dns`
// fixture verbatim -- same hostnames, same answers -- so the accept/reject
// cases below are the SAME oracle, not a Go reinterpretation of it.
var hermeticAnswers = map[string][]string{
	"api.openai.com":         {"104.18.33.45"},
	"api.anthropic.com":      {"160.79.104.10"},
	"my-gateway.example.com": {"93.184.216.34"},
	"localhost":              {"127.0.0.1", "::1"},
	"127.0.0.1":              {"127.0.0.1"},
	"::1":                    {"::1"},
	"2130706433":             {"127.0.0.1"},
	"127.1":                  {"127.0.0.1"},
	"0x7f.0.0.1":             {"127.0.0.1"},
	"mixed.example.com":      {"93.184.216.34", "10.0.0.5"},
	"evil.example":           {"93.184.216.34"},
}

var errNoTestDNSAnswer = errors.New("test DNS has no answer for host")

func hermeticResolver(_ context.Context, host string) ([]net.IP, error) {
	answers, ok := hermeticAnswers[host]
	if !ok {
		return nil, errNoTestDNSAnswer
	}
	ips := make([]net.IP, 0, len(answers))
	for _, a := range answers {
		ips = append(ips, net.ParseIP(a))
	}
	return ips, nil
}

// TestValidateBaseURL_AcceptsValid ports
// test_ssrf_shape_accepts_valid_base_urls 1:1.
func TestValidateBaseURL_AcceptsValid(t *testing.T) {
	for _, url := range []string{
		"",
		"https://api.openai.com/v1",
		"https://my-gateway.example.com/v1",
		"https://unresolvable.example.test/v1",
	} {
		t.Run(url, func(t *testing.T) {
			ok, reason := validateBaseURL(context.Background(), url, hermeticResolver)
			if !ok {
				t.Fatalf("expected accept, got reject: %q", reason)
			}
			if reason != "" {
				t.Fatalf("expected empty reason on accept, got %q", reason)
			}
		})
	}
}

// TestValidateBaseURL_RejectsUnsafe ports
// test_ssrf_shape_rejects_unsafe_base_urls 1:1.
func TestValidateBaseURL_RejectsUnsafe(t *testing.T) {
	for _, url := range []string{
		"http://api.openai.com",
		"https://api.openai.com@169.254.169.254/v1",
		"https://169.254.169.254/",
		"https://[::ffff:169.254.169.254]/",
		"https://localhost/",
		"https://[::1]/",
		"http://localhost/",
		"http://localhost:1234/v1",
		"http://127.0.0.1:8000",
		"https://10.0.0.5/",
		"https://192.168.1.1/",
		"https://2130706433/",
		"https://127.1/",
		"https://0x7f.0.0.1/",
		"https://mixed.example.com/v1",
		"ftp://api.openai.com/v1",
		"not-a-url",
	} {
		t.Run(url, func(t *testing.T) {
			ok, reason := validateBaseURL(context.Background(), url, hermeticResolver)
			if ok {
				t.Fatalf("expected reject, got accept")
			}
			if reason == "" {
				t.Fatalf("expected a non-empty reason on reject")
			}
		})
	}
}

func TestValidateBaseURL_PublicEntrypointUsesRealResolver(t *testing.T) {
	// Smoke-test the exported wrapper wires the default resolver -- an
	// empty base_url short-circuits before any DNS lookup happens, so this
	// never touches the network.
	ok, reason := ValidateBaseURL(context.Background(), "")
	if !ok || reason != "" {
		t.Fatalf("expected accept with empty reason, got ok=%v reason=%q", ok, reason)
	}
}
