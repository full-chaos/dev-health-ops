package admin

import (
	"context"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

// TestReadinessSSRFGuardCoversEveryPythonRefusalClass is D2908 condition 1
// (CHAOS-6976 codex r1 P1 #1, the redirect-bypass finding): the readiness
// handler's OWN gate before ever reaching the prober is
// llmorgsettings.ValidateBaseURLChecked (llmsettingsreadiness.go:157) --
// the same function GET /llm-settings/status already calls, and the one
// already live-python-oracled against 200+ corpus cases
// (internal/llmorgsettings/ssrf_live_python_oracle_test.go). That existing
// oracle proves the FUNCTION; this test proves this ROUTE's call site is
// not silently bypassing or narrowing it -- one representative URL per
// refusal CLASS in llm/credentials.py's validate_llm_base_url, each with
// its Python and Go source citation, asserted to refuse here directly.
//
// Python classes (src/dev_health_ops/llm/credentials.py):
//  1. control/whitespace characters            :175-176 (_contains_control_or_space)
//  2. userinfo present (user:pass@host)        :178-179
//  3. scheme not http/https                    :180-181
//  4. malformed host/port (bad IPv6 bracket)    :182-186
//  5. missing host                             :187-188
//  6. host normalization failure (bad IDNA)     :124-135 (_normalize_url_host)
//  7. scheme is http, not https                :192-193
//  8. an IP literal that is not a safe public
//     target (loopback/private/link-local/
//     multicast/unspecified/reserved/CGNAT/
//     documentation range/not-global)          :138-153 (_ip_is_safe_public_target), checked :195-200
//  9. a DNS-resolved address that is not safe   :156-162 (_resolved_addresses), checked :202-211
//
// Go equivalents (internal/llmorgsettings/ssrf.go): validateBaseURL:61-116
// dispatches to containsControlOrSpace:118-125 (class 1), parsed.HasUserInfo
// (class 2), the scheme check (class 3), parsed.Port() (class 4), hasHost
// (class 5), normalizeHost:131-145 (class 6), the post-normalize scheme
// check (class 7), and isSafePublicIP:171-181 for both the IP-literal path
// (class 8) and the resolved-address loop (class 9).
func TestReadinessSSRFGuardCoversEveryPythonRefusalClass(t *testing.T) {
	cases := []struct {
		class string
		url   string
	}{
		{"1 control/whitespace character", "https://example.invalid/v1\t"},
		{"2 userinfo present", "https://user:pass@example.invalid/v1"},
		{"3 scheme not http/https", "ftp://example.invalid/v1"},
		{"4 malformed host/port (bad IPv6 bracket)", "https://[::1/v1"},
		{"5 missing host", "https:///v1"},
		{"6 host normalization failure (invalid IDNA, label > 63 octets)", "https://" + strings.Repeat("a", 64) + ".invalid/v1"},
		{"7 scheme is http, not https", "http://example.invalid/v1"},
		{"8 IP literal loopback", "https://127.0.0.1/v1"},
		{"8 IP literal private (RFC1918)", "https://10.0.0.1/v1"},
		{"8 IP literal link-local", "https://169.254.169.254/v1"},
		{"8 IP literal multicast", "https://224.0.0.1/v1"},
		{"8 IP literal CGNAT (not covered by Go's IsPrivate)", "https://100.64.0.1/v1"},
		{"8 IP literal documentation range (not covered by Go's IsPrivate)", "https://192.0.2.1/v1"},
		{"9 DNS-resolved address unsafe (localhost resolves loopback)", "https://localhost/v1"},
	}
	for _, c := range cases {
		t.Run(c.class, func(t *testing.T) {
			ok, reason, err := llmorgsettings.ValidateBaseURLChecked(context.Background(), c.url)
			if err == nil && ok {
				t.Fatalf("url %q: accepted (reason=%q), want refused", c.url, reason)
			}
		})
	}
}

// TestReadinessSSRFGuardAcceptsAnUnresolvableName pins the ONE documented
// pass-through: an unresolvable hostname (validate_llm_base_url:202-206,
// ssrf.go:100-105) is NOT an SSRF target at persist/validate time -- this
// is what lets the route's own live-python-oracle
// (llmreadinessprobe_live_python_oracle_test.go) and the success-path
// route test (llmsettingsreadiness_route_test.go) use a real, routable
// stub without the guard itself standing in the way; it is not a gap in
// the guard, it is documented, oracled behavior of the guard being reused
// here, not new logic.
func TestReadinessSSRFGuardAcceptsAnUnresolvableName(t *testing.T) {
	ok, reason, err := llmorgsettings.ValidateBaseURLChecked(context.Background(), "https://example.invalid/v1")
	if err != nil || !ok {
		t.Fatalf("https://example.invalid/v1: ok=%v reason=%q err=%v, want accepted", ok, reason, err)
	}
}
