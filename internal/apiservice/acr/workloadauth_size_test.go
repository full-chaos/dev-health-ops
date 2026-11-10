package acr

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// countingWLVerifier counts the verifications that reach the workload verifier.
type countingWLVerifier struct {
	inner WorkloadVerifier
	calls int
}

func (c *countingWLVerifier) Verify(token string, now time.Time) (*authverify.WorkloadClaims, error) {
	c.calls++
	return c.inner.Verify(token, now)
}

// wlTokenOfLength signs a workload-shaped token of exactly n bytes: the header
// gets JSON whitespace and the claims a pad member, so only the length differs.
func wlTokenOfLength(t *testing.T, s wlSigner, n int) string {
	t.Helper()
	enc := base64.RawURLEncoding
	header, _ := json.Marshal(wlHeader())
	for space := 0; space < 4; space++ {
		h := strings.Replace(string(header), "{", "{"+strings.Repeat(" ", space), 1)
		for pad := 0; pad < n; pad++ {
			claims := wlClaims()
			claims["pad"] = strings.Repeat("p", pad)
			p, _ := json.Marshal(claims)
			input := enc.EncodeToString([]byte(h)) + "." + enc.EncodeToString(p)
			length := len(input) + 1 + enc.EncodedLen(64)
			if length == n {
				return input + "." + enc.EncodeToString(make([]byte, 64))
			}
			if length > n {
				break
			}
		}
	}
	t.Fatalf("no workload-shaped token of %d bytes", n)
	return ""
}

type wlAnswer struct {
	code   int
	body   string
	header http.Header
}

func answerOf(h http.Handler, path, bearer string) wlAnswer {
	rec := do(h, path, "Bearer "+bearer)
	return wlAnswer{rec.Code, rec.Body.String(), rec.Header()}
}

// An over-cap bearer gets the garbage answer, byte for byte, on every route, is
// refused as a workload token (outcome workload_refused, reason contract) and
// never reaches the verifier. The workload-shaped one used to reach it.
func TestOverCapBearerIsRefusedBeforeTheVerifier(t *testing.T) {
	signer := newWLSigner(t)
	overCap := map[string]string{
		"workload header": wlTokenOfLength(t, signer, contracts.WorkloadTokenMaxBytes+1),
		"garbage":         strings.Repeat("x", contracts.WorkloadTokenMaxBytes+1),
	}
	for _, route := range wlRoutesUnderTest {
		for name, token := range overCap {
			t.Run(route.key+"/"+name, func(t *testing.T) {
				garbage := answerOf(newWLHarness(t, wlAuth(t, signer, "dev-health-api")).handler, route.path, "garbage")
				counting := &countingWLVerifier{inner: wlAuth(t, signer, "dev-health-api").Verifier}
				auth := wlAuth(t, signer, "dev-health-api")
				auth.Verifier = counting
				h := newWLHarness(t, auth)
				before := wlCounts(t, "devhealth_acr_workload_refused_total")
				got := answerOf(h.handler, route.path, token)
				if counting.calls != 0 {
					t.Fatalf("%d verifications of a %d-byte bearer, want 0", counting.calls, len(token))
				}
				if !reflect.DeepEqual(got, garbage) {
					t.Fatalf("over-cap answer %+v, want the garbage answer %+v", got, garbage)
				}
				if delta := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); !reflect.DeepEqual(delta, map[string]int64{route.key + "/" + ReasonContract: 1}) {
					t.Fatalf("refused counter delta = %v, want %s/contract once", delta, route.key)
				}
				if route.key == routeEntitlement {
					if want := []auditRow{{"", someOrg, outcomeWorkloadRefused}}; !reflect.DeepEqual(h.creds.audits, want) {
						t.Fatalf("audits %+v, want %+v", h.creds.audits, want)
					}
				}
			})
		}
	}
}

// At exactly the bound a workload-shaped bearer still reaches the verifier, and
// a garbage one is still not a workload token.
func TestBearerAtTheCapIsStillDecided(t *testing.T) {
	signer := newWLSigner(t)
	atCap := wlTokenOfLength(t, signer, contracts.WorkloadTokenMaxBytes)
	garbage := strings.Repeat("x", contracts.WorkloadTokenMaxBytes)
	for _, route := range wlRoutesUnderTest {
		t.Run(route.key, func(t *testing.T) {
			counting := &countingWLVerifier{inner: wlAuth(t, signer, "dev-health-api").Verifier}
			auth := wlAuth(t, signer, "dev-health-api")
			auth.Verifier = counting
			h := newWLHarness(t, auth)
			do(h.handler, route.path, "Bearer "+atCap)
			if counting.calls != 1 {
				t.Fatalf("%d verifications of a token at the cap, want 1", counting.calls)
			}
			before := wlCounts(t, "devhealth_acr_workload_refused_total")
			do(h.handler, route.path, "Bearer "+garbage)
			if delta := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); len(delta) != 0 {
				t.Fatalf("garbage at the cap was counted as a workload refusal: %v", delta)
			}
		})
	}
}

// The bound is read before any byte of the bearer is decoded.
func TestOverCapBearerIsNotDecoded(t *testing.T) {
	token := wlTokenOfLength(t, newWLSigner(t), contracts.WorkloadTokenMaxBytes+1)
	if allocs := testing.AllocsPerRun(20, func() { isWorkloadToken(token) }); allocs != 0 {
		t.Fatalf("isWorkloadToken allocated %.0f times on an over-cap bearer, want 0: it decoded the header", allocs)
	}
	if allocs := testing.AllocsPerRun(20, func() { isWorkloadToken(token[:contracts.WorkloadTokenMaxBytes]) }); allocs == 0 {
		t.Fatal("isWorkloadToken did not decode a bearer at the cap: the allocation probe measures nothing")
	}
}

// A prefix-classed bearer stays another credential class whatever its length.
func TestOverCapPrefixedBearerStaysAnotherClass(t *testing.T) {
	signer := newWLSigner(t)
	h := newWLHarness(t, wlAuth(t, signer, "dev-health-api"))
	before := wlCounts(t, "devhealth_acr_workload_refused_total")
	do(h.handler, entitlementPath+someOrg, "Bearer svc_acr_"+strings.Repeat("a", contracts.WorkloadTokenMaxBytes))
	if delta := wlDelta(before, wlCounts(t, "devhealth_acr_workload_refused_total")); len(delta) != 0 {
		t.Fatalf("a prefixed bearer was counted as a workload refusal: %v", delta)
	}
	if want := []auditRow{{"", someOrg, outcomeMissingToken}}; !reflect.DeepEqual(h.creds.audits, want) {
		t.Fatalf("audits %+v, want %+v", h.creds.audits, want)
	}
}
