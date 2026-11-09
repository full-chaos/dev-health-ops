package authapi

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// workloadBearerOfLength is a workload-typed compact token of exactly n bytes
// with the audience and subject the caller gate admits, so only its length
// decides. The header gets JSON whitespace and the claims a pad member; the
// signature is junk, which only the verifier reads.
func workloadBearerOfLength(t *testing.T, subject string, n int) string {
	t.Helper()
	enc := base64.RawURLEncoding
	for space := 0; space < 4; space++ {
		header := `{` + strings.Repeat(" ", space) + `"alg":"EdDSA","typ":"` + contracts.WorkloadTokenTyp + `","kid":"k1"}`
		for pad := 0; pad < n; pad++ {
			payload := `{"aud":"auth-service","sub":"` + subject + `","pad":"` + strings.Repeat("p", pad) + `"}`
			input := enc.EncodeToString([]byte(header)) + "." + enc.EncodeToString([]byte(payload))
			length := len(input) + 1 + enc.EncodedLen(64)
			if length == n {
				return input + "." + enc.EncodeToString(make([]byte, 64))
			}
			if length > n {
				break
			}
		}
	}
	t.Fatalf("no workload bearer of %d bytes", n)
	return ""
}

// An over-cap bearer that is otherwise an admitted workload token is refused
// from its length alone: no verification and no key-set read. At the cap it
// still reaches the verifier.
func TestAuthorizeCallerRefusesAnOverCapWorkloadTokenBeforeTheVerifier(t *testing.T) {
	f := newAuthorizeFixture(t)
	gate := f.callers()
	counting := &countingVerifier{inner: gate.verifier}
	gate.verifier = counting

	_, reason, err := gate.Authenticate(workloadBearerOfLength(t, f.subject, contracts.WorkloadTokenMaxBytes+1))
	if reason != "oversized" || err != nil || counting.calls != 0 || f.keys.reads != 0 {
		t.Fatalf("over-cap: reason %q err %v, %d verifications, %d key-set reads; want oversized, 0, 0", reason, err, counting.calls, f.keys.reads)
	}
	if _, reason, _ := gate.Authenticate(workloadBearerOfLength(t, f.subject, contracts.WorkloadTokenMaxBytes)); reason == "oversized" || counting.calls != 1 {
		t.Fatalf("at the cap: reason %q, %d verifications; want a verifier decision", reason, counting.calls)
	}
}

// The bound is read before any byte of the bearer is decoded.
func TestClassifyIssueBearerReadsTheBoundFirst(t *testing.T) {
	bearer := workloadBearerOfLength(t, "prn_AAAAAAAAAAAAAAAAAAAAAA", contracts.WorkloadTokenMaxBytes+1)
	if allocs := testing.AllocsPerRun(20, func() { classifyIssueBearer(bearer) }); allocs != 0 {
		t.Fatalf("classifyIssueBearer allocated %.0f times on an over-cap bearer, want 0: it decoded the bearer", allocs)
	}
	if allocs := testing.AllocsPerRun(20, func() { classifyIssueBearer(bearer[:contracts.WorkloadTokenMaxBytes]) }); allocs == 0 {
		t.Fatal("classifyIssueBearer did not decode a bearer at the cap: the allocation probe measures nothing")
	}
}

// On the authorize routes an over-cap workload token gets the garbage answer.
func TestAuthorizeOverCapWorkloadTokenGetsTheGarbageAnswer(t *testing.T) {
	for _, pattern := range []string{PatternAuthorize, PatternAuthorizeBatch} {
		a := newAdmitFixture(t)
		garbage, _ := a.do(a.server, pattern, "garbage", a.payload(pattern), false)
		over, body := a.do(a.server, pattern, workloadBearerOfLength(t, a.subject, contracts.WorkloadTokenMaxBytes+1), a.payload(pattern), false)
		if over.Code != http.StatusUnauthorized || over.Code != garbage.Code || withoutRequestID(over) != withoutRequestID(garbage) || body.n != 0 || a.verifier.calls != 0 {
			t.Errorf("%s: over-cap %d %q, garbage %d %q, body bytes %d, verifications %d; want the garbage 401, 0, 0",
				pattern, over.Code, over.Body, garbage.Code, garbage.Body, body.n, a.verifier.calls)
		}
	}
}

// withoutRequestID is the answer body without the members minted per request
// (request_id, occurred_at).
func withoutRequestID(rec *httptest.ResponseRecorder) string {
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		return rec.Body.String()
	}
	delete(body, "request_id")
	delete(body, "occurred_at")
	out, _ := json.Marshal(body)
	return string(out)
}
