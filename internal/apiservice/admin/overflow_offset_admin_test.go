package admin

import (
	"net/http"
	"strings"
	"testing"
)

const overflowOffsetBody = `{"detail":[{"type":"less_than_equal","loc":["query","offset"],"msg":"Input should be less than or equal to 9223372036854775807"}]}`

// TestOverflowOffsetAnswersTheListRoutesRangeRefusal pins an offset past int64
// answering the pinned 422 range refusal, not a 500, on every admin list
// route that pages. The handlers run with no store: the refusal must answer
// before any store call.
func TestOverflowOffsetAnswersTheListRoutesRangeRefusal(t *testing.T) {
	withProcessLicense(t)
	h := &handlers{}
	routes := []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"audit list", h.listAuditLogs},
		{"platform audit list", h.listPlatformAuditLogs},
		{"ip allowlist list", h.listIPAllowlist},
		{"retention list", h.listRetentionPolicies},
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			rec := answerWith(route.handler, http.MethodGet, compose(withOrgClaim, withQuery("offset=99999999999999999999")))
			if rec.Code != http.StatusUnprocessableEntity || rec.Body.String() != overflowOffsetBody {
				t.Fatalf("status = %d body = %s, want 422 %s", rec.Code, rec.Body.String(), overflowOffsetBody)
			}
		})
	}
}

// TestOverflowOffsetRefusalEchoesNoInputText: the refusal echoes no input
// text (ruling 107), as the negative-offset 422 does not.
func TestOverflowOffsetRefusalEchoesNoInputText(t *testing.T) {
	withProcessLicense(t)
	h := &handlers{}
	const planted = "PLANTED-TEXT-10656"
	rec := answerWith(h.listRetentionPolicies, http.MethodGet, compose(withOrgClaim, withQuery("offset=99999999999999999999&x="+planted)))
	if strings.Contains(rec.Body.String(), "99999999999999999999") || strings.Contains(rec.Body.String(), planted) {
		t.Fatalf("an unrelated query value reached the body: %s", rec.Body.String())
	}
}

func TestNegativeOffsetStaysThePinned422(t *testing.T) {
	withProcessLicense(t)
	h := &handlers{}
	rec := answerWith(h.listIPAllowlist, http.MethodGet, compose(withOrgClaim, withQuery("offset=-1")))
	want := `{"detail":[{"type":"greater_than_equal","loc":["query","offset"],"msg":"Input should be greater than or equal to 0"}]}`
	if rec.Code != http.StatusUnprocessableEntity || rec.Body.String() != want {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}
