package billing

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOverflowPageAnswersTheRangeRefusalBeforeAnyStoreCall(t *testing.T) {
	h := handlers{logger: quiet()}
	const org = "37aa5733-344b-54a7-9210-c911e4ccf37a"
	const planted = "PLANTED-TEXT-10656"
	body := func(name string) string {
		return `{"detail":[{"type":"less_than_equal","loc":["query","` + name + `"],"msg":"Input should be less than or equal to 9223372036854775807"}]}`
	}
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		query   string
		loc     string
	}{
		{"subscriptions list", h.listSubscriptions, "offset=99999999999999999999", "offset"},
		{"subscription history", h.subscriptionHistory, "offset=99999999999999999999", "offset"},
		{"invoices list", h.listInvoices, "offset=99999999999999999999", "offset"},
		{"refunds list", h.listRefunds, "offset=99999999999999999999", "offset"},
		{"audit list offset", h.listAudit, "org_id=" + org + "&offset=99999999999999999999", "offset"},
		{"audit list limit", h.listAudit, "org_id=" + org + "&limit=99999999999999999999", "limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/x?"+tc.query+"&x="+planted, nil)
			recorder := httptest.NewRecorder()
			tc.handler(recorder, request)
			if recorder.Code != http.StatusUnprocessableEntity || recorder.Body.String() != body(tc.loc) {
				t.Fatalf("status = %d body = %s, want 422 %s", recorder.Code, recorder.Body.String(), body(tc.loc))
			}
			if strings.Contains(recorder.Body.String(), planted) || strings.Contains(recorder.Body.String(), "99999999999999999999") {
				t.Fatalf("input text reached the body: %s", recorder.Body.String())
			}
		})
	}
}
