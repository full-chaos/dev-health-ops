package syncadmin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const overflowOffsetBody = `{"detail":[{"type":"less_than_equal","loc":["query","offset"],"msg":"Input should be less than or equal to 9223372036854775807"}]}`

func TestOverflowOffsetAnswersTheRangeRefusalBeforeAnyStoreCall(t *testing.T) {
	h := &handlers{}
	for name, handler := range map[string]http.HandlerFunc{"backfill jobs": h.listBackfillJobs, "config jobs": h.listJobs} {
		t.Run(name, func(t *testing.T) {
			const planted = "PLANTED-TEXT-10656"
			request := httptest.NewRequest(http.MethodGet, "/x?offset=99999999999999999999&x="+planted, nil)
			recorder := httptest.NewRecorder()
			handler(recorder, request)
			if recorder.Code != http.StatusUnprocessableEntity || recorder.Body.String() != overflowOffsetBody {
				t.Fatalf("status = %d body = %s, want 422 %s", recorder.Code, recorder.Body.String(), overflowOffsetBody)
			}
			if strings.Contains(recorder.Body.String(), planted) || strings.Contains(recorder.Body.String(), "99999999999999999999") {
				t.Fatalf("input text reached the body: %s", recorder.Body.String())
			}
		})
	}
}
