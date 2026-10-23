package customerpush

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

func TestOverflowOffsetAnswersTheRangeRefusalBeforeAnyStoreCall(t *testing.T) {
	h := &handlers{}
	for _, tc := range []struct{ name, query, loc string }{
		{"list batches", "offset=99999999999999999999", "offset"},
		{"batch detail", "rejected_records_offset=99999999999999999999", "rejected_records_offset"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := h.listSourceBatches
			if tc.name == "batch detail" {
				handler = h.getBatch
			}
			req := httptest.NewRequest(http.MethodGet, "/x?"+tc.query, nil)
			req = req.WithContext(policy.WithUser(req.Context(), &policy.User{IsSuperuser: true}))
			rec := httptest.NewRecorder()
			handler(rec, req)
			want := `{"detail":[{"type":"less_than_equal","loc":["query","` + tc.loc + `"],"msg":"Input should be less than or equal to 9223372036854775807"}]}`
			if rec.Code != http.StatusUnprocessableEntity || rec.Body.String() != want {
				t.Fatalf("status = %d body = %s, want 422 %s", rec.Code, rec.Body.String(), want)
			}
		})
	}
}
