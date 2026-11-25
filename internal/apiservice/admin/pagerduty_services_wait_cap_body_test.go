package admin

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

const plantedPagerDutyBody = "planted-provider-body-marker"

type bodyPagerDuty struct {
	status  int
	headers http.Header
	calls   int
}

func (d *bodyPagerDuty) Do(request *http.Request) (*http.Response, error) {
	d.calls++
	return &http.Response{StatusCode: d.status, Header: d.headers.Clone(), Request: request,
		Body: io.NopCloser(strings.NewReader(`{"error":{"message":"` + plantedPagerDutyBody + `"}}`))}, nil
}

// The terminal answer of a refused wait keeps the status class of the response only: the provider's body text is in
// no field of it (CHAOS-14621).
func TestARefusedPagerDutyWaitCarriesNoProviderBodyText(t *testing.T) {
	for _, status := range []int{429, 503} {
		doer := &bodyPagerDuty{status: status, headers: http.Header{"Retry-After": {"86400"}}}
		h := &handlers{pagerDutySleep: func(context.Context, time.Duration) error { return nil }}
		body, failure := h.pagerDutyGET(context.Background(), doer, pagerDutyTarget, pagerDutyAuthForTest())
		if failure == nil || failure.err != nil || doer.calls != 1 {
			t.Fatalf("%d: failure %+v requests %d, WANT a terminal answer after 1 request", status, failure, doer.calls)
		}
		text := fmt.Sprintf("%s|%s|%s|%v|%s", body, failure.detail, failure.logNote, failure.header, http.StatusText(failure.status))
		if strings.Contains(text, plantedPagerDutyBody) {
			t.Fatalf("%d: the provider body text is in the terminal answer: %q", status, text)
		}
	}
}
