package categorize

import (
	"context"
	"net/http"
	"testing"
)

func TestAPeerIDEqualToAnEarlierClientIDFallsThroughToTheOwnClientID(t *testing.T) {
	firstSent := ""
	client, transport, _ := newTestClef(t, func(call int, req *http.Request) (*http.Response, error) {
		if call == 1 {
			firstSent = req.Header.Get("X-Request-Id")
			return clefAnswer(500, "")
		}
		resp, _ := clefAnswer(200, clefOKBody("clef"))
		resp.Header["Cf-Ray"] = []string{firstSent}
		return resp, nil
	})
	result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{"model":"clef"}`))
	if err != nil || len(result.Attempts) != 2 {
		t.Fatalf("err %v attempts %d, want 2", err, len(result.Attempts))
	}
	first, second := result.Attempts[0], result.Attempts[1]
	if first.RequestID == "" || first.RequestID != firstSent || first.RequestIDSource != RequestIDSourceClient {
		t.Fatalf("first attempt id %q source %q, want its sent id %q and client", first.RequestID, first.RequestIDSource, firstSent)
	}
	if second.RequestID == first.RequestID || second.RequestIDSource != RequestIDSourceClient {
		t.Fatalf("second attempt id %q source %q, want its own id (not %q) and client", second.RequestID, second.RequestIDSource, first.RequestID)
	}
	if sent := transport.requests[1].Header.Get("X-Request-Id"); sent != second.RequestID {
		t.Fatalf("second attempt id %q, want the id it sent %q", second.RequestID, sent)
	}
}
