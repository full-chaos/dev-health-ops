package categorize

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func clefWithHeaders(t *testing.T, headers map[string]string) (SystemOneAttempt, *clefTransport) {
	t.Helper()
	client, transport, _ := newTestClef(t, func(int, *http.Request) (*http.Response, error) {
		resp, err := clefAnswer(200, clefOKBody("clef"))
		for name, value := range headers {
			resp.Header[http.CanonicalHeaderKey(name)] = []string{value}
		}
		return resp, err
	})
	result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{"model":"clef"}`))
	if err != nil {
		t.Fatalf("an unusable id header must never fail the call: %v", err)
	}
	if len(result.Attempts) != 1 {
		t.Fatalf("attempts = %d, want 1", len(result.Attempts))
	}
	return result.Attempts[0], transport
}

func requireClientID(t *testing.T, att SystemOneAttempt, transport *clefTransport) {
	t.Helper()
	parsed, err := uuid.Parse(att.RequestID)
	if err != nil || parsed.Version() != 7 {
		t.Fatalf("request id = %q, want a UUID v7", att.RequestID)
	}
	if att.RequestIDSource != RequestIDSourceClient {
		t.Fatalf("source = %q, want client", att.RequestIDSource)
	}
	for _, values := range transport.requests[0].Header {
		for _, value := range values {
			if value == att.RequestID {
				return
			}
		}
	}
	t.Fatal("the client id is not sent as a request header")
}

func TestClefAttemptTakesCfRayWhenNoGatewayRouted(t *testing.T) {
	att, _ := clefWithHeaders(t, map[string]string{"cf-ray": "8a1b2c3d4e5f6789-AMS"})
	if att.RequestID != "8a1b2c3d4e5f6789-AMS" || att.RequestIDSource != RequestIDSourceRay {
		t.Fatalf("id %q source %q, want the cf-ray value and ray", att.RequestID, att.RequestIDSource)
	}
}

func TestClefAttemptIgnoresCfAigLogIDWhenTheCallIsNotRoutedThroughTheGateway(t *testing.T) {
	att, _ := clefWithHeaders(t, map[string]string{"cf-aig-log-id": "log-1234", "cf-ray": "8a1b2c3d4e5f6789-AMS"})
	if att.RequestID != "8a1b2c3d4e5f6789-AMS" || att.RequestIDSource != RequestIDSourceRay {
		t.Fatalf("id %q source %q, want the cf-ray value and ray", att.RequestID, att.RequestIDSource)
	}
}

func TestClefAttemptWithNoIDHeaderMintsAClientUUIDv7AndSendsIt(t *testing.T) {
	att, transport := clefWithHeaders(t, nil)
	requireClientID(t, att, transport)
}

func TestClefAttemptWithEmptyIDHeadersFallsThroughToTheClientID(t *testing.T) {
	att, transport := clefWithHeaders(t, map[string]string{"cf-ray": "", "cf-aig-log-id": ""})
	requireClientID(t, att, transport)
}

func TestClefAttemptWithAnUnknownIDHeaderFallsThroughWithoutError(t *testing.T) {
	att, transport := clefWithHeaders(t, map[string]string{"x-some-other-id": "abc123"})
	requireClientID(t, att, transport)
}

func TestClefAttemptWithAMalformedCfRayFallsThroughWithoutError(t *testing.T) {
	att, transport := clefWithHeaders(t, map[string]string{"cf-ray": "not an id!!" + strings.Repeat("x", 80)})
	requireClientID(t, att, transport)
}

func TestEveryClefAttemptOfARetriedCallHasItsOwnRequestID(t *testing.T) {
	client, _, _ := newTestClef(t, func(call int, _ *http.Request) (*http.Response, error) {
		if call == 1 {
			return clefAnswer(500, "")
		}
		return clefAnswer(200, clefOKBody("clef"))
	})
	result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{"model":"clef"}`))
	if err != nil || len(result.Attempts) != 2 {
		t.Fatalf("err %v attempts %d, want 2", err, len(result.Attempts))
	}
	if result.Attempts[0].RequestID == "" || result.Attempts[1].RequestID == "" || result.Attempts[0].RequestID == result.Attempts[1].RequestID {
		t.Fatalf("ids = %q and %q, want two non-empty different ids", result.Attempts[0].RequestID, result.Attempts[1].RequestID)
	}
	if result.RequestID != result.Attempts[1].RequestID {
		t.Fatalf("result id = %q, want the id of the last attempt", result.RequestID)
	}
}
