package categorize

import (
	"context"
	"errors"
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
	if got := transport.requests[0].Header.Get("X-Request-Id"); got != att.RequestID {
		t.Fatalf("X-Request-Id = %q, want the attempt's request id %q", got, att.RequestID)
	}
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

func TestAPeerIDRepeatedInOneCallDoesNotCollapseTwoAttempts(t *testing.T) {
	client, transport, _ := newTestClef(t, func(call int, _ *http.Request) (*http.Response, error) {
		var resp *http.Response
		if call == 1 {
			resp, _ = clefAnswer(500, "")
		} else {
			resp, _ = clefAnswer(200, clefOKBody("clef"))
		}
		resp.Header["Cf-Ray"] = []string{"shared-ray-1"}
		return resp, nil
	})
	result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{"model":"clef"}`))
	if err != nil || len(result.Attempts) != 2 {
		t.Fatalf("err %v attempts %d, want 2", err, len(result.Attempts))
	}
	first, second := result.Attempts[0], result.Attempts[1]
	if first.RequestID != "shared-ray-1" || first.RequestIDSource != RequestIDSourceRay {
		t.Fatalf("first attempt id %q source %q, want shared-ray-1 and ray", first.RequestID, first.RequestIDSource)
	}
	if second.RequestID == first.RequestID || second.RequestIDSource != RequestIDSourceClient {
		t.Fatalf("second attempt id %q source %q, want a different id and client", second.RequestID, second.RequestIDSource)
	}
	if sent := transport.requests[1].Header.Get("X-Request-Id"); sent != second.RequestID {
		t.Fatalf("second attempt id %q, want the id it sent %q", second.RequestID, sent)
	}
}

type failingRandom struct{}

func (failingRandom) Read([]byte) (int, error) {
	return 0, errors.New("injected random-source failure")
}

func TestAFailedRandomSourceGivesANonRandomClientIDWithoutPanic(t *testing.T) {
	uuid.SetRand(failingRandom{})
	defer uuid.SetRand(nil)
	client, _, logs := newTestClef(t, func(int, *http.Request) (*http.Response, error) {
		return clefAnswer(200, clefOKBody("clef"))
	})
	var ids []string
	for call := 1; call <= 2; call++ {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("call %d panicked: %v", call, r)
				}
			}()
			result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{"model":"clef"}`))
			if err != nil || len(result.Attempts) != 1 {
				t.Fatalf("call %d: err %v attempts %d", call, err, len(result.Attempts))
			}
			att := result.Attempts[0]
			if att.RequestID == "" || att.RequestIDSource != RequestIDSourceClient {
				t.Fatalf("call %d: id %q source %q, want a non-empty id and client", call, att.RequestID, att.RequestIDSource)
			}
			ids = append(ids, att.RequestID)
		}()
	}
	if len(ids) == 2 && ids[0] == ids[1] {
		t.Fatalf("two calls gave the same id %q", ids[0])
	}
	warns := 0
	for _, line := range strings.Split(logs.String(), "\n") {
		if !strings.Contains(line, `"level":"WARN"`) {
			continue
		}
		warns++
		for _, id := range ids {
			if strings.Contains(line, id) {
				t.Fatalf("a request id value is in a Warn line: %s", line)
			}
		}
	}
	if warns != 2 {
		t.Fatalf("Warn lines = %d, want one for each of the 2 failed mints", warns)
	}
}
