package producttelemetry

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type payloadVector struct {
	Name        string `json:"name"`
	PayloadJSON string `json:"payloadJson"`
	Drop        bool   `json:"drop"`
	Reason      string `json:"reason"`
}

type orgHashVector struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	OK    bool   `json:"ok"`
}

func loadVectors(t *testing.T) ([]payloadVector, []orgHashVector) {
	t.Helper()
	var table struct {
		Payload []payloadVector `json:"payloadVectors"`
		OrgHash []orgHashVector `json:"orgIdHashVectors"`
	}
	if err := json.Unmarshal(payloadRulesJSON, &table); err != nil {
		t.Fatal(err)
	}
	if len(table.Payload) < 30 || len(table.OrgHash) < 10 {
		t.Fatalf("the rule table holds %d payload vectors and %d hash vectors: too few to prove the rules", len(table.Payload), len(table.OrgHash))
	}
	return table.Payload, table.OrgHash
}

func TestPayloadRulesVectors(t *testing.T) {
	vectors, _ := loadVectors(t)
	dropped := map[string]int{}
	for _, vector := range vectors {
		t.Run(vector.Name, func(t *testing.T) {
			decoded, err := pyjson.DecodeString(vector.PayloadJSON)
			if err != nil {
				t.Fatal(err)
			}
			refusal := refusePayload(decoded.(*pyjson.Object))
			switch {
			case vector.Drop && refusal == nil:
				t.Fatalf("payload %s was allowed, want a %s refusal", vector.PayloadJSON, vector.Reason)
			case vector.Drop && refusal.reason != vector.Reason:
				t.Fatalf("refusal = %s, want %s", refusal.reason, vector.Reason)
			case !vector.Drop && refusal != nil:
				t.Fatalf("payload %s was refused (%s), want it allowed", vector.PayloadJSON, refusal.reason)
			}
			if refusal != nil {
				dropped[refusal.reason]++
			}
		})
	}
	for _, reason := range []string{refusedKey, refusedString, refusedNumber} {
		if dropped[reason] == 0 {
			t.Errorf("no vector of the table is refused for the reason %q: the rule is not pinned", reason)
		}
	}
}

func TestTheOrgHashRuleVectors(t *testing.T) {
	_, vectors := loadVectors(t)
	for _, vector := range vectors {
		t.Run(vector.Name, func(t *testing.T) {
			if got := ruleOrgIDHash.MatchString(vector.Value); got != vector.OK {
				t.Fatalf("%q matches = %v, want %v", vector.Value, got, vector.OK)
			}
		})
	}
}

const (
	secretMail  = "person@example.com"
	secretToken = "tok_ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func eventWith(id int, payload string) string {
	return fmt.Sprintf(`{"name":"page_viewed","schemaVersion":"1","eventId":"e%d","ts":"2026-09-23T02:00:00Z","sessionId":"s","anonymousUserId":"a","payload":%s}`, id, payload)
}

func post(t *testing.T, streams *recordingStreams, logs *bytes.Buffer, body string) *httptest.ResponseRecorder {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logs, nil))
	route := Routes(streams, logger)[0]
	request := httptest.NewRequest(http.MethodPost, route.Pattern, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	route.Handler.ServeHTTP(recorder, request)
	return recorder
}

func storedEventIDs(t *testing.T, streams *recordingStreams) []string {
	t.Helper()
	if len(streams.fields) != 1 {
		t.Fatalf("appends = %d, want 1", len(streams.fields))
	}
	for _, field := range streams.fields[0] {
		if field[0] != "events" {
			continue
		}
		var events []struct {
			EventID string `json:"eventId"`
		}
		if err := json.Unmarshal([]byte(field[1]), &events); err != nil {
			t.Fatal(err)
		}
		ids := make([]string, len(events))
		for index, event := range events {
			ids[index] = event.EventID
		}
		return ids
	}
	t.Fatal("the stream entry has no events field")
	return nil
}

func TestOnlyTheBadEventIsDropped(t *testing.T) {
	for _, badAt := range []int{0, 249, 499} {
		t.Run(fmt.Sprintf("bad event at %d", badAt), func(t *testing.T) {
			events := make([]string, 500)
			for index := range events {
				payload := `{"feature":"work_graph","count":1}`
				if index == badAt {
					payload = `{"view":"` + secretMail + `"}`
				}
				events[index] = eventWith(index, payload)
			}
			streams, logs := &recordingStreams{}, &bytes.Buffer{}
			recorder := post(t, streams, logs, `{"events":[`+strings.Join(events, ",")+`]}`)
			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status = %d, body %s", recorder.Code, recorder.Body.String())
			}
			body := recorder.Body.String()
			if !strings.Contains(body, `"items_received":499`) || !strings.Contains(body, `"items_dropped":1`) {
				t.Fatalf("body %s, want items_received 499 and items_dropped 1", body)
			}
			ids := storedEventIDs(t, streams)
			if len(ids) != 499 {
				t.Fatalf("stored %d events, want 499", len(ids))
			}
			next := 0
			for _, id := range ids {
				if next == badAt {
					next++
				}
				if want := fmt.Sprintf("e%d", next); id != want {
					t.Fatalf("stored event %s, want %s: the order of the good events changed", id, want)
				}
				next++
			}
		})
	}
}

func TestAllBadEventsAreDroppedWithoutAWrite(t *testing.T) {
	streams, logs := &recordingStreams{}, &bytes.Buffer{}
	body := `{"orgIdHash":"` + testOrgHash + `","events":[` +
		eventWith(1, `{"a":"`+secretMail+`"}`) + `,` + eventWith(2, `{"a b":"x"}`) + `,` + eventWith(3, `{"a":NaN}`) + `]}`
	recorder := post(t, streams, logs, body)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	got := recorder.Body.String()
	for _, want := range []string{`"items_received":0`, `"items_dropped":3`, `"stream":"product-telemetry:` + testOrgHash + `:events"`} {
		if !strings.Contains(got, want) {
			t.Errorf("body %s lacks %s", got, want)
		}
	}
	if len(streams.appended) != 0 {
		t.Fatalf("appends = %v, want none: a batch with no event left writes no entry", streams.appended)
	}
}

func TestADroppedValueIsNeverEchoed(t *testing.T) {
	streams, logs := &recordingStreams{}, &bytes.Buffer{}
	body := `{"events":[` + eventWith(1, `{"ok":"fine"}`) + `,` +
		eventWith(2, `{"view":"`+secretMail+`"}`) + `,` +
		eventWith(3, `{"view":"`+secretToken+`"}`) + `,` +
		eventWith(4, `{"`+secretMail+`":"x"}`) + `]}`
	recorder := post(t, streams, logs, body)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, body %s", recorder.Code, recorder.Body.String())
	}
	var stored strings.Builder
	for _, fields := range streams.fields {
		for _, field := range fields {
			stored.WriteString(field[1])
		}
	}
	for name, text := range map[string]string{"response": recorder.Body.String(), "stream": stored.String(), "log": logs.String()} {
		for _, secret := range []string{secretMail, secretToken, "person", "ABCDEFGHIJ"} {
			if strings.Contains(text, secret) {
				t.Errorf("the %s holds %q from a refused value", name, secret)
			}
		}
	}
	line := logs.String()
	for _, want := range []string{"product telemetry events dropped", "dropped=3", "page_viewed", invalidKeyToken, "view"} {
		if !strings.Contains(line, want) {
			t.Errorf("the log %q lacks %q", line, want)
		}
	}
	if strings.Count(line, "product telemetry events dropped") != 1 {
		t.Errorf("the log holds %d lines for one batch, want 1", strings.Count(line, "product telemetry events dropped"))
	}
}

func TestABadOrgHashCreatesNoKey(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"top level, plain text", `{"orgIdHash":"acme","events":[` + validEvent + `]}`},
		{"top level, snake case alias", `{"org_id_hash":"acme","events":[` + validEvent + `]}`},
		{"top level, path", `{"orgIdHash":"../x","events":[` + validEvent + `]}`},
		{"top level, lone surrogate", `{"orgIdHash":"\ud800","events":[` + validEvent + `]}`},
		{"top level, paired surrogates", `{"orgIdHash":"😀","events":[` + validEvent + `]}`},
		{"top level, long", `{"orgIdHash":"` + strings.Repeat("a", 100000) + `","events":[` + validEvent + `]}`},
		{"top level, number", `{"orgIdHash":5,"events":[` + validEvent + `]}`},
		{"event level", `{"events":[{"name":"page_viewed","schemaVersion":"1","eventId":"e1","ts":"2026-09-23T02:00:00Z","sessionId":"s","anonymousUserId":"a","orgIdHash":"acme","payload":{}}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			streams, logs := &recordingStreams{}, &bytes.Buffer{}
			recorder := post(t, streams, logs, tc.body)
			if recorder.Code != http.StatusUnprocessableEntity {
				t.Fatalf("status = %d, want 422; body %.200s", recorder.Code, recorder.Body.String())
			}
			if len(streams.appended) != 0 {
				t.Fatalf("appends = %v: a bad orgIdHash created a stream key", streams.appended)
			}
		})
	}
}

// The typed events of the web client, each with a payload as its emitter
// builds it: none may be dropped.
func TestEveryWebEventPayloadPasses(t *testing.T) {
	pattern := "/people/[person_id]/metrics/[metric]"
	payloads := map[string]string{
		"page_viewed":           `{"routePattern":"` + pattern + `","page":"` + pattern + `","referrerRoutePattern":null}`,
		"feature_viewed":        `{"feature":"work_graph","surface":"quadrant_panel","routePattern":"` + pattern + `"}`,
		"filter_changed":        `{"view":"investment","filterKey":"issueType","valueCount":3,"isCustomDateRange":false}`,
		"chart_interacted":      `{"chart":"timeseries","action":"point_selected","surface":"home","scope":null}`,
		"navigation_interacted": `{"group":"delivery","item":"work_graph","action":"item_selected"}`,
		"guide_opened":          `{"guide":"onboarding.first_sync","surface":"help_menu"}`,
		"session_started":       `{"entryRoutePattern":"/"}`,
		"session_ended":         `{"durationMs":123456.5,"pagesViewed":4,"interactions":17}`,
		"client_error":          `{"boundary":"route","digest":"2103915434","errorClass":"TypeError","routePattern":"` + pattern + `"}`,
	}
	if len(payloads) != len(eventNames) {
		t.Fatalf("%d fixtures for %d event names", len(payloads), len(eventNames))
	}
	for _, name := range eventNames {
		t.Run(name, func(t *testing.T) {
			event := strings.Replace(eventWith(1, payloads[name]), `"page_viewed"`, `"`+name+`"`, 1)
			streams, logs := &recordingStreams{}, &bytes.Buffer{}
			recorder := post(t, streams, logs, `{"events":[`+event+`]}`)
			if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"items_dropped":0`) {
				t.Fatalf("status = %d, body %s", recorder.Code, recorder.Body.String())
			}
			if ids := storedEventIDs(t, streams); len(ids) != 1 {
				t.Fatalf("stored %v, want the one event", ids)
			}
		})
	}
}

// The structure errors of a payload stay a 422 of the whole batch, as before.
func TestAStructureErrorOfAPayloadStaysAWholeBatch422(t *testing.T) {
	streams, logs := &recordingStreams{}, &bytes.Buffer{}
	recorder := post(t, streams, logs, `{"events":[`+eventWith(1, `{"a":"ok"}`)+`,`+eventWith(2, `{"a":["x"]}`)+`]}`)
	if recorder.Code != http.StatusUnprocessableEntity || len(streams.appended) != 0 {
		t.Fatalf("status = %d, appends = %v, want 422 and no write", recorder.Code, streams.appended)
	}
}

// The empty org hash names the anonymous stream, as before: it creates no new key.
func TestAnEmptyOrgHashStaysTheAnonymousStream(t *testing.T) {
	streams, logs := &recordingStreams{}, &bytes.Buffer{}
	recorder := post(t, streams, logs, `{"orgIdHash":"","events":[`+validEvent+`]}`)
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"stream":"product-telemetry:anonymous:events"`) {
		t.Fatalf("status = %d, body %s", recorder.Code, recorder.Body.String())
	}
}

// The shared event of the venue oracle (every pt: request of its protected-routes corpus)
// carries these payload values. Its non-ASCII string is the only thing the value rules
// refuse: remove it and the event is stored, so the drop of "pt: empty org hash" is the
// non-ASCII rule and no other refusal.
const venueSharedEvent = `{"name":"page_viewed","schemaVersion":"1","eventId":"e1","ts":"2026-09-23T02:00:00.123Z","sessionId":"s","anonymousUserId":"a","payload":%s}`

func TestTheEmptyOrgHashVenueRequestIsDroppedForTheNonASCIIStringOnly(t *testing.T) {
	withString := fmt.Sprintf(venueSharedEvent, `{"f":1.5,"i":7,"b":true,"n":null,"s":"é x","big":123456789012345678901234567890}`)
	streams, logs := &recordingStreams{}, &bytes.Buffer{}
	recorder := post(t, streams, logs, `{"orgIdHash":"","events":[`+withString+`]}`)
	body := recorder.Body.String()
	if recorder.Code != http.StatusAccepted || !strings.Contains(body, `"items_received":0`) || !strings.Contains(body, `"items_dropped":1`) {
		t.Fatalf("status = %d, body %s, want 202 with items_received 0 and items_dropped 1", recorder.Code, body)
	}
	if len(streams.appended) != 0 {
		t.Fatalf("appends = %v, want none", streams.appended)
	}
	// The cause is the string rule on the key "s", not the key, number or org hash rules.
	line := logs.String()
	for _, want := range []string{`reasons="string=1"`, "keys=s", "events=page_viewed"} {
		if !strings.Contains(line, want) {
			t.Errorf("the log %q lacks %q", line, want)
		}
	}
	for _, other := range []string{"key=", "number=", invalidKeyToken} {
		if strings.Contains(line, `reasons="`+other) || strings.Contains(line, "keys="+other) {
			t.Errorf("the log %q names another cause %q", line, other)
		}
	}
	// Control: the same request with an ASCII value in place of the non-ASCII string is stored.
	ascii := fmt.Sprintf(venueSharedEvent, `{"f":1.5,"i":7,"b":true,"n":null,"s":"ex","big":123456789012345678901234567890}`)
	streams, logs = &recordingStreams{}, &bytes.Buffer{}
	recorder = post(t, streams, logs, `{"orgIdHash":"","events":[`+ascii+`]}`)
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"items_received":1`) || !strings.Contains(recorder.Body.String(), `"items_dropped":0`) {
		t.Fatalf("control: status = %d, body %s, want the event stored", recorder.Code, recorder.Body.String())
	}
	// Control: every other payload value alone is allowed, and the empty org hash is the anonymous stream.
	if len(streams.appended) != 1 || streams.appended[0] != "product-telemetry:anonymous:events" {
		t.Fatalf("control: appends = %v, want the anonymous stream once", streams.appended)
	}
}
