package producttelemetry

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
)

const askDevPayload = `{"action":"terminal","surface":"panel","terminal":"limited","turnCount":3}`

func askDevEvent(id int) string {
	return strings.Replace(eventWith(id, askDevPayload), `"page_viewed"`, `"ask_dev_interacted"`, 1)
}

// The web client's Ask Dev interaction event is accepted and stored.
func TestTheAskDevInteractionEventIsAccepted(t *testing.T) {
	streams, logs := &recordingStreams{}, &bytes.Buffer{}
	recorder := post(t, streams, logs, `{"events":[`+askDevEvent(1)+`]}`)
	if recorder.Code != http.StatusAccepted || !strings.Contains(recorder.Body.String(), `"items_dropped":0`) {
		t.Fatalf("status = %d, body %.300s", recorder.Code, recorder.Body.String())
	}
	if ids := storedEventIDs(t, streams); len(ids) != 1 {
		t.Fatalf("stored %v, want the one event", ids)
	}
}

// An event name the intake does not know refuses the WHOLE batch with 422, so
// a client that emits a new name before ops accepts it loses the events that
// travel with it. This is why a new event name ships here first.
func TestAnUnknownEventNameRefusesTheWholeBatch(t *testing.T) {
	streams, logs := &recordingStreams{}, &bytes.Buffer{}
	unknown := strings.Replace(eventWith(2, `{}`), `"page_viewed"`, `"ask_dev_unknown_event"`, 1)
	recorder := post(t, streams, logs, `{"events":[`+eventWith(1, `{}`)+`,`+unknown+`]}`)
	if recorder.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422; body %.300s", recorder.Code, recorder.Body.String())
	}
	if len(streams.appended) != 0 {
		t.Fatalf("appends = %v: the valid event of a refused batch was stored", streams.appended)
	}
}
