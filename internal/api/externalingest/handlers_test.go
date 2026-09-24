package externalingest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestHandleGetSchema exercises router.py's get_schema directly -- no
// database needed, since schema discovery is unauthenticated.
func TestHandleGetSchema(t *testing.T) {
	deps := Deps{routeLimiters: newRouteLimiters(nil)}
	handler := deps.handleGetSchema()

	t.Run("unknown version is 404", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/x", nil)
		r.SetPathValue("schema_version", "external-ingest.v99")
		recorder := httptest.NewRecorder()
		handler(recorder, r)
		if recorder.Code != 404 {
			t.Fatalf("status = %d, want 404", recorder.Code)
		}
	})

	t.Run("known version returns the document with an ETag, and a matching If-None-Match 304s", func(t *testing.T) {
		r := httptest.NewRequest("GET", "/x", nil)
		r.SetPathValue("schema_version", schemaVersion)
		recorder := httptest.NewRecorder()
		handler(recorder, r)
		if recorder.Code != 200 {
			t.Fatalf("status = %d, want 200, body=%s", recorder.Code, recorder.Body.String())
		}
		etag := recorder.Header().Get("ETag")
		if etag == "" {
			t.Fatal("no ETag header")
		}

		cached := httptest.NewRequest("GET", "/x", nil)
		cached.SetPathValue("schema_version", schemaVersion)
		cached.Header.Set("If-None-Match", etag)
		cachedRecorder := httptest.NewRecorder()
		handler(cachedRecorder, cached)
		if cachedRecorder.Code != 304 {
			t.Fatalf("status = %d, want 304 for a matching If-None-Match", cachedRecorder.Code)
		}
		if got := cachedRecorder.Header().Get("ETag"); got != etag {
			t.Fatalf("304 ETag = %q, want %q (a 304 must carry the validator)", got, etag)
		}
	})
}

// TestRecoverToIngestErrorAnswersWithThisPackagesOwnEnvelope pins that an
// unhandled panic on this route group still writes errors.py's {"error":
// {...}} shape -- not the app-wide {"detail": ...} shape the shared
// transport middleware uses elsewhere -- because this package's own
// recoverToIngestError runs first.
func TestRecoverToIngestErrorAnswersWithThisPackagesOwnEnvelope(t *testing.T) {
	panicking := recoverToIngestError(nil, func(http.ResponseWriter, *http.Request) {
		panic("induced unhandled error")
	})

	r := httptest.NewRequest("GET", "/x", nil)
	recorder := httptest.NewRecorder()
	panicking(recorder, r)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", recorder.Code)
	}
	var body struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("response is not this package's {\"error\":...} envelope: %v, body=%s", err, recorder.Body.String())
	}
	if body.Error.Code != "internal_error" {
		t.Fatalf("error.code = %q, want internal_error", body.Error.Code)
	}
	if body.Error.Message == "" {
		t.Fatal("error.message is empty")
	}
	// Calling the wrapped handler directly (not through the shared server's
	// own Recover middleware) means a miss here would panic this test
	// itself, not fall through to some other envelope -- the strongest
	// available proof that recoverToIngestError, not the shared transport's
	// Recover, is the one that caught it.
}
