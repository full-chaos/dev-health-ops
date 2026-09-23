package externalingest

import (
	"net/http/httptest"
	"testing"
	"time"
)

func TestPageParamsClampsToPythonsBounds(t *testing.T) {
	cases := []struct {
		name                  string
		query                 string
		wantLimit, wantOffset int
	}{
		{"defaults", "", 50, 0},
		{"over the 200 cap", "limit=201", 200, 0},
		{"under the 1 floor", "limit=0", 1, 0},
		{"negative limit", "limit=-5", 1, 0},
		{"negative offset ignored, default kept", "offset=-1", 50, 0},
		{"within bounds", "limit=10&offset=20", 10, 20},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/x?"+c.query, nil)
			limit, offset := pageParams(r, "limit", "offset")
			if limit != c.wantLimit || offset != c.wantOffset {
				t.Errorf("got (%d,%d), want (%d,%d)", limit, offset, c.wantLimit, c.wantOffset)
			}
		})
	}
}

func TestPageParamsUsesTheNamedParamPair(t *testing.T) {
	r := httptest.NewRequest("GET", "/x?limit=999&offset=999&errorLimit=5&errorOffset=6", nil)
	limit, offset := pageParams(r, "errorLimit", "errorOffset")
	if limit != 5 || offset != 6 {
		t.Fatalf("got (%d,%d), want (5,6) -- errorLimit/errorOffset must not fall back to limit/offset", limit, offset)
	}
}

func TestTimeRangeParams(t *testing.T) {
	r := httptest.NewRequest("GET", "/x?createdAfter=2026-01-01T00:00:00Z&createdBefore=2026-01-02T00:00:00Z", nil)
	after, before := timeRangeParams(r)
	if after == nil || !after.Equal(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("after = %v", after)
	}
	if before == nil || !before.Equal(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("before = %v", before)
	}

	empty := httptest.NewRequest("GET", "/x", nil)
	after, before = timeRangeParams(empty)
	if after != nil || before != nil {
		t.Fatalf("absent params must stay nil: after=%v before=%v", after, before)
	}
}

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
	})
}
