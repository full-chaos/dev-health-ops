package policy

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

type fakeRouteWriter struct {
	http.ResponseWriter
	model bool
}

func (w fakeRouteWriter) ResponseModelRoute() bool { return w.model }
func (w fakeRouteWriter) RouteKey() string         { return "GET /x" }

type wrapped struct{ http.ResponseWriter }

func (w wrapped) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// TestWriterViolationsCountTheWrongWriter pins the writer check: a
// success body written with WriteJSON on a response_model route, and any
// WriteModel on a route without one, count a violation (through writer
// wrappers too); an error body with WriteJSON on a response_model route,
// the right writer, and a writer outside any registered route do not.
func TestWriterViolationsCountTheWrongWriter(t *testing.T) {
	body := pyjson.NewObject()
	cases := []struct {
		name  string
		write func()
		want  int64
	}{
		{"WriteJSON 200 on a model route", func() { WriteJSON(fakeRouteWriter{httptest.NewRecorder(), true}, 200, body, nil) }, 1},
		{"WriteJSON 201 through a wrapper", func() { WriteJSON(wrapped{fakeRouteWriter{httptest.NewRecorder(), true}}, 201, body, nil) }, 1},
		{"WriteModel on a plain route", func() { WriteModel(fakeRouteWriter{httptest.NewRecorder(), false}, 200, body, nil) }, 1},
		{"WriteJSON 404 on a model route", func() { WriteJSON(fakeRouteWriter{httptest.NewRecorder(), true}, 404, body, nil) }, 0},
		{"WriteModel on a model route", func() { WriteModel(fakeRouteWriter{httptest.NewRecorder(), true}, 200, body, nil) }, 0},
		{"WriteJSON 200 on a plain route", func() { WriteJSON(fakeRouteWriter{httptest.NewRecorder(), false}, 200, body, nil) }, 0},
		{"a recorder outside any route", func() {
			WriteModel(httptest.NewRecorder(), 200, body, nil)
			WriteJSON(httptest.NewRecorder(), 200, body, nil)
		}, 0},
	}
	for _, test := range cases {
		before := WriterViolations()
		test.write()
		if got := WriterViolations() - before; got != test.want {
			t.Errorf("%s: %d violations, want %d", test.name, got, test.want)
		}
	}
}
