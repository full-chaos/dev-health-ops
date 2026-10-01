package pyheaders

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/platform/buildstamp"
)

// UnhandledErrorShape gives a 500 for an unhandled error the Python api's
// shape: Starlette's ServerErrorMiddleware answers it outside every other
// middleware, so the response carries only its content headers (no
// security, CORS, correlation or impersonation header). It must be the
// outermost installed middleware except the provenance stamp, which sits
// outside it so scope rejections carry it too and which this shape keeps.
func UnhandledErrorShape(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&unhandledWriter{ResponseWriter: w}, r)
	})
}

type unhandledWriter struct {
	http.ResponseWriter
	wrote bool
}

func (w *unhandledWriter) commit() {
	if w.wrote {
		return
	}
	w.wrote = true
	header := w.Header()
	if header.Get(policy.UnhandledErrorHeader) == "" {
		return
	}
	for key := range header {
		switch key {
		case "Content-Type", "Content-Length", "Connection":
		case http.CanonicalHeaderKey(buildstamp.PlaneHeader), http.CanonicalHeaderKey(buildstamp.BuildHeader):
			// Provenance the prover reads from every response, error or not;
			// uvicorn behind the ingress carries neither, so nothing is lost.
		default:
			delete(header, key)
		}
	}
}

func (w *unhandledWriter) WriteHeader(status int) {
	w.commit()
	w.ResponseWriter.WriteHeader(status)
}

func (w *unhandledWriter) Write(body []byte) (int, error) {
	w.commit()
	return w.ResponseWriter.Write(body)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *unhandledWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
