package policy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
)

const bodyKey contextKey = 101

// BodyFrom returns the body BodyFirst stored in ctx, and whether one was
// stored at all (a route wrapped only with Wrap never sets it).
func BodyFrom(ctx context.Context) (pybody.Body, bool) {
	body, ok := ctx.Value(bodyKey).(pybody.Body)
	return body, ok
}

// BodyFirst reads r's body before authenticating, as FastAPI does: a
// pydantic body parameter is validated before any Depends() runs, so a
// decode failure -- 422, or 400 for bytes that are not UTF-8, or 413 over
// the size cap -- is answered before the credential is ever looked at. On
// success next runs behind Wrap(level, ...), with the decoded body
// available to it via BodyFrom.
func (g *Guard) BodyFirst(level Authz, next http.Handler) http.Handler {
	guarded := g.Wrap(level, next)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, outcome, failure, err := pybody.Read(r)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				WriteDetail(w, http.StatusRequestEntityTooLarge, "Request Entity Too Large", nil)
				return
			}
			g.logger.ErrorContext(r.Context(), "api route failed", slog.String("path", r.URL.Path),
				slog.String("step", "read body"), slog.String("error", err.Error()))
			WriteInternal(w)
			return
		}
		switch outcome {
		case pybody.DecodeFailed:
			WriteJSON(w, http.StatusUnprocessableEntity, pybody.Detail([]pybody.Error{*failure}), nil)
			return
		case pybody.ParseFailed:
			WriteDetail(w, http.StatusBadRequest, "There was an error parsing the body", nil)
			return
		}
		guarded.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), bodyKey, body)))
	})
}
