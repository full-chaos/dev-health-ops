package legacyingest

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// SignatureTimestampHeader carries the unix-seconds timestamp that the
// signature covers when Options.RequireSignatureTimestamp is set.
const SignatureTimestampHeader = "X-Signature-Timestamp"

// SignatureWindow is the largest accepted distance between the signed
// timestamp and the server clock.
const SignatureWindow = 300 * time.Second

// Options hardens the routes; the zero value leaves them as Routes builds them.
type Options struct {
	// Deprecation marks every response with a Deprecation header.
	Deprecation bool
	// Sunset, when not zero, is sent as a Sunset header (RFC 8594).
	Sunset time.Time
	// RequireSignatureTimestamp makes the HMAC cover timestamp + "." + body.
	RequireSignatureTimestamp bool
	// Now is the clock for the timestamp window; nil means time.Now.
	Now func() time.Time
}

type optionsKey struct{}

func optionsFrom(ctx context.Context) Options {
	options, _ := ctx.Value(optionsKey{}).(Options)
	return options
}

// Harden wraps routes so that options reach their handlers and responses.
func Harden(routes []httpapi.Route, options Options) []httpapi.Route {
	out := make([]httpapi.Route, len(routes))
	for index, item := range routes {
		inner := item.Handler
		item.Handler = hardened{options: options, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if options.Deprecation {
				setDeprecation(w, options)
			}
			inner.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), optionsKey{}, options)))
		})}
		out[index] = item
	}
	return out
}

type hardened struct {
	http.Handler
	options Options
}

// HeaderLayers returns the layer that sets the deprecation headers on every
// response whose path is one of the hardened routes, whichever handler or
// router answers it (the router's own 405 included); none when no route is
// hardened.
func HeaderLayers(routes []httpapi.Route) []func(http.Handler) http.Handler {
	paths := map[string]bool{}
	var options Options
	for _, item := range routes {
		if handler, ok := item.Handler.(hardened); ok && handler.options.Deprecation {
			paths[item.Pattern] = true
			options = handler.options
		}
	}
	if len(paths) == 0 {
		return nil
	}
	return []func(http.Handler) http.Handler{func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if paths[r.URL.Path] {
				setDeprecation(w, options)
			}
			next.ServeHTTP(w, r)
		})
	}}
}

func setDeprecation(w http.ResponseWriter, options Options) {
	w.Header().Set("Deprecation", "true")
	if !options.Sunset.IsZero() {
		w.Header().Set("Sunset", options.Sunset.UTC().Format(http.TimeFormat))
	}
}

// CredentialConfigured reports whether INGEST_API_KEYS or INGEST_SIGNING_SECRET
// holds a usable value in getenv.
func CredentialConfigured(getenv func(string) string) bool {
	return len(apiKeys(getenv)) > 0 || getenv("INGEST_SIGNING_SECRET") != ""
}

// signedTimestamp validates the timestamp header: digits only, within the window.
func signedTimestamp(value string, now time.Time) bool {
	if value == "" || len(value) > 12 {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false
	}
	distance := now.Sub(time.Unix(seconds, 0))
	if distance < 0 {
		distance = -distance
	}
	return distance <= SignatureWindow
}
