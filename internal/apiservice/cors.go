package apiservice

import (
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
)

// The Python api's CORS parameters (src/dev_health_ops/api/_middleware.py
// register_middleware). Only the origins are configuration.
var (
	corsAllowMethods    = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}
	corsAllowHeaders    = []string{"Authorization", "Content-Type", "X-Org-Id", "X-Request-ID"}
	corsExposeHeaders   = []string{"X-Request-ID"}
	corsSafelisted      = []string{"Accept", "Accept-Language", "Content-Language", "Content-Type"}
	corsMaxAgeSeconds   = 600
	corsAllowCredential = true
)

// CORS reproduces Starlette's CORSMiddleware (starlette 1.3.1, the version
// uv.lock pins) for the Python api's parameters, so a browser sees the same
// headers from either plane. Each branch below names the Starlette line it
// mirrors.
type CORS struct {
	allowOrigins      []string
	allowAllOrigins   bool
	explicitPreflight bool
	allowHeadersLow   []string
	simpleHeaders     [][2]string
	preflightHeaders  [][2]string
}

// NewCORS precomputes the header sets exactly as CORSMiddleware.__init__
// does.
func NewCORS(allowOrigins []string) *CORS {
	c := &CORS{allowOrigins: slices.Clone(allowOrigins)}
	c.allowAllOrigins = slices.Contains(allowOrigins, "*")
	// preflight_explicit_allow_origin = not allow_all_origins or allow_credentials
	c.explicitPreflight = !c.allowAllOrigins || corsAllowCredential

	if c.allowAllOrigins {
		c.simpleHeaders = append(c.simpleHeaders, [2]string{"Access-Control-Allow-Origin", "*"})
	}
	if corsAllowCredential {
		c.simpleHeaders = append(c.simpleHeaders, [2]string{"Access-Control-Allow-Credentials", "true"})
	}
	if len(corsExposeHeaders) > 0 {
		c.simpleHeaders = append(c.simpleHeaders, [2]string{"Access-Control-Expose-Headers", strings.Join(corsExposeHeaders, ", ")})
	}

	if c.explicitPreflight {
		c.preflightHeaders = append(c.preflightHeaders, [2]string{"Vary", "Origin"})
	} else {
		c.preflightHeaders = append(c.preflightHeaders, [2]string{"Access-Control-Allow-Origin", "*"})
	}
	c.preflightHeaders = append(c.preflightHeaders,
		[2]string{"Access-Control-Allow-Methods", strings.Join(corsAllowMethods, ", ")},
		[2]string{"Access-Control-Max-Age", strconv.Itoa(corsMaxAgeSeconds)},
	)
	// allow_headers = sorted(SAFELISTED_HEADERS | set(allow_headers))
	union := map[string]struct{}{}
	for _, header := range append(slices.Clone(corsSafelisted), corsAllowHeaders...) {
		union[header] = struct{}{}
	}
	allowHeaders := make([]string, 0, len(union))
	for header := range union {
		allowHeaders = append(allowHeaders, header)
	}
	sort.Strings(allowHeaders)
	c.preflightHeaders = append(c.preflightHeaders, [2]string{"Access-Control-Allow-Headers", strings.Join(allowHeaders, ", ")})
	if corsAllowCredential {
		c.preflightHeaders = append(c.preflightHeaders, [2]string{"Access-Control-Allow-Credentials", "true"})
	}
	for _, header := range allowHeaders {
		c.allowHeadersLow = append(c.allowHeadersLow, strings.ToLower(header))
	}
	return c
}

func (c *CORS) isAllowedOrigin(origin string) bool {
	if c.allowAllOrigins {
		return true
	}
	return slices.Contains(c.allowOrigins, origin)
}

// Wrap is the middleware (CORSMiddleware.__call__).
func (c *CORS) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// origin = headers.get("origin"); None means no CORS at all. A
		// present-but-empty Origin is NOT None in Starlette, so presence is
		// what counts here, not emptiness.
		origins := r.Header.Values("Origin")
		if len(origins) == 0 {
			next.ServeHTTP(w, r)
			return
		}
		origin := origins[0]
		if r.Method == http.MethodOptions && len(r.Header.Values("Access-Control-Request-Method")) > 0 {
			c.preflight(w, r, origin)
			return
		}
		writer := &headerWriter{ResponseWriter: w, commit: func(header http.Header) {
			c.applySimple(header, origin)
		}}
		next.ServeHTTP(writer, r)
		writer.ensureCommitted()
	})
}

// preflight mirrors CORSMiddleware.preflight_response.
func (c *CORS) preflight(w http.ResponseWriter, r *http.Request, origin string) {
	header := w.Header()
	for _, pair := range c.preflightHeaders {
		header.Set(pair[0], pair[1])
	}
	var failures []string
	if c.isAllowedOrigin(origin) {
		if c.explicitPreflight {
			header.Set("Access-Control-Allow-Origin", origin)
		}
	} else {
		failures = append(failures, "origin")
	}
	if !slices.Contains(corsAllowMethods, r.Header.Get("Access-Control-Request-Method")) {
		failures = append(failures, "method")
	}
	// allow_all_headers is never true for the Python parameters, so only the
	// per-header check applies.
	if requested := r.Header.Values("Access-Control-Request-Headers"); len(requested) > 0 {
		for _, name := range strings.Split(strings.ToLower(requested[0]), ",") {
			if !slices.Contains(c.allowHeadersLow, strings.TrimSpace(name)) {
				failures = append(failures, "headers")
				break
			}
		}
	}
	// allow_private_network is False, so any private-network request fails.
	if len(r.Header.Values("Access-Control-Request-Private-Network")) > 0 {
		failures = append(failures, "private-network")
	}
	body, status := "OK", http.StatusOK
	if len(failures) > 0 {
		body, status = "Disallowed CORS "+strings.Join(failures, ", "), http.StatusBadRequest
	}
	header.Set("Content-Type", "text/plain; charset=utf-8")
	header.Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	writeFixedBody(w, []byte(body))
}

// applySimple mirrors CORSMiddleware.send for http.response.start.
func (c *CORS) applySimple(header http.Header, origin string) {
	for _, pair := range c.simpleHeaders {
		header.Set(pair[0], pair[1])
	}
	switch {
	case c.allowAllOrigins && corsAllowCredential:
		allowExplicitOrigin(header, origin)
	case !c.allowAllOrigins && c.isAllowedOrigin(origin):
		allowExplicitOrigin(header, origin)
	}
}

// allowExplicitOrigin mirrors CORSMiddleware.allow_explicit_origin and
// MutableHeaders.add_vary_header: the existing FIRST Vary value, if any, is
// joined with "Origin" and replaces every Vary entry.
func allowExplicitOrigin(header http.Header, origin string) {
	header.Set("Access-Control-Allow-Origin", origin)
	vary := "Origin"
	if existing := header.Values("Vary"); len(existing) > 0 {
		vary = existing[0] + ", Origin"
	}
	header.Set("Vary", vary)
}
