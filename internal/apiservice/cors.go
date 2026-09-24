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

// CORS reproduces Starlette's CORSMiddleware (starlette 1.7.0, the version
// uv.lock pins and the deployed Python api runs) for the Python api's
// parameters, so a browser and a cache see the same headers from either
// plane. Each branch below names the Starlette line it mirrors.
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

	// preflight_headers always opens with this Vary; the allow-origin "*"
	// only when the preflight need not echo the origin.
	c.preflightHeaders = append(c.preflightHeaders, [2]string{"Vary",
		"Origin, Access-Control-Request-Method, Access-Control-Request-Headers, Access-Control-Request-Private-Network"})
	if !c.explicitPreflight {
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
		// origin = headers.get("origin"): the first Origin, or None when
		// absent. A present-but-empty Origin is NOT None in Starlette, so
		// presence is what counts here, not emptiness. A request without
		// one is still a simple response: its Vary gains "Origin".
		origins := r.Header.Values("Origin")
		hasOrigin := len(origins) > 0
		var origin string
		if hasOrigin {
			origin = origins[0]
		}
		if hasOrigin && r.Method == http.MethodOptions && len(r.Header.Values("Access-Control-Request-Method")) > 0 {
			c.preflight(w, r, origin)
			return
		}
		writer := &headerWriter{ResponseWriter: w, commit: func(header http.Header) {
			c.applySimple(header, hasOrigin, origin)
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

// applySimple mirrors CORSMiddleware.send for http.response.start: the
// simple headers only with an Origin; the Origin echoed (with Vary) when it
// may be; otherwise Vary still gains "Origin", with or without one.
func (c *CORS) applySimple(header http.Header, hasOrigin bool, origin string) {
	if hasOrigin {
		for _, pair := range c.simpleHeaders {
			header.Set(pair[0], pair[1])
		}
	}
	switch {
	case hasOrigin && c.allowAllOrigins && corsAllowCredential:
		allowExplicitOrigin(header, origin)
	case hasOrigin && !c.allowAllOrigins && c.isAllowedOrigin(origin):
		allowExplicitOrigin(header, origin)
	default:
		addVaryOrigin(header)
	}
}

// allowExplicitOrigin mirrors CORSMiddleware.allow_explicit_origin.
func allowExplicitOrigin(header http.Header, origin string) {
	header.Set("Access-Control-Allow-Origin", origin)
	addVaryOrigin(header)
}

// addVaryOrigin is `headers["Vary"] = ", ".join([*headers.getlist("Vary"),
// "Origin"])`: every Vary value the handler set, in order, then "Origin",
// as one Vary header.
func addVaryOrigin(header http.Header) {
	header.Set("Vary", strings.Join(append(header.Values("Vary"), "Origin"), ", "))
}
