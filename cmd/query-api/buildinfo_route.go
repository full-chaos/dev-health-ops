package main

// GET /buildinfo, and the gated mount of POST /query/proof.
//
// Both exist for CHAOS-5425, and both answer the same underlying
// complaint: a proof receipt is only evidence if it can name what
// produced it.
//
// /buildinfo answers "which BUILD is this process". Until now nothing
// did. `dev-hops go-api routing enable --candidate-build <sha>` documents
// that flag as "the ops commit sha the running query-api image was built
// from" -- BY CONVENTION, verified by nothing. The fifteen live routing
// rows carry b18e56fa79cfe20ce0f75df148144b832d92be36 because somebody
// typed it. A receipt built from a typed sha proves that somebody typed a
// sha, so `go-api-prove` refuses to write one until the process itself
// says what it is (chris, 2026-09-08 acceptance: "Do not construct a
// receipt from a digest or an arbitrary build name").
//
// It is a SEPARATE route from /registry deliberately (team-lead ruling
// R51, 2026-09-09). registry_route.go's doc comment states that its body
// is exactly the schema digest and the operation map -- "every value here
// is already public in the repository ... but that is the reason to keep
// the body to exactly this, not an invitation to add build paths, env, or
// pool state later". That reasoning holds; a different question gets a
// different route.
//
// And unlike /registry, this one is AUTHENTICATED with the same envelope
// verifier /query uses. /registry is unauthenticated because both its
// values are pure functions of checked-in files. A build identity is not:
// it names a specific deployed artifact and whether its tree was dirty,
// which is deployment detail rather than repository content.

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/full-chaos/dev-health-ops/cmd/query-api/internal/principal"
	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// proofRouteEnabledEnv opts the measurement-only route in. Absent or
// anything other than "true" means the handler is never registered.
const proofRouteEnabledEnv = "GO_API_PROOF_ROUTE_ENABLED"

// deploymentEnvEnv is the platform's existing deployment-posture signal
// (internal/platform/config's DEV_HEALTH_ENV). Reused rather than
// inventing a second posture variable, so there is one answer to "is this
// production" and not two that can disagree.
const deploymentEnvEnv = "DEV_HEALTH_ENV"

// productionPostures are the DEV_HEALTH_ENV values that mean production.
// Matching is case-insensitive and trimmed.
//
// This set is NOT the gate. The gate is that the posture must be DECLARED
// and must not be in this set -- an UNSET DEV_HEALTH_ENV refuses too. An
// earlier version treated only these values as disqualifying, which meant
// a production deployment that simply never set the variable got the proof
// route: a fail-OPEN default on the one check whose whole job is to keep a
// measurement route out of production (codex r1 F6, reproduced -- with
// DEV_HEALTH_ENV="" and the flag set, the route registered and logged
// routes_registered=1). A posture nobody declared is not evidence of a
// non-production posture.
var productionPostures = map[string]bool{"prod": true, "production": true}

// planeHeaderName and buildHeaderName are what the proof route stamps on
// every response it serves.
//
// Both exist because of codex r1's F1 and F5. The Python edge stamps
// x-dev-health-plane on the responses IT serves, so a canary/primary
// candidate leg can prove which plane answered -- but /query/proof does not
// go through that edge, so a proof-route response carried NO plane evidence
// at all, and go-api-prove skipped the check for exactly that reason. A
// mis-pointed --proof-url at any endpoint returning a plausible 200 then
// produced a MATCH receipt (reproduced: a bare httptest server with no
// headers yielded terminal_state=match). Stamping here means the proof
// route's own responses carry the same evidence the edge provides, so the
// runner can assert the plane on EVERY route with no exception.
//
// The build header closes the other half: it names the build of the
// process that actually served THIS request, so a receipt cannot claim a
// build read from /buildinfo on one replica while another replica served
// the query.
const (
	planeHeaderName = "x-dev-health-plane"
	buildHeaderName = "x-dev-health-build"
)

// buildInfoResponse is GET /buildinfo's body.
//
// Deliberately just the build identity. Same discipline as
// registryResponse: this route answers one question, and the next person
// who wants to add pool state or environment to it should add a route.
type buildInfoResponse struct {
	Service   string `json:"service"`
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildTime string `json:"build_time"`
	GoVersion string `json:"go_version"`
	// Modified reports a dirty tree at build time. A consumer that needs
	// a build to IDENTIFY source must refuse on true -- the commit does
	// not describe what was actually compiled.
	Modified bool `json:"modified"`
}

// newBuildInfoHandler serves GET /buildinfo behind the same bearer
// envelope /query requires.
//
// The values come from internal/platform/version, which
// docker/query-api.Dockerfile already stamps via -ldflags (VERSION,
// COMMIT, BUILD_TIME) and which falls back to Go's own VCS build
// settings. So this route reports what was actually built; it does not
// take the answer from the environment at run time, where a wrong value
// would be indistinguishable from a right one.
func newBuildInfoHandler(verifier *principal.Verifier) http.HandlerFunc {
	info := version.Current("query-api")
	body, err := json.Marshal(buildInfoResponse{
		Service:   info.Service,
		Version:   info.Version,
		Commit:    info.Commit,
		BuildTime: info.BuildTime,
		GoVersion: info.GoVersion,
		Modified:  info.Modified,
	})
	if err != nil {
		// Six scalar fields; unreachable in practice. But a silently
		// empty body would make go-api-prove refuse for the wrong reason,
		// and a refusal for the wrong reason teaches an operator to
		// bypass the check. Fail loudly at construction instead -- the
		// same choice newRegistryHandler makes.
		log.Fatalf("query-api: /buildinfo response could not be encoded: %v", err)
	}

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		token, ok := bearerToken(r.Header.Get("Authorization"))
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		verifyCtx := principal.WithRequestMeta(r.Context(), r.RemoteAddr, envelopeRequestID(r))
		if _, err := verifier.Verify(verifyCtx, token); err != nil {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

// mountProofRoute registers POST /query/proof only when this deployment
// has explicitly opted in AND is not production.
//
// Why the route exists: routeswitch.PostgresSwitch admits canary|primary
// only, so the deployed Go build cannot execute a SHADOW-mode operation
// at all -- /query answers 404 (measured live 2026-09-07, enablement
// artifact 51-harness-control.json). The four shadow operations could
// therefore never be proven, and the parity defects that put them in
// shadow (CHAOS-5447-5451) could never be re-measured after a fix without
// canarying them, which exposes real traffic to the divergence under
// investigation.
//
// Why it is safe: it is a second handler on a second path over the same
// pipeline, differing ONLY in reachability; the Python edge forwards to
// /query and never here, so no product traffic can reach it; it is
// unregistered unless an operator sets the flag; it refuses outright in a
// production posture regardless of the flag; it performs the same
// bearer/envelope verification and org-context resolution; and every
// receipt produced through it records measurement_route='proof'.
// Production /query is byte-identical to before this change.
//
// The absent case LOGS, with an explicit zero. "The proof route is off"
// and "the proof route is on but nothing used it" are different states,
// and an operator debugging a shadow operation that will not measure must
// be able to tell them apart from the process log alone -- the same
// lesson logRoutingStateDrift exists to encode one table over.
// withProofProvenance wraps the proof handler so every response it serves
// names the plane and the build that served it. See planeHeaderName.
func withProofProvenance(handler http.HandlerFunc, commit string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Set BEFORE the handler runs: once the wrapped handler calls
		// WriteHeader, the header map is already on the wire and a later
		// write is silently dropped.
		w.Header().Set(planeHeaderName, "go")
		if commit != "" {
			w.Header().Set(buildHeaderName, commit)
		}
		handler(w, r)
	}
}

func mountProofRoute(mux *http.ServeMux, handler http.HandlerFunc) {
	posture := strings.ToLower(strings.TrimSpace(os.Getenv(deploymentEnvEnv)))
	enabled := strings.EqualFold(strings.TrimSpace(os.Getenv(proofRouteEnabledEnv)), "true")

	switch {
	case posture == "":
		// Fail CLOSED: an undeclared posture is not a non-production one.
		log.Printf(
			"query-api: /query/proof NOT registered: %s is unset, so this deployment has not declared a non-production posture (routes_registered=0); %s=%v is ignored without one",
			deploymentEnvEnv, proofRouteEnabledEnv, enabled,
		)
		return
	case productionPostures[posture]:
		// Refused even with the flag set: a measurement route that can
		// execute an operation an operator deliberately did NOT canary
		// has no business existing in production, and "someone set the
		// env var" must not be the only thing standing between it and a
		// production process.
		log.Printf(
			"query-api: /query/proof NOT registered: %s=%q is a production posture (routes_registered=0); %s=%v is ignored there",
			deploymentEnvEnv, posture, proofRouteEnabledEnv, enabled,
		)
		return
	case !enabled:
		log.Printf(
			"query-api: /query/proof NOT registered: %s is not \"true\" (routes_registered=0); shadow-mode operations cannot be measured in this deployment",
			proofRouteEnabledEnv,
		)
		return
	case handler == nil:
		// Unreachable while buildQueryRoute always populates it, but a
		// nil handler registered on a mux panics at request time rather
		// than at boot, and a proof route that 500s is worse than one
		// that says it is absent.
		log.Printf("query-api: /query/proof NOT registered: no handler was built (routes_registered=0)")
		return
	}

	mux.HandleFunc("/query/proof", withProofProvenance(handler, version.Current("query-api").Commit))
	log.Printf(
		"query-api: /query/proof REGISTERED (routes_registered=1, %s=%q): measurement-only, admits shadow, unreachable from the Python edge",
		deploymentEnvEnv, posture,
	)
}
