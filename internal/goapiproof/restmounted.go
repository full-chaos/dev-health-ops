package goapiproof

import "sort"

// MountedRESTPaths is query-api's own mounted /api/v1/* path list, CHECKED
// IN rather than parsed from source at runtime.
//
// WHY NOT READ cmd/query-api's SOURCE AT RUNTIME (as this binary's own
// coverage check originally did, via migrationmatrix.LoadQueryAPIMuxRoutes
// pointed at a live source directory): the operator tools image
// (docker/go-api-tools.Dockerfile) ships ONLY the compiled binaries, never
// the Go source tree -- it exists to be `kubectl exec`ed into for one-off
// verb runs, not to carry a checkout. Requiring cmd/query-api's full
// source there, purely so a regex could re-derive this list's literal path
// strings from it on every invocation, meant the tool could not run in
// its own deployed image at all (found verifying this exact gap: the
// runtime stage copies the go-api-rest-prove BINARY but never the source
// tree it would need). A stale checked-in list that a test catches at
// build/CI time is a smaller, more honest failure mode than an operator
// tool that cannot start without a source tree it never otherwise needs.
//
// PINNED, NOT HAND-TRUSTED: TestMountedRESTPathsMatchesTheRealQueryAPIMux
// (cmd/go-api-rest-prove/main_test.go) runs
// migrationmatrix.LoadQueryAPIMuxRoutes against the REAL cmd/query-api
// source tree and fails, printing the exact diff, the moment a route is
// added, removed or renamed here without this list being updated to
// match. Regenerate by hand: run that test, read the diff, copy the new
// list in below. Never regenerated at runtime, never read from disk --
// this is a plain Go literal, compiled directly into the binary, exactly
// like every other value in this file.
//
// PATH-LEVEL, NOT (METHOD, PATH): the live mux this list mirrors has no
// mechanically-derivable method per registration (query-api's own
// http.ServeMux.HandleFunc calls carry a path only; method dispatch
// happens INSIDE each handler via its own routeswitch operation
// constants) -- restendpoints.go's own doc comment already establishes
// path-level matching as this service's deliberate convention, for
// exactly that reason, and AssertRESTPathCoverage's own signature
// (mounted []string) matches it. A method-aware check would have to
// compare against the CORPUS's own operation names for the method half,
// which would just be checking restEndpointSpecs against itself, not
// against an independent source -- see restcorpus.go's package doc
// comment on the import direction this file preserves (no
// internal/migrationmatrix import here; the pin test lives in
// cmd/go-api-rest-prove instead, which already imports both).
var mountedRESTPaths = []string{
	"/api/v1/drilldown/issues",           // GET, POST
	"/api/v1/drilldown/prs",              // GET, POST
	"/api/v1/explain",                    // GET, POST
	"/api/v1/filters/options",            // GET
	"/api/v1/flame",                      // GET
	"/api/v1/investment/explain",         // POST
	"/api/v1/meta",                       // GET
	"/api/v1/people",                     // GET
	"/api/v1/people/{person_id}/metric",  // GET
	"/api/v1/people/{person_id}/summary", // GET
	"/api/v1/quadrant",                   // GET
}

// MountedRESTPaths returns a fresh, sorted copy of the checked-in mounted
// path list -- a copy so a caller can never mutate the package-level
// literal through the slice it gets back.
func MountedRESTPaths() []string {
	out := make([]string, len(mountedRESTPaths))
	copy(out, mountedRESTPaths)
	sort.Strings(out)
	return out
}
