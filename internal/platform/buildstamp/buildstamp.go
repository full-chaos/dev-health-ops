// Package buildstamp is the one implementation of how a Go service names the
// plane and the build that answered a request, and of the /buildinfo body the
// REST and GraphQL provers bind every receipt to. query-api and the api both
// use it, so the header names, the "is this build identifiable" rule and the
// body shape cannot drift apart.
package buildstamp

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/version"
)

// Header names a prover reads from every candidate response.
const (
	PlaneHeader = "x-dev-health-plane"
	BuildHeader = "x-dev-health-build"
)

// IsKnownBuild says whether a commit string actually identifies a build.
// An unstamped binary does not produce an empty commit: version.Current
// returns the literal "unknown", and stamping that would present an
// unbindable measurement as a bound one.
func IsKnownBuild(commit string) bool {
	commit = strings.TrimSpace(commit)
	return commit != "" && commit != "unknown"
}

// SetProvenance names the plane (always go) and, when commit identifies a
// build, the build. Call it before the handler commits its status line.
func SetProvenance(header http.Header, commit string) {
	header.Set(PlaneHeader, "go")
	if IsKnownBuild(commit) {
		header.Set(BuildHeader, strings.TrimSpace(commit))
	}
}

// Body is the /buildinfo body for info: its JSON encoding plus the trailing
// newline the encoder writes.
func Body(info version.Info) ([]byte, error) {
	body, err := json.Marshal(info)
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}
