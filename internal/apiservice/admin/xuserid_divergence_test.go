//go:build integration

package admin_test

import (
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// CHAOS-10082, a named divergence from Python (ruling 2026-10-18, record
// integrity over parity): created_by_id comes from the verified principal and
// the X-User-Id request header is ignored. Python trusted the header, so
// these requests, which Python rejected (422/400/500) or answered from the
// header, are answered differently by Go. Their Python answers stay in the
// frozen golden and are consumed unread; the Go side is checked by hand after
// the parity run and its row comparisons, so the two planes' tables were
// still equal when compared (Python created no row for any of them).
var xUserIDDivergentRequests = map[string]bool{
	"W ip create with malformed X-User-Id":       true,
	"W ip create with 31-char X-User-Id":         true,
	"W ip create with non-hex 32-char X-User-Id": true,
	"W ip create with unknown X-User-Id":         true,
	"W create with malformed X-User-Id":          true,
	"W create with non-hex 32-char X-User-Id":    true,
	"W create with unknown X-User-Id":            true,
}

// splitXUserIDDivergent returns the requests and answers Diff compares, and the
// answers of the divergent requests, which the caller consumes.
func splitXUserIDDivergent(requests []venueoracle.Request, answers []venueoracle.Response) (compared []venueoracle.Request, comparedAnswers, divergentAnswers []venueoracle.Response) {
	for i, request := range requests {
		if xUserIDDivergentRequests[request.Name] {
			divergentAnswers = append(divergentAnswers, answers[i])
			continue
		}
		compared = append(compared, request)
		comparedAnswers = append(comparedAnswers, answers[i])
	}
	return compared, comparedAnswers, divergentAnswers
}
