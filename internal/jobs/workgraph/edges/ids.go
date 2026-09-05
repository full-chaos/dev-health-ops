package edges

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// GeneratePRID is the Go port of work_graph/ids.py::generate_pr_id.
//
// Format: "{repo_uuid}#pr{number}", where {repo_uuid} is the UUID's canonical
// lowercase-with-hyphens string form -- identical between Python's
// str(uuid.UUID) and Go's uuid.UUID.String(), so no reformatting is needed to
// match Python byte-for-byte.
func GeneratePRID(repoID uuid.UUID, prNumber int) string {
	return fmt.Sprintf("%s#pr%d", repoID.String(), prNumber)
}

// GeneratePRIDFromDigits is GeneratePRID for a PR number sourced from free
// text (e.g. a github.com/.../pull/N URL matched out of incident evidence)
// rather than a bounded table column. Python's int(number) has arbitrary
// precision and never overflows; a Go `int` does, silently wrapping on a
// pathological input (codex round chaos-4924-pr-a, finding 5: a PR number
// exceeding 64-bit int range wrapped to 0 instead of erroring or matching
// Python's output). digits must already be normalized to ASCII '0'-'9' (see
// operationaledges' unicodeDigitsToASCII, which also handles Python's `\d`
// matching non-ASCII decimal digits RE2's literal `\d` does not) --
// leading zeros are stripped here, matching int()'s own normalization, but
// no digit-value conversion happens at this layer.
func GeneratePRIDFromDigits(repoID uuid.UUID, digits string) string {
	trimmed := strings.TrimLeft(digits, "0")
	if trimmed == "" {
		trimmed = "0"
	}
	return fmt.Sprintf("%s#pr%s", repoID.String(), trimmed)
}

// GenerateFeatureFlagID is the Go port of
// work_graph/ids.py::generate_feature_flag_id: a SHA-256 hex digest of
// "flag:{org_id}/{provider}/{project_key}/{flag_key}".
func GenerateFeatureFlagID(orgID, provider, projectKey, flagKey string) string {
	canonical := "flag:" + orgID + "/" + provider + "/" + projectKey + "/" + flagKey
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
