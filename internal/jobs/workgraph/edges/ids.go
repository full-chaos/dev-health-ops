package edges

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

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

// GenerateFeatureFlagID is the Go port of
// work_graph/ids.py::generate_feature_flag_id: a SHA-256 hex digest of
// "flag:{org_id}/{provider}/{project_key}/{flag_key}".
func GenerateFeatureFlagID(orgID, provider, projectKey, flagKey string) string {
	canonical := "flag:" + orgID + "/" + provider + "/" + projectKey + "/" + flagKey
	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}
