package units

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
	"github.com/google/uuid"
)

// ParseRepoID reproduces materialize.py:1077-1084's _parse_repo_id: a
// CLASSIFIER, not a validator (see pythonparity.ParseUUID's doc comment for
// the distinction) -- a value the reference's uuid.UUID() constructor
// rejects becomes the unattributed-repo bucket (nil), never an error. That
// bucket is a real, counted outcome downstream (chwrite records with a nil
// RepoID), not a failure this function should surface.
func ParseRepoID(repoID string) *uuid.UUID {
	if repoID == "" {
		return nil
	}
	parsed, err := pythonparity.ParseUUID(repoID)
	if err != nil {
		return nil
	}
	return &parsed
}

// ParsePRFromID reproduces work_graph/ids.py:189-207's parse_pr_from_id --
// the canonical PR id format "{repo_uuid}#pr{number}". Python's
// pr_id.split("#pr") splits on EVERY occurrence, so a value containing the
// separator twice fails the len(parts) != 2 check exactly as it does there;
// strings.Split has the same all-occurrences semantics.
func ParsePRFromID(prID string) (repoID *uuid.UUID, number int, ok bool) {
	parts := strings.Split(prID, "#pr")
	if len(parts) != 2 {
		return nil, 0, false
	}
	parsed, err := pythonparity.ParseUUID(parts[0])
	if err != nil {
		return nil, 0, false
	}
	value, valid := parsePythonInt(parts[1])
	if !valid {
		return nil, 0, false
	}
	return &parsed, value, true
}

// ParseCommitFromID reproduces work_graph/ids.py:210-227's
// parse_commit_from_id -- the canonical commit id format
// "{repo_uuid}@{sha}".
func ParseCommitFromID(commitID string) (repoID *uuid.UUID, hash string, ok bool) {
	parts := strings.Split(commitID, "@")
	if len(parts) != 2 {
		return nil, "", false
	}
	parsed, err := pythonparity.ParseUUID(parts[0])
	if err != nil {
		return nil, "", false
	}
	return &parsed, parts[1], true
}
