package providersync

import (
	"errors"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/issueprlinks"
)

// GitLab's own reference form for a cross-project item is "<project path>!<iid>" (merge request) or
// "<project path>#<iid>" (issue), the references.full field. This is the one parser for it: every site that turns a
// GitLab reference into a work-item id goes through it, and no site completes a missing project path from the source
// item, since a guessed project can resolve an iid to the wrong local item.
const (
	gitlabMergeRequestMarker = '!'
	gitlabIssueMarker        = '#'
)

var (
	errGitLabReferenceMissing = errors.New("gitlab reference: empty")
	errGitLabReferenceShape   = errors.New("gitlab reference: no marker, or the wrong one")
	errGitLabReferencePath    = errors.New("gitlab reference: project path outside GitLab's path grammar")
	errGitLabReferenceIID     = errors.New("gitlab reference: iid is not a canonical positive 32-bit integer")
)

type gitlabReference struct {
	Path string
	IID  uint32
}

// parseGitLabReference parses references.full for the given marker. The project path is "/"-separated segments of
// [A-Za-z0-9_.-], none empty, "." or "..". The iid is a canonical positive integer that fits uint32, the type
// issueprlinks.Derive parses and the pull-request number column holds, so the route and Derive cannot disagree. No
// whitespace is trimmed: a reference with it is a changed answer shape, not something to repair.
func parseGitLabReference(full string, marker byte) (gitlabReference, error) {
	if full == "" {
		return gitlabReference{}, errGitLabReferenceMissing
	}
	index := strings.LastIndexByte(full, marker)
	if index < 0 {
		return gitlabReference{}, errGitLabReferenceShape
	}
	path, number := full[:index], full[index+1:]
	if !validGitLabProjectPath(path) {
		return gitlabReference{}, errGitLabReferencePath
	}
	parsed, err := strconv.ParseUint(number, 10, 32)
	if err != nil || parsed == 0 || strconv.FormatUint(parsed, 10) != number {
		return gitlabReference{}, errGitLabReferenceIID
	}
	if marker == gitlabMergeRequestMarker {
		// Derive's own parse of the id this reference becomes; a disagreement is a reject here, not a "synced" row there.
		source, ok := issueprlinks.ParsePRSource("gitlab:" + path + "!" + number)
		if !ok || source.PRNumber != uint32(parsed) || source.RepoSlug != path {
			return gitlabReference{}, errGitLabReferenceIID
		}
	}
	return gitlabReference{Path: path, IID: uint32(parsed)}, nil
}

func validGitLabProjectPath(path string) bool {
	if path == "" {
		return false
	}
	for _, segment := range strings.Split(path, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
		for _, char := range segment {
			switch {
			case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9', char == '_', char == '-', char == '.':
			default:
				return false
			}
		}
	}
	return true
}
