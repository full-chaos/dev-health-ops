package providersync

import (
	"errors"
	"strconv"
	"strings"
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
// issueprlinks.ParsePRSource parses and the pull-request number column holds (providersync must not import issueprlinks: its
// logging would join this package's dependency set; TestParseGitLabReferenceAgreesWithDerive pins the agreement). No
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

// gitlabMergeRequestSourceID mints the work-item id of a merge request named by a web URL's path and number, through
// parseGitLabReference; rejected is true when either fails the grammar.
func gitlabMergeRequestSourceID(projectPath, number string) (source string, rejected bool) {
	reference, err := parseGitLabReference(projectPath+string(rune(gitlabMergeRequestMarker))+number, gitlabMergeRequestMarker)
	if err != nil {
		return "", true
	}
	return "gitlab:" + reference.Path + "!" + strconv.FormatUint(uint64(reference.IID), 10), false
}
