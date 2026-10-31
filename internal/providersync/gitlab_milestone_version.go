package providersync

import (
	"context"
	"log/slog"
	"regexp"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// GitLab 16.7 deprecated include_parent_milestones for include_ancestors and refuses both together with HTTP 400 (gitlab-org/gitlab issue 433298, MR 138391, milestone 16.7).
const (
	gitlabAncestorsMinMajor = 16
	gitlabAncestorsMinMinor = 7
)

var gitlabVersionPrefix = regexp.MustCompile(`^(\d+)\.(\d+)`)

// gitlabMilestoneAncestorParam names the query parameter that lists the
// ancestor groups' milestones: include_parent_milestones before GitLab 16.7,
// include_ancestors from it on. Never both. A version read that fails falls
// back to include_ancestors.
func gitlabMilestoneAncestorParam(ctx context.Context, client *providerfoundation.HTTPClient, ids ...any) string {
	var payload struct {
		Version string `json:"version"`
	}
	if err := fetchObjectRoute(ctx, client, gitlabVersionRoute, "/api/v4/version", &payload); err != nil {
		slog.Default().ErrorContext(ctx, "gitlab_version_probe_failed", append(ids, "error_class", logging.ErrorClass(err), "error_type", logging.ErrorType(err))...)
		return "include_ancestors"
	}
	match := gitlabVersionPrefix.FindStringSubmatch(payload.Version)
	if match == nil {
		slog.Default().ErrorContext(ctx, "gitlab_version_probe_failed", append(ids, "error_class", "version_unparseable")...)
		return "include_ancestors"
	}
	major, _ := strconv.Atoi(match[1])
	minor, _ := strconv.Atoi(match[2])
	if major < gitlabAncestorsMinMajor || (major == gitlabAncestorsMinMajor && minor < gitlabAncestorsMinMinor) {
		return "include_parent_milestones"
	}
	return "include_ancestors"
}
