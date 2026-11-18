package providersync

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// githubTestsExclusionReasonNoReportMember: the artifact's zip directory lists
// no member the report parser reads, so it takes no slot under the per-run
// artifact cap.
const githubTestsExclusionReasonNoReportMember = "no_report_member"

// githubTestsArtifactsPerPage is the per_page of a run's artifact listing,
// which reads one page. githubTestsMaxDirectoryChecksPerRun bounds the
// directory checks of one run by it, even if a page carried more items: a
// candidate past the bound is not classified and is handled as before the
// check existed (it takes a slot, or is cut as per_run_cap).
const (
	githubTestsArtifactsPerPage         = 100
	githubTestsMaxDirectoryChecksPerRun = githubTestsArtifactsPerPage
)

// githubTestsAdmitReportArtifacts applies the per-run artifact cap after a
// report-member check (CHAOS-14000). Only when the candidates exceed the cap,
// every one's zip directory is read by range, in order: an artifact that lists
// no report-candidate member takes no slot and is returned in noReport; one
// whose directory cannot be listed keeps its slot, as before. capped says a
// candidate that may hold reports was left past the cap. used counts every
// wire request the checks sent.
func githubTestsAdmitReportArtifacts(
	ctx context.Context, client *providerfoundation.HTTPClient, root string,
	artifacts []githubTestsArtifactPayload, maxArtifacts int,
) (admitted, noReport []githubTestsArtifactPayload, capped bool, used int, err error) {
	if len(artifacts) <= maxArtifacts {
		return artifacts, nil, false, 0, nil
	}
	admitted = make([]githubTestsArtifactPayload, 0, maxArtifacts)
	for index, artifact := range artifacts {
		listed, hasReport := false, false
		if index < githubTestsMaxDirectoryChecksPerRun {
			var requests int
			var checkErr error
			listed, hasReport, requests, checkErr = githubTestsArtifactListsReportMember(ctx, client, root, string(artifact.ID), artifact.listedSize())
			used += requests
			if checkErr != nil {
				return nil, nil, false, used, checkErr
			}
		}
		switch {
		case listed && !hasReport:
			noReport = append(noReport, artifact)
		case len(admitted) == maxArtifacts:
			capped = true
		default:
			admitted = append(admitted, artifact)
		}
	}
	return admitted, noReport, capped, used, nil
}

// githubTestsArtifactListsReportMember reads only an artifact's zip directory
// by range and says whether it lists a member the parser reads. listed is
// false when the directory cannot be listed by range (no range support, an
// unusable reply, a directory over its bound, an artifact gone or without a
// download location): the caller keeps the artifact as a candidate and its
// download decides, as before. Every other failure keeps its class.
func githubTestsArtifactListsReportMember(
	ctx context.Context, client *providerfoundation.HTTPClient, root, artifactID string, listedSize int64,
) (listed, hasReport bool, used int, err error) {
	ctx, wire := providerfoundation.WithWireCount(ctx)
	defer func() { used = wire.Requests() }()
	path := root + "/actions/artifacts/" + url.PathEscape(artifactID) + "/zip"
	var fetch githubTestsArtifactFetch = func(ctx context.Context) (*http.Response, error) {
		return client.DoRoute(ctx, http.MethodGet, githubArtifactZipRoute, path, nil)
	}
	var refresh func(context.Context) (githubTestsArtifactFetch, error)
	response, err := fetch(ctx)
	if err != nil {
		if githubTestsGoneError(err) {
			return false, false, 0, nil
		}
		return false, false, 0, err
	}
	response.Body.Close()
	switch {
	case response.StatusCode >= 300 && response.StatusCode < 400:
		location := response.Header.Get("Location")
		if location == "" {
			return false, false, 0, nil
		}
		api := fetch
		fetch = githubTestsArtifactBlobFetch(client, location)
		refresh = func(ctx context.Context) (githubTestsArtifactFetch, error) {
			return githubTestsArtifactRelocate(ctx, client, api)
		}
	case !strings.EqualFold(strings.TrimSpace(response.Header.Get("Accept-Ranges")), "bytes"):
		return false, false, 0, nil
	}
	listed, hasReport, err = githubTestsDirectoryListsReportMember(ctx, fetch, refresh, listedSize)
	return listed, hasReport, 0, err
}

// githubTestsDirectoryListsReportMember reads the artifact's zip directory by
// range through fetch, the way readGitHubTestsArtifactReports does.
func githubTestsDirectoryListsReportMember(
	ctx context.Context, fetch githubTestsArtifactFetch,
	refresh func(context.Context) (githubTestsArtifactFetch, error), size int64,
) (listed, hasReport bool, err error) {
	source := &githubTestsRangeReaderAt{ctx: ctx, fetch: fetch, refresh: refresh, size: size}
	// The whole bounded directory, not the parser's entry window: a report member past it still marks the artifact as one
	// that holds reports.
	members, err := source.directoryMembers()
	var unusable *githubTestsRangeUnusableError
	switch {
	case errors.As(err, &unusable), errors.Is(err, errGitHubTestsRangeGone), errors.Is(err, ErrGitHubTestsArtifactUnavailable):
		return false, false, nil
	case err != nil:
		return false, false, err
	}
	for _, member := range members {
		if githubTestsReportCandidateMember(member) {
			return true, true, nil
		}
	}
	return true, false, nil
}

// githubTestsNoReportSampleRecord is the excluded-artifact sample entry of an
// artifact that lists no report member.
func githubTestsNoReportSampleRecord(artifact githubTestsArtifactPayload) string {
	return fmt.Sprintf("%s (%s)", githubTestsTruncateArtifactName(artifact.Name), githubTestsExclusionReasonNoReportMember)
}
