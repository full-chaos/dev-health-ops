package providersync

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

const (
	gitLabMalformedItemValidFirst  = "0123456789abcdef0123456789abcdef01234567"
	gitLabMalformedItemValidSecond = "89abcdef0123456789abcdef0123456789abcdef"
)

func collectGitLabCommitListingHashes(t *testing.T, listing string) ([]string, error, *time.Time, Claim) {
	t.Helper()
	responses := append(gitLabOneMergeRequestResponses(), gitLabPullRequestResponse{body: listing})
	claim := nativeTestClaim("gitlab", "prs")
	batch, err := (GitLabPullRequestRouteHandler{}).Collect(
		context.Background(), claim, providerfoundation.Credential{},
		gitLabRepositoryClient(t, fakehttp.Client(&gitLabPullRequestDoer{t: t, responses: responses}), "https://gitlab.example"),
		time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	)
	if err != nil || len(batch.Effects) != 4 {
		return nil, err, batch.Watermark, claim
	}
	var hashes []string
	for _, raw := range batch.Effects[3].Rows {
		var link prCommitLinkRow
		if err := json.Unmarshal(raw, &link); err != nil {
			t.Fatal(err)
		}
		hashes = append(hashes, link.CommitHash)
	}
	return hashes, nil, batch.Watermark, claim
}

// Control: an all-valid listing writes every link and acknowledges the window, as main does.
func TestGitLabCommitListingAllValidItemsAsMain(t *testing.T) {
	hashes, err, watermark, claim := collectGitLabCommitListingHashes(t,
		`[{"id":"`+gitLabMalformedItemValidFirst+`"},{"id":"`+gitLabMalformedItemValidSecond+`"}]`)
	if err != nil || watermark == nil || !watermark.Equal(*claim.BeforeAt) ||
		len(hashes) != 2 || hashes[0] != gitLabMalformedItemValidFirst || hashes[1] != gitLabMalformedItemValidSecond {
		t.Fatalf("err = %v, watermark = %v, links = %v: want both links and the claim's end", err, watermark, hashes)
	}
}

// One malformed item in an answered commits listing must not drop the valid links of the same merge request: they
// are the provider's statement of which commits the merge request holds, and the merge request is listed again only
// when it is updated again. WANT the valid links written and the window acknowledged. RED at main: the whole listing
// is dropped, so the merge request is written with no links on every sync.
func TestGitLabCommitListingMalformedItemKeepsTheValidLinks(t *testing.T) {
	for name, malformed := range map[string]string{
		"missing_id":    `{"short_id":"abc"}`,
		"not_an_object": `"abc"`,
	} {
		t.Run(name, func(t *testing.T) {
			hashes, err, watermark, claim := collectGitLabCommitListingHashes(t,
				`[{"id":"`+gitLabMalformedItemValidFirst+`"},`+malformed+`,{"id":"`+gitLabMalformedItemValidSecond+`"}]`)
			if err != nil || watermark == nil || !watermark.Equal(*claim.BeforeAt) {
				t.Fatalf("err = %v, watermark = %v: want the window acknowledged", err, watermark)
			}
			if len(hashes) != 2 || hashes[0] != gitLabMalformedItemValidFirst || hashes[1] != gitLabMalformedItemValidSecond {
				t.Fatalf("links = %v: want the two valid links kept", hashes)
			}
		})
	}
}
