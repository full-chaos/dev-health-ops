package providersync

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// repositoryRow is the frozen `repos` projection. Field order and JSON names
// mirror the Python ClickHouse sink (`ClickHouseStore.insert_repo`) so the
// effect digest is stable and the row is byte-comparable during live parity.
type repositoryRow struct {
	ID         string    `json:"id"`
	OrgID      string    `json:"org_id"`
	Repo       string    `json:"repo"`
	Ref        *string   `json:"ref"`
	CreatedAt  time.Time `json:"created_at"`
	Settings   string    `json:"settings"`
	Tags       string    `json:"tags"`
	Provider   string    `json:"provider"`
	LastSynced time.Time `json:"last_synced"`
}

// repositorySettings preserves the Python insertion order of the settings
// document. Python writes a dict literal, and dict order is preserved by
// json.dumps, so an alphabetically sorted Go map would not round-trip
// byte-identically.
type repositorySettings struct {
	Source            string `json:"source"`
	GitHubInstanceURL string `json:"github_instance_url"`
	RepoID            int64  `json:"repo_id"`
	URL               string `json:"url"`
	DefaultBranch     string `json:"default_branch"`
}

type gitHubRepositoryPayload struct {
	ID            json.Number `json:"id"`
	Name          string      `json:"name"`
	FullName      string      `json:"full_name"`
	HTMLURL       string      `json:"html_url"`
	DefaultBranch string      `json:"default_branch"`
	Language      string      `json:"language"`
	Archived      bool        `json:"archived"`
	UpdatedAt     string      `json:"updated_at"`
	PushedAt      string      `json:"pushed_at"`
}

// GitHubRepositoryRouteHandler is the native Go complete-route handler for
// (github, repo-metadata) — the CUT-09 pattern-setting slice. It replaces the
// stranded shadow-only fetch path: it emits one bounded `repos` effect batch
// through the effect ledger, keeps Go as the only lease owner, and honours the
// dataset's WatermarkNone contract by never advancing a watermark.
//
// The pair is still RouteReady=false. Fixture parity is covered here; live
// non-empty parity (plan §9 CUT-09 acceptance) has not been captured, and
// capability metadata is not execution evidence (TRD §10.1).
type GitHubRepositoryRouteHandler struct{ Now func() time.Time }

func (handler GitHubRepositoryRouteHandler) now() time.Time {
	if handler.Now != nil {
		return handler.Now().UTC()
	}
	return time.Now().UTC()
}

func (handler GitHubRepositoryRouteHandler) Collect(
	ctx context.Context,
	claim Claim,
	_ providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
) (CompleteRouteBatch, error) {
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		claim.Dataset != "repo-metadata" || client == nil ||
		client.Provider != "github" || client.BaseURL == nil ||
		normalizedAt.IsZero() {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	normalizedAt = normalizedAt.UTC().Truncate(time.Millisecond)
	owner, repository, err := splitGitHubRepository(claim.SourceExternalID)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	instance, ok := normalizedProviderInstance("github", client.BaseURL)
	if !ok {
		return CompleteRouteBatch{}, ErrInvalidConfiguration
	}
	var payload gitHubRepositoryPayload
	path := "/repos/" + url.PathEscape(owner) + "/" + url.PathEscape(repository)
	if err := fetchObject(ctx, client, path, &payload); err != nil {
		return CompleteRouteBatch{}, err
	}
	fullName := payload.FullName
	if fullName == "" {
		fullName = claim.SourceExternalID
	}
	repoID, err := payload.ID.Int64()
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	settings, err := json.Marshal(repositorySettings{
		Source:            "github",
		GitHubInstanceURL: instance,
		RepoID:            repoID,
		URL:               payload.HTMLURL,
		DefaultBranch:     payload.DefaultBranch,
	})
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	tagValues := []string{"github"}
	if payload.Language != "" {
		tagValues = append(tagValues, payload.Language)
	}
	tags, err := json.Marshal(tagValues)
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	row := repositoryRow{
		ID:         repositoryIdentity(fullName),
		OrgID:      claim.OrgID,
		Repo:       fullName,
		CreatedAt:  normalizedAt,
		Settings:   string(settings),
		Tags:       string(tags),
		Provider:   "github",
		LastSynced: normalizedAt,
	}
	if err := row.validate(claim); err != nil {
		return CompleteRouteBatch{}, err
	}
	// `repos` is ReplacingMergeTree(last_synced) keyed by (org_id, id), so a
	// replayed identical batch collapses to the same row: replay-safe, no
	// readback required.
	effect, err := effectBatchFromValues("repos", EffectReplaySafe, []repositoryRow{row})
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"repos_synced":   1,
			"repo":           fullName,
			"repo_id":        repoID,
			"default_branch": payload.DefaultBranch,
			"archived":       payload.Archived,
		},
		// repo-metadata is WatermarkNone in both registries: a reference
		// dataset never advances an incremental cursor.
		Watermark: nil,
		Evidence: FetchEvidence{
			Provider: claim.Provider, Dataset: claim.Dataset,
			Requests: 1, Pages: 1, Records: 1,
		},
	}, nil
}

func (row repositoryRow) validate(claim Claim) error {
	if row.OrgID == "" || row.OrgID != claim.OrgID || row.Repo == "" ||
		row.Provider != claim.Provider || len(row.ID) != 36 ||
		row.CreatedAt.IsZero() || row.LastSynced.IsZero() {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

// repositoryIdentity reproduces Python's get_repo_uuid_from_repo: the first
// sixteen bytes of SHA-256 over the case-folded, trimmed repo identifier,
// rendered as a UUID without version or variant rewriting. The Go and Python
// sinks must agree on this value or every downstream repo_id foreign key
// diverges.
func repositoryIdentity(repo string) string {
	digest := sha256.Sum256([]byte(strings.ToLower(strings.TrimSpace(repo))))
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" +
		encoded[16:20] + "-" + encoded[20:32]
}

// normalizedProviderInstance mirrors
// dev_health_ops.models.operational_identity.normalized_operational_provider_instance
// for the github/gitlab host family.
func normalizedProviderInstance(provider string, base *url.URL) (string, bool) {
	if base == nil {
		return "", false
	}
	host := strings.ToLower(base.Hostname())
	if host == "" || host == "none" || host == "null" {
		return "", false
	}
	if net.ParseIP(host) == nil {
		for _, label := range strings.Split(host, ".") {
			if !validHostLabel(label) {
				return "", false
			}
		}
	}
	if provider == "github" && (host == "api.github.com" || host == "github.com") {
		return "github.com", true
	}
	scheme := strings.ToLower(base.Scheme)
	if scheme == "" {
		scheme = "https"
	}
	port := base.Port()
	switch {
	case port == "":
	case scheme == "https" && port == "443":
		port = ""
	case scheme == "http" && port == "80":
		port = ""
	}
	if port != "" {
		if _, err := strconv.Atoi(port); err != nil {
			return "", false
		}
		return host + ":" + port, true
	}
	return host, true
}

func validHostLabel(label string) bool {
	if label == "" || !isAlphanumeric(label[0]) ||
		!isAlphanumeric(label[len(label)-1]) {
		return false
	}
	for index := 0; index < len(label); index++ {
		if !isAlphanumeric(label[index]) && label[index] != '-' {
			return false
		}
	}
	return true
}

func isAlphanumeric(character byte) bool {
	return (character >= '0' && character <= '9') ||
		(character >= 'a' && character <= 'z') ||
		(character >= 'A' && character <= 'Z')
}

var _ CompleteRouteHandler = GitHubRepositoryRouteHandler{}
