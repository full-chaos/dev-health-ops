package providersync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// marshalRepositoryJSON matches the canonical Python ClickHouse repository
// encoder: sorted keys, compact separators, UTF-8 preserved, and no HTML-only
// escaping. The marshal/decode step deliberately turns structs into maps so
// the final encoder applies the same key ordering as Python's sort_keys=True.
func marshalRepositoryJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var canonical any
	if err := decoder.Decode(&canonical); err != nil {
		return nil, err
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(canonical); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(encoded.Bytes(), []byte{'\n'}), nil
}

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

// repositorySettings defines the GitHub repository settings fields. The
// shared encoder canonicalizes their persisted order across runtimes.
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
// The pair is RouteReady=true as of CHAOS-3123 (canary staging and live
// traffic parity are waived for this program; fixture-level field parity
// against the Python collector is the acceptance bar — see
// TestGitHubRepositoryRouteEmitsOneBoundedReposEffect and
// TestRepositoryIdentityMatchesPythonDerivation). Live non-empty parity
// against a real credentialed GitHub repository has NOT been captured; that
// remains open (TestGitHubRepositoryLiveParityHarness). There is no route
// enablement switch left to gate this on (CHAOS-4054); what actually stands
// between this pair and live traffic is CHAOS-4060's executed-proof gate,
// recorded on the descriptor as an explicit, dated ExecutedProofWaiver until
// a live unit reports persisted evidence. Capability metadata is not
// execution evidence (TRD §10.1).
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
	path := providerRelativePath(client, "repos", owner, repository)
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
	identity, err := repositoryIdentity(fullName)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	// Python coerces a missing, null, or empty default_branch to "main"
	// (providers/github/code_client.py::_repo_from_item). Persisting "" would
	// diverge for a freshly created or unusual repository.
	defaultBranch := payload.DefaultBranch
	if defaultBranch == "" {
		defaultBranch = "main"
	}
	settings, err := marshalRepositoryJSON(repositorySettings{
		Source:            "github",
		GitHubInstanceURL: instance,
		RepoID:            repoID,
		URL:               payload.HTMLURL,
		DefaultBranch:     defaultBranch,
	})
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	tagValues := []string{"github"}
	if payload.Language != "" {
		tagValues = append(tagValues, payload.Language)
	}
	tags, err := marshalRepositoryJSON(tagValues)
	if err != nil {
		return CompleteRouteBatch{}, providerfoundation.ErrNormalizationInvalid
	}
	row := repositoryRow{
		ID:         identity,
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
	// `repos` is ReplacingMergeTree(last_synced) keyed by (org_id, id), but
	// deduplication is asynchronous: between a crash-recovery reinsert and the
	// next merge, raw readers that join `repos` without FINAL/argMax would see
	// two physical rows and double their joined metrics. A blind replay is
	// therefore not safe, so the effect requires readback fencing.
	effect, err := effectBatchFromValues(
		"repos", EffectReadbackRequired, []repositoryRow{row},
	)
	if err != nil {
		return CompleteRouteBatch{}, err
	}
	return CompleteRouteBatch{
		Effects: []EffectBatch{effect},
		Result: map[string]any{
			"repos_synced":   1,
			"repo":           fullName,
			"repo_id":        repoID,
			"default_branch": defaultBranch,
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

// providerRelativePath joins path segments underneath the credential's
// configured base path. A leading-slash absolute path would make url.Parse
// *replace* the base path, silently dropping a GitHub Enterprise install's
// required `/api/v3` prefix and issuing the request against a non-API route.
func providerRelativePath(
	client *providerfoundation.HTTPClient,
	segments ...string,
) string {
	escaped := make([]string, 0, len(segments))
	for _, segment := range segments {
		escaped = append(escaped, url.PathEscape(segment))
	}
	joined := strings.Join(escaped, "/")
	if client == nil || client.BaseURL == nil {
		return "/" + joined
	}
	base := client.BaseURL.EscapedPath()
	if base == "" || base == "/" {
		return "/" + joined
	}
	return strings.TrimSuffix(base, "/") + "/" + joined
}

// repositoryIdentity mirrors Python's get_repo_uuid_from_repo for the ASCII
// repository names GitHub actually issues: the first sixteen bytes of SHA-256
// over the trimmed, lowercased identifier, rendered as a UUID without version
// or variant rewriting. Go and Python must agree on this value or every
// downstream repo_id foreign key forks.
//
// Two documented divergences are fail-closed rather than reproduced:
//
//   - Python honours a process-global REPO_UUID override. Go never hydrates
//     identity from process-global state (the same rule Unit.Validate applies
//     to environment-sourced credentials), so this returns an error rather
//     than writing an identity Python would not have written.
//   - Python's str.lower() applies full Unicode case mapping (U+0130 lowers to
//     "i" plus a combining dot), while Go's strings.ToLower applies simple
//     per-rune mapping. GitHub owner and repository names are restricted to
//     [A-Za-z0-9._-], so a non-ASCII identifier means something upstream is
//     already wrong; refusing it beats writing a forked repo_id.
func repositoryIdentity(repo string) (string, error) {
	// Python truthiness-checks the variable (`if env_uuid:`), so an empty
	// REPO_UUID is not an override there and must not be one here.
	if os.Getenv("REPO_UUID") != "" {
		return "", ErrRepositoryIdentityAmbiguous
	}
	trimmed := strings.TrimSpace(repo)
	if trimmed == "" {
		return "", providerfoundation.ErrNormalizationInvalid
	}
	for index := 0; index < len(trimmed); index++ {
		if trimmed[index] >= utf8.RuneSelf {
			return "", ErrRepositoryIdentityAmbiguous
		}
	}
	digest := sha256.Sum256([]byte(strings.ToLower(trimmed)))
	encoded := hex.EncodeToString(digest[:16])
	return encoded[0:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" +
		encoded[16:20] + "-" + encoded[20:32], nil
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
		// Python's urlsplit.port raises for a value outside 1-65535, which
		// normalized_operational_provider_instance turns into a rejection.
		number, err := strconv.Atoi(port)
		if err != nil || number < 1 || number > 65535 {
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
