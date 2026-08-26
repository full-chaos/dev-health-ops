package providersync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/google/uuid"
)

const gitHubProjectsV2IntegrationConfigKey = "github_projects_v2"

// GitHubProjectV2Target is durable, non-secret integration configuration.
//
// RATIFIED (D18, cutover Decision Log): targets are durable integration-scoped
// configuration read from the claim, and collection uses the already
// claim-resolved Credential/HTTPClient. Environment target/token fallback is
// not part of the Go route.
//
// Two deliberate divergences from the active Python producer follow from that,
// and are divergences of record rather than porting defects:
//
//   - Python reads targets from process-global GITHUB_PROJECTS_V2
//     (metrics/work_items.py:433) and swallows every malformed entry with a
//     bare `except Exception: continue`. Go reads durable config and fails
//     closed, because a silent-skip grammar over an operator-visible config
//     column would hide a typo as "no projects configured".
//   - Python builds a SECOND client from process-global GITHUB_TOKEN
//     (metrics/work_items.py:403-409), ignoring both the resolved integration
//     credential and its base URL. On GitHub Enterprise Server that client
//     reaches github.com while this one honors the claim's base URL.
//
// Ratification is not activation: this collector still owns no registration,
// readiness, watermark, or alias.
// KNOWN, NOT FIXED: a project_number above 2^53 loses precision before this
// struct ever sees it. The claim's integration_config is decoded by a plain
// json.Unmarshal into map[string]any, so every number is a float64 by the time
// it reaches here -- 9007199254740993 arrives as ...992. Documented rather than
// fixed because GitHub project numbers are small sequential integers per
// organisation and nothing near that bound is reachable; a json.Decoder with
// UseNumber() at the repository boundary would be the fix if that ever changes.
type GitHubProjectV2Target struct {
	OrgLogin      string `json:"org_login"`
	ProjectNumber int    `json:"project_number"`
}

// GitHubProjectV2Usage is the credential-free actual request accounting for
// Projects v2. Python classifies unprefixed GraphQL work-item traffic under
// work_item_prs/graphql_cost; keeping that vocabulary lets the eventual D16
// composer join actuals to the provider budget without exposing query or
// target details.
type GitHubProjectV2Usage struct {
	Transport    string
	RouteFamily  string
	Dimension    string
	RequestCount int
}

// GitHubProjectV2FetchResult is a semantic, unregistered fetch result. It is
// intentionally not CompleteRouteBatch and owns no effects or watermark.
type GitHubProjectV2FetchResult struct {
	Rows     githubWorkItemRows
	Evidence FetchEvidence
	Usage    GitHubProjectV2Usage
	Targets  int
	// MembershipSkips counts board items that produced NO membership row, by
	// bounded reason (CHAOS-4194). It exists because the defect this ticket
	// fixes was a SILENT drop: PR items were fetched fully hydrated and
	// discarded with no counter and no log, so the gap was invisible for as
	// long as it existed. Every remaining not-emitted case is now countable by
	// a label that says which case it is, so the next such gap is visible
	// before someone has to read the normalizer to find it.
	//
	// Issue items are deliberately absent from this map (CHAOS-4193): they are
	// no longer skipped at all, only deferred to the read-then-diff pass in
	// github_project_membership_snapshot_diff.go, which has no per-item
	// membership row to skip in the first place.
	MembershipSkips map[string]int
	// Snapshots is one entry per fetched project, carrying every board item
	// this sync could positively identify (CHAOS-4193). It is the current
	// half of the snapshot-diff producer's read-then-diff -- the ClickHouse
	// read supplies the prior half -- and is otherwise unused by this Fetch
	// call, which never touches ClickHouse itself.
	Snapshots []githubProjectV2BoardSnapshot
	// Incomplete carries durable evidence for a board response that GitHub
	// returned in a structurally degraded shape. The route keeps the other
	// rows, but this entry withholds the family watermark until a later sync
	// observes the board authoritatively.
	Incomplete []GitHubWorkItemsIncomplete
}

const (
	githubProjectsV2IncompleteComponent = "projects_v2"
	githubProjectsV2NullOrganization    = "null_organization"
	githubProjectsV2NullProject         = "null_project"
	githubProjectsV2StructuralDegraded  = "structural_degradation"
	githubProjectsV2UnidentifiedItem    = "unidentified_item"
)

func githubProjectsV2DegradedCause(cause string) bool {
	switch cause {
	case githubProjectsV2NullOrganization, githubProjectsV2NullProject,
		githubProjectsV2StructuralDegraded, githubProjectsV2UnidentifiedItem:
		return true
	default:
		return false
	}
}

// GitHubProjectV2Fetcher preserves Python's per-source fanout: callers may
// invoke it once for each existing work-items claim, and each invocation
// fetches the claim's complete durable target list.
//
// That fanout is an AMPLIFICATION, not a mirror, and D18 accepts it only
// temporarily. Python calls parse_github_projects_v2_env() once per JOB —
// job_work_items.py sits it at the same indentation as
// `for discovered_repo in discovered_repos`, i.e. OUTSIDE the repo loop — and
// merges the result once at the end. The Go unit boundary is per-source, so
// every claim refetches the same org-wide projects. Collapsing that to
// one fetch per integration moves the D16 unit boundary and is therefore a
// separately tracked follow-up with its own oracle strategy; it must not be
// smuggled in here, and it must not gate the all-or-nothing alias activation.
type GitHubProjectV2Fetcher struct{}

func githubProjectV2Targets(claim Claim) ([]GitHubProjectV2Target, error) {
	value, configured := claim.IntegrationConfig[gitHubProjectsV2IntegrationConfigKey]
	// Absent key: Projects v2 is simply not configured for this integration.
	if !configured {
		return []GitHubProjectV2Target{}, nil
	}
	// A present key holding JSON null deliberately does NOT return here. It
	// FAILED OPEN until CHAOS-3123, when this branch read
	// `!configured || value == nil` and answered "not configured" for a key the
	// operator had written -- Postgres JSONB null decodes to an untyped nil
	// interface, so that is the shape production actually delivers, not the
	// Go-literal typed nil a test would reach for. Dropping the clause is the
	// whole fix: null now falls through, marshals to `null`, decodes to a nil
	// target list, and is refused by the `targets == nil` check below.
	//
	// No separate `if value == nil { refuse }` guard sits here on purpose. One
	// was written and measured first: its mutation SURVIVED, because deleting it
	// changed no behaviour at all -- the decode path already refuses. That is
	// the third unkillable clause this file has grown and removed; a guard whose
	// removal is undetectable is not defence in depth, it is a coverage claim
	// nothing backs.
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalidConfiguration
	}
	// TRIPWIRE -- the case-insensitivity guarantee below is EMERGENT, not
	// chosen. encoding/json matches field names case-insensitively, so a
	// miscased duplicate key (`Org_Login` beside `org_login`) is a candidate to
	// win. It cannot today only because `value` arrived as a map[string]any and
	// json.Marshal emits map keys in sorted byte order: uppercase and `_` sort
	// below lowercase, so the canonical spelling is emitted last and
	// deterministically wins the decode.
	//
	// That holds ONLY while the value makes the map -> re-marshal round trip.
	// If the one-fetch-per-integration follow-up ever decodes raw JSONB bytes
	// straight from Postgres, operator key order wins instead, and a trailing
	// miscased duplicate WOULD take effect. Anyone making that change owns
	// re-deciding this -- DisallowUnknownFields does not catch it, because a
	// miscased key MATCHES rather than being unknown.
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var targets []GitHubProjectV2Target
	if err := decoder.Decode(&targets); err != nil || targets == nil {
		return nil, ErrInvalidConfiguration
	}
	for index := range targets {
		targets[index].OrgLogin = strings.TrimSpace(targets[index].OrgLogin)
		if targets[index].OrgLogin == "" || targets[index].ProjectNumber < 1 {
			return nil, ErrInvalidConfiguration
		}
	}
	return targets, nil
}

func (GitHubProjectV2Fetcher) Fetch(
	ctx context.Context,
	claim Claim,
	credential providerfoundation.Credential,
	client *providerfoundation.HTTPClient,
	normalizedAt time.Time,
	resolveIdentity githubIdentityResolver,
) (GitHubProjectV2FetchResult, error) {
	// There is deliberately no `credential.ID == ""` clause. claim.Validate()
	// has already returned nil by the time this is evaluated, and Unit.Validate
	// refuses an empty CredentialID (lease.go), so an empty credential.ID is
	// necessarily unequal to the claim's and the equality clause below already
	// decides it. A clause that cannot fail on its own is not defence in depth;
	// it is an unkillable mutation that reads as coverage forever — measured as
	// exactly that before removal.
	if ctx == nil || claim.Validate() != nil || claim.Provider != "github" ||
		!isWorkItemFamilyDataset(claim.Dataset) || credential.Provider != "github" ||
		credential.ID != claim.CredentialID || client == nil ||
		client.Provider != "github" || client.BaseURL == nil || client.Lease == nil ||
		normalizedAt.IsZero() {
		return GitHubProjectV2FetchResult{}, ErrInvalidConfiguration
	}
	targets, err := githubProjectV2Targets(claim)
	if err != nil {
		return GitHubProjectV2FetchResult{}, err
	}
	result := GitHubProjectV2FetchResult{
		Rows: githubWorkItemRows{
			WorkItems: []githubWorkItemRow{}, StatusTransitions: []githubWorkItemTransitionRow{},
			Dependencies: []githubWorkItemDependencyRow{}, ReopenEvents: []githubWorkItemReopenRow{},
			Interactions: []githubWorkItemInteractionRow{}, Sprints: []githubSprintRow{},
			AIAttributions:     []githubAIAttributionRow{},
			ProjectMemberships: []projectmembership.Row{}, Projects: []projectmembership.CatalogRow{},
		},
		MembershipSkips: map[string]int{},
		Evidence:        FetchEvidence{Provider: "github", Dataset: "projects-v2"},
		Usage: GitHubProjectV2Usage{
			Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost,
		},
		Targets: len(targets),
	}
	if len(targets) == 0 {
		return result, nil
	}
	counted := *client
	counted.Doer = gitHubProjectV2CountingDoer{
		delegate: client.Doer, requests: &result.Evidence.Requests,
		graphqlRequests: &result.Usage.RequestCount, graphqlPath: gitHubGraphQLPath(client),
	}
	// Python's dict preserves first insertion position while later values win.
	// Preserve that exact behavior across targets, including duplicate targets.
	workItemIndex := map[string]int{}
	for _, target := range targets {
		targetResult, err := fetchGitHubProjectV2Target(ctx, &counted, target, &result.Evidence)
		if err != nil {
			return finishGitHubProjectV2Fetch(result), err
		}
		items := targetResult.Items
		projectScopeID := fmt.Sprintf("ghprojv2:%s#%d", target.OrgLogin, target.ProjectNumber)
		// The board itself has to exist in `projects` before anything can
		// point at it. Nothing wrote this row before CHAOS-4194 -- the fetcher
		// stamped the id onto work items and the entity it named was never
		// created -- so every github membership would have been filtered out
		// by the resolve-to-`projects` constraint. `projects` is a
		// ReplacingMergeTree keyed (org_id, provider, id), so emitting it once
		// per sync converges rather than accumulating.
		result.Rows.Projects = append(result.Rows.Projects, projectmembership.EnsureProjectsRow(
			claim.OrgID, "github", projectScopeID, "",
			fmt.Sprintf("%s #%d", target.OrgLogin, target.ProjectNumber), normalizedAt,
		))
		boardSubjects := make([]githubProjectV2SnapshotSubject, 0, len(items))
		// boardIncomplete tracks whether this sync could positively identify
		// EVERY board item that has a subject to name (codex round 1 finding,
		// CHAOS-4193d). A DraftIssue genuinely has no subject -- omitting it is
		// complete information, not a gap. An unidentifiable PullRequest or
		// Issue (missing repository/number, or a typename GitHub has not told
		// us about yet) is different: the item IS still on the board, this
		// sync simply could not name it, and treating its absence from
		// boardSubjects as "gone" would tell the diff a still-present subject
		// was removed. See githubProjectV2BoardSnapshot.Complete.
		boardIncomplete := false
		for _, item := range items {
			// Membership is decided BEFORE the work-item normalization, and
			// independently of it. A pull request is not a work item and never
			// becomes one -- PRs live in git_pull_requests -- but it is a first
			// class board member, and conflating the two questions is exactly
			// how its membership came to be discarded: the normalizer answered
			// "not a work item" and the loop read that as "nothing here".
			if membership, ok := githubProjectV2MembershipRow(claim, item, projectScopeID, normalizedAt); ok {
				result.Rows.ProjectMemberships = append(result.Rows.ProjectMemberships, membership)
			} else if item.Content.Typename != "Issue" {
				// An Issue's membership is no longer a skip at all
				// (CHAOS-4193): it is deferred to the snapshot-diff pass
				// below, which has no per-item row to skip in the first
				// place. Every OTHER not-emitted case still counts here
				// unchanged.
				result.MembershipSkips[githubProjectV2MembershipSkipReason(item)]++
			}
			if subject, ok := githubProjectV2ItemSubject(item); ok {
				boardSubjects = append(boardSubjects, subject)
			} else if item.Content.Typename != "DraftIssue" {
				boardIncomplete = true
			}
			row, transitions, emitted, err := normalizeGitHubProjectV2Item(
				claim, item, projectScopeID, resolveIdentity, normalizedAt,
			)
			if err != nil {
				return finishGitHubProjectV2Fetch(result), err
			}
			if !emitted {
				continue
			}
			if index, exists := workItemIndex[row.WorkItemID]; exists {
				result.Rows.WorkItems[index] = row
			} else {
				workItemIndex[row.WorkItemID] = len(result.Rows.WorkItems)
				result.Rows.WorkItems = append(result.Rows.WorkItems, row)
			}
			result.Rows.StatusTransitions = append(result.Rows.StatusTransitions, transitions...)
		}
		complete := targetResult.Complete && !boardIncomplete
		result.Snapshots = append(result.Snapshots, githubProjectV2BoardSnapshot{
			ProjectScopeID: projectScopeID, Subjects: boardSubjects, Complete: complete,
		})
		if targetResult.DegradedReason != "" {
			result.Incomplete = append(result.Incomplete, GitHubWorkItemsIncomplete{
				Component: githubProjectsV2IncompleteComponent,
				Cause:     targetResult.DegradedReason,
			})
		}
		// boardIncomplete already suppressed this board's removals (Complete
		// above), but that alone left the family watermark free to advance --
		// a later incremental sync would then start from a point that never
		// saw the unidentified item positively, with no durable signal a
		// retry is owed. Record it exactly like a target-level degradation so
		// the route withholds the watermark too (codex adversarial review,
		// CHAOS-4289 round 1).
		if boardIncomplete {
			client.Metrics.RecordProjectsV2DegradedSnapshot(githubProjectsV2UnidentifiedItem)
			slog.Warn("github_projects_v2.degraded_snapshot",
				"reason", githubProjectsV2UnidentifiedItem, "org_login", target.OrgLogin,
				"project_number", target.ProjectNumber)
			result.Incomplete = append(result.Incomplete, GitHubWorkItemsIncomplete{
				Component: githubProjectsV2IncompleteComponent,
				Cause:     githubProjectsV2UnidentifiedItem,
			})
		}
	}
	reportGitHubProjectV2MembershipSkips(claim, result.MembershipSkips)
	return finishGitHubProjectV2Fetch(result), nil
}

// gitHubProjectV2AttentionSkips are the skip reasons that mean something
// CHANGED, as opposed to something we deliberately deferred.
//
// A board item whose typename we do not recognise means GitHub shipped a new
// content kind; a PullRequest missing its repository, number or createdAt means
// the query or the payload shape moved under us. Neither is self-correcting and
// both are invisible without this line.
var gitHubProjectV2AttentionSkips = []string{"pull_request_incomplete", "unknown_content_type"}

// reportGitHubProjectV2MembershipSkips emits the one structured record that
// makes MembershipSkips observable in production.
//
// A count on a result struct is not observability: nothing reads
// GitHubProjectV2FetchResult except the caller that built it, and this
// collector owns no registration to publish a Prometheus series through (D18),
// so without this the counter would be exactly what it replaced -- a drop
// nobody can see, with a nicer name in the source. The reserved metric name for
// when that collector is activated is
// worker_github_project_v2_membership_skips_total{reason}; it is deliberately
// not declared yet, because a series with no emitter can never move and would
// read as coverage of a case nobody exercised.
//
// Emitted only when something was actually skipped. A line on every sync
// regardless of content is noise operators learn to filter, which costs the
// record the attention it exists to buy.
//
// The LEVEL is chosen by content rather than fixed. Every sync of a real board
// defers every issue on it, so a fixed WARN would warn permanently about
// working as designed and would bury the reasons that are genuinely news inside
// that noise. A fixed INFO would put a provider schema change at the level
// operators filter out. So a deferral logs INFO and an attention reason
// escalates the same record to WARN.
//
// Each reason is its own attribute and a reason with no occurrences is OMITTED,
// not written as zero -- a permanent zero reads as coverage of a case nobody
// exercised. Attributes are sorted so the record is stable across runs; Go map
// iteration order is randomised, and an unstable attribute order makes two
// identical fetches look like different events to a log backend.
//
// Not called on the error return paths. A failed fetch's partial skip counts
// describe how far it got, not what it decided, and reporting them would put a
// misleading "we skipped these" record next to the error that actually matters.
func reportGitHubProjectV2MembershipSkips(claim Claim, skips map[string]int) {
	if len(skips) == 0 {
		return
	}
	reasons := make([]string, 0, len(skips))
	for reason := range skips {
		reasons = append(reasons, reason)
	}
	sort.Strings(reasons)
	attrs := []any{
		"provider", claim.Provider, "dataset", claim.Dataset,
		"org_id", claim.OrgID, "unit", claim.ID, "integration", claim.IntegrationID,
	}
	level := slog.LevelInfo
	for _, reason := range reasons {
		attrs = append(attrs, reason, int64(skips[reason]))
		if skips[reason] > 0 && slices.Contains(gitHubProjectV2AttentionSkips, reason) {
			level = slog.LevelWarn
		}
	}
	slog.Log(context.Background(), level, "github_projects_v2.membership_skips", attrs...)
}

func finishGitHubProjectV2Fetch(result GitHubProjectV2FetchResult) GitHubProjectV2FetchResult {
	result.Evidence.Records = len(result.Rows.WorkItems) + len(result.Rows.StatusTransitions) +
		len(result.Rows.ProjectMemberships) + len(result.Rows.Projects)
	return result
}

type gitHubProjectV2CountingDoer struct {
	delegate        providerfoundation.HTTPDoer
	requests        *int
	graphqlRequests *int
	graphqlPath     string
}

type gitHubProjectV2TargetFetchResult struct {
	Items          []gitHubProjectV2ItemPayload
	Complete       bool
	DegradedReason string
}

func (doer gitHubProjectV2CountingDoer) Do(request *http.Request) (*http.Response, error) {
	*doer.requests++
	if request.URL.EscapedPath() == doer.graphqlPath {
		*doer.graphqlRequests++
	}
	return doer.delegate.Do(request)
}

func fetchGitHubProjectV2Target(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	target GitHubProjectV2Target,
	evidence *FetchEvidence,
) (gitHubProjectV2TargetFetchResult, error) {
	items := []gitHubProjectV2ItemPayload{}
	outerCursor := ""
	seenOuter := map[string]struct{}{}
	for {
		variables := map[string]any{
			"login": target.OrgLogin, "number": target.ProjectNumber,
			"first": 50, "after": nil,
		}
		if outerCursor != "" {
			variables["after"] = outerCursor
		}
		var envelope gitHubProjectV2ItemsEnvelope
		if err := fetchGitHubProjectV2GraphQL(ctx, client, gitHubProjectsV2ItemsQuery, variables, &envelope, evidence); err != nil {
			return gitHubProjectV2TargetFetchResult{}, err
		}
		if envelope.Data.Organization == nil {
			client.Metrics.RecordProjectsV2DegradedSnapshot(githubProjectsV2NullOrganization)
			slog.Warn("github_projects_v2.degraded_snapshot",
				"reason", githubProjectsV2NullOrganization, "org_login", target.OrgLogin,
				"project_number", target.ProjectNumber)
			return gitHubProjectV2TargetFetchResult{
				Items: items, DegradedReason: githubProjectsV2NullOrganization,
			}, nil
		}
		if envelope.Data.Organization.ProjectV2 == nil {
			client.Metrics.RecordProjectsV2DegradedSnapshot(githubProjectsV2NullProject)
			slog.Warn("github_projects_v2.degraded_snapshot",
				"reason", githubProjectsV2NullProject, "org_login", target.OrgLogin,
				"project_number", target.ProjectNumber)
			return gitHubProjectV2TargetFetchResult{
				Items: items, DegradedReason: githubProjectsV2NullProject,
			}, nil
		}
		if envelope.Data.Organization.ProjectV2.Items == nil || envelope.Data.Organization.ProjectV2.Items.Nodes == nil {
			client.Metrics.RecordProjectsV2DegradedSnapshot(githubProjectsV2StructuralDegraded)
			slog.Warn("github_projects_v2.degraded_snapshot",
				"reason", githubProjectsV2StructuralDegraded, "org_login", target.OrgLogin,
				"project_number", target.ProjectNumber)
			return gitHubProjectV2TargetFetchResult{
				Items: items, DegradedReason: githubProjectsV2StructuralDegraded,
			}, nil
		}
		connection := *envelope.Data.Organization.ProjectV2.Items
		paginationComplete := connection.PageInfo != nil && connection.PageInfo.HasNextPage != nil
		for index := range connection.Nodes {
			item := connection.Nodes[index]
			if item.Changes.PageInfo == nil || item.Changes.PageInfo.HasNextPage == nil {
				paginationComplete = false
			} else if *item.Changes.PageInfo.HasNextPage {
				cursor := strings.TrimSpace(item.Changes.PageInfo.EndCursor)
				if cursor == "" || strings.TrimSpace(item.ID) == "" {
					return gitHubProjectV2TargetFetchResult{}, providerfoundation.ErrPaginationInvalid
				}
				seenChanges := map[string]struct{}{}
				for {
					if _, repeated := seenChanges[cursor]; repeated {
						return gitHubProjectV2TargetFetchResult{}, providerfoundation.ErrPaginationInvalid
					}
					seenChanges[cursor] = struct{}{}
					var continuation gitHubProjectV2ChangesEnvelope
					if err := fetchGitHubProjectV2GraphQL(ctx, client, gitHubProjectsV2ChangesQuery,
						map[string]any{"itemId": item.ID, "after": cursor}, &continuation, evidence); err != nil {
						return gitHubProjectV2TargetFetchResult{}, err
					}
					if continuation.Data.Node == nil {
						return gitHubProjectV2TargetFetchResult{}, providerfoundation.ErrPaginationInvalid
					}
					more := continuation.Data.Node.Changes
					item.Changes.Nodes = append(item.Changes.Nodes, more.Nodes...)
					if more.PageInfo == nil || more.PageInfo.HasNextPage == nil {
						paginationComplete = false
						break
					}
					if !*more.PageInfo.HasNextPage {
						break
					}
					next := strings.TrimSpace(more.PageInfo.EndCursor)
					if next == "" || next == cursor {
						return gitHubProjectV2TargetFetchResult{}, providerfoundation.ErrPaginationInvalid
					}
					cursor = next
				}
			}
			items = append(items, item)
		}
		if !paginationComplete {
			client.Metrics.RecordProjectsV2DegradedSnapshot(githubProjectsV2StructuralDegraded)
			slog.Warn("github_projects_v2.degraded_snapshot",
				"reason", githubProjectsV2StructuralDegraded, "org_login", target.OrgLogin,
				"project_number", target.ProjectNumber)
			return gitHubProjectV2TargetFetchResult{
				Items: items, DegradedReason: githubProjectsV2StructuralDegraded,
			}, nil
		}
		if !*connection.PageInfo.HasNextPage {
			return gitHubProjectV2TargetFetchResult{Items: items, Complete: true}, nil
		}
		next := strings.TrimSpace(connection.PageInfo.EndCursor)
		if next == "" || next == outerCursor {
			return gitHubProjectV2TargetFetchResult{}, providerfoundation.ErrPaginationInvalid
		}
		if _, repeated := seenOuter[next]; repeated {
			return gitHubProjectV2TargetFetchResult{}, providerfoundation.ErrPaginationInvalid
		}
		seenOuter[next] = struct{}{}
		outerCursor = next
	}
}

func fetchGitHubProjectV2GraphQL(
	ctx context.Context,
	client *providerfoundation.HTTPClient,
	query string,
	variables map[string]any,
	destination any,
	evidence *FetchEvidence,
) error {
	body, err := json.Marshal(map[string]any{"query": query, "variables": variables})
	if err != nil {
		return providerfoundation.ErrNormalizationInvalid
	}
	response, err := client.Do(ctx, http.MethodPost, gitHubGraphQLPath(client), bytes.NewReader(body))
	if err != nil {
		return err
	}
	evidence.Pages++
	defer response.Body.Close()
	decoder := json.NewDecoder(response.Body)
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return providerfoundation.ErrPaginationInvalid
	}
	var errorsHolder interface{ graphQLErrors() []json.RawMessage }
	if typed, ok := destination.(interface{ graphQLErrors() []json.RawMessage }); ok {
		errorsHolder = typed
	}
	if errorsHolder != nil && len(errorsHolder.graphQLErrors()) > 0 {
		return providerfoundation.ErrGraphQLResponse
	}
	return nil
}

type gitHubProjectV2PageInfo struct {
	HasNextPage *bool  `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

type gitHubProjectV2Connection[T any] struct {
	Nodes    []T                      `json:"nodes"`
	PageInfo *gitHubProjectV2PageInfo `json:"pageInfo"`
}

type gitHubProjectV2ItemsEnvelope struct {
	Data struct {
		Organization *struct {
			ProjectV2 *struct {
				Items *gitHubProjectV2Connection[gitHubProjectV2ItemPayload] `json:"items"`
			} `json:"projectV2"`
		} `json:"organization"`
	} `json:"data"`
	Errors []json.RawMessage `json:"errors"`
}

func (envelope *gitHubProjectV2ItemsEnvelope) graphQLErrors() []json.RawMessage {
	return envelope.Errors
}

type gitHubProjectV2ChangesEnvelope struct {
	Data struct {
		Node *struct {
			Changes gitHubProjectV2Connection[gitHubProjectV2ChangePayload] `json:"changes"`
		} `json:"node"`
	} `json:"data"`
	Errors []json.RawMessage `json:"errors"`
}

func (envelope *gitHubProjectV2ChangesEnvelope) graphQLErrors() []json.RawMessage {
	return envelope.Errors
}

type gitHubProjectV2ItemPayload struct {
	ID          string                                                      `json:"id"`
	CreatedAt   *string                                                     `json:"createdAt"`
	UpdatedAt   *string                                                     `json:"updatedAt"`
	Content     gitHubProjectV2ContentPayload                               `json:"content"`
	FieldValues gitHubProjectV2Connection[gitHubProjectV2FieldValuePayload] `json:"fieldValues"`
	Changes     gitHubProjectV2Connection[gitHubProjectV2ChangePayload]     `json:"changes"`
}

type gitHubProjectV2ContentPayload struct {
	Typename   string  `json:"__typename"`
	ID         string  `json:"id"`
	Number     int     `json:"number"`
	Title      string  `json:"title"`
	Body       *string `json:"body"`
	URL        *string `json:"url"`
	State      *string `json:"state"`
	CreatedAt  *string `json:"createdAt"`
	UpdatedAt  *string `json:"updatedAt"`
	ClosedAt   *string `json:"closedAt"`
	Repository struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"repository"`
	Labels    gitHubProjectV2Connection[githubWorkItemLabelPayload] `json:"labels"`
	Assignees gitHubProjectV2Connection[githubWorkItemUserPayload]  `json:"assignees"`
	Author    *githubWorkItemUserPayload                            `json:"author"`
}

type gitHubProjectV2FieldValuePayload struct {
	Typename string      `json:"__typename"`
	Name     *string     `json:"name"`
	Title    *string     `json:"title"`
	ID       *string     `json:"id"`
	Number   json.Number `json:"number"`
	Field    struct {
		Name string `json:"name"`
	} `json:"field"`
}

type gitHubProjectV2ChangePayload struct {
	Field struct {
		Name string `json:"name"`
	} `json:"field"`
	PreviousValue *struct {
		Name *string `json:"name"`
	} `json:"previousValue"`
	NewValue *struct {
		Name *string `json:"name"`
	} `json:"newValue"`
	CreatedAt *string `json:"createdAt"`
	Actor     *struct {
		Login *string `json:"login"`
	} `json:"actor"`
}

// githubProjectV2MembershipRow builds the board-membership row for one Projects
// v2 item, or reports that the item cannot carry one.
//
// PULL REQUESTS ARE THE POINT. Before CHAOS-4194 a PR item was fetched fully
// hydrated -- the GraphQL query already selects the `... on PullRequest`
// fragment -- and then discarded by the normalizer with no counter and no log,
// so a PR's board membership existed nowhere in the graph. That was a
// normalizer POLICY choice, not an API limitation, which is why removing it is
// the whole fix on this side.
//
// occurred_at is the ITEM's createdAt, not the sync clock, and that choice is
// load-bearing rather than cosmetic. A ProjectV2Item's createdAt is when the
// subject was added to this board, which is precisely the membership event --
// and it is stable across re-syncs, so the content-determined event_id derived
// from it is stable too and ReplacingMergeTree collapses a re-sync back to one
// row. Stamping the sync time instead would put a per-sync value into the
// sorting key and accumulate one row per sync of a single membership, which is
// the same interaction that made occurred_at required at the sink.
//
// An item missing any of repository, number, or createdAt yields NO row rather
// than a defaulted one. Each of those is a sorting-key member or the basis of
// one, so a placeholder would key the row to something that does not exist
// while looking entirely well-formed -- and the caller counts the skip, so the
// absence is visible.
//
// Issues are NOT emitted here, and stay that way even now that CHAOS-4193's
// snapshot-diff pass has shipped: emitting an issue row from THIS loop would
// have to invent an occurred_at for a change nobody observed, which is exactly
// what the diff exists to avoid -- it uses the diff's own observation time
// instead (ruling A case (2), github_project_membership_snapshot_diff.go).
// Issue additions and removals of either subject kind are that file's job, not
// this function's; this function stays the ruling A case (1) native-event path
// for pull requests only.
func githubProjectV2MembershipRow(
	claim Claim,
	item gitHubProjectV2ItemPayload,
	projectScopeID string,
	normalizedAt time.Time,
) (projectmembership.Row, bool) {
	if claim.Validate() != nil || item.Content.Typename != "PullRequest" ||
		strings.TrimSpace(projectScopeID) == "" || normalizedAt.IsZero() {
		return projectmembership.Row{}, false
	}
	repository := strings.TrimSpace(item.Content.Repository.NameWithOwner)
	addedAt := parseGitHubWorkItemTime(item.CreatedAt)
	if repository == "" || item.Content.Number <= 0 || addedAt == nil {
		return projectmembership.Row{}, false
	}
	identity, err := repositoryIdentity(repository)
	if err != nil {
		return projectmembership.Row{}, false
	}
	repoID, err := uuid.Parse(identity)
	if err != nil {
		return projectmembership.Row{}, false
	}
	row := projectmembership.Row{
		OrgID: claim.OrgID, SubjectKind: projectmembership.SubjectPullRequest,
		SubjectID: strconv.Itoa(item.Content.Number), RepoID: repoID,
		Provider: "github",
		// from_* is empty: a snapshot observation of current membership has no
		// observed predecessor, which is the same first-assignment case
		// work_item_transitions already spells "" rather than NULL.
		//
		// to_project_key is empty because GitHub Projects v2 HAS no project key
		// -- a board is a number and a title. The sink admits an id without a
		// key for exactly this reason; only a key without an id is refused,
		// since that is what cannot resolve to a `projects` row.
		ToProjectID: projectScopeID,
		OccurredAt:  addedAt.UTC(), LastSynced: normalizedAt.UTC(),
	}
	row.EventID = projectmembership.EventID(row)
	return row, true
}

// githubProjectV2MembershipSkipReason labels one not-emitted item with a
// BOUNDED reason. Bounded because it becomes a Prometheus label: an unbounded
// one (the raw typename, say) would let a provider schema change mint
// unbounded series.
//
// The three values are genuinely different situations and are kept apart
// rather than folded into one "skipped" bucket, since the response to each
// differs: a draft issue can never carry a membership at all, an incomplete
// pull request is a real signal that the query or the payload changed, and an
// unknown typename means GitHub added a content kind nobody has looked at.
//
// Issue is deliberately ABSENT from this switch (CHAOS-4193, retiring the
// former "issue_deferred_to_snapshot_diff" label): the caller no longer calls
// this function for an Issue item at all, because an Issue is no longer
// skipped -- it is positively handled by the snapshot-diff pass, which has no
// per-item row to label a skip against.
func githubProjectV2MembershipSkipReason(item gitHubProjectV2ItemPayload) string {
	switch item.Content.Typename {
	case "PullRequest":
		return "pull_request_incomplete"
	case "DraftIssue":
		return "draft_issue_has_no_subject"
	default:
		return "unknown_content_type"
	}
}

func normalizeGitHubProjectV2Item(
	claim Claim,
	item gitHubProjectV2ItemPayload,
	projectScopeID string,
	resolveIdentity githubIdentityResolver,
	normalizedAt time.Time,
) (githubWorkItemRow, []githubWorkItemTransitionRow, bool, error) {
	if claim.Validate() != nil || claim.Provider != "github" || !isWorkItemFamilyDataset(claim.Dataset) ||
		strings.TrimSpace(projectScopeID) == "" || normalizedAt.IsZero() {
		return githubWorkItemRow{}, nil, false, ErrInvalidConfiguration
	}
	if item.Content.Typename == "PullRequest" || (item.Content.Typename != "Issue" && item.Content.Typename != "DraftIssue") {
		return githubWorkItemRow{}, []githubWorkItemTransitionRow{}, false, nil
	}
	statusRaw, sprintName, sprintID := "", "", ""
	var storyPoints *float64
	for _, value := range item.FieldValues.Nodes {
		fieldName := normalizeWorkItemLabel(value.Field.Name)
		switch value.Typename {
		case "ProjectV2ItemFieldSingleSelectValue":
			if fieldName == "status" && value.Name != nil {
				statusRaw = *value.Name
			}
		case "ProjectV2ItemFieldIterationValue":
			if strings.Contains(fieldName, "iteration") || strings.Contains(fieldName, "sprint") {
				if value.Title != nil {
					sprintName = *value.Title
				}
				if value.ID != nil {
					sprintID = *value.ID
				}
			}
		case "ProjectV2ItemFieldNumberValue":
			if fieldName == "estimate" || fieldName == "points" || fieldName == "story points" || fieldName == "size" {
				if parsed, err := strconv.ParseFloat(string(value.Number), 64); err == nil {
					storyPoints = &parsed
				}
			}
		}
	}
	content := item.Content
	workItemID := "ghproj:" + item.ID
	if content.Typename == "Issue" && strings.TrimSpace(content.Repository.NameWithOwner) != "" && content.Number > 0 {
		workItemID = "gh:" + content.Repository.NameWithOwner + "#" + strconv.Itoa(content.Number)
	}
	transitions := make([]githubWorkItemTransitionRow, 0, len(item.Changes.Nodes))
	for _, change := range item.Changes.Nodes {
		fieldName := normalizeWorkItemLabel(change.Field.Name)
		if fieldName != "status" && fieldName != "phase" {
			continue
		}
		occurredAt := parseGitHubWorkItemTime(change.CreatedAt)
		if occurredAt == nil || change.NewValue == nil || change.NewValue.Name == nil || strings.TrimSpace(*change.NewValue.Name) == "" {
			continue
		}
		fromRaw, toRaw := "", strings.TrimSpace(*change.NewValue.Name)
		if change.PreviousValue != nil && change.PreviousValue.Name != nil {
			fromRaw = strings.TrimSpace(*change.PreviousValue.Name)
		}
		var actor *string
		if change.Actor != nil && change.Actor.Login != nil {
			identity := resolveGitHubWorkItemIdentity(githubWorkItemUserPayload{Login: change.Actor.Login}, resolveIdentity)
			if identity != "" && identity != "unknown" {
				actor = &identity
			}
		}
		transition := githubWorkItemTransitionRow{
			WorkItemID: workItemID, Provider: "github", OccurredAt: occurredAt.UTC(),
			FromStatusRaw: nullableString(fromRaw), ToStatusRaw: nullableString(toRaw),
			FromStatus: githubProjectV2Status(fromRaw, nil, ""),
			ToStatus:   githubProjectV2Status(toRaw, nil, ""), Actor: actor, OrgID: claim.OrgID,
			LastSynced: normalizedAt.UTC(),
		}
		if err := transition.validate(claim); err != nil {
			return githubWorkItemRow{}, nil, false, err
		}
		transitions = append(transitions, transition)
	}
	createdAt := parseGitHubWorkItemTime(content.CreatedAt)
	if createdAt == nil {
		createdAt = parseGitHubWorkItemTime(item.CreatedAt)
	}
	if createdAt == nil {
		fallback := normalizedAt.UTC()
		createdAt = &fallback
	}
	updatedAt := parseGitHubWorkItemTime(content.UpdatedAt)
	if updatedAt == nil {
		updatedAt = parseGitHubWorkItemTime(item.UpdatedAt)
	}
	if updatedAt == nil {
		copy := *createdAt
		updatedAt = &copy
	}
	closedAt := parseGitHubWorkItemTime(content.ClosedAt)
	labels := make([]string, 0, len(content.Labels.Nodes))
	for _, label := range content.Labels.Nodes {
		if label.Name != "" {
			labels = append(labels, label.Name)
		}
	}
	assignees := make([]string, 0, len(content.Assignees.Nodes))
	for _, user := range content.Assignees.Nodes {
		identity := resolveGitHubWorkItemIdentity(user, resolveIdentity)
		if identity != "" && identity != "unknown" {
			assignees = append(assignees, identity)
		}
	}
	var reporter *string
	if content.Author != nil {
		identity := resolveGitHubWorkItemIdentity(*content.Author, resolveIdentity)
		if identity != "" && identity != "unknown" {
			reporter = &identity
		}
	}
	state := ""
	if content.State != nil {
		state = *content.State
	}
	status := githubProjectV2Status(statusRaw, labels, state)
	row := githubWorkItemRow{
		WorkItemID: workItemID, Provider: "github", Title: content.Title, Type: "issue",
		Status: status, StatusRaw: nullableString(statusRaw), Description: content.Body,
		RepoID: nil, ProjectID: stringPointer(projectScopeID), Assignees: assignees,
		Reporter: reporter, CreatedAt: createdAt.UTC(), UpdatedAt: updatedAt.UTC(),
		ClosedAt: closedAt, Labels: labels, StoryPoints: storyPoints, URL: content.URL,
		OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
	}
	if statusRaw == "" {
		row.StatusRaw = nullableString(state)
	}
	if sprintID != "" {
		row.SprintID = &sprintID
	}
	if sprintName != "" {
		row.SprintName = &sprintName
	}
	if closedAt != nil {
		completed := closedAt.UTC()
		row.CompletedAt = &completed
	}
	if content.Typename == "Issue" {
		row.Type = githubIssueType(labels)
	}
	if row.Description != nil && *row.Description == "" {
		row.Description = nil
	}
	if err := validateGitHubProjectV2Row(claim, row); err != nil {
		return githubWorkItemRow{}, nil, false, err
	}
	return row, transitions, true, nil
}

func githubProjectV2Status(statusRaw string, labels []string, state string) string {
	if statusRaw != "" {
		switch normalizeWorkItemLabel(statusRaw) {
		case "backlog", "icebox", "triage":
			return "backlog"
		case "todo", "to do", "ready", "ready for dev":
			return "todo"
		case "in progress", "doing", "wip":
			return "in_progress"
		case "in review", "review", "code review", "qa":
			return "in_review"
		case "blocked", "on hold":
			return "blocked"
		case "done", "closed", "resolved":
			return "done"
		case "canceled", "cancelled", "won't do":
			return "canceled"
		}
	}
	return githubIssueStatus(state, labels)
}

func validateGitHubProjectV2Row(claim Claim, row githubWorkItemRow) error {
	if row.Provider != "github" || row.OrgID == "" || row.OrgID != claim.OrgID ||
		row.WorkItemID == "" || row.RepoID != nil || row.ProjectID == nil ||
		!strings.HasPrefix(*row.ProjectID, "ghprojv2:") || row.Type == "" || row.Status == "" ||
		row.CreatedAt.IsZero() || row.UpdatedAt.IsZero() || row.Assignees == nil || row.Labels == nil {
		return providerfoundation.ErrInvalidScope
	}
	return nil
}

func mergeGitHubProjectV2Rows(repository, projects githubWorkItemRows) githubWorkItemRows {
	merged := repository
	merged.WorkItems = append([]githubWorkItemRow{}, repository.WorkItems...)
	index := make(map[string]int, len(merged.WorkItems)+len(projects.WorkItems))
	for position, row := range merged.WorkItems {
		index[row.WorkItemID] = position
	}
	for _, row := range projects.WorkItems {
		if position, exists := index[row.WorkItemID]; exists {
			merged.WorkItems[position] = row
		} else {
			index[row.WorkItemID] = len(merged.WorkItems)
			merged.WorkItems = append(merged.WorkItems, row)
		}
	}
	merged.StatusTransitions = append(append([]githubWorkItemTransitionRow{}, repository.StatusTransitions...), projects.StatusTransitions...)
	// Appended, not last-wins. Memberships and catalogue rows are append-only
	// facts keyed on their own identity, so the work-item merge's
	// overwrite-by-id rule does not apply to them; taking `merged := repository`
	// alone would silently drop everything the Projects v2 half produced.
	merged.ProjectMemberships = append(append([]projectmembership.Row{}, repository.ProjectMemberships...), projects.ProjectMemberships...)
	merged.Projects = append(append([]projectmembership.CatalogRow{}, repository.Projects...), projects.Projects...)
	return merged
}

// These literals intentionally preserve Python's documented leaf
// truncations (labels 50, assignees 10, fieldValues 20). The outer items and
// nested changes connections are fully paginated by the fetcher.
const gitHubProjectsV2ItemsQuery = `
query($login: String!, $number: Int!, $after: String, $first: Int!) {
  organization(login: $login) {
    projectV2(number: $number) {
      items(first: $first, after: $after) {
        nodes {
          id createdAt updatedAt
          content {
            __typename
            ... on Issue { id number title url state createdAt updatedAt closedAt repository { nameWithOwner } labels(first: 50) { nodes { name } } assignees(first: 10) { nodes { login email name } } author { login email name } }
            ... on PullRequest { id number title url state createdAt updatedAt closedAt mergedAt repository { nameWithOwner } labels(first: 50) { nodes { name } } assignees(first: 10) { nodes { login email name } } author { login email name } }
            ... on DraftIssue { id title createdAt updatedAt }
          }
          fieldValues(first: 20) { nodes {
            __typename
            ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2SingleSelectField { name } } }
            ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2FieldCommon { name } } }
            ... on ProjectV2ItemFieldIterationValue { title id field { ... on ProjectV2FieldCommon { name } } }
            ... on ProjectV2ItemFieldNumberValue { number field { ... on ProjectV2FieldCommon { name } } }
          } }
          changes(first: 100, orderBy: {field: CREATED_AT, direction: ASC}) { nodes { field { ... on ProjectV2FieldCommon { name } } previousValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } newValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } createdAt actor { login } } pageInfo { hasNextPage endCursor } }
        }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`

const gitHubProjectsV2ChangesQuery = `
query($itemId: ID!, $after: String) {
  node(id: $itemId) {
    ... on ProjectV2Item {
      changes(first: 100, after: $after, orderBy: {field: CREATED_AT, direction: ASC}) {
        nodes { field { ... on ProjectV2FieldCommon { name } } previousValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } newValue { ... on ProjectV2ItemFieldSingleSelectValue { name } } createdAt actor { login } }
        pageInfo { hasNextPage endCursor }
      }
    }
  }
}`
