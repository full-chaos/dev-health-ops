package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// maxGitHubUserResponseBytes bounds one GET /users/{login} body. GitHub's
// user object is a few hundred bytes; this is a defensive ceiling, not a
// realistic size.
const maxGitHubUserResponseBytes = 1 << 20

const (
	githubTeamCatalogDefaultPerPage = 100
	githubTeamCatalogDefaultPages   = 100
	githubTeamCatalogMaxPages       = 10_000
)

// GitHubTeamCatalogEvidence is the executed-proof surface for one collection
// pass: request/page counts and whether each surface (teams, repos-per-team,
// members-per-team) completed without hitting a pagination cap.
type GitHubTeamCatalogEvidence struct {
	Requests        int `json:"requests"`
	Pages           int `json:"pages"`
	TeamsObserved   int `json:"teams_observed"`
	MembersObserved int `json:"members_observed"`
	// SkippedTeamMemberships counts teams whose member-roster fetch (or a
	// per-member email lookup) failed and was skipped rather than aborting
	// the whole collection -- see the Collect doc comment on the members
	// block. Complete stays true: an org-level snapshot with a per-team gap
	// is still an authoritative partial result, mirroring Python's
	// "log and continue" behavior for the same failure.
	SkippedTeamMemberships int  `json:"skipped_team_memberships"`
	Complete               bool `json:"complete"`
}

// GitHubTeamCatalogResult is the CHAOS-4434 analog of team_autoimport_github.
// py's populate() return summary -- same field names as the Python dict
// wherever a Go counterpart exists, so a caller building the post-sync log
// line does not have to invent a second vocabulary.
type GitHubTeamCatalogResult struct {
	TeamsImported            int  `json:"teams_imported"`
	MembersImported          int  `json:"members_imported"`
	TeamMembershipsImported  int  `json:"team_memberships_imported"`
	RosterPreservationFailed bool `json:"roster_preservation_failed"`
}

// GitHubTeamCatalogRouteHandler owns the provider-only GitHub org teams +
// members walk. It is deliberately a plain Go service (not a
// CompleteRouteHandler/Claim-leased route): CHAOS-4434's call site is the
// post-sync team-autoimport job (one Go-native OrgID-scoped call per org, no
// enclosing sync_run_units claim to lease against), the same execution shape
// TeamRepoOwnershipDerivationService already uses for CHAOS-4365 item 1b.
type GitHubTeamCatalogRouteHandler struct {
	Client   *providerfoundation.HTTPClient
	OrgName  string
	PerPage  int
	MaxPages int
	Now      func() time.Time
	// ResolveEmail fetches GET /users/{login} for a team member with no email
	// on the team-members response, mirroring PyGithub's lazy NamedUser.email
	// completion (team_membership.py's discover_members_github accesses
	// member.email on a "simple user" object, which PyGithub completes via
	// exactly this endpoint). Nil disables the extra per-login request and
	// always reports no email -- useful for a fixture/oracle run with no live
	// credential, and honest about the extra API cost live callers accept.
	ResolveEmail bool
	// Strict mirrors team_autoimport_github.py's `strict` flag
	// (_populate_async / _membership_rows(..., strict=strict)): false (the
	// default, matching the non-strict post-sync caller) skips only the
	// failing team's memberships on a fetch/email/normalization error and
	// keeps going; true (the reference-discovery caller) re-raises instead,
	// matching Python's "except Exception: if strict: raise" exactly.
	Strict bool
}

func (collector GitHubTeamCatalogRouteHandler) now() time.Time {
	if collector.Now != nil {
		return collector.Now().UTC()
	}
	return time.Now().UTC()
}

func (collector GitHubTeamCatalogRouteHandler) limits() (int, int, error) {
	perPage, maxPages := collector.PerPage, collector.MaxPages
	if perPage == 0 {
		perPage = githubTeamCatalogDefaultPerPage
	}
	if maxPages == 0 {
		maxPages = githubTeamCatalogDefaultPages
	}
	if perPage < 1 || perPage > githubTeamCatalogDefaultPerPage || maxPages < 1 || maxPages > githubTeamCatalogMaxPages {
		return 0, 0, ErrInvalidConfiguration
	}
	return perPage, maxPages, nil
}

// Collect walks GET /orgs/{org}/teams, then for every team (when wantTeams)
// GET /orgs/{org}/teams/{slug}/repos for repo_patterns, and (when
// wantMembers) GET /orgs/{org}/teams/{slug}/members for the roster. It never
// writes anything -- ClickHouse effects are the caller's job
// (GitHubTeamCatalogClickHouseEffects), same separation the Linear reference
// catalog and every CompleteRouteHandler route already use.
func (collector GitHubTeamCatalogRouteHandler) Collect(
	ctx context.Context,
	orgID string,
	wantTeams, wantMembers bool,
) (githubTeamCatalogRows, GitHubTeamCatalogEvidence, error) {
	evidence := GitHubTeamCatalogEvidence{}
	if ctx == nil || collector.Client == nil || collector.Client.Provider != "github" ||
		collector.Client.BaseURL == nil || strings.TrimSpace(orgID) == "" ||
		strings.TrimSpace(collector.OrgName) == "" || (!wantTeams && !wantMembers) {
		return githubTeamCatalogRows{}, evidence, ErrInvalidConfiguration
	}
	perPage, maxPages, err := collector.limits()
	if err != nil {
		return githubTeamCatalogRows{}, evidence, err
	}
	normalizedAt := collector.now().Truncate(time.Microsecond)
	org := strings.TrimSpace(collector.OrgName)

	teamPages, err := providerfoundation.CollectGitHubLinkPages(ctx, collector.Client, providerfoundation.GitHubPageOptions{
		Path: "/orgs/" + url.PathEscape(org) + "/teams", Query: perPageQuery(perPage),
		MaxPages: maxPages,
	})
	evidence.Requests += teamPages.Pages
	evidence.Pages += teamPages.Pages
	if err != nil {
		return githubTeamCatalogRows{}, evidence, err
	}
	if teamPages.PageBudgetExhausted {
		return githubTeamCatalogRows{}, evidence, ErrPaginationCapExceeded
	}
	evidence.TeamsObserved = len(teamPages.Items)

	rows := githubTeamCatalogRows{Teams: make([]githubTeamRow, 0, len(teamPages.Items))}
	emailCache := make(map[string]*string)

	for _, raw := range teamPages.Items {
		var payload githubTeamPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return githubTeamCatalogRows{}, evidence, providerfoundation.ErrNormalizationInvalid
		}
		slug := strings.TrimSpace(payload.Slug)
		if slug == "" {
			continue
		}

		var repoPatterns []string
		if wantTeams {
			repoPages, err := providerfoundation.CollectGitHubLinkPages(ctx, collector.Client, providerfoundation.GitHubPageOptions{
				Path:  "/orgs/" + url.PathEscape(org) + "/teams/" + url.PathEscape(slug) + "/repos",
				Query: perPageQuery(perPage), MaxPages: maxPages,
			})
			evidence.Requests += repoPages.Pages
			evidence.Pages += repoPages.Pages
			if err != nil {
				return githubTeamCatalogRows{}, evidence, err
			}
			if repoPages.PageBudgetExhausted {
				return githubTeamCatalogRows{}, evidence, ErrPaginationCapExceeded
			}
			repoPatterns = make([]string, 0, len(repoPages.Items))
			for _, repoRaw := range repoPages.Items {
				var repoPayload githubTeamRepoPayload
				if err := json.Unmarshal(repoRaw, &repoPayload); err != nil {
					return githubTeamCatalogRows{}, evidence, providerfoundation.ErrNormalizationInvalid
				}
				name := strings.TrimSpace(repoPayload.Name)
				if name == "" {
					continue
				}
				// Mirrors team_discovery.py's discover_github exactly:
				// f"{org_name}/{repo.name}", the CONFIGURED org name, not
				// whatever owner login the repo API response reports.
				repoPatterns = append(repoPatterns, org+"/"+name)
			}

			team, err := normalizeGitHubTeam(orgID, payload, repoPatterns, normalizedAt)
			if err != nil {
				return githubTeamCatalogRows{}, evidence, err
			}
			rows.Teams = append(rows.Teams, team)
			// team_repo_ownership (CHAOS-4434 correction): the only source
			// for GitHub's team<->repo grants -- see githubTeamRow's doc
			// comment. Same repoPatterns already fetched for the team row,
			// no extra request.
			for _, repoFullName := range repoPatterns {
				ownership, err := normalizeGitHubTeamRepoOwnership(orgID, slug, repoFullName, normalizedAt)
				if err != nil {
					return githubTeamCatalogRows{}, evidence, err
				}
				rows.RepoOwnership = append(rows.RepoOwnership, ownership)
			}
		}

		if wantMembers {
			// team_membership.py's discover_members_github failure (including a
			// PyGithub lazy .email completion request raised while building the
			// DiscoveredMember list) is caught per-team by
			// team_autoimport_github.py's _membership_rows: "except Exception:
			// if strict: raise; else: continue" -- Strict=false (post-sync,
			// default) skips only this team's memberships and keeps going;
			// Strict=true (reference discovery) re-raises, matching Python
			// exactly.
			memberships, requests, ok, memberErr := collector.collectTeamMemberships(
				ctx, orgID, org, slug, perPage, maxPages, normalizedAt, emailCache,
			)
			evidence.Requests += requests
			if ok {
				rows.Memberships = append(rows.Memberships, memberships...)
				evidence.MembersObserved += len(memberships)
			} else if collector.Strict {
				return githubTeamCatalogRows{}, evidence, memberErr
			} else {
				evidence.SkippedTeamMemberships++
				// CHAOS-4461: this team must not have its roster silently
				// rebuilt to [] below -- the caller confirms and carries
				// forward its currently-persisted roster instead.
				rows.FailedMemberFetchTeamIDs = append(rows.FailedMemberFetchTeamIDs, githubTeamID(slug))
			}
		}
	}

	rows.Teams = dedupeGitHubTeams(rows.Teams)
	rows.Memberships = dedupeGitHubMemberships(rows.Memberships)
	rows.RepoOwnership = dedupeGitHubTeamRepoOwnership(rows.RepoOwnership)
	if wantMembers {
		roster := githubTeamRosterFromMemberships(rows.Memberships)
		for index := range rows.Teams {
			rows.Teams[index].Members = roster[rows.Teams[index].ID]
			if rows.Teams[index].Members == nil {
				rows.Teams[index].Members = []string{}
			}
		}
	}
	evidence.Complete = true
	return rows, evidence, nil
}

// collectTeamMemberships fetches GET /orgs/{org}/teams/{slug}/members and
// normalizes every entry. ok=false means the caller must skip this team's
// memberships entirely (a fetch/email/normalization failure, or this run's
// own pagination ceiling) -- never partially write a truncated roster.
func (collector GitHubTeamCatalogRouteHandler) collectTeamMemberships(
	ctx context.Context,
	orgID, org, slug string,
	perPage, maxPages int,
	normalizedAt time.Time,
	emailCache map[string]*string,
) ([]githubMembershipRow, int, bool, error) {
	requests := 0
	pages, err := providerfoundation.CollectGitHubLinkPages(ctx, collector.Client, providerfoundation.GitHubPageOptions{
		Path:  "/orgs/" + url.PathEscape(org) + "/teams/" + url.PathEscape(slug) + "/members",
		Query: perPageQuery(perPage), MaxPages: maxPages,
	})
	requests += pages.Pages
	if err != nil {
		return nil, requests, false, err
	}
	if pages.PageBudgetExhausted {
		return nil, requests, false, ErrPaginationCapExceeded
	}
	memberships := make([]githubMembershipRow, 0, len(pages.Items))
	for _, memberRaw := range pages.Items {
		var memberPayload githubTeamMemberPayload
		if err := json.Unmarshal(memberRaw, &memberPayload); err != nil {
			return nil, requests, false, providerfoundation.ErrNormalizationInvalid
		}
		login := strings.TrimSpace(memberPayload.Login)
		if login == "" {
			continue
		}
		email := ""
		resolved, emailRequests, err := collector.resolveEmail(ctx, emailCache, login)
		requests += emailRequests
		if err != nil {
			return nil, requests, false, err
		}
		if resolved != nil {
			email = *resolved
		}
		membership, err := normalizeGitHubMembership(orgID, slug, login, email, normalizedAt)
		if err != nil {
			return nil, requests, false, err
		}
		memberships = append(memberships, membership)
	}
	return memberships, requests, true, nil
}

// resolveEmail fetches GET /users/{login} once per login per collection
// pass. GitHub returns a nullable "email" field; a private/unset email
// legitimately yields nil, matching what PyGithub's completion returns for
// the same account today.
func (collector GitHubTeamCatalogRouteHandler) resolveEmail(
	ctx context.Context, cache map[string]*string, login string,
) (*string, int, error) {
	if !collector.ResolveEmail {
		return nil, 0, nil
	}
	if cached, ok := cache[login]; ok {
		return cached, 0, nil
	}
	// A single user resource, not a list -- CollectGitHubLinkPages expects a
	// bare-array or DataKey-wrapped page and cannot decode this shape, so the
	// request goes straight through the client.
	response, err := collector.Client.Do(ctx, http.MethodGet, "/users/"+url.PathEscape(login), nil)
	if err != nil {
		return nil, 1, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxGitHubUserResponseBytes+1))
	if err != nil || len(body) > maxGitHubUserResponseBytes {
		return nil, 1, providerfoundation.ErrPaginationInvalid
	}
	var payload struct {
		Email *string `json:"email"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, 1, providerfoundation.ErrNormalizationInvalid
	}
	cache[login] = payload.Email
	return payload.Email, 1, nil
}

func perPageQuery(perPage int) url.Values {
	values := url.Values{}
	values.Set("per_page", strconv.Itoa(perPage))
	return values
}
