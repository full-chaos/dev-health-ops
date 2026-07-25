# Integration Plan: Adopt atlassian Client in dev-health-ops

## Executive Summary

Replace the custom Jira client in `dev-health-ops` with the shared `atlassian` client library.
This consolidates Jira API integration, adds changelog/worklog/sprint support, and prepares for 
GraphQL (AGG) adoption.

## Current State

### dev-health-ops Jira Provider

| Component | Location | Status |
|-----------|----------|--------|
| Client | `providers/jira/client.py` | Custom requests-based client |
| Normalizer | `providers/jira/normalize.py` | Maps raw JSON to internal models |
| Provider | `providers/jira/provider.py` | Entry point |

**Capabilities:**
- Issue search via JQL (`/rest/api/3/search/jql`)
- Issue comments (`/rest/api/3/issue/{key}/comment`)
- Sprint fetch (`/rest/agile/1.0/sprint/{id}`)
- Projects (`/rest/api/3/project/search`)
- Rate limiting with backoff

**Gaps:**
- No changelog pagination (truncated on large issues)
- No worklog iteration
- No board-level sprint iteration
- No GraphQL support
- No canonical models (raw dict passing)

### atlassian Client Library

| Component | Location | Coverage |
|-----------|----------|----------|
| REST Client | `python/atlassian/rest/client.py` | Full |
| REST Issues | `python/atlassian/rest/api/jira_issues.py` | `iter_issues_via_rest()` → `JiraIssue` |
| REST Changelog | `python/atlassian/rest/api/jira_changelog.py` | `iter_issue_changelog_via_rest()` → `JiraChangelogEvent` |
| REST Worklogs | `python/atlassian/rest/api/jira_worklogs.py` | `iter_issue_worklogs_via_rest()` → `JiraWorklog` |
| REST Sprints | `python/atlassian/rest/api/jira_sprints.py` | `iter_board_sprints_via_rest()` → `JiraSprint` |
| REST Projects | `python/atlassian/rest/api/jira_projects.py` | Projects iteration |
| GraphQL Issues | `python/atlassian/graph/api/jira_issues.py` | AGG GraphQL queries |
| GraphQL Worklogs | `python/atlassian/graph/api/jira_worklogs.py` | AGG GraphQL queries |
| GraphQL Sprints | `python/atlassian/graph/api/jira_sprints.py` | AGG GraphQL queries |
| Canonical Models | `python/atlassian/canonical_models.py` | Typed dataclasses |

---

## Integration Phases

### Phase 0: Foundation (Prerequisite)
**Goal:** Make atlassian installable as a dependency

- [ ] Add `atlassian` as git dependency or publish to internal PyPI
- [ ] Verify import works: `from atlassian.rest.api import iter_issues_via_rest`
- [ ] Document required env vars in dev-health-ops

**Env vars needed:**
```bash
# Basic auth (simpler for Cloud)
ATLASSIAN_EMAIL=
ATLASSIAN_API_TOKEN=
ATLASSIAN_JIRA_BASE_URL=https://your-org.atlassian.net

# Or OAuth (for AGG)
ATLASSIAN_OAUTH_ACCESS_TOKEN=
ATLASSIAN_CLOUD_ID=
```

---

### Phase 1: Issues Migration
**Goal:** Replace issue fetching with atlassian client

| Step | Action | Risk |
|------|--------|------|
| 1.1 | Create adapter: `atlassian.JiraIssue` → internal `WorkItem` | Low |
| 1.2 | Wire `iter_issues_via_rest()` into provider | Medium |
| 1.3 | Deprecate `JiraClient.iter_issues()` | Low |
| 1.4 | Remove legacy issue code after validation | Low |

**Mapping:** `atlassian.JiraIssue` → `dev_health_ops.models.WorkItem`

---

### Phase 2: Changelog Integration (NEW capability)
**Goal:** Add proper changelog pagination

| Step | Action | Risk |
|------|--------|------|
| 2.1 | Integrate `iter_issue_changelog_via_rest()` | Low |
| 2.2 | Create mapper: `JiraChangelogEvent` → internal changelog model | Low |
| 2.3 | Update processors to consume changelog events | Medium |

**Note:** Current dev-health-ops uses `expand=changelog` which truncates. This phase enables full history.

---

### Phase 3: Worklog Integration (NEW capability)
**Goal:** Add worklog iteration for time tracking metrics

| Step | Action | Risk |
|------|--------|------|
| 3.1 | Integrate `iter_issue_worklogs_via_rest()` | Low |
| 3.2 | Create mapper: `JiraWorklog` → internal worklog model | Low |
| 3.3 | Add worklog sink for persistence | Medium |

---

### Phase 4: Sprint Iteration (Enhanced capability)
**Goal:** Replace single-sprint fetch with board-level iteration

| Step | Action | Risk |
|------|--------|------|
| 4.1 | Integrate `iter_board_sprints_via_rest()` | Low |
| 4.2 | Create mapper: `JiraSprint` → internal sprint model | Low |
| 4.3 | Update sprint sync to iterate boards | Medium |

**Current:** `JiraClient.get_sprint(sprint_id)` fetches one sprint at a time.
**New:** Iterate all sprints for a board with state filtering.

---

### Phase 5: AGG GraphQL (Future)
**Goal:** Enable Atlassian GraphQL Gateway for enhanced queries

| Step | Action | Risk |
|------|--------|------|
| 5.1 | Add GraphQL client setup (OAuth tokens) | Medium |
| 5.2 | Create hybrid strategy: REST fallback, GraphQL primary | High |
| 5.3 | Enable Opsgenie team correlation via AGG | Medium |

**Depends on:** Issue #226 (AGG integration framework)

---

## Migration Strategy

### Approach: Parallel Run

1. **Add atlassian client alongside existing client**
2. **Feature flag to toggle between implementations**
3. **Run both in shadow mode, compare outputs**
4. **Validate via metrics: same issue counts, same changelog lengths**
5. **Remove legacy client after 1 sprint of stable operation**

### Rollback Plan

- Feature flag reverts to legacy client instantly
- No database schema changes in Phases 1-4
- Canonical models are additive, not replacing

---

## Effort Estimates

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 0: Foundation | 2h | None |
| Phase 1: Issues | 4h | Phase 0 |
| Phase 2: Changelog | 3h | Phase 1 |
| Phase 3: Worklogs | 3h | Phase 1 |
| Phase 4: Sprints | 2h | Phase 1 |
| Phase 5: AGG GraphQL | 8h | Phases 1-4, #226 |

**Total:** ~22h (excluding Phase 5)

---

## Success Criteria

- [ ] All existing Jira sync tests pass with new client
- [ ] Changelog fully paginated (no truncation)
- [ ] Worklog data available in metrics
- [ ] Sprint board iteration working
- [ ] No performance regression (same or better API call count)
- [ ] Feature flag for instant rollback

---

## Related Issues

- #225 - Jira Server/DC support (blocked until AGG or Server API added to atlassian)
- #226 - AGG integration framework (enables Phase 5)
- #227 - GitLab Epic sync (separate track)
