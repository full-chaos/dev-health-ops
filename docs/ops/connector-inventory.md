# Connector Inventory and Implementation Status

This document provides a comprehensive audit of all data connectors (GitHub, GitLab, Jira, Linear) including their current implementation status, missing features, sync capabilities, and recommendations.

---

## Executive Summary

| Provider | Work Items | Git Data | CI/CD | Deployments | Incidents | Sprints | Overall |
|----------|------------|----------|-------|-------------|-----------|---------|---------|
| **GitHub** | Full | Full | Full | Full | Partial | Full (Milestones) | 90% |
| **GitLab** | Full | Full | Full | Full | Partial | Full (Milestones) | 90% |
| **Jira** | Full | N/A | N/A | N/A | N/A | Full | 95% |
| **Linear** | Full | N/A | N/A | N/A | N/A | Full (Cycles) | 90% |

---

## 1. GitHub Connector

### 1.1 Architecture Overview

**Location:** `src/dev_health_ops/`
- Provider: `providers/github/provider.py` (work items)
- Client: `providers/github/client.py` (PyGithub + GraphQL)
- Processor: `processors/github.py` (git data sync)
- Connector: `connectors/github.py` (low-level API)
- Normalizer: `providers/github/normalize.py`

### 1.2 Implemented Data Types

| Data Type | Status | API Used | Notes |
|-----------|--------|----------|-------|
| Repository metadata | Implemented | REST | Full support |
| Commits | Implemented | REST | Up to 100 recent commits |
| Commit stats (per-file) | Implemented | REST | Last 50 commits (rate limits) |
| Pull requests | Implemented | REST | Full history with pagination |
| PR reviews | Implemented | REST | Includes review comments |
| PR comments | Implemented | REST | Issue + review comments |
| Issues | Implemented | REST | Full work item support |
| Issue events | Implemented | REST | For status transitions |
| Milestones | Implemented | REST | Mapped to sprints |
| Workflow runs (CI/CD) | Implemented | REST | GitHub Actions |
| Deployments | Implemented | REST | Environment deployments |
| Incidents | **Partial** | REST | Issues labeled "incident" only |
| Projects v2 | Implemented | GraphQL | Full field tracking + changes |
| Blame | Implemented | GraphQL | Per-file blame ranges |
| File tree | Implemented | REST | For backfill operations |

### 1.3 Authentication

| Method | Support | Environment Variable |
|--------|---------|---------------------|
| Personal Access Token | Full | `GITHUB_TOKEN` |
| GitHub Enterprise | Full | `GITHUB_BASE_URL` |
| GitHub App | Not implemented | - |
| OAuth App | Not implemented | - |

**Required Token Scopes:**
- `repo` - Required for private repositories
- `read:org` - Recommended for organization repos

### 1.4 Rate Limiting

| Feature | Implementation |
|---------|----------------|
| REST API (5,000/hr) | Exponential backoff with shared gate |
| GraphQL API | Cost-based tracking |
| Retry-After header | Honored |
| Concurrent request coordination | Via `RateLimitGate` |

### 1.5 Missing Implementations

| Feature | Priority | Effort | Notes |
|---------|----------|--------|-------|
| GitHub App authentication | Medium | Medium | Better for org-wide access |
| Discussions | Low | Low | Not commonly needed for metrics |
| Wiki changes | Low | Low | Rarely impacts dev health |
| Security advisories | Medium | Medium | Useful for security metrics |
| Code scanning alerts | Medium | Medium | Useful for quality metrics |
| Dependabot alerts | Medium | Medium | Useful for maintenance metrics |
| Repository topics/tags | Low | Low | Enrichment data |
| Webhooks (real-time) | High | High | Would enable real-time sync |
| Branch protection status | Low | Low | Governance metrics |

### 1.6 Known Issues

1. **Commit depth limit**: REST API limited to recent commits; full history requires clone
2. **Blame API costs**: Per-file API calls; expensive for large repos
3. **Rate limit sharing**: Multiple concurrent syncs can exhaust limits
4. **Incident detection**: Only detects issues with "incident" label

### 1.7 Recommendations

1. **Implement webhooks** for near-real-time data ingestion
2. **Add security/Dependabot alert sync** for risk metrics
3. **Consider GitHub App auth** for better rate limits and org access
4. **Add commit signature verification** for security audit

---

## 2. GitLab Connector

### 2.1 Architecture Overview

**Location:** `src/dev_health_ops/`
- Provider: `providers/gitlab/provider.py` (work items)
- Client: `providers/gitlab/client.py` (python-gitlab)
- Processor: `processors/gitlab.py` (git data sync)
- Connector: `connectors/gitlab.py` (low-level API)
- Normalizer: `providers/gitlab/normalize.py`

### 2.2 Implemented Data Types

| Data Type | Status | API Used | Notes |
|-----------|--------|----------|-------|
| Project metadata | Implemented | REST | Full support |
| Commits | Implemented | REST | Paginated with date filtering |
| Commit stats | Implemented | REST | Aggregate per commit |
| Merge requests | Implemented | REST | Full history with pagination |
| MR notes/comments | Implemented | REST | Full comment history |
| Issues | Implemented | REST | Full work item support |
| Issue links | Implemented | REST | For dependencies |
| Resource label events | Implemented | REST | Status transitions |
| Resource state events | Implemented | REST | Reopen detection |
| Project milestones | Implemented | REST | Mapped to sprints |
| Group milestones | Implemented | REST | Mapped to sprints |
| Pipelines (CI/CD) | Implemented | REST | Full pipeline history |
| Deployments | Implemented | REST | Environment deployments |
| Incidents | **Partial** | REST | Issues labeled "incident" only |
| Blame | Implemented | REST | Per-file blame |
| Repository tree | Implemented | REST | For file listing |

### 2.3 Authentication

| Method | Support | Environment Variable |
|--------|---------|---------------------|
| Personal Access Token | Full | `GITLAB_TOKEN` |
| Self-hosted instances | Full | `GITLAB_URL` |
| Group Access Token | Supported | `GITLAB_TOKEN` |
| OAuth tokens | Not implemented | - |
| Deploy tokens | Not implemented | - |

**Required Token Scopes:**
- `read_api` - Required for API access
- `read_repository` - Required for repository data

### 2.4 Rate Limiting

| Feature | Implementation |
|---------|----------------|
| REST API (10 req/sec) | Exponential backoff with shared gate |
| Retry-After header | Honored |
| Concurrent request coordination | Via `RateLimitGate` |

### 2.5 Missing Implementations

| Feature | Priority | Effort | Notes |
|---------|----------|--------|-------|
| GraphQL API usage | Medium | Medium | More efficient for some queries |
| Epics | Medium | Medium | GitLab Premium feature |
| MR approvals | Medium | Low | Review metrics |
| MR diff stats | Low | Low | Already have aggregate stats |
| Package registry | Low | Medium | Not commonly needed |
| Container registry | Low | Medium | Not commonly needed |
| Webhooks (real-time) | High | High | Would enable real-time sync |
| Value stream analytics | Medium | Medium | Pre-computed metrics |
| DORA metrics API | High | Medium | Built-in DevOps metrics |

### 2.6 Known Issues

1. **Commit stats aggregate only**: Per-file stats not available via API
2. **Pipeline job details**: Only pipeline-level status, not job-level
3. **Incident detection**: Only detects issues with "incident" label
4. **Self-hosted rate limits**: May vary from gitlab.com defaults

### 2.7 Recommendations

1. **Integrate DORA metrics API** for pre-computed deployment metrics
2. **Add webhook support** for real-time data ingestion
3. **Consider GraphQL migration** for more efficient queries
4. **Add Epic sync** for portfolio-level tracking
5. **Add MR approval sync** for review metrics

---

## 3. Jira Connector

### 3.1 Architecture Overview

**Location:** `src/dev_health_ops/`
- Provider: `providers/jira/provider.py` (work items)
- Client: `providers/jira/client.py` (custom REST)
- Normalizer: `providers/jira/normalize.py`

**External:** `atlassian/` subproject for GraphQL Gateway (AGG)

### 3.2 Implemented Data Types

| Data Type | Status | API Used | Notes |
|-----------|--------|----------|-------|
| Issues | Implemented | REST v3 | Full JQL support |
| Issue changelog | Implemented | REST v3 | Status transitions |
| Issue links | Implemented | REST v3 | Dependencies |
| Issue comments | Implemented | REST v3 | Interaction events |
| Projects | Implemented | REST v3 | For team mapping |
| Sprints | Implemented | Agile REST | Sprint metadata |
| Custom fields | Implemented | REST v3 | Story points, epic links |
| Status categories | Implemented | REST v3 | Normalized to canonical |

### 3.3 Authentication

| Method | Support | Environment Variables |
|--------|---------|----------------------|
| API Token (Cloud) | Full | `JIRA_EMAIL`, `JIRA_API_TOKEN`, `JIRA_BASE_URL` |
| OAuth | Not implemented | - |
| Personal Access Token | Not implemented | - |

### 3.4 Rate Limiting

| Feature | Implementation |
|---------|----------------|
| HTTP 429 handling | Exponential backoff |
| Retry-After header | Honored |
| Concurrent request coordination | Via `RateLimitGate` |

### 3.5 Jira Configuration Options

| Variable | Purpose | Default |
|----------|---------|---------|
| `JIRA_PROJECT_KEYS` | Filter to specific projects | All accessible |
| `JIRA_JQL` | Custom JQL override | Built-in windowed query |
| `JIRA_FETCH_ALL` | Ignore date window | `false` |
| `JIRA_FETCH_COMMENTS` | Include comments | `true` |
| `JIRA_COMMENTS_LIMIT` | Max comments per issue | `0` (unlimited) |
| `JIRA_STORY_POINTS_FIELD` | Custom field ID | Auto-detect |
| `JIRA_SPRINT_FIELD` | Custom field ID | `customfield_10020` |
| `JIRA_EPIC_LINK_FIELD` | Custom field ID | Auto-detect |

### 3.6 Missing Implementations

| Feature | Priority | Effort | Notes |
|---------|----------|--------|-------|
| Jira Server/DC support | Medium | Medium | Different auth model |
| Boards API | Low | Low | Board configuration |
| Agile estimation | Low | Low | Planning poker data |
| Tempo worklogs | Medium | Medium | Time tracking integration |
| Advanced Roadmaps | Medium | High | Portfolio data |
| Webhooks (real-time) | High | High | Would enable real-time sync |
| Assets/Insight | Low | Medium | CMDB data |
| GraphQL Gateway (AGG) | Medium | High | More efficient queries |

### 3.7 Known Issues

1. **Changelog truncation**: Large issues may have truncated changelogs
2. **Custom field discovery**: Requires manual configuration per instance
3. **Cloud API changes**: `/rest/api/3/search` deprecated for `/search/jql`
4. **Rate limits vary**: Different limits for different Jira plans

### 3.8 Recommendations

1. **Add Jira Server/DC support** for on-premise deployments
2. **Integrate AGG (GraphQL)** from `atlassian/` subproject
3. **Add webhook support** for real-time updates
4. **Implement Tempo integration** for time tracking
5. **Add bulk change detection** for large backlogs

---

## 4. Linear Connector

### 4.1 Architecture Overview

**Location:** `src/dev_health_ops/`
- Provider: `providers/linear/provider.py` (work items)
- Client: `providers/linear/client.py` (GraphQL API)
- Normalizer: `providers/linear/normalize.py`

### 4.2 Implemented Data Types

| Data Type | Status | API Used | Notes |
|-----------|--------|----------|-------|
| Issues | Implemented | GraphQL | Full issue support with pagination |
| Issue history | Implemented | GraphQL | Status transitions |
| Issue comments | Implemented | GraphQL | Interaction events |
| Cycles | Implemented | GraphQL | Mapped to sprints |
| Teams | Implemented | GraphQL | Team iteration |
| Labels | Implemented | GraphQL | Label extraction |
| Priority | Implemented | GraphQL | Full priority mapping |
| Estimates | Implemented | GraphQL | Story points support |
| Parent issues | Implemented | GraphQL | Hierarchy support |

### 4.3 Authentication

| Method | Support | Environment Variable |
|--------|---------|---------------------|
| API Key | Full | `LINEAR_API_KEY` |

**Required:** Linear API key with read access to issues, teams, and cycles.

### 4.4 Configuration Options

| Variable | Purpose | Default |
|----------|---------|---------|
| `LINEAR_API_KEY` | API authentication | Required |
| `LINEAR_FETCH_COMMENTS` | Include comments | `true` |
| `LINEAR_FETCH_HISTORY` | Include status history | `true` |
| `LINEAR_FETCH_CYCLES` | Include cycles as sprints | `true` |
| `LINEAR_COMMENTS_LIMIT` | Max comments per issue | `100` |

### 4.5 Data Mapping

#### Priority Mapping

| Linear Priority | priority_raw | service_class |
|-----------------|--------------|---------------|
| 0 (No priority) | none | intangible |
| 1 (Urgent) | urgent | expedite |
| 2 (High) | high | fixed_date |
| 3 (Medium) | medium | standard |
| 4 (Low) | low | intangible |

#### State Type Mapping

| Linear state.type | WorkItemStatusCategory |
|-------------------|------------------------|
| backlog | backlog |
| unstarted | todo |
| started | in_progress |
| completed | done |
| canceled | canceled |

### 4.6 Missing Implementations

| Feature | Priority | Effort | Notes |
|---------|----------|--------|-------|
| Blocking relationships | Medium | Low | Linear supports issue blocking |
| Projects | Medium | Medium | Project-level grouping |
| Initiatives | Low | Medium | High-level roadmap items |
| Webhooks (real-time) | High | Medium | Would enable real-time sync |
| SLA tracking | Low | Medium | Linear SLA fields |

### 4.7 Known Issues

1. **No blocking relationships**: Issue dependencies not yet extracted
2. **Team filtering**: Currently fetches all teams; specific team filtering via `repo` parameter
3. **Rate limiting**: Linear has aggressive rate limits; client handles backoff

### 4.8 Recommendations

1. **Add blocking relationship sync** for dependency tracking
2. **Implement webhooks** for real-time data ingestion
3. **Add project-level aggregation** for portfolio metrics

---

## 5. Cross-Connector Issues

### 4.1 Common Gaps

| Gap | Impact | Recommendation |
|-----|--------|----------------|
| No real-time sync | Data staleness | Implement webhooks |
| Manual identity mapping | User fragmentation | Auto-detect common patterns |
| No incremental blame | Slow backfills | Cache blame data |
| Limited incident detection | Incomplete metrics | Add dedicated incident providers |

### 4.2 Data Model Alignment

All connectors normalize to unified models:
- `WorkItem` - Issues, PRs, MRs
- `WorkItemStatusTransition` - State changes
- `WorkItemDependency` - Links/blockers
- `WorkItemInteractionEvent` - Comments/reviews
- `Sprint` - Iterations/milestones
- `GitCommit`, `GitPullRequest`, etc. - Git data

### 4.3 Rate Limit Coordination

Shared `RateLimitGate` infrastructure ensures:
- Exponential backoff on 429s
- Retry-After header compliance
- Cross-worker coordination

---

## 6. Implementation Roadmap

### Phase 1: High Priority (Recommended)

1. **Webhook ingestion framework**
   - Real-time data for all providers
   - Reduces sync latency from hours to seconds

2. **Security/compliance data**
   - GitHub: Security advisories, Dependabot
   - GitLab: Vulnerability reports
   - Impact: Risk/Security investment category

3. **DORA metrics integration**
   - GitLab DORA API
   - GitHub deployment frequency
   - Impact: Pre-computed DevOps metrics

### Phase 2: Medium Priority

4. **Authentication improvements**
   - GitHub App auth
   - Jira Server/DC support
   - OAuth flows for all providers

5. **AGG integration for Jira**
   - Use GraphQL Gateway from `atlassian/` subproject
   - More efficient bulk queries

6. **Epic/Portfolio tracking**
   - GitLab Epics
   - Jira Advanced Roadmaps
   - GitHub Projects hierarchy

### Phase 3: Low Priority

7. **Additional data enrichment**
   - Repository topics/tags
   - Code quality metrics
   - Test coverage integration

---

## 7. Testing Recommendations

### Unit Tests Required

- [ ] Rate limit handling for each provider
- [ ] Pagination edge cases
- [ ] Error recovery scenarios
- [ ] Custom field mapping

### Integration Tests Required

- [ ] Full sync cycle for each provider
- [ ] Incremental sync validation
- [ ] Multi-provider identity correlation
- [ ] Large repository handling

### Performance Tests Required

- [ ] Concurrent sync capacity
- [ ] Memory usage under load
- [ ] Rate limit recovery time
