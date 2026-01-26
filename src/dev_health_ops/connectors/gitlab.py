"""
GitLab connector using python-gitlab and REST API.

This connector provides methods to retrieve groups, projects,
contributors, statistics, merge requests, and blame information from GitLab.
"""

import logging
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Callable

import gitlab
from gitlab.exceptions import GitlabAuthenticationError, GitlabError

from dev_health_ops.connectors.base import BatchResult, GitConnector, RateLimitException
from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
)
from dev_health_ops.connectors.models import (
    Author,
    BlameRange,
    CommitStats,
    DORAMetric,
    DORAMetrics,
    FileBlame,
    Organization,
    PullRequest,
    PullRequestCommit,
    Repository,
    RepoStats,
)
from dev_health_ops.connectors.utils import (
    GitLabRESTClient,
    retry_with_backoff,
    match_project_pattern,
)

logger = logging.getLogger(__name__)


def _parse_retry_after_seconds(headers: object) -> Optional[float]:
    if not isinstance(headers, dict):
        return None
    retry_after = headers.get("Retry-After")
    if retry_after is None:
        return None
    try:
        return max(0.0, float(retry_after))
    except (TypeError, ValueError):
        return None


class GitLabConnector(GitConnector):
    """
    Production-grade GitLab connector using python-gitlab and REST API.

    Provides methods to retrieve data from GitLab with automatic
    pagination, rate limiting, and error handling.
    """

    def __init__(
        self,
        url: str = "https://gitlab.com",
        private_token: Optional[str] = None,
        per_page: int = 100,
        max_workers: int = 4,
        rest_timeout: int = 15,
    ):
        """
        Initialize GitLab connector.

        :param url: GitLab instance URL.
        :param private_token: GitLab private token.
        :param per_page: Number of items per page for pagination.
        :param max_workers: Maximum concurrent workers for operations.
        :param rest_timeout: REST API timeout in seconds.
        """
        super().__init__(per_page=per_page, max_workers=max_workers)
        self.url = url
        self.private_token = private_token

        # Initialize python-gitlab client
        self.gitlab = gitlab.Gitlab(
            url=url,
            private_token=private_token,
            timeout=rest_timeout,
        )

        # Authenticate if token provided
        if private_token:
            try:
                self.gitlab.auth()
            except GitlabAuthenticationError as e:
                raise AuthenticationException(f"GitLab authentication failed: {e}")

        # Initialize REST client for operations not supported by python-gitlab
        api_url = f"{url}/api/v4"
        self.rest_client = GitLabRESTClient(
            base_url=api_url,
            private_token=private_token,
            timeout=rest_timeout,
        )

    def _handle_gitlab_exception(self, e: Exception) -> None:
        """
        Handle GitLab API exceptions and convert to connector exceptions.

        :param e: Exception from GitLab API.
        :raises: Appropriate connector exception.
        """
        if isinstance(e, GitlabAuthenticationError):
            raise AuthenticationException(f"GitLab authentication failed: {e}")
        elif isinstance(e, GitlabError):
            if hasattr(e, "response_code"):
                if e.response_code == 429:
                    retry_after = _parse_retry_after_seconds(
                        getattr(e, "response_headers", None)
                    )
                    raise RateLimitException(
                        f"GitLab rate limit exceeded: {e}",
                        retry_after_seconds=retry_after,
                    )
                elif e.response_code == 404:
                    raise NotFoundException(
                        "GitLab resource not found (404). "
                        "This can also mean the token lacks access. "
                        f"Details: {e}"
                    )
            raise APIException(f"GitLab API error: {e}")
        else:
            raise APIException(f"Unexpected error: {e}")

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_groups(
        self,
        max_groups: Optional[int] = None,
    ) -> List[Organization]:
        """
        List groups accessible to the authenticated user.

        :param max_groups: Maximum number of groups to retrieve.
        :return: List of Organization objects (representing GitLab groups).
        """
        try:
            groups = []

            gl_groups = self.gitlab.groups.list(
                per_page=self.per_page,
                get_all=False,
            )

            for gl_group in gl_groups:
                if max_groups and len(groups) >= max_groups:
                    break

                group = Organization(
                    id=gl_group.id,
                    name=gl_group.name,
                    description=(
                        gl_group.description
                        if hasattr(gl_group, "description")
                        else None
                    ),
                    url=gl_group.web_url if hasattr(gl_group, "web_url") else None,
                )
                groups.append(group)
                logger.debug(f"Retrieved group: {group.name}")

            logger.info(f"Retrieved {len(groups)} groups")
            return groups

        except Exception as e:
            self._handle_gitlab_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_projects(
        self,
        group_id: Optional[int] = None,
        group_name: Optional[str] = None,
        user_name: Optional[str] = None,
        search: Optional[str] = None,
        pattern: Optional[str] = None,
        max_projects: Optional[int] = None,
    ) -> List[Repository]:
        """
        List projects for a group, user, or all accessible projects.

        :param group_id: Optional group ID. If provided, lists group projects.
        :param group_name: Optional group name/path. If provided, lists group projects.
                          Takes precedence over group_id if both are provided.
        :param user_name: Optional username. If provided, lists that user's projects.
                         group_name takes precedence over user_name if both are provided.
        :param search: Optional search query to filter projects by name.
        :param pattern: Optional fnmatch-style pattern to filter projects by full name
                       (e.g., 'group/p*', '*/api-*'). Pattern matching is performed
                       client-side after fetching projects. Case-insensitive.
        :param max_projects: Maximum number of projects to retrieve. If None, retrieves all.
        :return: List of Repository objects (representing GitLab projects).

        Examples:
            - pattern='group/p*' matches 'group/project'
            - pattern='*/sync*' matches 'anygroup/sync-tool'
            - user_name='johndoe' fetches johndoe's projects
        """
        try:
            projects = []
            logger.info(
                "Listing GitLab projects (group=%s user=%s search=%s pattern=%s max=%s)",
                group_name or group_id,
                user_name,
                search,
                pattern,
                max_projects,
            )

            # Build common list parameters
            list_params = {"per_page": self.per_page, "get_all": (max_projects is None)}
            if search:
                list_params["search"] = search

            # Determine source and fetch projects
            if group_name or group_id:
                # Get group by name or ID and list its projects
                group_identifier = group_name if group_name else group_id
                if group_identifier is None:
                    return []
                group = self.gitlab.groups.get(group_identifier)
                gl_projects = group.projects.list(**list_params)
            elif user_name:
                # Get user's projects
                users = self.gitlab.users.list(username=user_name)
                if not users:
                    logger.warning(f"User '{user_name}' not found")
                    return []
                user = users[0]
                gl_projects = user.projects.list(**list_params)
                logger.info(f"Fetching projects for user '{user_name}'")
            else:
                # List all accessible projects
                gl_projects = self.gitlab.projects.list(**list_params)

            for gl_project in gl_projects:
                if max_projects and len(projects) >= max_projects:
                    break

                # Get full project details
                if hasattr(gl_project, "path_with_namespace"):
                    full_name = gl_project.path_with_namespace
                elif hasattr(gl_project, "name"):
                    full_name = gl_project.name
                else:
                    full_name = str(gl_project.id)

                # Apply pattern filter early to avoid unnecessary object creation
                if pattern and not match_project_pattern(full_name, pattern):
                    continue

                project = Repository(
                    id=gl_project.id,
                    name=(
                        gl_project.name
                        if hasattr(gl_project, "name")
                        else str(gl_project.id)
                    ),
                    full_name=full_name,
                    default_branch=(
                        gl_project.default_branch
                        if hasattr(gl_project, "default_branch")
                        else "main"
                    ),
                    description=(
                        gl_project.description
                        if hasattr(gl_project, "description")
                        else None
                    ),
                    url=gl_project.web_url if hasattr(gl_project, "web_url") else None,
                    created_at=(
                        datetime.fromisoformat(
                            gl_project.created_at.replace("Z", "+00:00")
                        )
                        if hasattr(gl_project, "created_at") and gl_project.created_at
                        else None
                    ),
                    updated_at=(
                        datetime.fromisoformat(
                            gl_project.last_activity_at.replace("Z", "+00:00")
                        )
                        if hasattr(gl_project, "last_activity_at")
                        and gl_project.last_activity_at
                        else None
                    ),
                    language=None,  # GitLab doesn't provide primary language in list
                    stars=(
                        gl_project.star_count
                        if hasattr(gl_project, "star_count")
                        else 0
                    ),
                    forks=(
                        gl_project.forks_count
                        if hasattr(gl_project, "forks_count")
                        else 0
                    ),
                )
                projects.append(project)
                logger.debug(f"Retrieved project: {project.full_name}")

            pattern_msg = f" matching pattern '{pattern}'" if pattern else ""
            logger.info(f"Retrieved {len(projects)} projects{pattern_msg}")
            return projects

        except Exception as e:
            self._handle_gitlab_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_contributors_by_project(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        max_contributors: Optional[int] = None,
    ) -> List[Author]:
        """
        Get contributors for a project.

        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param max_contributors: Maximum number of contributors to retrieve.
        :return: List of Author objects.
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        try:
            project = self.gitlab.projects.get(project_identifier)
            contributors = []

            gl_contributors = project.repository_contributors(
                per_page=self.per_page,
                get_all=False,
            )

            for contributor in gl_contributors:
                if max_contributors and len(contributors) >= max_contributors:
                    break

                author = Author(
                    id=0,  # GitLab contributors API doesn't provide user ID
                    username=contributor.get("name", "Unknown"),
                    email=contributor.get("email"),
                    name=contributor.get("name"),
                    url=None,
                )
                contributors.append(author)
                logger.debug(f"Retrieved contributor: {author.username}")

            logger.info(
                f"Retrieved {len(contributors)} contributors for project {project_identifier}"
            )
            return contributors

        except Exception as e:
            self._handle_gitlab_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_commit_stats_by_project(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        sha: Optional[str] = None,
    ) -> CommitStats:
        """
        Get statistics for a specific commit.

        :param sha: Commit SHA.
        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :return: CommitStats object.
        """
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        if not sha:
            raise ValueError("sha must be provided")

        try:
            gl_proj = self.gitlab.projects.get(project_identifier)
            commit = gl_proj.commits.get(sha)

            return CommitStats(
                additions=(
                    commit.stats.get("additions", 0) if hasattr(commit, "stats") else 0
                ),
                deletions=(
                    commit.stats.get("deletions", 0) if hasattr(commit, "stats") else 0
                ),
                commits=1,
            )

        except Exception as e:
            self._handle_gitlab_exception(e)
            return CommitStats(additions=0, deletions=0, commits=0)

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_repo_stats_by_project(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        max_commits: Optional[int] = None,
    ) -> RepoStats:
        """
        Get aggregated statistics for a project.

        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param max_commits: Maximum number of commits to analyze.
        :return: RepoStats object.
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        try:
            project = self.gitlab.projects.get(project_identifier)

            total_additions = 0
            total_deletions = 0
            commit_count = 0
            authors_dict = {}

            commits = project.commits.list(per_page=self.per_page, get_all=False)

            for commit in commits:
                if max_commits and commit_count >= max_commits:
                    break

                commit_count += 1

                # Get detailed commit with stats
                detailed_commit = project.commits.get(commit.id)
                if hasattr(detailed_commit, "stats"):
                    total_additions += detailed_commit.stats.get("additions", 0)
                    total_deletions += detailed_commit.stats.get("deletions", 0)

                # Track unique authors
                author_name = (
                    commit.author_name if hasattr(commit, "author_name") else "Unknown"
                )
                author_email = (
                    commit.author_email if hasattr(commit, "author_email") else ""
                )

                author_key = f"{author_name}:{author_email}"
                if author_key not in authors_dict:
                    authors_dict[author_key] = Author(
                        id=0,  # GitLab doesn't provide author ID in commit API
                        username=author_name,
                        name=author_name,
                        email=author_email,
                        url=None,
                    )

            # Calculate commits per week
            created_at = None
            if hasattr(project, "created_at") and project.created_at:
                created_at = datetime.fromisoformat(
                    project.created_at.replace("Z", "+00:00")
                )

            if created_at:
                age_days = (datetime.now(timezone.utc) - created_at).days
                weeks = max(age_days / 7, 1)
                commits_per_week = commit_count / weeks
            else:
                commits_per_week = 0.0

            return RepoStats(
                total_commits=commit_count,
                additions=total_additions,
                deletions=total_deletions,
                commits_per_week=commits_per_week,
                authors=list(authors_dict.values()),
            )

        except Exception as e:
            self._handle_gitlab_exception(e)
            return RepoStats(
                total_commits=0,
                additions=0,
                deletions=0,
                commits_per_week=0.0,
                authors=[],
            )

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_merge_requests(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        state: str = "all",
        max_mrs: Optional[int] = None,
    ) -> List[PullRequest]:
        """
        Get merge requests for a project using REST API.

        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param state: State filter ('opened', 'closed', 'merged', 'all').
        :param max_mrs: Maximum number of merge requests to retrieve.
        :return: List of PullRequest objects (representing GitLab merge requests).
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        actual_project_id: int
        # If project_name is provided, we need to get the project_id for the REST API
        if project_name:
            try:
                gl_proj = self.gitlab.projects.get(project_identifier)
                actual_project_id = int(gl_proj.id)
            except Exception as e:
                self._handle_gitlab_exception(e)
                return []
        elif project_id:
            actual_project_id = project_id
        else:
            return []

        try:
            merge_requests = []
            page = 1

            while True:
                if max_mrs and len(merge_requests) >= max_mrs:
                    break

                mrs = self.rest_client.get_merge_requests(
                    project_id=actual_project_id,
                    state=state,
                    page=page,
                    per_page=self.per_page,
                )

                if not mrs:
                    break

                for mr in mrs:
                    if max_mrs and len(merge_requests) >= max_mrs:
                        break

                    author = None
                    if mr.get("author"):
                        author_data = mr["author"]
                        author = Author(
                            id=author_data.get("id", 0),
                            username=author_data.get("username", "Unknown"),
                            name=author_data.get("name"),
                            email=None,  # Not provided in MR API
                            url=author_data.get("web_url"),
                        )

                    # Parse dates
                    created_at = None
                    if mr.get("created_at"):
                        try:
                            created_at = datetime.fromisoformat(
                                mr["created_at"].replace("Z", "+00:00")
                            )
                        except Exception:
                            # Invalid date format, leave as None
                            pass

                    merged_at = None
                    if mr.get("merged_at"):
                        try:
                            merged_at = datetime.fromisoformat(
                                mr["merged_at"].replace("Z", "+00:00")
                            )
                        except Exception:
                            # Invalid date format, leave as None
                            pass

                    closed_at = None
                    if mr.get("closed_at"):
                        try:
                            closed_at = datetime.fromisoformat(
                                mr["closed_at"].replace("Z", "+00:00")
                            )
                        except Exception:
                            # Invalid date format, leave as None
                            pass

                    pr = PullRequest(
                        id=mr.get("id", 0),
                        number=mr.get("iid", 0),
                        title=mr.get("title", ""),
                        state=mr.get("state", "unknown"),
                        author=author,
                        created_at=created_at,
                        merged_at=merged_at,
                        closed_at=closed_at,
                        body=mr.get("description"),
                        url=mr.get("web_url"),
                        base_branch=mr.get("target_branch"),
                        head_branch=mr.get("source_branch"),
                    )
                    merge_requests.append(pr)
                    logger.debug(f"Retrieved MR !{pr.number}: {pr.title}")

                page += 1

            logger.info(
                f"Retrieved {len(merge_requests)} merge requests for project {project_identifier}"
            )
            return merge_requests

        except Exception as e:
            self._handle_gitlab_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_merge_request_commits(
        self,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        iid: int = 0,
    ) -> List[PullRequestCommit]:
        """
        Get commits for a specific merge request.

        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param iid: Merge request internal ID (iid).
        :return: List of PullRequestCommit objects.
        """
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        try:
            project = self.gitlab.projects.get(project_identifier)
            mr = project.mergerequests.get(iid)
            commits = []
            for c in mr.commits():
                authored_at = None
                if c.authored_date:
                    try:
                        authored_at = datetime.fromisoformat(
                            c.authored_date.replace("Z", "+00:00")
                        )
                    except Exception as e:
                        # If the authored_date cannot be parsed, fall back to None
                        logger.debug(
                            "Failed to parse authored_date '%s' for commit '%s': %s",
                            c.authored_date,
                            getattr(c, "id", "<unknown>"),
                            e,
                        )
                commits.append(
                    PullRequestCommit(
                        sha=c.id,
                        authored_at=authored_at,
                        message=c.message,
                        author_name=c.author_name,
                        author_email=c.author_email,
                    )
                )
            return commits
        except Exception as e:
            self._handle_gitlab_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_file_blame_by_project(
        self,
        file_path: str,
        project_id: Optional[int] = None,
        project_name: Optional[str] = None,
        ref: str = "HEAD",
    ) -> FileBlame:
        """
        Get blame information for a file using GitLab REST API.

        :param file_path: File path within the repository.
        :param project_id: GitLab project ID (deprecated, use project_name).
        :param project_name: GitLab project name/path (e.g., 'group/project').
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: FileBlame object.
        """
        # Use project_name if provided, otherwise fall back to project_id
        project_identifier = project_name if project_name else project_id
        if not project_identifier:
            raise ValueError("Either project_id or project_name must be provided")

        actual_project_id: int
        # If project_name is provided, we need to get the project_id for the REST API
        if project_name:
            try:
                gl_proj = self.gitlab.projects.get(project_identifier)
                actual_project_id = int(gl_proj.id)
            except Exception as e:
                self._handle_gitlab_exception(e)
                return FileBlame(file_path=file_path, ranges=[])
        elif project_id:
            actual_project_id = project_id
        else:
            return FileBlame(file_path=file_path, ranges=[])

        try:
            blame_data = self.rest_client.get_file_blame(
                actual_project_id, file_path, ref
            )

            ranges = []
            current_line = 1

            for blame_item in blame_data:
                lines = blame_item.get("lines", [])
                commit = blame_item.get("commit", {})

                # Calculate age in seconds
                committed_date_str = commit.get("committed_date")
                age_seconds = 0
                if committed_date_str:
                    try:
                        committed_date = datetime.fromisoformat(
                            committed_date_str.replace("Z", "+00:00")
                        )
                        age_seconds = int(
                            (
                                datetime.now(timezone.utc) - committed_date
                            ).total_seconds()
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse date {committed_date_str}: {e}"
                        )

                num_lines = len(lines)
                if num_lines > 0:
                    blame_range = BlameRange(
                        starting_line=current_line,
                        ending_line=current_line + num_lines - 1,
                        commit_sha=commit.get("id", ""),
                        author=commit.get("author_name", "Unknown"),
                        author_email=commit.get("author_email", ""),
                        age_seconds=age_seconds,
                    )
                    ranges.append(blame_range)
                    current_line += num_lines

            logger.info(
                f"Retrieved blame for project {project_identifier}:{file_path} "
                f"with {len(ranges)} ranges"
            )
            return FileBlame(file_path=file_path, ranges=ranges)

        except Exception as e:
            self._handle_gitlab_exception(e)
            return FileBlame(file_path=file_path, ranges=[])

    # =========================================================================
    # Base class interface implementations (adapters for GitLab-style methods)
    # =========================================================================

    def list_organizations(
        self,
        max_orgs: Optional[int] = None,
    ) -> List[Organization]:
        """List groups accessible to the authenticated user."""
        return self.list_groups(max_groups=max_orgs) or []

    def list_repositories(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        search: Optional[str] = None,
        pattern: Optional[str] = None,
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """List projects for a group, user, or all accessible projects."""
        return (
            self.list_projects(
                group_name=org_name,
                user_name=user_name,
                search=search,
                pattern=pattern,
                max_projects=max_repos,
            )
            or []
        )

    def get_contributors(
        self,
        owner: str,
        repo: str,
        max_contributors: Optional[int] = None,
    ) -> List[Author]:
        """Get contributors for a repository using owner/repo style parameters."""
        return self.get_contributors_by_project(
            project_name=f"{owner}/{repo}", max_contributors=max_contributors
        )

    def get_commit_stats(
        self,
        owner: str,
        repo: str,
        sha: str,
    ) -> CommitStats:
        """Get statistics for a specific commit using owner/repo style parameters."""
        return self.get_commit_stats_by_project(project_name=f"{owner}/{repo}", sha=sha)

    def get_repo_stats(
        self,
        owner: str,
        repo: str,
        max_commits: Optional[int] = None,
    ) -> RepoStats:
        """Get aggregated statistics for a repository using owner/repo style parameters."""
        return self.get_repo_stats_by_project(
            project_name=f"{owner}/{repo}", max_commits=max_commits
        )

    def get_projects_with_stats(
        self,
        group_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        batch_size: int = 10,
        max_concurrent: int = 4,
        rate_limit_delay: float = 1.0,
        max_commits_per_repo: Optional[int] = None,
        max_repos: Optional[int] = None,
        on_project_complete: Optional[Callable[[BatchResult], None]] = None,
    ) -> List[BatchResult]:
        """Alias for get_repos_with_stats using project nomenclature."""
        return self.get_repos_with_stats(
            org_name=group_name,
            user_name=user_name,
            pattern=pattern,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            rate_limit_delay=rate_limit_delay,
            max_commits_per_repo=max_commits_per_repo,
            max_repos=max_repos,
            on_repo_complete=on_project_complete,
        )

    async def get_projects_with_stats_async(
        self,
        group_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        batch_size: int = 10,
        max_concurrent: int = 4,
        rate_limit_delay: float = 1.0,
        max_commits_per_repo: Optional[int] = None,
        max_repos: Optional[int] = None,
        on_project_complete: Optional[Callable[[BatchResult], None]] = None,
    ) -> List[BatchResult]:
        """Alias for get_repos_with_stats_async using project nomenclature."""
        return await self.get_repos_with_stats_async(
            org_name=group_name,
            user_name=user_name,
            pattern=pattern,
            batch_size=batch_size,
            max_concurrent=max_concurrent,
            rate_limit_delay=rate_limit_delay,
            max_commits_per_repo=max_commits_per_repo,
            max_repos=max_repos,
            on_repo_complete=on_project_complete,
        )

    def _get_repositories_for_processing(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        pattern: Optional[str] = None,
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """GitLab-specific repository discovery, favoring group extraction from patterns."""
        effective_org = org_name
        effective_user = user_name

        if not org_name and not user_name and pattern:
            if "/" in pattern:
                # For GitLab, the first part is more likely a group (namespace)
                prefix = pattern.split("/")[0]
                if prefix and "*" not in prefix and "?" not in prefix:
                    effective_org = prefix
                    logger.info(f"Extracted group '{prefix}' from pattern '{pattern}'")

        return self.list_repositories(
            org_name=effective_org,
            user_name=effective_user,
            pattern=pattern,
            max_repos=max_repos,
        )

    def _get_projects_for_processing(self, **kwargs) -> List[Repository]:
        """Internal helper for project listing, aliasing _get_repositories_for_processing."""
        if "group_name" in kwargs and "org_name" not in kwargs:
            kwargs["org_name"] = kwargs.pop("group_name")
        return self._get_repositories_for_processing(**kwargs)

    def _process_single_project_stats(self, project, max_commits) -> BatchResult:
        """Internal helper for single project processing, aliasing _process_single_repo_stats."""
        return self._process_single_repo_stats(project, max_commits)

    def get_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        max_prs: Optional[int] = None,
    ) -> List[PullRequest]:
        """Get pull requests (merge requests) for a repository."""
        return self.get_merge_requests(
            project_name=f"{owner}/{repo}", state=state, max_mrs=max_prs
        )

    def get_file_blame(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str = "HEAD",
    ) -> FileBlame:
        """Get blame information for a file using owner/repo style parameters."""
        return self.get_file_blame_by_project(
            project_name=f"{owner}/{repo}", file_path=path, ref=ref
        )

    def get_dora_metrics(
        self,
        project_name: str,
        metric: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        interval: str = "daily",
    ) -> DORAMetrics:
        """Retrieve DORA metrics for a GitLab project."""
        try:
            # First get project ID
            gl_proj = self.gitlab.projects.get(project_name)
            project_id = int(gl_proj.id)

            raw_metrics = self.rest_client.get_dora_metrics(
                project_id=project_id,
                metric=metric,
                start_date=start_date,
                end_date=end_date,
                interval=interval,
            )

            data_points = []
            for item in raw_metrics:
                dt = datetime.fromisoformat(item["date"])
                data_points.append(
                    DORAMetric(
                        date=dt,
                        value=float(item.get("value", 0.0)),
                    )
                )

            return DORAMetrics(
                metric_name=metric,
                data_points=data_points,
            )

        except Exception as e:
            try:
                self._handle_gitlab_exception(e)
            except Exception:
                # Silently catch re-raised exceptions to return empty metrics
                pass
            return DORAMetrics(metric_name=metric, data_points=[])

    def get_rate_limit(self) -> Dict[str, Any]:
        """Get current rate limit status."""
        return {}

    def close(self) -> None:
        """Cleanup GitLab client resources."""
        pass
