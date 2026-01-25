"""
GitHub connector using PyGithub and GraphQL.

This connector provides methods to retrieve organizations, repositories,
contributors, statistics, pull requests, and blame information from GitHub.
"""

import logging
import inspect
import time
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any

from github import Auth, Github, GithubException, RateLimitExceededException

from dev_health_ops.connectors.base import (
    GitConnector,
    RateLimitException,
)
from dev_health_ops.connectors.exceptions import (
    APIException,
    AuthenticationException,
    NotFoundException,
)
from dev_health_ops.connectors.models import (
    Author,
    BlameRange,
    CommitStats,
    FileBlame,
    Organization,
    PullRequest,
    PullRequestCommit,
    PullRequestReview,
    Repository,
    RepoStats,
)
from dev_health_ops.connectors.utils import (
    GitHubGraphQLClient,
    retry_with_backoff,
    match_repo_pattern,
)

logger = logging.getLogger(__name__)


class GitHubConnector(GitConnector):
    """
    Production-grade GitHub connector using PyGithub and GraphQL.

    Provides methods to retrieve data from GitHub with automatic
    pagination, rate limiting, and error handling.
    """

    def __init__(
        self,
        token: str,
        base_url: Optional[str] = None,
        per_page: int = 100,
        max_workers: int = 4,
    ):
        """
        Initialize GitHub connector.

        :param token: GitHub personal access token.
        :param base_url: Optional base URL for GitHub Enterprise.
        :param per_page: Number of items per page for pagination.
        :param max_workers: Maximum concurrent workers for operations.
        """
        super().__init__(per_page=per_page, max_workers=max_workers)
        self.token = token

        # Initialize PyGithub client
        auth = Auth.Token(token)
        if base_url:
            self.github = Github(base_url=base_url, auth=auth, per_page=per_page)
        else:
            self.github = Github(auth=auth, per_page=per_page)

        # Initialize GraphQL client for blame operations
        self.graphql = GitHubGraphQLClient(token)

    def _handle_github_exception(self, e: Exception) -> None:
        """
        Handle GitHub API exceptions and convert to connector exceptions.

        :param e: Exception from GitHub API.
        :raises: Appropriate connector exception.
        """
        if isinstance(e, RateLimitExceededException):
            raise RateLimitException(
                f"GitHub rate limit exceeded: {e}",
                retry_after_seconds=self._rate_limit_reset_delay_seconds(),
            )
        elif isinstance(e, GithubException):
            if e.status == 401:
                raise AuthenticationException(f"GitHub authentication failed: {e}")
            elif e.status == 404:
                raise NotFoundException(
                    "GitHub resource not found (404). "
                    "This can also mean the token lacks access "
                    "(fine-grained PAT / GitHub App tokens can 404). "
                    f"Details: {e}"
                )
            else:
                raise APIException(f"GitHub API error: {e}")
        else:
            raise APIException(f"Unexpected error: {e}")

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_organizations(
        self,
        max_orgs: Optional[int] = None,
    ) -> List[Organization]:
        """
        List organizations accessible to the authenticated user.

        :param max_orgs: Maximum number of organizations to retrieve.
        :return: List of Organization objects.
        """
        try:
            orgs = []
            user = self.github.get_user()

            for gh_org in user.get_orgs():
                if max_orgs and len(orgs) >= max_orgs:
                    break

                org = Organization(
                    id=gh_org.id,
                    name=gh_org.login,
                    description=gh_org.description,
                    url=gh_org.html_url,
                )
                orgs.append(org)
                logger.debug(f"Retrieved organization: {org.name}")

            logger.info(f"Retrieved {len(orgs)} organizations")
            return orgs

        except Exception as e:
            self._handle_github_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def list_repositories(
        self,
        org_name: Optional[str] = None,
        user_name: Optional[str] = None,
        search: Optional[str] = None,
        pattern: Optional[str] = None,
        max_repos: Optional[int] = None,
    ) -> List[Repository]:
        """
        List repositories for an organization, user, or search query.

        :param org_name: Optional organization name. If provided, lists organization repos.
        :param user_name: Optional user name. If provided, lists that user's repos.
        :param search: Optional search query to filter repositories.
                      If provided with org_name/user_name, searches within that scope.
                      If provided alone, performs global search.
        :param pattern: Optional fnmatch-style pattern to filter repositories by full name
                       (e.g., 'chrisgeo/m*', '*/api-*'). Pattern matching is performed
                       client-side after fetching repositories. Case-insensitive.
        :param max_repos: Maximum number of repositories to retrieve. If None, retrieves all.
        :return: List of Repository objects.

        Examples:
            - pattern='chrisgeo/m*' matches 'chrisgeo/dev-health-ops'
            - pattern='*/sync*' matches 'anyorg/sync-tool'
        """
        try:
            repos = []

            # Determine the appropriate API method and parameters
            if search:
                # Build search query with optional scope qualifiers
                query_parts = [search]
                if org_name:
                    query_parts.append(f"org:{org_name}")
                elif user_name:
                    query_parts.append(f"user:{user_name}")
                gh_repos = self.github.search_repositories(query=" ".join(query_parts))
            else:
                # Fetch repositories without search
                if org_name:
                    source = self.github.get_organization(org_name)
                elif user_name:
                    source = self.github.get_user(user_name)
                else:
                    source = self.github.get_user()
                gh_repos = source.get_repos()

            for gh_repo in gh_repos:
                if max_repos and len(repos) >= max_repos:
                    break

                # Apply pattern filter early to avoid unnecessary object creation
                if pattern and not match_repo_pattern(gh_repo.full_name, pattern):
                    continue

                repo = Repository(
                    id=gh_repo.id,
                    name=gh_repo.name,
                    full_name=gh_repo.full_name,
                    default_branch=gh_repo.default_branch,
                    description=gh_repo.description,
                    url=gh_repo.html_url,
                    created_at=gh_repo.created_at,
                    updated_at=gh_repo.updated_at,
                    language=gh_repo.language,
                    stars=gh_repo.stargazers_count,
                    forks=gh_repo.forks_count,
                )

                repos.append(repo)
                logger.debug(f"Retrieved repository: {repo.full_name}")

            pattern_msg = f" matching pattern '{pattern}'" if pattern else ""
            logger.info(f"Retrieved {len(repos)} repositories{pattern_msg}")
            return repos

        except Exception as e:
            self._handle_github_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_contributors(
        self,
        owner: str,
        repo: str,
        max_contributors: Optional[int] = None,
    ) -> List[Author]:
        """
        Get contributors for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param max_contributors: Maximum number of contributors to retrieve.
        :return: List of Author objects.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            contributors = []

            for contributor in gh_repo.get_contributors():
                if max_contributors and len(contributors) >= max_contributors:
                    break

                author = Author(
                    id=contributor.id,
                    username=contributor.login,
                    name=contributor.name,
                    email=contributor.email,
                    url=contributor.html_url,
                )
                contributors.append(author)
                logger.debug(f"Retrieved contributor: {author.username}")

            logger.info(
                f"Retrieved {len(contributors)} contributors for {owner}/{repo}"
            )
            return contributors

        except Exception as e:
            self._handle_github_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_commit_stats(
        self,
        owner: str,
        repo: str,
        sha: str,
    ) -> CommitStats:
        """
        Get statistics for a specific commit.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param sha: Commit SHA.
        :return: CommitStats object.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            commit = gh_repo.get_commit(sha)

            stats = commit.stats

            return CommitStats(
                additions=stats.additions,
                deletions=stats.deletions,
                commits=1,
            )

        except Exception as e:
            self._handle_github_exception(e)
            raise

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_repo_stats(
        self,
        owner: str,
        repo: str,
        max_commits: Optional[int] = None,
    ) -> RepoStats:
        """
        Get aggregated statistics for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param max_commits: Maximum number of commits to analyze.
        :return: RepoStats object.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")

            total_additions = 0
            total_deletions = 0
            commit_count = 0
            authors_dict = {}

            commits = gh_repo.get_commits()

            for commit in commits:
                if max_commits and commit_count >= max_commits:
                    break

                commit_count += 1

                # Get commit stats without triggering extra API calls.
                # In PyGithub, `commit.stats` is a property that completes the
                # object via an additional request per commit. That explodes
                # rate-limit usage in batch mode.
                stats_value = inspect.getattr_static(commit, "stats", None)
                if stats_value is None or isinstance(stats_value, property):
                    continue

                total_additions += getattr(stats_value, "additions", 0) or 0
                total_deletions += getattr(stats_value, "deletions", 0) or 0

                # Track unique authors
                if commit.author:
                    # Some commits reference users that no longer exist
                    # (deleted/suspended), and PyGithub will 404 when trying
                    # to lazily fetch extra user fields like name/email.
                    # Keep this robust by only using stable fields.
                    try:
                        author_id = commit.author.id
                        author_login = commit.author.login
                    except Exception:
                        author_id = None
                        author_login = None

                    if not author_id or not author_login:
                        continue

                    if author_id not in authors_dict:
                        authors_dict[author_id] = Author(
                            id=author_id,
                            username=author_login,
                        )

            # Calculate commits per week (rough estimate based on repo age)
            created_at = gh_repo.created_at
            age_days = (datetime.now(timezone.utc) - created_at).days
            weeks = max(age_days / 7, 1)
            commits_per_week = commit_count / weeks

            return RepoStats(
                total_commits=commit_count,
                additions=total_additions,
                deletions=total_deletions,
                commits_per_week=commits_per_week,
                authors=list(authors_dict.values()),
            )

        except Exception as e:
            self._handle_github_exception(e)
            raise

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_pull_requests(
        self,
        owner: str,
        repo: str,
        state: str = "all",
        max_prs: Optional[int] = None,
    ) -> List[PullRequest]:
        """
        Get pull requests for a repository.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param state: State filter ('open', 'closed', 'all').
        :param max_prs: Maximum number of pull requests to retrieve.
        :return: List of PullRequest objects.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            prs = []

            # per_page is set at Github client level during initialization
            for gh_pr in gh_repo.get_pulls(state=state):
                if max_prs and len(prs) >= max_prs:
                    break

                author = None
                if gh_pr.user:
                    author = Author(
                        id=gh_pr.user.id,
                        username=gh_pr.user.login,
                        name=gh_pr.user.name,
                        email=gh_pr.user.email,
                        url=gh_pr.user.html_url,
                    )

                prs.append(
                    PullRequest(
                        id=gh_pr.id,
                        number=gh_pr.number,
                        title=gh_pr.title,
                        state=gh_pr.state,
                        author=author,
                        created_at=gh_pr.created_at,
                        merged_at=gh_pr.merged_at,
                        closed_at=gh_pr.closed_at,
                        body=gh_pr.body,
                        url=gh_pr.html_url,
                        base_branch=gh_pr.base.ref,
                        head_branch=gh_pr.head.ref,
                    )
                )
            return prs
        except Exception as e:
            self._handle_github_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_pull_request_reviews(
        self,
        owner: str,
        repo: str,
        number: int,
    ) -> List[PullRequestReview]:
        """
        Get reviews for a specific pull request.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param number: Pull request number.
        :return: List of PullRequestReview objects.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            gh_pr = gh_repo.get_pull(number)
            reviews = []
            for r in gh_pr.get_reviews():
                reviews.append(
                    PullRequestReview(
                        id=str(r.id),
                        reviewer=r.user.login if r.user else "Unknown",
                        state=r.state,
                        submitted_at=r.submitted_at,
                        body=r.body,
                        url=gh_pr.html_url + f"#pullrequestreview-{r.id}",
                    )
                )
            return reviews
        except Exception as e:
            self._handle_github_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_pull_request_commits(
        self,
        owner: str,
        repo: str,
        number: int,
    ) -> List[PullRequestCommit]:
        """
        Get commits for a specific pull request.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param number: Pull request number.
        :return: List of PullRequestCommit objects.
        """
        try:
            gh_repo = self.github.get_repo(f"{owner}/{repo}")
            gh_pr = gh_repo.get_pull(number)
            commits = []
            for c in gh_pr.get_commits():
                authored_at = None
                author_name = None
                author_email = None
                if c.commit and c.commit.author:
                    authored_at = c.commit.author.date
                    author_name = c.commit.author.name
                    author_email = c.commit.author.email
                commits.append(
                    PullRequestCommit(
                        sha=c.sha,
                        authored_at=authored_at,
                        message=c.commit.message if c.commit else None,
                        author_name=author_name,
                        author_email=author_email,
                    )
                )
            return commits
        except Exception as e:
            self._handle_github_exception(e)
            return []

    @retry_with_backoff(
        max_retries=3,
        initial_delay=1.0,
        exceptions=(RateLimitException, APIException),
    )
    def get_file_blame(
        self,
        owner: str,
        repo: str,
        path: str,
        ref: str = "HEAD",
    ) -> FileBlame:
        """
        Get blame information for a file using GitHub GraphQL API.

        :param owner: Repository owner.
        :param repo: Repository name.
        :param path: File path within the repository.
        :param ref: Git reference (branch, tag, or commit SHA).
        :return: FileBlame object.
        """
        try:
            result = self.graphql.get_blame(owner, repo, path, ref)

            ranges = []
            repo_data = result.get("repository", {})
            obj_data = repo_data.get("object", {})
            blame_data = obj_data.get("blame", {})
            ranges_data = blame_data.get("ranges", [])

            for range_item in ranges_data:
                commit = range_item.get("commit", {})
                author_info = commit.get("author", {})

                # Calculate age in seconds
                authored_date_str = commit.get("authoredDate")
                age_seconds = 0
                if authored_date_str:
                    try:
                        authored_date = datetime.fromisoformat(
                            authored_date_str.replace("Z", "+00:00")
                        )
                        age_seconds = int(
                            (datetime.now(timezone.utc) - authored_date).total_seconds()
                        )
                    except Exception as e:
                        logger.warning(f"Failed to parse date {authored_date_str}: {e}")

                blame_range = BlameRange(
                    starting_line=range_item.get("startingLine", 0),
                    ending_line=range_item.get("endingLine", 0),
                    commit_sha=commit.get("oid", ""),
                    author=author_info.get("name", "Unknown"),
                    author_email=author_info.get("email", ""),
                    age_seconds=age_seconds,
                )
                ranges.append(blame_range)

            logger.info(
                f"Retrieved blame for {owner}/{repo}:{path} with {len(ranges)} ranges"
            )
            return FileBlame(file_path=path, ranges=ranges)

        except Exception as e:
            self._handle_github_exception(e)
            raise

    def get_rate_limit(self) -> Dict[str, Any]:
        """
        Get current rate limit status.

        :return: Dictionary with rate limit information.
        """
        try:
            # PyGithub's get_rate_limit().core is the core rate limit
            # but sometimes it's nested differently depending on version
            rl = self.github.get_rate_limit()
            core = getattr(rl, "core", rl)
            search = getattr(rl, "search", None)

            res = {
                "limit": getattr(core, "limit", 0),
                "remaining": getattr(core, "remaining", 0),
                "reset": getattr(core, "reset", datetime.now(timezone.utc)).isoformat(),
            }

            if search:
                res["search"] = {
                    "limit": getattr(search, "limit", 0),
                    "remaining": getattr(search, "remaining", 0),
                    "reset": getattr(
                        search, "reset", datetime.now(timezone.utc)
                    ).isoformat(),
                }

            return res
        except Exception as e:
            self._handle_github_exception(e)
            return {}

    def _rate_limit_reset_delay_seconds(self) -> float:
        """Get delay in seconds until rate limit resets."""
        try:
            rl = self.github.get_rate_limit()
            core = getattr(rl, "core", rl)
            reset_time = getattr(core, "reset", datetime.now(timezone.utc)).timestamp()
            now = time.time()
            return max(1.0, float(reset_time - now) + 1.0)
        except Exception:
            return 60.0

    def close(self) -> None:
        """Cleanup GitHub client resources."""
        if hasattr(self.github, "close"):
            self.github.close()
