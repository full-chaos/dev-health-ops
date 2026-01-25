from datetime import datetime, timezone

from dev_health_ops.connectors.github import GitHubConnector
from github.GithubException import UnknownObjectException


class _AuthorWith404Name:
    id = 123
    login = "deleted-user"

    @property
    def name(self):
        raise UnknownObjectException(404, {"message": "Not Found"}, {})

    @property
    def email(self):
        raise UnknownObjectException(404, {"message": "Not Found"}, {})

    @property
    def html_url(self):
        raise UnknownObjectException(404, {"message": "Not Found"}, {})


class _Stats:
    def __init__(self, additions=1, deletions=2):
        self.additions = additions
        self.deletions = deletions


class _Commit:
    def __init__(self):
        self.stats = _Stats()
        self.author = _AuthorWith404Name()


class _Repo:
    created_at = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def get_commits(self):
        return [_Commit()]


def test_get_repo_stats_does_not_fail_on_author_profile_404(monkeypatch):
    connector = GitHubConnector(token="test_token")

    try:
        monkeypatch.setattr(
            connector.github,
            "get_repo",
            lambda _name: _Repo(),
        )

        stats = connector.get_repo_stats("o", "r", max_commits=1)
        assert stats.total_commits == 1
        assert stats.additions == 1
        assert stats.deletions == 2
        assert len(stats.authors) == 1
        assert stats.authors[0].username == "deleted-user"
    finally:
        connector.close()
