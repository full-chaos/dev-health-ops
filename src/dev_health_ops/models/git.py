import hashlib
import logging
import os
import uuid
from datetime import datetime, timezone

from git import Repo as GitRepo
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    Text,
    TypeDecorator,
    CHAR,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import declarative_base, relationship
from typing import Optional, List, Tuple

Base = declarative_base()


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type for PostgreSQL, otherwise uses
    CHAR(32), storing as stringified hex values.
    """

    impl = CHAR
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PGUUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == "postgresql":
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return str(uuid.UUID(value)).replace("-", "")
            else:
                # hex string
                return value.hex

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                return uuid.UUID(value)
            else:
                return value


def get_repo_uuid_from_repo(repo: str) -> uuid.UUID:
    """Generate a deterministic UUID from a repo identifier string.

    This is used for non-local repositories where we don't have a filesystem
    path to derive an ID from.

    Priority order:
    1. REPO_UUID environment variable (if set)
    2. SHA256(repo identifier)
    """
    env_uuid = os.getenv("REPO_UUID")
    if env_uuid:
        return uuid.UUID(env_uuid)

    if not repo:
        raise ValueError("repo identifier is required")

    normalized = repo.strip().lower()
    hash_obj = hashlib.sha256(normalized.encode("utf-8"))
    uuid_bytes = hash_obj.digest()[:16]
    return uuid.UUID(bytes=uuid_bytes)


def get_repo_uuid(repo_path: str) -> uuid.UUID:
    """
    Generate a deterministic UUID for a repository based on git data.

    Priority order:
    1. REPO_UUID environment variable (if set)
    2. Remote URL (if repository has a remote configured)
    3. Absolute path of the repository (fallback)

    :param repo_path: Path to the git repository.
    :return: UUID for the repository.
    """
    # First check if REPO_UUID is explicitly set
    env_uuid = os.getenv("REPO_UUID")
    if env_uuid:
        return uuid.UUID(env_uuid)

    try:
        # Try to get repository information from git
        git_repo = GitRepo(repo_path)

        # Try to get remote URL (most reliable identifier)
        if git_repo.remotes:
            remote_url = None
            # Try to get the origin remote first
            if "origin" in [r.name for r in git_repo.remotes]:
                origin = git_repo.remote("origin")
                remote_url = list(origin.urls)[0] if origin.urls else None
            else:
                # Use first available remote
                remote_url = (
                    list(git_repo.remotes[0].urls)[0]
                    if git_repo.remotes[0].urls
                    else None
                )

            if remote_url:
                # Create deterministic UUID from remote URL
                # Use SHA256 hash and convert to UUID format
                hash_obj = hashlib.sha256(remote_url.encode("utf-8"))
                # Take first 16 bytes of hash and create UUID
                uuid_bytes = hash_obj.digest()[:16]
                return uuid.UUID(bytes=uuid_bytes)

        # Fallback to absolute path if no remote
        abs_path = os.path.abspath(repo_path)
        hash_obj = hashlib.sha256(abs_path.encode("utf-8"))
        uuid_bytes = hash_obj.digest()[:16]
        return uuid.UUID(bytes=uuid_bytes)

    except Exception as e:
        try:
            abs_path = os.path.abspath(repo_path)
            hash_obj = hashlib.sha256(abs_path.encode("utf-8"))
            uuid_bytes = hash_obj.digest()[:16]
            return uuid.UUID(bytes=uuid_bytes)
        except Exception:
            logging.warning(
                "Could not derive UUID from git repository data or path: %s. "
                "Generating random UUID.",
                e,
            )
            return uuid.uuid4()


class Repo(Base, GitRepo):
    __tablename__ = "repos"

    def __init__(self, repo_path: Optional[str] = None, **kwargs):
        """
        Initialize the Repo class with the given repository path.

        Automatically derives a deterministic UUID from the repository
        if one is not explicitly provided.

        :param repo_path: Path to the git repository (optional).
        :param kwargs: Additional keyword arguments for SQLAlchemy ORM.
        """
        # Auto-derive UUID if not explicitly provided.
        # This is critical for MongoDB upserts (Repo.id may otherwise be None
        # until SQLAlchemy flush/commit, causing all upserts to collide).
        if "id" not in kwargs:
            if repo_path:
                kwargs["id"] = get_repo_uuid(repo_path)
            else:
                repo_identifier = kwargs.get("repo")
                if repo_identifier:
                    try:
                        kwargs["id"] = get_repo_uuid_from_repo(repo_identifier)
                    except Exception as e:
                        logging.warning(
                            "Could not derive UUID from repo identifier: %s. "
                            "Generating random UUID.",
                            e,
                        )
                        kwargs["id"] = uuid.uuid4()

        # Move 'tags' to 'repo_tags' to avoid conflict with GitRepo.tags property
        if "tags" in kwargs:
            kwargs["repo_tags"] = kwargs.pop("tags")

        super().__init__(**kwargs)  # Initialize SQLAlchemy ORM
        if repo_path:
            GitRepo.__init__(self, repo_path)  # Initialize GitPython Repo

    id = Column(
        GUID,
        primary_key=True,
        default=uuid.uuid4,
        comment="MergeStat identifier for the repo",
    )
    repo = Column(Text, nullable=False, comment="URL for the repo")
    ref = Column(Text, comment="ref for the repo")
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="timestamp of when the MergeStat repo entry was created",
    )
    settings = Column(
        JSON, nullable=False, default=dict, comment="JSON settings for the repo"
    )
    repo_tags = Column(
        "tags",
        JSON,
        key="repo_tags",
        nullable=False,
        default=list,
        comment="array of tags for the repo",
    )
    # repo_import_id = Column(
    #     GUID,
    #     ForeignKey("mergestat.repo_imports.id", ondelete="CASCADE"),
    #     comment="foreign key for mergestat.repo_imports.id",
    # )
    # provider = Column(
    #     GUID,
    #     ForeignKey("mergestat.providers.id", ondelete="CASCADE"),
    #     nullable=False,
    # )

    # Relationships
    git_refs = relationship("GitRef", back_populates="repo")
    git_files = relationship("GitFile", back_populates="repo")
    git_commits = relationship("GitCommit", back_populates="repo")
    git_commit_stats = relationship("GitCommitStat", back_populates="repo")
    git_blames = relationship("GitBlame", back_populates="repo")
    git_pull_requests = relationship("GitPullRequest", back_populates="repo")
    ci_pipeline_runs = relationship(
        "CiPipelineRun",
        back_populates="repo",
        cascade="all, delete-orphan",
    )
    deployments = relationship(
        "Deployment",
        back_populates="repo",
        cascade="all, delete-orphan",
    )
    incidents = relationship(
        "Incident",
        back_populates="repo",
        cascade="all, delete-orphan",
    )


class GitRef(Base):
    __tablename__ = "git_refs"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    full_name = Column(Text, primary_key=True)
    hash = Column(Text, comment="hash of the commit for refs that are not of type tag")
    name = Column(Text, comment="name of the ref")
    remote = Column(Text, comment="remote of the ref")
    target = Column(Text, comment="target of the ref")
    type = Column(Text, comment="type of the ref")
    tag_commit_hash = Column(
        Text, comment="hash of the commit for refs that are of type tag"
    )
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_refs")


class GitFile(Base):
    __tablename__ = "git_files"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    path = Column(Text, primary_key=True, comment="path of the file")
    executable = Column(
        Boolean,
        nullable=False,
        comment="boolean to determine if the file is an executable",
    )
    contents = Column(Text, comment="contents of the file")
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_files")


class GitCommit(Base):
    __tablename__ = "git_commits"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    hash = Column(Text, primary_key=True, comment="hash of the commit")
    message = Column(Text, comment="message of the commit")
    author_name = Column(Text, comment="name of the author of the modification")
    author_email = Column(Text, comment="email of the author of the modification")
    author_when = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="timestamp of when the modification was authored",
    )
    committer_name = Column(
        Text, comment="name of the author who committed the modification"
    )
    committer_email = Column(
        Text, comment="email of the author who committed the modification"
    )
    committer_when = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="timestamp of when the commit was made",
    )
    parents = Column(
        Integer, nullable=False, comment="the number of parents of the commit"
    )
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_commits")


class GitCommitStat(Base):
    __tablename__ = "git_commit_stats"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    commit_hash = Column(Text, primary_key=True, comment="hash of the commit")
    file_path = Column(
        Text, primary_key=True, comment="path of the file the modification was made in"
    )
    additions = Column(
        Integer,
        nullable=False,
        comment="the number of additions in this path of the commit",
    )
    deletions = Column(
        Integer,
        nullable=False,
        comment="the number of deletions in this path of the commit",
    )
    old_file_mode = Column(
        Text,
        nullable=False,
        default="unknown",
        comment="old file mode derived from git mode",
    )
    new_file_mode = Column(
        Text, default="unknown", comment="new file mode derived from git mode"
    )
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_commit_stats")


class GitBlameMixin:
    """
    Mixin to provide functionality for fetching blame data using gitpython.
    """

    @staticmethod
    def fetch_blame(
        repo_path: str,
        filepath: str,
        repo_uuid: uuid.UUID,
        repo: Optional[GitRepo] = None,
    ) -> List[Tuple]:
        """
        Fetch blame data for a given file using gitpython.

        :param repo_path: Path to the git repository.
        :param filepath: Path to the file to fetch blame data for.
        :param repo_uuid: UUID of the repository.
        :param repo: Optional existing Repo instance to reuse (improves performance).
        :return: List of blame data tuples.
        """
        blame_data = []
        if repo is None:
            repo = GitRepo(repo_path)
        rel_path = os.path.relpath(filepath, repo_path)
        try:
            blame_info = repo.blame("HEAD", rel_path)
            if blame_info:
                line_no = 1
                for item in blame_info:
                    if not isinstance(item, (list, tuple)) or len(item) < 2:
                        continue
                    commit = item[0]
                    lines = item[1]
                    if commit is None or lines is None:
                        continue
                    for line in lines:
                        author_email = getattr(commit.author, "email", "unknown")
                        author_name = getattr(commit.author, "name", "unknown")
                        committed_datetime = getattr(
                            commit, "committed_datetime", datetime.now(timezone.utc)
                        )
                        hexsha = getattr(commit, "hexsha", "unknown")

                        blame_data.append(
                            (
                                repo_uuid,
                                author_email,
                                author_name,
                                committed_datetime,
                                hexsha,
                                line_no,
                                line.rstrip("\n") if line else "",
                                rel_path,
                            )
                        )
                        line_no += 1
        except Exception as e:
            logging.warning(f"Error processing {rel_path}: {e}")
        return blame_data


class GitBlame(Base, GitBlameMixin):
    __tablename__ = "git_blame"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    path = Column(
        Text, primary_key=True, comment="path of the file the modification was made in"
    )
    line_no = Column(
        Integer, primary_key=True, comment="line number of the modification"
    )
    author_email = Column(Text, comment="email of the author who modified the line")
    author_name = Column(Text, comment="name of the author who modified the line")
    author_when = Column(
        DateTime(timezone=True),
        comment="timestamp of when the modification was authored",
    )
    commit_hash = Column(
        Text, comment="hash of the commit the modification was made in"
    )
    line = Column(Text, comment="content of the line")
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_blames")

    @classmethod
    def process_file(cls, repo_path, filepath, repo_uuid, repo=None):
        """
        Process a file to fetch blame data and return it as a list of GitBlame objects.

        :param repo_path: Path to the git repository.
        :param filepath: Path to the file to process.
        :param repo_uuid: UUID of the repository.
        :param repo: Optional existing Repo instance to reuse (improves performance).
        :return: List of GitBlame objects.
        """
        blame_data = cls.fetch_blame(repo_path, filepath, repo_uuid, repo=repo)
        return [
            cls(
                repo_id=row[0],
                author_email=row[1],
                author_name=row[2],
                author_when=row[3],
                commit_hash=row[4],
                line_no=row[5],
                line=row[6],
                path=row[7],
            )
            for row in blame_data
        ]


class GitPullRequest(Base):
    __tablename__ = "git_pull_requests"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
        comment="foreign key for public.repos.id",
    )
    number = Column(Integer, primary_key=True, comment="pull request number")
    title = Column(Text, comment="title of the pull request")
    body = Column(Text, comment="description/body of the pull request")
    state = Column(Text, comment="state of the pull request (open, closed, merged)")
    author_name = Column(Text, comment="username of the author")
    author_email = Column(Text, comment="email of the author (if available)")
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="timestamp when PR was created",
    )
    merged_at = Column(
        DateTime(timezone=True),
        comment="timestamp when PR was merged",
    )
    closed_at = Column(
        DateTime(timezone=True),
        comment="timestamp when PR was closed",
    )
    head_branch = Column(Text, comment="name of the head branch")
    base_branch = Column(Text, comment="name of the base branch")
    additions = Column(Integer, comment="total line additions in the PR")
    deletions = Column(Integer, comment="total line deletions in the PR")
    changed_files = Column(Integer, comment="total number of files changed in the PR")
    first_review_at = Column(
        DateTime(timezone=True), comment="timestamp of the first review"
    )
    first_comment_at = Column(
        DateTime(timezone=True), comment="timestamp of the first comment"
    )
    changes_requested_count = Column(
        Integer, default=0, comment="number of times changes were requested"
    )
    reviews_count = Column(Integer, default=0, comment="total number of reviews")
    comments_count = Column(Integer, default=0, comment="total number of comments")
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
        comment="timestamp when record was synced into the MergeStat database",
    )

    # Relationships
    repo = relationship("Repo", back_populates="git_pull_requests")
    reviews = relationship(
        "GitPullRequestReview",
        primaryjoin="and_(GitPullRequest.repo_id==GitPullRequestReview.repo_id, GitPullRequest.number==GitPullRequestReview.number)",
        foreign_keys="[GitPullRequestReview.repo_id, GitPullRequestReview.number]",
        back_populates="pr",
        cascade="all, delete-orphan",
    )


class GitPullRequestReview(Base):
    __tablename__ = "git_pull_request_reviews"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
    )
    number = Column(
        Integer,
        primary_key=True,
    )
    review_id = Column(
        Text,
        primary_key=True,
        comment="unique identifier for the review (e.g. GitHub review ID)",
    )
    reviewer = Column(Text, nullable=False, comment="identity of the reviewer")
    state = Column(
        Text,
        nullable=False,
        comment="state of the review (APPROVED, CHANGES_REQUESTED, etc.)",
    )
    submitted_at = Column(
        DateTime(timezone=True),
        nullable=False,
        comment="timestamp when review was submitted",
    )
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    # Relationships
    pr = relationship(
        "GitPullRequest",
        primaryjoin="and_(GitPullRequestReview.repo_id==GitPullRequest.repo_id, GitPullRequestReview.number==GitPullRequest.number)",
        foreign_keys="[GitPullRequestReview.repo_id, GitPullRequestReview.number]",
        back_populates="reviews",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["repo_id", "number"],
            ["git_pull_requests.repo_id", "git_pull_requests.number"],
            ondelete="CASCADE",
        ),
    )


class CiPipelineRun(Base):
    __tablename__ = "ci_pipeline_runs"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
    )
    run_id = Column(Text, primary_key=True)
    status = Column(Text)
    queued_at = Column(DateTime(timezone=True))
    started_at = Column(DateTime(timezone=True), nullable=False)
    finished_at = Column(DateTime(timezone=True))
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    repo = relationship("Repo", back_populates="ci_pipeline_runs")


class Deployment(Base):
    __tablename__ = "deployments"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
    )
    deployment_id = Column(Text, primary_key=True)
    status = Column(Text)
    environment = Column(Text)
    started_at = Column(DateTime(timezone=True))
    finished_at = Column(DateTime(timezone=True))
    deployed_at = Column(DateTime(timezone=True))
    merged_at = Column(DateTime(timezone=True))
    pull_request_number = Column(Integer)
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    repo = relationship("Repo", back_populates="deployments")


class Incident(Base):
    __tablename__ = "incidents"
    repo_id = Column(
        GUID,
        ForeignKey("repos.id", ondelete="CASCADE"),
        primary_key=True,
    )
    incident_id = Column(Text, primary_key=True)
    status = Column(Text)
    started_at = Column(DateTime(timezone=True), nullable=False)
    resolved_at = Column(DateTime(timezone=True))
    last_synced = Column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )

    repo = relationship("Repo", back_populates="incidents")
