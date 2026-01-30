from __future__ import annotations

import argparse
import asyncio
import logging
import os

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


def _get_db_url(ns: argparse.Namespace) -> str:
    dsn = (
        getattr(ns, "db", None)
        or os.getenv("DATABASE_URI")
        or os.getenv("DATABASE_URL")
    )
    if not dsn:
        raise RuntimeError("DATABASE_URI or DATABASE_URL must be set (or use --db)")
    if dsn.startswith("postgresql://"):
        dsn = dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    return dsn


async def _get_session(ns: argparse.Namespace) -> AsyncSession:
    engine = create_async_engine(_get_db_url(ns), pool_pre_ping=True)
    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    return async_session()


async def _create_user_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import UserService

    session = await _get_session(ns)
    try:
        svc = UserService(session)
        user = await svc.create(
            email=ns.email,
            password=ns.password,
            username=ns.username,
            full_name=ns.full_name,
            is_superuser=ns.superuser,
            is_verified=True,
        )
        await session.commit()
        print(f"Created user: {user.email} (id: {user.id})")
        if ns.superuser:
            print("  [superuser]")
        return 0
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def create_user(ns: argparse.Namespace) -> int:
    return asyncio.run(_create_user_async(ns))


async def _create_org_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import OrganizationService, UserService

    session = await _get_session(ns)
    try:
        org_svc = OrganizationService(session)

        owner_user_id = None
        if ns.owner_email:
            user_svc = UserService(session)
            owner = await user_svc.get_by_email(ns.owner_email)
            if not owner:
                print(f"Error: User with email {ns.owner_email} not found")
                return 1
            owner_user_id = str(owner.id)

        org = await org_svc.create(
            name=ns.name,
            slug=ns.slug,
            description=ns.description,
            tier=ns.tier,
            owner_user_id=owner_user_id,
        )
        await session.commit()
        print(f"Created organization: {org.name} (slug: {org.slug}, id: {org.id})")
        if owner_user_id:
            print(f"  Owner: {ns.owner_email}")
        return 0
    except ValueError as e:
        print(f"Error: {e}")
        return 1
    finally:
        await session.close()


def create_org(ns: argparse.Namespace) -> int:
    return asyncio.run(_create_org_async(ns))


async def _list_users_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import UserService

    session = await _get_session(ns)
    try:
        svc = UserService(session)
        users = await svc.list_all(limit=ns.limit, active_only=not ns.include_inactive)
        if not users:
            print("No users found.")
            return 0
        print(
            f"{'ID':<40} {'Email':<30} {'Username':<20} {'Superuser':<10} {'Active':<8}"
        )
        print("-" * 108)
        for u in users:
            print(
                f"{str(u.id):<40} {u.email:<30} {(u.username or ''):<20} "
                f"{'Yes' if u.is_superuser else 'No':<10} {'Yes' if u.is_active else 'No':<8}"
            )
        return 0
    finally:
        await session.close()


def list_users(ns: argparse.Namespace) -> int:
    return asyncio.run(_list_users_async(ns))


async def _list_orgs_async(ns: argparse.Namespace) -> int:
    from dev_health_ops.api.services.users import OrganizationService

    session = await _get_session(ns)
    try:
        svc = OrganizationService(session)
        orgs = await svc.list_all(limit=ns.limit, active_only=not ns.include_inactive)
        if not orgs:
            print("No organizations found.")
            return 0
        print(f"{'ID':<40} {'Slug':<20} {'Name':<30} {'Tier':<10} {'Active':<8}")
        print("-" * 108)
        for o in orgs:
            print(
                f"{str(o.id):<40} {o.slug:<20} {o.name:<30} "
                f"{o.tier:<10} {'Yes' if o.is_active else 'No':<8}"
            )
        return 0
    finally:
        await session.close()


def list_orgs(ns: argparse.Namespace) -> int:
    return asyncio.run(_list_orgs_async(ns))


def register_commands(subparsers: argparse._SubParsersAction) -> None:
    admin_parser = subparsers.add_parser(
        "admin", help="User and organization management."
    )
    admin_sub = admin_parser.add_subparsers(dest="admin_command", required=True)

    create_user_parser = admin_sub.add_parser("create-user", help="Create a new user.")
    create_user_parser.add_argument("--db", help="Database URI.")
    create_user_parser.add_argument(
        "--email", required=True, help="User email address."
    )
    create_user_parser.add_argument(
        "--password", required=True, help="User password (min 8 chars)."
    )
    create_user_parser.add_argument("--username", help="Optional username.")
    create_user_parser.add_argument(
        "--full-name", dest="full_name", help="User's full name."
    )
    create_user_parser.add_argument(
        "--superuser", action="store_true", help="Grant superuser privileges."
    )
    create_user_parser.set_defaults(func=create_user)

    create_org_parser = admin_sub.add_parser(
        "create-org", help="Create a new organization."
    )
    create_org_parser.add_argument("--db", help="Database URI.")
    create_org_parser.add_argument("--name", required=True, help="Organization name.")
    create_org_parser.add_argument(
        "--slug", help="URL-safe slug (auto-generated if omitted)."
    )
    create_org_parser.add_argument("--description", help="Organization description.")
    create_org_parser.add_argument(
        "--tier", default="free", help="Subscription tier (default: free)."
    )
    create_org_parser.add_argument(
        "--owner-email", dest="owner_email", help="Email of the initial owner."
    )
    create_org_parser.set_defaults(func=create_org)

    list_users_parser = admin_sub.add_parser("list-users", help="List all users.")
    list_users_parser.add_argument("--db", help="Database URI.")
    list_users_parser.add_argument(
        "--limit", type=int, default=100, help="Max users to list."
    )
    list_users_parser.add_argument(
        "--include-inactive", action="store_true", help="Include inactive users."
    )
    list_users_parser.set_defaults(func=list_users)

    list_orgs_parser = admin_sub.add_parser("list-orgs", help="List all organizations.")
    list_orgs_parser.add_argument("--db", help="Database URI.")
    list_orgs_parser.add_argument(
        "--limit", type=int, default=100, help="Max orgs to list."
    )
    list_orgs_parser.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive organizations.",
    )
    list_orgs_parser.set_defaults(func=list_orgs)
