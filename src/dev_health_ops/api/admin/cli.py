from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dev_health_ops.db import resolve_db_uri

logger = logging.getLogger(__name__)


async def _get_session(ns: argparse.Namespace) -> AsyncSession:
    engine = create_async_engine(resolve_db_uri(ns), pool_pre_ping=True)
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

    users_parser = admin_sub.add_parser("users", help="User management.")
    users_sub = users_parser.add_subparsers(dest="users_command", required=True)

    users_create = users_sub.add_parser("create", help="Create a new user.")
    users_create.add_argument("--email", required=True, help="User email address.")
    users_create.add_argument(
        "--password", required=True, help="User password (min 8 chars)."
    )
    users_create.add_argument("--username", help="Optional username.")
    users_create.add_argument("--full-name", dest="full_name", help="User's full name.")
    users_create.add_argument(
        "--superuser", action="store_true", help="Grant superuser privileges."
    )
    users_create.set_defaults(func=create_user)

    users_list = users_sub.add_parser("list", help="List all users.")
    users_list.add_argument("--limit", type=int, default=100, help="Max users to list.")
    users_list.add_argument(
        "--include-inactive", action="store_true", help="Include inactive users."
    )
    users_list.set_defaults(func=list_users)

    orgs_parser = admin_sub.add_parser("orgs", help="Organization management.")
    orgs_sub = orgs_parser.add_subparsers(dest="orgs_command", required=True)

    orgs_create = orgs_sub.add_parser("create", help="Create a new organization.")
    orgs_create.add_argument("--name", required=True, help="Organization name.")
    orgs_create.add_argument(
        "--slug", help="URL-safe slug (auto-generated if omitted)."
    )
    orgs_create.add_argument("--description", help="Organization description.")
    orgs_create.add_argument(
        "--tier", default="free", help="Subscription tier (default: free)."
    )
    orgs_create.add_argument(
        "--owner-email", dest="owner_email", help="Email of the initial owner."
    )
    orgs_create.set_defaults(func=create_org)

    orgs_list = orgs_sub.add_parser("list", help="List all organizations.")
    orgs_list.add_argument("--limit", type=int, default=100, help="Max orgs to list.")
    orgs_list.add_argument(
        "--include-inactive",
        action="store_true",
        help="Include inactive organizations.",
    )
    orgs_list.set_defaults(func=list_orgs)
