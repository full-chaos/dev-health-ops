"""User, Organization, and Membership CRUD services."""

from __future__ import annotations

import logging
import re
import secrets

import bcrypt
import uuid
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dev_health_ops.models.users import (
    AuthProvider,
    MemberRole,
    Membership,
    Organization,
    User,
)

logger = logging.getLogger(__name__)

PASSWORD_MIN_LENGTH = 8


def _hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except ValueError:
        return False


def _slugify(name: str) -> str:
    slug = name.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[-\s]+", "-", slug)
    return slug[:50]


class UserService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, user_id: str) -> User | None:
        stmt = select(User).where(User.id == uuid.UUID(user_id))
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_email(self, email: str) -> User | None:
        stmt = select(User).where(func.lower(User.email) == email.lower())
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_username(self, username: str) -> User | None:
        stmt = select(User).where(func.lower(User.username) == username.lower())
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_all(
        self, limit: int = 100, offset: int = 0, active_only: bool = True
    ) -> list[User]:
        stmt = select(User).order_by(User.created_at.desc()).limit(limit).offset(offset)
        if active_only:
            stmt = stmt.where(User.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def create(
        self,
        email: str,
        password: str | None = None,
        username: str | None = None,
        full_name: str | None = None,
        auth_provider: str = AuthProvider.LOCAL.value,
        auth_provider_id: str | None = None,
        is_verified: bool = False,
        is_superuser: bool = False,
    ) -> User:
        if await self.get_by_email(email):
            raise ValueError(f"User with email {email} already exists")

        if username and await self.get_by_username(username):
            raise ValueError(f"User with username {username} already exists")

        password_hash = None
        if password:
            if len(password) < PASSWORD_MIN_LENGTH:
                raise ValueError(
                    f"Password must be at least {PASSWORD_MIN_LENGTH} characters"
                )
            password_hash = _hash_password(password)

        user = User(
            id=uuid.uuid4(),
            email=email.lower().strip(),
            username=username.lower().strip() if username else None,
            password_hash=password_hash,
            full_name=full_name,
            auth_provider=auth_provider,
            auth_provider_id=auth_provider_id,
            is_active=True,
            is_verified=is_verified,
            is_superuser=is_superuser,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self.session.add(user)
        await self.session.flush()
        return user

    async def update(
        self,
        user_id: str,
        email: str | None = None,
        username: str | None = None,
        full_name: str | None = None,
        avatar_url: str | None = None,
        is_active: bool | None = None,
        is_verified: bool | None = None,
    ) -> User | None:
        user = await self.get_by_id(user_id)
        if not user:
            return None

        if email and email.lower() != user.email:
            existing = await self.get_by_email(email)
            if existing:
                raise ValueError(f"Email {email} already in use")
            user.email = email.lower().strip()

        if username is not None:
            if username and username.lower() != (user.username or "").lower():
                existing = await self.get_by_username(username)
                if existing:
                    raise ValueError(f"Username {username} already in use")
            user.username = username.lower().strip() if username else None

        if full_name is not None:
            user.full_name = full_name
        if avatar_url is not None:
            user.avatar_url = avatar_url
        if is_active is not None:
            user.is_active = is_active
        if is_verified is not None:
            user.is_verified = is_verified

        user.updated_at = datetime.now(timezone.utc)
        await self.session.flush()
        return user

    async def set_password(self, user_id: str, password: str) -> bool:
        user = await self.get_by_id(user_id)
        if not user:
            return False
        if len(password) < PASSWORD_MIN_LENGTH:
            raise ValueError(
                f"Password must be at least {PASSWORD_MIN_LENGTH} characters"
            )
        user.password_hash = _hash_password(password)
        user.updated_at = datetime.now(timezone.utc)
        await self.session.flush()
        return True

    async def verify_password(self, user_id: str, password: str) -> bool:
        user = await self.get_by_id(user_id)
        if not user or not user.password_hash:
            return False
        return _verify_password(password, user.password_hash)

    async def authenticate(self, email: str, password: str) -> User | None:
        user = await self.get_by_email(email)
        if not user or not user.is_active:
            return None
        if not user.password_hash or not _verify_password(password, user.password_hash):
            return None
        user.last_login_at = datetime.now(timezone.utc)
        await self.session.flush()
        return user

    async def delete(self, user_id: str) -> bool:
        user = await self.get_by_id(user_id)
        if not user:
            return False
        await self.session.delete(user)
        await self.session.flush()
        return True


class OrganizationService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_by_id(self, org_id: str) -> Organization | None:
        stmt = select(Organization).where(Organization.id == uuid.UUID(org_id))
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_slug(self, slug: str) -> Organization | None:
        stmt = select(Organization).where(func.lower(Organization.slug) == slug.lower())
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_all(
        self, limit: int = 100, offset: int = 0, active_only: bool = True
    ) -> list[Organization]:
        stmt = (
            select(Organization)
            .order_by(Organization.created_at.desc())
            .limit(limit)
            .offset(offset)
        )
        if active_only:
            stmt = stmt.where(Organization.is_active == True)  # noqa: E712
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list_for_user(self, user_id: str) -> list[Organization]:
        stmt = (
            select(Organization)
            .join(Membership, Membership.org_id == Organization.id)
            .where(Membership.user_id == uuid.UUID(user_id))
            .where(Organization.is_active == True)  # noqa: E712
            .order_by(Organization.name)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def create(
        self,
        name: str,
        slug: str | None = None,
        description: str | None = None,
        settings: dict[str, Any] | None = None,
        tier: str = "free",
        owner_user_id: str | None = None,
    ) -> Organization:
        slug = slug or _slugify(name)
        if await self.get_by_slug(slug):
            slug = f"{slug}-{secrets.token_hex(4)}"

        org = Organization(
            id=uuid.uuid4(),
            slug=slug,
            name=name,
            description=description,
            settings=settings or {},
            tier=tier,
            is_active=True,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self.session.add(org)
        await self.session.flush()

        if owner_user_id:
            membership_svc = MembershipService(self.session)
            await membership_svc.add_member(
                org_id=str(org.id),
                user_id=owner_user_id,
                role=MemberRole.OWNER.value,
            )

        return org

    async def update(
        self,
        org_id: str,
        name: str | None = None,
        description: str | None = None,
        settings: dict[str, Any] | None = None,
        tier: str | None = None,
        is_active: bool | None = None,
    ) -> Organization | None:
        org = await self.get_by_id(org_id)
        if not org:
            return None

        if name is not None:
            org.name = name
        if description is not None:
            org.description = description
        if settings is not None:
            org.settings = settings
        if tier is not None:
            org.tier = tier
        if is_active is not None:
            org.is_active = is_active

        org.updated_at = datetime.now(timezone.utc)
        await self.session.flush()
        return org

    async def delete(self, org_id: str) -> bool:
        org = await self.get_by_id(org_id)
        if not org:
            return False
        await self.session.delete(org)
        await self.session.flush()
        return True


class MembershipService:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_membership(self, org_id: str, user_id: str) -> Membership | None:
        stmt = select(Membership).where(
            Membership.org_id == uuid.UUID(org_id),
            Membership.user_id == uuid.UUID(user_id),
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_members(
        self, org_id: str, role: str | None = None
    ) -> list[Membership]:
        stmt = select(Membership).where(Membership.org_id == uuid.UUID(org_id))
        if role:
            stmt = stmt.where(Membership.role == role)
        stmt = stmt.order_by(Membership.created_at)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list_user_memberships(self, user_id: str) -> list[Membership]:
        stmt = (
            select(Membership)
            .where(Membership.user_id == uuid.UUID(user_id))
            .order_by(Membership.created_at)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def add_member(
        self,
        org_id: str,
        user_id: str,
        role: str = MemberRole.MEMBER.value,
        invited_by_id: str | None = None,
    ) -> Membership:
        existing = await self.get_membership(org_id, user_id)
        if existing:
            raise ValueError("User is already a member of this organization")

        membership = Membership(
            id=uuid.uuid4(),
            org_id=uuid.UUID(org_id),
            user_id=uuid.UUID(user_id),
            role=role,
            invited_by_id=uuid.UUID(invited_by_id) if invited_by_id else None,
            joined_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self.session.add(membership)
        await self.session.flush()
        return membership

    async def update_role(
        self, org_id: str, user_id: str, role: str
    ) -> Membership | None:
        membership = await self.get_membership(org_id, user_id)
        if not membership:
            return None

        if role not in [r.value for r in MemberRole]:
            raise ValueError(f"Invalid role: {role}")

        membership.role = role
        membership.updated_at = datetime.now(timezone.utc)
        await self.session.flush()
        return membership

    async def remove_member(self, org_id: str, user_id: str) -> bool:
        membership = await self.get_membership(org_id, user_id)
        if not membership:
            return False

        if membership.role == MemberRole.OWNER.value:
            owners = await self.list_members(org_id, role=MemberRole.OWNER.value)
            if len(owners) <= 1:
                raise ValueError("Cannot remove the last owner of an organization")

        await self.session.delete(membership)
        await self.session.flush()
        return True

    async def get_user_role(self, org_id: str, user_id: str) -> str | None:
        membership = await self.get_membership(org_id, user_id)
        return membership.role if membership else None

    async def is_admin(self, org_id: str, user_id: str) -> bool:
        role = await self.get_user_role(org_id, user_id)
        return role in (MemberRole.OWNER.value, MemberRole.ADMIN.value)

    async def transfer_ownership(
        self, org_id: str, from_user_id: str, to_user_id: str
    ) -> bool:
        from_membership = await self.get_membership(org_id, from_user_id)
        to_membership = await self.get_membership(org_id, to_user_id)

        if not from_membership or from_membership.role != MemberRole.OWNER.value:
            raise ValueError("Source user is not an owner")
        if not to_membership:
            raise ValueError("Target user is not a member")

        from_membership.role = MemberRole.ADMIN.value
        to_membership.role = MemberRole.OWNER.value
        from_membership.updated_at = datetime.now(timezone.utc)
        to_membership.updated_at = datetime.now(timezone.utc)
        await self.session.flush()
        return True
