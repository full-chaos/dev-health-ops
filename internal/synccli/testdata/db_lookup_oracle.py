"""Live-Python oracle for the two Postgres reads of `dho sync <target>` (CHAOS-6710).

A line protocol on stdin/stdout, one JSON object per line, so the Go test can
change the database between two answers:

  {"op": "seed", "orgs": [...], "creds": [...]}      -> {"ok": true}
  {"op": "resolve", "args": [...], "env": {...}}     -> the typed result

`seed` rewrites organizations and integration_credentials through the real ORM
models; every ciphertext is produced by the real core.encryption under the
case's own SETTINGS_ENCRYPTION_KEY / _SALT. `resolve` runs the REAL producers:
build_parser().parse_args, main()'s _resolve_org and first-organization lookup
(cli._resolve_first_org_id, against the real database), then
processors.sync._resolve_github_sync_credentials -- nothing replaced.

Every leaf is tagged {"t": type, "v": string}.
"""

import asyncio
import base64
import contextlib
import hashlib
import io
import json
import os
import sys
import uuid
from datetime import datetime

from cryptography.fernet import Fernet
from sqlalchemy import delete
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from dev_health_ops import cli as devhops_cli
from dev_health_ops.core import encryption
from dev_health_ops.models.settings import IntegrationCredential
from dev_health_ops.models.users import Organization
from dev_health_ops.processors import sync as sync_mod

MANAGED_ENV = [
    "CLICKHOUSE_URI",
    "POSTGRES_URI",
    "DATABASE_URI",
    "DATABASE_URL",
    "ORG_ID",
    "GITHUB_TOKEN",
    "GITHUB_APP_ID",
    "GITHUB_APP_PRIVATE_KEY_PATH",
    "GITHUB_APP_INSTALLATION_ID",
    "GITHUB_URL",
    "GITHUB_BASE_URL",
    "SETTINGS_ENCRYPTION_KEY",
    "SETTINGS_ENCRYPTION_SALT",
]


def tag(value):
    if value is None:
        return {"t": "null", "v": ""}
    if isinstance(value, bool):
        return {"t": "bool", "v": "true" if value else "false"}
    if isinstance(value, int):
        return {"t": "int", "v": str(value)}
    return {"t": "str", "v": str(value)}


def set_env(env):
    for name in MANAGED_ENV:
        os.environ.pop(name, None)
    for name, value in env.items():
        os.environ[name] = value


def encrypt(spec):
    """Real ciphertext for spec["payload"] under spec["env"] (v1 or legacy)."""
    set_env(spec.get("env") or {})
    if spec.get("legacy"):
        digest = hashlib.sha256(os.environ["SETTINGS_ENCRYPTION_KEY"].encode()).digest()
        return Fernet(base64.urlsafe_b64encode(digest)).encrypt(spec["payload"].encode()).decode()
    return encryption.encrypt_value(spec["payload"])


async def seed(db_url, request):
    engine = create_async_engine(db_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        await session.execute(delete(IntegrationCredential))
        await session.execute(delete(Organization))
        for org in (request.get("orgs") or []):
            session.add(
                Organization(
                    id=uuid.UUID(org["id"]),
                    slug=org["slug"],
                    name=org["slug"],
                    created_at=datetime.fromisoformat(org["created_at"]),
                )
            )
        for cred in (request.get("creds") or []):
            ciphertext = cred.get("raw")
            if cred.get("payload") is not None:
                ciphertext = encrypt(cred)
            session.add(
                IntegrationCredential(
                    org_id=cred["org_id"],
                    provider=cred.get("provider", "github"),
                    name=cred.get("name", "default"),
                    is_active=cred.get("active", True),
                    credentials_encrypted=ciphertext,
                    config={},
                )
            )
        await session.commit()
    await engine.dispose()


def resolve(request):
    set_env(request.get("env") or {})
    sink = io.StringIO()
    try:
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(sink):
            ns = devhops_cli.build_parser().parse_args(["sync", *request["args"]])
    except SystemExit as exc:
        return {"stage": tag("argparse"), "code": tag(exc.code)}
    devhops_cli._resolve_org(ns)
    if devhops_cli._should_resolve_org(ns):
        ns.org = devhops_cli._resolve_first_org_id(getattr(ns, "db", None))
    result = {"org": tag(ns.org)}
    try:
        with contextlib.redirect_stderr(sink), contextlib.redirect_stdout(sink):
            credentials = sync_mod._resolve_github_sync_credentials(ns)
    except SystemExit as exc:
        result.update(stage=tag("exit"), message=tag(exc.code))
        return result
    except Exception as exc:  # an uncaught traceback: exit 1
        result.update(stage=tag("error"), type=tag(type(exc).__name__))
        return result
    result.update(
        stage=tag("ok"),
        mode=tag("app" if credentials.is_app_auth else "pat"),
        name=tag(credentials.credential_name),
        # Truthiness is what every consumer reads, so "" and None are one value.
        token=tag(credentials.token or None),
        app_id=tag(credentials.app_id or None),
        private_key=tag(credentials.private_key or None),
        installation_id=tag(credentials.installation_id or None),
        base_url=tag(credentials.base_url or None),
    )
    return result


def main():
    db_url = sys.argv[1]
    for line in sys.stdin:
        request = json.loads(line)
        if request["op"] == "seed":
            asyncio.run(seed(db_url, request))
            answer = {"ok": True}
        else:
            answer = resolve(request)
        print(json.dumps(answer), flush=True)


main()
