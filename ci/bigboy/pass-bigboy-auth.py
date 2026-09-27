#!/usr/bin/env python3
"""Bigboy pre-prod venue AUTH pass (gwc-web-ingress, CHAOS-6963, child of CHAOS-6259): the 22 D2679(b)
session/registration/orgs-me-write/telemetry-write rows that cannot go in internal/goapiproof/restcorpus.go
(non-idempotent, session-stateful -- login mutates login_attempts, refresh rotates a token family, register
creates a user, etc). Same shape as pass-bigboy-admin2.sh (CHAOS-6688): a disposable identity on the Fixture
Org, writes on ONE plane at a time with a same-plane and cross-plane read-back, restore to the pre-run state,
ABORTS up front if a throwaway email this script owns already exists.

Runs on the HOST (not inside a container): dev-health-api-1 has no docker CLI/socket, so SQL against
dev-health-postgres-1 and HTTP against dev-health-api-1 are each their own `docker exec`, driven from here.
HTTP is still network-direct to each plane's service from INSIDE the api container (one `docker exec ...
python3 -c` per request) -- same "bypasses ingress, proves the candidate origin" property pass-bigboy-admin2.sh
has, just via per-call docker exec instead of one long-lived in-container process, because the SQL this pass
needs (creating/reading throwaway users and tokens) can only run from dev-health-postgres-1.

Covered (Part A -- read-only, once a session exists from a Part-B login/accept-invite):
  GET /api/v1/auth/me, /api/v1/auth/me/organizations, /api/v1/auth/onboarding/state, GET /api/v1/auth/verify
  with a garbage token (deterministic 400/404, no side effect) -- restEndpointSpecs also carries this shape
  now for the prod STEP; this script's copies are a live cross-check, not a substitute.

Covered (Part B -- writes, each plane run separately with its OWN throwaway user to keep the shared, IP-keyed
rate limiters -- login 20/15m, refresh 10/15m, validate 30/15m -- from one plane's failures poisoning the
other's budget; this script's total call count per limiter stays under half of each):
  session lifecycle:      login -> me -> me/organizations -> switch-org -> validate -> refresh (+ same-token
                           replay inside the 30s grace, expect the SAME successor jti) -> logout
  registration lifecycle: register -> read the email_verification_tokens row, rebuild the raw token with the
                           SAME HMAC the app signs it with (internal/auth/signedtoken.Build /
                           services/email_verification.py:_build_token -- both derive it from JWT_SECRET_KEY,
                           so no email capture is needed) -> verify -> resend-verification (already verified,
                           deterministic response) -> forgot-password -> read password_reset_tokens, rebuild
                           -> reset-password
  invite lifecycle:       one throwaway invitee PER PLANE (accept-invite needs an EXISTING authenticated
                           caller -- session.go wraps it in policy.Authenticated, get_current_user on the
                           Python side -- so the invitee logs in first): login -> create an invite through
                           the ALREADY-LIVE go-api admin route (POST /api/v1/admin/orgs/{org}/invites,
                           bigboy-admin-proof.token) -> read org_invites, rebuild its token the same way ->
                           accept-invite (bearer + token) -> onboarding/state -> onboarding/skip-integration
                           -> accept-invite again with the SAME now-consumed token (single-use guard, must
                           answer the same error every time)
  org + telemetry writes: PATCH orgs/me (a probe description, restored after read-back on both planes) ->
                           telemetry opt-in -> telemetry/status read-back -> telemetry/report -> opt-out ->
                           telemetry/status read-back -> product-telemetry/events (public, no auth)

Cross-plane acceptance (one extra throwaway user, run once, not per-plane): login on plane A, GET /me and
POST /auth/validate the SAME token on plane B -- the token-signing secret is shared, so a token minted by
either plane's login must be accepted by the other (TestSessionVenueOracle already proves this in-process;
this is the live confirmation).

NOT covered here, by design (same "expected gap, not hidden" spirit as pass-bigboy-admin2.sh's llm-settings
rows): social-login (needs a live OAuth provider or the venue's fake-provider env, neither present on bigboy),
the failed-login lockout ladder (5x401/429, would burn a large slice of the shared login budget for a property
TestSessionVenueOracle already pins), SSO/SAML/OIDC (CHAOS-6261, separate ticket, no Go route exists yet).

ABORTS before any write if a throwaway email this run would create already exists on either plane (state not
clean -- a previous run did not finish its teardown). Every created row is torn down in a `finally` regardless
of how the run ends: throwaway users (cascades memberships/refresh_tokens/*_tokens via FK ondelete=CASCADE),
throwaway invites, and the Fixture Org's name/description restored to what they were before this run touched
them.

HANDED TO prod-ops to run at the rev 193 pre-cut; the author does not run it against bigboy (same convention
as pass-bigboy-admin2.sh). Output: <out>/table.txt (one row per request, admin2's own table shape).

First live run (prod-ops, post rev192, ba222c858/ops 5ecfcf84) found and this version fixes two harness bugs,
neither a product defect -- every throwaway user's password_hash/membership rows were inserted correctly and
the harness itself did not crash (rc=0, clean teardown both times):
- Every email used @venue.invalid (RFC 2606 reserved), and unlike bootstrap-admin-proof.sh's identities
  (minted directly via AuthService, no request validation ever runs) this script's users go through the REAL
  POST /api/v1/auth/login and /register bodies, whose `email: EmailStr` field refuses reserved/special-use
  domains by design (internal/testsupport/sessionscenario/sessionscenario.go:522 pins exactly this refusal
  for another special-use domain). No token was ever minted, so every downstream row failed the same way.
  Fixed: @example.com, the domain the app's own real venue oracle already uses for hundreds of real
  login/register calls.
- POST /api/v1/auth/register is behind OriginValidationMiddleware
  (src/dev_health_ops/api/middleware/csrf.py, default `protected_paths={"/api/v1/auth/register"}` -- no
  other route here needs it) and this script's `http()` had no Origin option at all, unlike
  pass-bigboy-admin2.sh's own `origin=` flag. Fixed: an `origin` parameter on `http()`, sent only on the
  register calls, matching `_parse_cors_origins`'s documented default (`http://localhost:3000`, unset
  CORS_ALLOWED_ORIGINS on bigboy) -- the SAME value admin2.sh's ORIGIN constant already uses for its own
  origin-aware reads.

Usage: pass-bigboy-auth.py [out_dir]
"""

from __future__ import annotations

import hashlib
import hmac
import json
import subprocess
import sys
import uuid
from pathlib import Path

API_CONTAINER = "dev-health-api-1"
PG_CONTAINER = "dev-health-postgres-1"
ADMIN_TOKEN_FILE = "/home/ubuntu/devhealth/.go-api-dev/bigboy-admin-proof.token"
ORG = "67f1add8-9fcb-4272-addb-044b70c442c8"
PLANES = ("go", "python")
# OriginValidationMiddleware (src/dev_health_ops/api/middleware/csrf.py) protects
# POST /api/v1/auth/register by default; CORS_ALLOWED_ORIGINS is unset on bigboy, so
# _parse_cors_origins' documented default applies -- same value pass-bigboy-admin2.sh's
# own ORIGIN constant already uses.
REGISTER_ORIGIN = "http://localhost:3000"

RUN = uuid.uuid4().hex[:8]


def sql(query: str) -> str:
    p = subprocess.run(
        [
            "docker",
            "exec",
            "-i",
            PG_CONTAINER,
            "psql",
            "-U",
            "devhealth",
            "-d",
            "devhealth",
            "-X",
            "-q",
            "-v",
            "ON_ERROR_STOP=1",
            "-t",
            "-A",
        ],
        input=query,
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"psql failed: {p.stderr}\nSQL: {query}")
    return p.stdout.strip()


_HTTP_SNIPPET = r"""
import sys, json, http.client
req = json.load(sys.stdin)
PLANES = {"go": ("go-api", 8000), "python": ("localhost", 8000)}
host, port = PLANES[req["plane"]]
h = {}
if req.get("bearer"):
    h["Authorization"] = "Bearer " + req["bearer"]
if req.get("origin"):
    h["Origin"] = req["origin"]
body = req.get("body")
if body is not None:
    h["Content-Type"] = "application/json"
c = http.client.HTTPConnection(host, port, timeout=25)
try:
    c.request(req["method"], req["path"], body=body, headers=h)
    r = c.getresponse(); data = r.read()
    hdr = {k.lower(): v for k, v in r.getheaders()}
    try:
        js = json.loads(data)
    except Exception:
        js = None
    print(json.dumps({"status": r.status, "body": data.decode("utf-8", "replace"), "json": js, "headers": hdr}))
except Exception as e:
    print(json.dumps({"status": 0, "body": "", "json": None, "headers": {}, "error": type(e).__name__}))
"""


def http(
    plane: str,
    method: str,
    path: str,
    body=None,
    bearer: str | None = None,
    origin: str | None = None,
) -> dict:
    req = {
        "plane": plane,
        "method": method,
        "path": path,
        "bearer": bearer,
        "origin": origin,
    }
    if body is not None:
        req["body"] = json.dumps(body)
    p = subprocess.run(
        ["docker", "exec", "-i", API_CONTAINER, "python3", "-c", _HTTP_SNIPPET],
        input=json.dumps(req),
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(
            f"http call failed rc={p.returncode} stderr={p.stderr} req={req}"
        )
    return json.loads(p.stdout.strip().splitlines()[-1])


def bcrypt_hash(password: str) -> str:
    code = "import bcrypt,sys,json; print(bcrypt.hashpw(json.load(sys.stdin)['pw'].encode(), bcrypt.gensalt(12)).decode())"
    p = subprocess.run(
        ["docker", "exec", "-i", API_CONTAINER, "python3", "-c", code],
        input=json.dumps({"pw": password}),
        capture_output=True,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"bcrypt_hash failed: {p.stderr}")
    return p.stdout.strip()


def rebuild_token(secret: str, token_id: str) -> str:
    """internal/auth/signedtoken.Build / services/email_verification.py:_build_token --
    id.hex + '.' + hmac_sha256(secret, id.hex).hexdigest(). Same construction for
    email_verification_tokens, password_reset_tokens and org_invites (all three call the identical
    shape, just against different tables)."""
    tid = token_id.replace("-", "")
    sig = hmac.new(secret.encode(), bytes.fromhex(tid), hashlib.sha256).hexdigest()
    return f"{token_id}.{sig}"


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "pass-auth")
    out.mkdir(parents=True, exist_ok=True)
    admin_token = Path(ADMIN_TOKEN_FILE).read_text().strip()

    # JWT_SECRET_KEY lives in the api container's env, not Postgres -- read it the same way the app does.
    p = subprocess.run(
        ["docker", "exec", API_CONTAINER, "sh", "-c", 'echo "$JWT_SECRET_KEY"'],
        capture_output=True,
        text=True,
        check=True,
    )
    secret = p.stdout.strip()
    if not secret:
        p = subprocess.run(
            [
                "docker",
                "exec",
                API_CONTAINER,
                "sh",
                "-c",
                'echo "$SETTINGS_ENCRYPTION_KEY"',
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        secret = p.stdout.strip() or "dev-key-not-for-prod"

    created_user_ids: list[str] = []
    created_invite_emails: list[str] = []
    created_org_ids: list[
        str
    ] = []  # /onboard create_org makes a NEW org, not the Fixture Org -- cascade
    # on user delete does not remove it, so it needs its own cleanup row.
    org_backup: tuple[str, str | None] | None = None
    rows: list[tuple[str, str, int]] = []  # (label, plane, status)

    def record(label: str, plane: str, resp: dict) -> dict:
        (out / (f"{label}.{plane}.status.txt")).write_text(str(resp.get("status")))
        (out / (f"{label}.{plane}.body.json")).write_text(
            json.dumps(resp.get("json"), indent=1)
            if resp.get("json") is not None
            else resp.get("body", "")
        )
        rows.append((label, plane, resp.get("status", 0)))
        return resp

    def cleanup() -> None:
        for org_id in created_org_ids:
            sql(f"DELETE FROM organizations WHERE id = '{org_id}';")
        for uid in created_user_ids:
            sql(f"DELETE FROM users WHERE id = '{uid}';")
        for email in created_invite_emails:
            sql(f"DELETE FROM org_invites WHERE email = '{email}';")
        if org_backup is not None:
            name, desc = org_backup
            name_sql = "'{}'".format(name.replace("'", "''"))
            desc_sql = (
                "NULL" if desc is None else "'{}'".format(desc.replace("'", "''"))
            )
            sql(
                f"UPDATE organizations SET name = {name_sql}, description = {desc_sql} WHERE id = '{ORG}';"
            )

    all_emails = [
        f"venue-auth-session-go-{RUN}@example.com",
        f"venue-auth-session-python-{RUN}@example.com",
        f"venue-auth-register-go-{RUN}@example.com",
        f"venue-auth-register-python-{RUN}@example.com",
        f"venue-auth-invite-go-{RUN}@example.com",
        f"venue-auth-invite-python-{RUN}@example.com",
        f"venue-auth-orgtel-{RUN}@example.com",
        f"venue-auth-crossplane-{RUN}@example.com",
    ]
    existing = sql(
        "SELECT email FROM users WHERE email IN ({});".format(
            ",".join(f"'{e}'" for e in all_emails)
        )
    )
    if existing.strip():
        print(f"ABORT: throwaway email(s) already present: {existing}")
        return 3

    try:
        # ============= Part B: session lifecycle, one throwaway user per plane =============
        for plane in PLANES:
            email = f"venue-auth-session-{plane}-{RUN}@example.com"
            pw = f"VenueProbe-{RUN}-1!"
            h = bcrypt_hash(pw)
            uid = sql(f"""INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser)
                         VALUES (gen_random_uuid(), '{email}', '{h}', true, true, false) RETURNING id;""")
            created_user_ids.append(uid)
            sql(
                f"INSERT INTO memberships (id, user_id, org_id, role) VALUES (gen_random_uuid(), '{uid}', '{ORG}', 'member');"
            )

            r = record(
                "s-login",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/login",
                    {"email": email, "password": pw, "org_id": ORG},
                ),
            )
            js = r.get("json") or {}
            access, refresh = js.get("access_token"), js.get("refresh_token")

            record("s-me", plane, http(plane, "GET", "/api/v1/auth/me", bearer=access))
            record(
                "s-me-orgs",
                plane,
                http(plane, "GET", "/api/v1/auth/me/organizations", bearer=access),
            )
            r = record(
                "s-switch-org",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/switch-org",
                    {"org_id": ORG},
                    bearer=access,
                ),
            )
            switched = (r.get("json") or {}).get("access_token") or access

            record(
                "s-validate",
                plane,
                http(plane, "POST", "/api/v1/auth/validate", {"token": switched}),
            )

            r = record(
                "s-refresh1",
                plane,
                http(plane, "POST", "/api/v1/auth/refresh", {"refresh_token": refresh}),
            )
            successor1 = (r.get("json") or {}).get("refresh_token")
            # replay the ORIGINAL refresh token inside the 30s grace window: must answer the SAME successor
            r = record(
                "s-refresh1-replay",
                plane,
                http(plane, "POST", "/api/v1/auth/refresh", {"refresh_token": refresh}),
            )
            successor2 = (r.get("json") or {}).get("refresh_token")
            print(
                f"REFRESH_GRACE plane={plane} same_successor={successor1 == successor2 and successor1 is not None}"
            )

            record(
                "s-logout",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/logout",
                    {"refresh_token": successor1 or refresh},
                ),
            )

        # ============= Part B: registration lifecycle, one throwaway user per plane =============
        for plane in PLANES:
            email = f"venue-auth-register-{plane}-{RUN}@example.com"
            pw = f"VenueProbe-{RUN}-2!"
            r = record(
                "r-register",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/register",
                    {"email": email, "password": pw, "full_name": "Venue Probe"},
                    origin=REGISTER_ORIGIN,
                ),
            )
            new_user_id = (r.get("json") or {}).get("user_id")
            if new_user_id:
                created_user_ids.append(new_user_id)

            row = (
                sql(
                    f"SELECT id FROM email_verification_tokens WHERE user_id = '{new_user_id}' ORDER BY created_at DESC LIMIT 1;"
                )
                if new_user_id
                else ""
            )
            if row:
                tok = rebuild_token(secret, row)
                record(
                    "r-verify",
                    plane,
                    http(plane, "GET", f"/api/v1/auth/verify?token={tok}"),
                )
            else:
                print(
                    f"r-verify plane={plane} SKIPPED no email_verification_tokens row"
                )

            record(
                "r-resend-verification-already-verified",
                plane,
                http(
                    plane, "POST", "/api/v1/auth/resend-verification", {"email": email}
                ),
            )

            record(
                "r-forgot-password",
                plane,
                http(plane, "POST", "/api/v1/auth/forgot-password", {"email": email}),
            )
            row = (
                sql(
                    f"SELECT id FROM password_reset_tokens WHERE user_id = '{new_user_id}' ORDER BY created_at DESC LIMIT 1;"
                )
                if new_user_id
                else ""
            )
            current_pw = pw
            if row:
                tok = rebuild_token(secret, row)
                current_pw = f"VenueProbe-{RUN}-3!"
                record(
                    "r-reset-password",
                    plane,
                    http(
                        plane,
                        "POST",
                        "/api/v1/auth/reset-password",
                        {"token": tok, "new_password": current_pw},
                    ),
                )
            else:
                print(
                    f"r-reset-password plane={plane} SKIPPED no password_reset_tokens row"
                )

            # onboard: this user registered with no org_name, so it holds no membership yet -- login
            # (register's own response carries no bearer) then create_org through /onboard.
            if new_user_id:
                r = record(
                    "r-onboard-login",
                    plane,
                    http(
                        plane,
                        "POST",
                        "/api/v1/auth/login",
                        {"email": email, "password": current_pw},
                    ),
                )
                onboard_bearer = (r.get("json") or {}).get("access_token")
                r = record(
                    "r-onboard",
                    plane,
                    http(
                        plane,
                        "POST",
                        "/api/v1/auth/onboard",
                        {
                            "action": "create_org",
                            "org_name": f"Venue Probe Org {RUN} {plane}",
                        },
                        bearer=onboard_bearer,
                    ),
                )
                new_org_id = (r.get("json") or {}).get("org_id")
                if new_org_id:
                    created_org_ids.append(new_org_id)

        # ============= Part B: invite + onboarding lifecycle, one throwaway invite PER PLANE (accept-invite
        #               needs an EXISTING authenticated caller -- session.go wraps it in policy.Authenticated,
        #               get_current_user on the Python side -- an invite is single-use, so each plane needs
        #               its own invite/invitee or only the first plane ever sees the happy path) =============
        for plane in PLANES:
            invite_email = f"venue-auth-invite-{plane}-{RUN}@example.com"
            created_invite_emails.append(invite_email)
            pw = f"VenueProbe-{RUN}-6!"
            h = bcrypt_hash(pw)
            uid = sql(f"""INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser)
                         VALUES (gen_random_uuid(), '{invite_email}', '{h}', true, true, false) RETURNING id;""")
            created_user_ids.append(uid)
            r = record(
                "i-invitee-login",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/login",
                    {"email": invite_email, "password": pw},
                ),
            )
            invitee_bearer = (r.get("json") or {}).get("access_token")

            record(
                "i-create-invite-admin",
                "go",
                http(
                    "go",
                    "POST",
                    f"/api/v1/admin/orgs/{ORG}/invites",
                    {"email": invite_email, "role": "member"},
                    bearer=admin_token,
                ),
            )
            row = sql(
                f"SELECT id FROM org_invites WHERE email = '{invite_email}' ORDER BY created_at DESC LIMIT 1;"
            )
            if not row:
                print(f"i-accept-invite plane={plane} SKIPPED no org_invites row")
                continue
            tok = rebuild_token(secret, row)
            r = record(
                "i-accept-invite",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/accept-invite",
                    {"token": tok},
                    bearer=invitee_bearer,
                ),
            )
            if r.get("status") == 200:
                access = (r.get("json") or {}).get("access_token")
                record(
                    "i-onboarding-state",
                    plane,
                    http(plane, "GET", "/api/v1/auth/onboarding/state", bearer=access),
                )
                record(
                    "i-onboarding-skip",
                    plane,
                    http(
                        plane,
                        "POST",
                        "/api/v1/auth/onboarding/skip-integration",
                        bearer=access,
                    ),
                )
            # replay on the SAME plane: the invite is now consumed, so this must answer the SAME
            # "already accepted"-shaped error every time -- a live check of the single-use guard.
            record(
                "i-accept-invite-replay",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/accept-invite",
                    {"token": tok},
                    bearer=invitee_bearer,
                ),
            )

        # ============= Part B: orgs/me PATCH + telemetry writes, one throwaway session per plane (the
        #               read side of both orgs/me and telemetry/status is already live at rev 192) =============
        NULLSENTINEL = "\x01NULL\x01"
        raw = sql(
            f"SELECT name, COALESCE(description, '{NULLSENTINEL}') FROM organizations WHERE id = '{ORG}';"
        )
        name_part, desc_part = raw.split("|", 1)
        org_backup = (name_part, None if desc_part == NULLSENTINEL else desc_part)

        email = f"venue-auth-orgtel-{RUN}@example.com"
        pw = f"VenueProbe-{RUN}-4!"
        h = bcrypt_hash(pw)
        uid = sql(f"""INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser)
                     VALUES (gen_random_uuid(), '{email}', '{h}', true, true, false) RETURNING id;""")
        created_user_ids.append(uid)
        sql(
            f"INSERT INTO memberships (id, user_id, org_id, role) VALUES (gen_random_uuid(), '{uid}', '{ORG}', 'admin');"
        )

        for plane in PLANES:
            r = record(
                "t-login",
                plane,
                http(
                    plane,
                    "POST",
                    "/api/v1/auth/login",
                    {"email": email, "password": pw, "org_id": ORG},
                ),
            )
            access = (r.get("json") or {}).get("access_token")

            record(
                "o-patch-me",
                plane,
                http(
                    plane,
                    "PATCH",
                    "/api/v1/orgs/me",
                    {"description": f"venue probe {RUN} {plane}"},
                    bearer=access,
                ),
            )
            for rb in PLANES:
                record(
                    f"o-patch-me-rb-{rb}",
                    plane,
                    http(rb, "GET", "/api/v1/orgs/me", bearer=access),
                )
            # restore immediately -- do not leave the shared Fixture Org mutated between plane iterations
            http(
                plane,
                "PATCH",
                "/api/v1/orgs/me",
                {"description": org_backup[1]},
                bearer=access,
            )

            record(
                "tel-opt-in",
                plane,
                http(plane, "POST", "/api/v1/telemetry/opt-in", bearer=access),
            )
            record(
                "tel-status-after-opt-in",
                plane,
                http(plane, "GET", "/api/v1/telemetry/status", bearer=access),
            )
            record(
                "tel-report",
                plane,
                http(plane, "POST", "/api/v1/telemetry/report", bearer=access),
            )
            record(
                "tel-opt-out",
                plane,
                http(plane, "POST", "/api/v1/telemetry/opt-out", bearer=access),
            )
            record(
                "tel-status-after-opt-out",
                plane,
                http(plane, "GET", "/api/v1/telemetry/status", bearer=access),
            )

            evt = {
                "name": "page_viewed",
                "schemaVersion": "1",
                "eventId": str(uuid.uuid4()),
                "ts": "2026-01-01T00:00:00Z",
                "sessionId": str(uuid.uuid4()),
                "anonymousUserId": str(uuid.uuid4()),
                "payload": {},
            }
            record(
                "pt-events",
                plane,
                http(
                    plane, "POST", "/api/v1/product-telemetry/events", {"events": [evt]}
                ),
            )

        # ============= cross-plane acceptance: one throwaway user, login on ONE plane, use the token on the
        #               OTHER =============
        email = f"venue-auth-crossplane-{RUN}@example.com"
        pw = f"VenueProbe-{RUN}-5!"
        h = bcrypt_hash(pw)
        uid = sql(f"""INSERT INTO users (id, email, password_hash, is_active, is_verified, is_superuser)
                     VALUES (gen_random_uuid(), '{email}', '{h}', true, true, false) RETURNING id;""")
        created_user_ids.append(uid)
        sql(
            f"INSERT INTO memberships (id, user_id, org_id, role) VALUES (gen_random_uuid(), '{uid}', '{ORG}', 'member');"
        )
        r = record(
            "x-login-go",
            "go",
            http(
                "go",
                "POST",
                "/api/v1/auth/login",
                {"email": email, "password": pw, "org_id": ORG},
            ),
        )
        cross_plane_token = (r.get("json") or {}).get("access_token")
        record(
            "x-me-on-python",
            "python",
            http("python", "GET", "/api/v1/auth/me", bearer=cross_plane_token),
        )
        record(
            "x-validate-on-python",
            "python",
            http(
                "python", "POST", "/api/v1/auth/validate", {"token": cross_plane_token}
            ),
        )

        # ============= Part A: garbage-token verify, both planes (no side effect, deterministic) =============
        for plane in PLANES:
            garbage = uuid.uuid4().hex + "." + "0" * 64
            record(
                "a-verify-garbage",
                plane,
                http(plane, "GET", f"/api/v1/auth/verify?token={garbage}"),
            )

        print(f"PART_B_DONE run={RUN}")
        rc = 0
    finally:
        cleanup()
        print(
            f"CLEANUP_DONE users={len(created_user_ids)} "
            f"invites={len(created_invite_emails)} org_restored={org_backup is not None}"
        )

    table = out / "table.txt"
    with table.open("w") as f:
        f.write("label | plane | status\n")
        for label, plane, status in rows:
            f.write(f"{label} | {plane} | {status}\n")
    print(table.read_text())
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
