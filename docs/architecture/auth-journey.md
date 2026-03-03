# Auth User Journeys

Backend authentication and authorization flows for the Dev Health platform. Each journey documents the API endpoint behavior, database operations, and response shapes.

All auth endpoints live under `/api/v1/auth/` in `src/dev_health_ops/api/auth/router.py`.

## Journey 1: Registration

A new user registers with email and password. The backend creates the user, an organization, and a membership in a single transaction, then sends a verification email asynchronously.

```mermaid
sequenceDiagram
    participant C as Client
    participant R as POST /register
    participant V as Password Validator
    participant DB as PostgreSQL
    participant E as Email Service

    C->>R: RegisterRequest {email, password, full_name?, org_name?}
    R->>V: validate_password(password)
    alt password violations
        V-->>R: violations list
        R-->>C: 422 {violations}
    end
    R->>DB: SELECT user WHERE email = normalized
    alt email exists
        R-->>C: 400 "Email already registered"
    end
    R->>DB: INSERT User (is_verified=false, auth_provider="local")
    R->>DB: INSERT Organization (tier="community")
    R->>DB: INSERT Membership (role="owner")
    R->>DB: create_email_verification_token
    R->>DB: COMMIT
    R->>E: send_verification_email (async, non-blocking)
    R-->>C: 201 RegisterResponse {message, user_id, org_id}
```

**Rate limit:** `AUTH_REGISTER_LIMIT` (3/hour per IP).

**Key detail:** Registration auto-creates org + membership, so newly registered users do NOT need onboarding (`needs_onboarding=false` on login).

## Journey 2: Email Verification

User clicks the verification link from their email. The backend validates the token and marks the user as verified.

```mermaid
sequenceDiagram
    participant C as Client
    participant V as GET /verify
    participant DB as PostgreSQL

    C->>V: ?token=xxx
    V->>DB: verify_email_token(token)
    alt token invalid or expired
        V-->>C: 400 "Invalid or expired verification token"
    end
    V->>DB: SET is_verified=true
    V->>DB: COMMIT
    V-->>C: 200 VerifyEmailResponse {message, verified: true}
```

**Rate limit:** 10/hour per IP.

**Resend flow:** `POST /resend-verification` accepts `{email}`, creates a new token, and resends. Returns a generic message regardless of whether the account exists (prevents enumeration). Rate limited to 3/hour.

## Journey 3: Login (Happy Path — Verified User)

User submits credentials. Backend validates password, checks verification status, resolves membership, and returns tokens.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as POST /login
    participant DB as PostgreSQL
    participant A as AuthService

    C->>L: LoginRequest {email, password, org_id?}
    L->>DB: check_lockout(email)
    L->>DB: SELECT User WHERE email = normalized
    L->>L: bcrypt.checkpw(password, hash)
    Note over L: Constant-time comparison<br/>using DUMMY_PASSWORD_HASH<br/>even for missing users
    L->>DB: clear_attempts(email)
    L->>L: Check is_verified == true
    L->>DB: SELECT Membership WHERE user_id
    L->>DB: UPDATE last_login_at
    L->>DB: emit_audit_log(LOGIN)
    L->>DB: COMMIT
    L->>A: create_token_pair(user_id, email, org_id, role)
    L->>DB: INSERT refresh_token record
    L-->>C: 200 LoginResponse {access_token, refresh_token, needs_onboarding, user}
```

**Rate limits:**
- `AUTH_LOGIN_IP_LIMIT` per IP
- `AUTH_LOGIN_LIMIT` per auth key

**`needs_onboarding`:** `true` only when user has no memberships and is not superuser. Since registration auto-creates a membership, this is typically `false` for self-registered users.

## Journey 4: Login (Unverified Email)

User has valid credentials but has not verified their email address.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as POST /login
    participant DB as PostgreSQL

    C->>L: LoginRequest {email, password}
    L->>DB: check_lockout(email)
    L->>DB: SELECT User WHERE email = normalized
    L->>L: bcrypt.checkpw — password matches
    L->>DB: clear_attempts(email)
    L->>L: Check auth_provider == "local" AND is_verified == false
    L->>DB: emit_audit_log(LOGIN_FAILED, "email not verified")
    L->>DB: COMMIT
    L-->>C: 200 EmailVerificationRequiredResponse {status, email, message}
```

**Important:** This returns HTTP 200 (not 401) with `status: "email_verification_required"`. The frontend detects this response shape and shows an amber verification banner instead of an error toast.

## Journey 5: Login (Invalid Credentials)

Password does not match, user does not exist, or account is disabled.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as POST /login
    participant DB as PostgreSQL

    C->>L: LoginRequest {email, password}
    L->>DB: check_lockout(email)
    alt account locked
        L-->>C: 429 {message, retry_after_seconds}
    end
    L->>DB: SELECT User WHERE email = normalized
    L->>L: bcrypt.checkpw(password, hash_or_dummy)
    L->>DB: record_failed_attempt(email)
    L->>DB: emit_audit_log(LOGIN_FAILED)
    L-->>C: 401 "Invalid credentials"
```

**Failure reasons (all return 401 with same message):**
- User not found
- Account disabled (`is_active=false`)
- No password hash (OAuth-only account)
- Password mismatch

**Account lockout:** After repeated failures, `check_lockout` returns `true` and the endpoint returns 429 with `retry_after_seconds`.

## Journey 6: Onboarding

For users who authenticated but have no organization membership (e.g., invited users who haven't accepted yet). Supports two actions: `create_org` or `join_org`.

```mermaid
sequenceDiagram
    participant C as Client
    participant O as POST /onboard
    participant DB as PostgreSQL
    participant A as AuthService

    C->>O: OnboardRequest {action, org_name?, invite_code?}
    O->>DB: SELECT User WHERE id = jwt.sub
    O->>DB: SELECT Membership WHERE user_id
    alt already has membership
        O-->>C: 400 "Already onboarded"
    end
    alt action == "create_org"
        O->>DB: INSERT Organization
        O->>DB: INSERT Membership (role="owner")
        O->>DB: emit_audit_log(CREATE, ORGANIZATION)
    else action == "join_org"
        O->>DB: validate_org_invite(invite_code)
        O->>DB: accept_org_invite — INSERT Membership
        O->>DB: emit_audit_log(MEMBER_JOINED)
    end
    O->>DB: COMMIT
    O->>A: create_token_pair (new tokens with org_id)
    O-->>C: 200 OnboardResponse {tokens, org_id, org_name, role}
```

**Requires authentication:** JWT bearer token in `Authorization` header.

## Journey 7: Password Reset

Two-step flow: request reset email, then submit new password with token.

```mermaid
flowchart TD
    A[Client] -->|POST /forgot-password| B[Backend]
    B --> C{User exists?}
    C -->|No| D[Return generic message]
    C -->|Yes| E[Create reset token]
    E --> F[Send reset email]
    F --> D
    D --> G[Client receives 200]

    H[Client] -->|POST /reset-password| I[Backend]
    I --> J{Token valid?}
    J -->|No| K[400 Invalid or expired]
    J -->|Yes| L[Reset password]
    L --> M[200 Password reset successful]
```

**Anti-enumeration:** `POST /forgot-password` always returns the same generic message regardless of whether the account exists.

**Rate limit:** 3/hour for forgot-password.

## Journey 8: Invite Accept

Authenticated user accepts an organization invite. Creates membership and returns new tokens scoped to the organization.

```mermaid
sequenceDiagram
    participant C as Client
    participant AI as POST /accept-invite
    participant DB as PostgreSQL
    participant A as AuthService

    C->>AI: AcceptInviteRequest {token} + Bearer JWT
    AI->>DB: SELECT User WHERE id = jwt.sub
    AI->>DB: validate_org_invite(token)
    alt invite invalid
        AI-->>C: 400 "Invalid or expired invite"
    end
    AI->>DB: SELECT Organization WHERE id = invite.org_id
    AI->>DB: accept_org_invite — INSERT Membership
    AI->>DB: emit_audit_log(MEMBER_JOINED)
    AI->>DB: COMMIT
    AI->>A: create_token_pair (scoped to new org)
    AI-->>C: 200 AcceptInviteResponse {tokens, org_id, org_name, role}
```

**Requires authentication:** JWT bearer token in `Authorization` header.

## Journey 9: Token Refresh

Client exchanges a refresh token for a new access token. Implements token rotation with reuse detection.

```mermaid
sequenceDiagram
    participant C as Client
    participant R as POST /refresh
    participant DB as PostgreSQL
    participant A as AuthService

    C->>R: TokenRefreshRequest {refresh_token}
    R->>A: validate_token(refresh_token, type="refresh")
    alt token invalid
        R-->>C: 401 "Invalid or expired refresh token"
    end
    R->>DB: find_by_hash(jti)
    alt token revoked (reuse detected)
        R->>DB: revoke_family(family_id)
        R-->>C: 401 "Refresh token reuse detected"
    end
    R->>DB: SELECT User WHERE id = sub
    R->>A: create_refresh_token (same family_id)
    R->>DB: rotate_token(old_jti, new_jti)
    R->>A: create_access_token
    R->>DB: emit_audit_log(LOGIN, "Access token refreshed")
    R-->>C: 200 TokenRefreshResponse {access_token, refresh_token, user}
```

**Security:** Refresh tokens are single-use. If a revoked token is reused, the entire token family is revoked (reuse detection).

**Rate limit:** `AUTH_REFRESH_LIMIT`.

## Journey 10: Logout

Client submits refresh token for revocation.

```mermaid
sequenceDiagram
    participant C as Client
    participant L as POST /logout
    participant DB as PostgreSQL

    C->>L: LogoutRequest {refresh_token} + Bearer JWT (optional)
    L->>L: validate refresh_token
    alt valid refresh token
        L->>DB: revoke_token(jti)
    end
    alt authenticated user
        L->>DB: emit_audit_log(LOGOUT)
        L->>DB: COMMIT
    end
    L-->>C: 200 {message: "Logout successful"}
```

**Note:** The bearer JWT is optional — logout still revokes the refresh token even without it.

## Endpoint Reference

| Endpoint | Method | Auth | Rate Limit | Response |
|----------|--------|------|------------|----------|
| `/register` | POST | None | 3/hour | `RegisterResponse` (201) |
| `/verify` | GET | None | 10/hour | `VerifyEmailResponse` |
| `/resend-verification` | POST | None | 3/hour | `VerifyEmailResponse` |
| `/login` | POST | None | Per IP + key | `LoginResponse` or `EmailVerificationRequiredResponse` |
| `/forgot-password` | POST | None | 3/hour | `VerifyEmailResponse` |
| `/reset-password` | POST | None | None | `VerifyEmailResponse` |
| `/onboard` | POST | Bearer | None | `OnboardResponse` |
| `/accept-invite` | POST | Bearer | None | `AcceptInviteResponse` |
| `/refresh` | POST | None | Per limit | `TokenRefreshResponse` |
| `/validate` | POST | None | Per limit | `TokenValidateResponse` |
| `/me` | GET | Bearer | None | `MeResponse` |
| `/logout` | POST | Optional | None | `{message}` |

## Security Notes

- **Constant-time password comparison:** Even for nonexistent users, bcrypt compares against `DUMMY_PASSWORD_HASH` to prevent timing attacks.
- **Account lockout:** Failed login attempts are tracked per email. After threshold, returns 429 with retry delay.
- **Token rotation:** Refresh tokens are single-use with family-based reuse detection.
- **Anti-enumeration:** Forgot-password and resend-verification return generic messages regardless of account existence.
- **Audit logging:** All auth events (login, logout, registration, failures) are recorded with IP and user-agent.
