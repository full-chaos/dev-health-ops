---
page_id: ref-auth-principal-envelope
summary: The signed effective-principal envelope that query-api verifies - claim schema, verification rules, signing-key management and key-file custody.
content_type: reference
owner: engineering
source_of_truth:
  - internal/queryapi/principal
  - internal/signingkey
applicability: current
lifecycle: active
---

# Effective-principal envelope reference

The envelope class is retired: no `query-api` route accepts it, and a validly signed envelope gets the answer of an unaccepted bearer. This page describes the claim schema, the verifier and the signing keys that remain until their removal.

`query-api` does not re-derive auth state from Postgres or Valkey. The edge issues a short-lived, audience-bound, signed envelope that carries the effective principal for one request. Disabled-user and token-version revocation, org-switch membership, active impersonation and tier fallback are all part of it. It is not a bare JWT signature check.
{: .fc-page-lede }

## Verification

`internal/queryapi/principal` (`Verifier`, `Claims`) verifies the envelope. The verifier looks up the `kid` in a JWKS and allows only the `EdDSA` algorithm. It checks `iss`, `aud`, `exp` (required) and `v`.

The verifier rejects:

- a wrong audience or issuer;
- an expired envelope, or one with no `exp`;
- an unknown `kid`;
- a signature from a key that is not in the JWKS;
- an `alg` other than `EdDSA` (algorithm confusion);
- a `v` that the verifier does not handle (`ErrUnsupportedSchemaVersion`).

## Claim schema

The `v` claim changes whenever a claim is added, removed, or changes meaning. A verifier must reject an envelope whose `v` it does not handle.

**v1** (current):

| Claim | Type | Meaning |
|---|---|---|
| `v` | int | Claim schema version (`1`) |
| `sub` | string | User id |
| `org_id` | string | Active org for this request |
| `role` | string | User's role in `org_id` |
| `is_superuser` | bool | Platform superuser flag |
| `is_superuser_verified` | bool | Superuser bit re-verified against the database for this request |
| `permissions` | string[] | Full resolved permission set. During impersonation this is the permission set of the target role, not of the real user. |
| `token_version` | int | The token-version value that passed the revocation check for this request |
| `tier` | string | Resolved license tier: the org license tier if set, else the organization tier, else community |
| `licensed_features` | string[] | Feature keys the org currently has access to |
| `impersonated_by` | string \| null | Real user id, when an admin or superuser is impersonating |
| `impersonation_active` | bool | Whether impersonation is active for this request |
| `iss` | string | `dev-health-ops-edge` (overridable by environment) |
| `aud` | string | `query-api` (overridable per verifier) |
| `iat` / `exp` | int (unix) | Issued-at and expiry. The default TTL is 60 seconds. |
| `jti` | string | Unique per envelope |

There is no `disabled` claim. A disabled or deactivated user is treated as unauthenticated, so no envelope is issued for that user. The absence of a valid envelope is the disabled signal.

## Key management

The envelope uses EdDSA (Ed25519), an asymmetric algorithm. The signing key is separate from the user-facing access-token HS256 secret (`JWT_SECRET_KEY`). The envelope crosses a process and language boundary, so verification uses a JWKS. `query-api` holds only the public key. It cannot forge a user session token.

The key pair is generated as two files by `dho mint envelope-keys` (a PKCS#8 Ed25519 PEM and the public JWKS). No Go tool mints the envelope any more. The JWKS verifier accepts Ed25519 keys only.

## Signing key custody

One contract applies to signing-key files across the platform (the web `readPrivateKey`, `internal/auth/keystore` and `internal/signingkey`). The file must:

- open with `O_NOFOLLOW`;
- be a regular file of 1 byte up to a size bound (16 KiB here);
- have no group or other permission bits (0600 or stricter);
- parse as Ed25519, or the load fails.

Supply the key as `GO_API_ENVELOPE_PRIVATE_KEY_FILE` (a path). The Go key loader reads the file only; a raw `GO_API_ENVELOPE_PRIVATE_KEY` value is not read by it. Mounted Secret files need `defaultMode: 0400`.
