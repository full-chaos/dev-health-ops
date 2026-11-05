---
page_id: op-rb-auth-client-address-proof
summary: Turn on, check, rotate and roll back the signed client address that go-api sends to auth-service, and read the proof outcome metric.
content_type: runbook
owner: platform-operations
applicability: current
lifecycle: active
---

# Auth client-address proof

go-api forwards auth routes and session issues to auth-service with one `X-Forwarded-For` value: the client address
go-api trusts. With a signing key, go-api also sends `X-Forwarded-Client-Proof`, an HMAC-SHA256 over that address,
the method, the request target and go-api's `X-Request-Id`, with a key id and a time. auth-service can then name the
client from a valid proof instead of from the peer's pod address (`AUTH_SERVICE_TRUSTED_PROXIES`).

The chart does not render these variables yet; a separate chart change wires them. Until then the defaults hold:
no key, mode `cidr`, which is the trusted-proxy rule alone.

## Settings

| Service | Variable | Value |
|---|---|---|
| go-api | `AUTH_CLIENT_ADDR_SIGNING_KEY` | one `<kid>:<key>` entry. Unset: no proof is sent. |
| auth-service | `AUTH_CLIENT_ADDR_VERIFY_KEY` (or `_FILE`) | one or two `<kid>:<key>` entries, comma-separated. |
| auth-service | `AUTH_CLIENT_ADDR_MODE` | `cidr` (default), `both` or `signed`. |

- `<kid>` is 1 to 16 characters `[a-z0-9-]`. `<key>` is at least 32 random bytes, base64url without padding.
- Both services refuse to start on a value they cannot read. The error names the variable and the entry number, never a value.
- auth-service refuses to start in mode `both` or `signed` without a verify key.

## Modes

| Mode | Valid proof | No valid proof |
|---|---|---|
| `cidr` | the trusted-proxy rule (the proof is counted only) | the trusted-proxy rule |
| `both` | the proved address | the trusted-proxy rule |
| `signed` | the proved address | the socket peer, except the well-known CIDR exception |

**Well-known CIDR exception.** In mode `signed` the trusted-proxy list is read for these four requests only: `GET` and
`HEAD` of `/.well-known/jwks.json` and of `/.well-known/openid-configuration` (exact path, no escaped form). The
ingress controller serves them to the public and cannot sign. There the address keys only the public rate limit;
those routes write no audit row and no session. Every other request without a valid proof is named by its socket peer.

A proof is valid for 60 seconds after its time and 5 seconds before it. A proof is bound to one request: another
method, target, request id or address makes it fail.

## Readback

The counter `auth.client_addr.proofs` (labels `outcome`, `route`) counts every check while a verify key is set, in every
mode. `outcome` is one of `valid`, `absent`, `malformed`, `unknown_kid`, `bad_mac`, `stale`, `future`; `route` is
`well_known` or `other`. A failed check also logs `client address proof refused` with the outcome and route, at most
once a minute per outcome; the line holds no address, key id or MAC.

- After go-api has the signing key and auth-service has the verify key (mode `cidr`): every proxied request counts
  `outcome=valid,route=other`.
- Stop line: any outcome other than `valid` with `route=other` for more than 2 minutes after a roll. In mode `signed`
  such requests are keyed by go-api's pod address, so all users share one rate-limit bucket.

## Turn on

Each step is a roll of one service.

1. Create the Secret with the key entry from a stdin pipe; no value in any values file.
2. Roll auth-service with `AUTH_CLIENT_ADDR_VERIFY_KEY` and mode `cidr`.
3. Roll go-api with `AUTH_CLIENT_ADDR_SIGNING_KEY`. Check the readback.
4. Roll auth-service with mode `both`. Check the readback.
5. Roll auth-service with mode `signed`. Keep `AUTH_SERVICE_TRUSTED_PROXIES` for the well-known CIDR exception.

## Rotate

1. Add the new entry to `AUTH_CLIENT_ADDR_VERIFY_KEY` (two entries). Roll auth-service.
2. Set `AUTH_CLIENT_ADDR_SIGNING_KEY` to the new entry. Roll go-api. Check `outcome=unknown_kid` stays 0.
3. Remove the old entry from `AUTH_CLIENT_ADDR_VERIFY_KEY`. Roll auth-service.

## Roll back

Set the previous mode and roll auth-service: from `signed` to `both`, from `both` to `cidr`. Mode `cidr` ignores the
proof for the address. Removing the go-api signing key stops the proof; do that only after auth-service is in mode `cidr`.
