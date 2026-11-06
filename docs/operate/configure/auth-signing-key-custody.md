---
page_id: op-cfg-auth-key-custody
summary: Keep the auth-service signing key in custody, set the issuer and the access-token class for auth-service, go-api and query-api, and check that the key and the issuer work before a flip.
content_type: task-guide
owner: platform-operations
source_of_truth:
  - internal/auth/keystore/keystore.go
  - internal/auth/authconfig/config.go
  - internal/auth/authconfig/options.go
  - internal/apiservice/accessverify.go
  - internal/queryapi/server/access_verify.go
  - deploy/helm/dev-health/values.yaml
applicability: current
lifecycle: active
---

# Signing-key custody and environment

This guide covers the Ed25519 key that auth-service signs access tokens with, and the settings that make
go-api and query-api verify those tokens. Every setting has one place in the
[configuration reference](../../reference/auth/configuration.md). This page gives the order and the checks.
It names no value, no path of a deployment and no Secret.

## Rules

- The private key is a file. A direct value in `AUTH_SIGNING_KEY` is refused at start. No variable, argument
  or log carries key material.
- Only auth-service holds the private key. go-api, query-api and every other verifier read the public keys
  from the [JWKS](../../reference/auth/jwks-and-discovery.md).
- One key file holds one key. A rotated key is a new file with a new `kid`, never a second block in the old file.
- Never paste a key, a token or a Secret value into a command, a ticket or a log.

## The key file

auth-service checks the key file on every readiness request. The file must pass all of these:

1. It is a regular file, opened without following a symbolic link.
2. It is at most 8 KiB.
3. Its mode has no group bit and no world bit (`0600` or tighter).
4. It has exactly one PEM block of the type `PRIVATE KEY`, and nothing after it.
5. The block is an Ed25519 key in PKCS#8 form.

A fault of the file does not stop the process. The process starts, `/healthz` stays 200, and `/readyz` answers
503 with the reason: `key_file_unreadable`, `key_file_not_regular`, `key_file_mode_too_permissive`,
`key_file_too_large`, `key_file_not_pem`, `key_file_ambiguous_pem`, `key_file_unparseable` or `key_file_wrong_algorithm`.
Under the eddsa class every route that mints a token checks the key first and answers 503 if it fails.
The readiness check also signs a probe with the active key and verifies it with the published public key.

On Kubernetes a Secret volume cannot meet the file rules directly. The chart stages a copy at mode `0400` in a
memory-backed volume with an init container, and only that copy reaches the auth-service container.

## The key set

For one key, set `authService.signingKey.secretName` and `authService.signingKey.keyId` (the `kid`: letters,
digits, `-`, `_`, `.`, at most 64). For a set of keys, set `authService.keySet.enabled` and list each key:

```yaml
authService:
  keySet:
    enabled: true
    keys:
      - kid: KID_CURRENT
        state: active
        file: KID_CURRENT.pem
        secretName: SECRET_NAME_CURRENT
        secretKey: signing-key.pem
```

`keySet` replaces `signingKey`. The chart renders only `kid`, `state` and `file` into a ConfigMap, and it never
carries key material. Exactly one key is `active`. A `file` is a bare name. The pod holds a static copy, so a
change to the set is a new rollout. The states and the moves between them are in the key rotation runbooks: [planned rotation](../runbooks/auth-signing-key-planned-rotation.md) and [emergency rotation and rollback](../runbooks/auth-signing-key-emergency-rotation-and-rollback.md).

## Settings by service

| Service | Setting | Rule |
| --- | --- | --- |
| auth-service | `AUTH_ACCESS_TOKEN_CLASS` | `eddsa` (default); the retired `hs256_compat` is a start error. Chart key `authService.accessToken.class`, needs `authService.phase` 2 or more. |
| auth-service | `AUTH_TOKEN_ISSUER_URL` | The issuer, and the `iss` of every token. Chart key `authService.accessToken.issuerURL`. Required by `eddsa`. It mounts the discovery document. |
| auth-service | `AUTH_JWT_AUDIENCE` | `dev-health-api` for the access token. `acr-api` and `acr-mcp` are refused. |
| auth-service | `AUTH_LEGACY_JWT_AUDIENCE` | The audience that go-api's HS256 token is checked with under `eddsa`. Default `dev-health-api`. |
| auth-service | `AUTH_ACCESS_TOKEN_LIFETIME` | Whole minutes, 5 to 60. Default 60. Needs `eddsa`. |
| go-api, query-api | `AUTH_ACCESS_TOKEN_ISSUER` | The same URL, byte for byte. The chart renders it only under the `eddsa` class. Unset: neither service verifies an EdDSA token. |

Rules for the issuer URL:

- It is `https`, with a lowercase host, no trailing slash, no user information, no query and no fragment.
- It must be the same string in `AUTH_TOKEN_ISSUER_URL` and `AUTH_ACCESS_TOKEN_ISSUER`. A token is refused when
  its `iss` differs by one byte.
- go-api and query-api read the JWKS from it over `https`. Their pods must resolve it, reach it and trust its
  certificate.

## Check before the flip

1. `/readyz` of every auth-service pod answers 200, and the log has no custody reason.
2. `GET /.well-known/openid-configuration` on the issuer answers the `issuer` you set. `GET /.well-known/jwks.json`
   lists the `kid` of the active key. See [JWKS and discovery](../../reference/auth/jwks-and-discovery.md#check-the-documents-by-hand).
3. From the go-api pod network, the same two requests answer 200 with the certificate trusted.
4. The counter `devhealth_auth_access_token_refused_total` for go-api and query-api has no reason `unavailable`.
5. The preconditions of the eddsa class are met. See
   [Auth sessions flip and rollback](../runbooks/auth-sessions-flip-and-rollback.md#access-token-class-eddsa-w3-d1)
   and [legacy compatibility and rollback](../runbooks/auth-legacy-compat-and-rollback.md).
