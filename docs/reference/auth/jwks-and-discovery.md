---
page_id: ref-auth-jwks-discovery
summary: The two public documents of the token issuer (discovery and JWKS), which keys the JWKS lists, the cache rules of the server and of the Go SDK client, and what a verifier does on an unknown kid or an outage.
content_type: reference
owner: engineering
source_of_truth:
  - internal/auth/authapi/wellknown.go
  - internal/auth/contracts/discovery.go
  - internal/token/keyset.go
  - internal/token/states.go
applicability: current
lifecycle: active
---

# JWKS and discovery

auth-service publishes the public keys that verify its access tokens. A verifier reads two documents from the
issuer, caches the keys, and fetches again only under the rules below. The cache rules decide how fast a
key rotation reaches every verifier.
{: .fc-page-lede }

## The two documents

Both routes are public, answer `GET` and `HEAD`, and need no credential. They are under the issuer URL
(`AUTH_TOKEN_ISSUER_URL`).

| Path | Served | Content |
| --- | --- | --- |
| `/.well-known/jwks.json` | always | The key set (`jwks.v1`). |
| `/.well-known/openid-configuration` | only when `AUTH_TOKEN_ISSUER_URL` is set | The discovery document (`discovery.v1`). |

The discovery document has three members and no others:

```json
{
  "issuer": "https://ISSUER_URL",
  "jwks_uri": "https://ISSUER_URL/.well-known/jwks.json",
  "id_token_signing_alg_values_supported": ["EdDSA"]
}
```

`jwks_uri` always equals `issuer` plus `/.well-known/jwks.json`. A verifier checks that, and so a discovery
document cannot point a verifier to another key set.

Each key in the JWKS has the members `kty` `OKP`, `crv` `Ed25519`, `alg` `EdDSA`, `use` `sig`, `kid` and `x`
(the public key, base64url). The JWKS lists the keys in the states `pending`, `active` and `retiring`.
`retired` and `compromised` keys are never listed. So a key is listed from the moment it is registered until
it is retired, and it is never listed again after a compromise.

## What the server sends

- `Cache-Control: public, max-age=300` (5 minutes, the constant `token.JWKSCacheTTL`) and a strong `ETag`.
  A request with a matching `If-None-Match` gets `304`.
- When the key set cannot be read, the JWKS answers `503` with `Retry-After: 1`, a body with no key
  material, and no cache header.
- The rotation floors are built on the 5 minutes: a new key is listed at least 5 minutes before it signs, so
  a verifier that follows `max-age` has the key before the first token that needs it.

## What a verifier does

The Go SDK client (`authverify.JWKSClient`) is the client that go-api and query-api use, with its defaults.

| Setting | Default | Bounds | Meaning |
| --- | --- | --- | --- |
| `CacheTTL` | 5 minutes | at most 1 hour | Keys are served from memory for this time. |
| `RefreshFloor` | 30 seconds | at most `CacheTTL` | The least time between two fetch attempts, a failed attempt included. |
| `OutageGrace` | 10 minutes | at most 1 hour | How long after `CacheTTL` the last good keys stay usable when every refresh fails. |
| `Timeout` | 5 seconds | at most 30 seconds | One refresh (discovery and JWKS together). |
| `MaxBodyBytes` | 64 KiB | | Each response. A larger body fails the refresh. |
| `MaxKeys` | 32 | | A JWKS with more keys fails the refresh. |

The client works like this:

1. **Pinned issuer.** The client accepts one `https` issuer. It never follows a redirect.
2. **Discovery first.** A refresh fetches the discovery document, checks it (three members, same issuer,
   the fixed `jwks_uri`, exactly `EdDSA`), then fetches the JWKS. Any check that fails fails the refresh.
3. **Unknown kid.** A `kid` that is not in memory causes one refresh, shared by all callers at that moment,
   and at most one for each `RefreshFloor`. A `kid` that is still unknown after it is refused as
   `unknown_kid`.
4. **Expiry.** Past `CacheTTL` the client tries a refresh before it answers. If the refresh fails, it uses the
   last good keys until `CacheTTL` plus `OutageGrace`. After that no key is served.
5. **Outage.** When the last refresh failed and the `kid` is not in memory, the caller gets
   `ErrKeySourceUnavailable`. go-api and query-api answer `503` for it, not `401`. A `kid` that is in
   memory still verifies inside the grace time.

go-api and query-api keep no list of denied `kid` values. A key that leaves the JWKS stops verifying when
each verifier's memory of it ends, at most `CacheTTL` after its last good refresh, and a
compromised key also needs the sessions that hold its tokens revoked.

## Check the documents by hand

```sh
curl -sS https://ISSUER_URL/.well-known/openid-configuration
curl -sSi https://ISSUER_URL/.well-known/jwks.json
```

The second answer shows `Cache-Control` and `ETag`. List the `kid` values and compare them with the `kid`
of the token header (the first part of the token, base64url). Never paste a token into a ticket or a log.

## Verified against

- `internal/auth/authapi/wellknown.go`: `WellKnownRoutes`, `serveJWKS`, `serveDiscovery`, `writeCacheable`, `writeKeySetUnavailable`.
- `internal/auth/contracts/discovery.go`: `DiscoveryJWKSPath`, `ParseDiscovery`.
- `internal/token/keyset.go`: `KeySet.Published`. `internal/token/states.go`: `JWKSCacheTTL`.
- `github.com/full-chaos/dev-health-go` v0.37.0, `authverify/jwks_client.go`: `JWKSClientConfig`, `NewJWKSClient`, `PublicKey`, `ResolveKey`, `cached`, `refresh`, `discoveryValid`.
- `internal/apiservice/accessverify.go` and `internal/queryapi/server/access_verify.go`: both build the client with the default settings.
