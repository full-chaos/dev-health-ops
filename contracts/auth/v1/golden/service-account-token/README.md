# Signed service-account-token.v1 golden corpus

The cross-language contract for every service account token verifier: each row of
`manifest.json` names a vector, the verifier's fixed clock (`now`) and the
audience it is (`verify_as_audience`, the one service the verifier is), and the
decision it must reach, in `decision_order`.

- `jwks.json`: the public Ed25519 keys the vectors are verified against. Only
  public keys are committed. Every key is generated at corpus-build time and the
  private half is discarded; no key, token or secret is a literal in the tree.
- `vectors/<name>.json`: `header_json`, `payload_json` (raw text, so a repeated
  member is representable) and `signature_hex`. The signing input is
  `base64url(header_json) + "." + base64url(payload_json)`. No compact JWS
  string is stored.
- The token's `typ` is `sa+jwt`, exact bytes. A case variant, a missing typ,
  `JWT`, `jwt`, `at+jwt`, `workload+jwt` and an `application/`-prefixed form are
  refused.
- A service account token names exactly one audience, `dev-health-api` (the one
  audience whose classes include `service_account_token`); a token for any other
  audience is refused at the contract step, and a valid token presented to a
  verifier that is another service is refused with reason `audience`.
- It names the account (`sub`, a `prn_` id), the account's organization
  (`org_id`, a lowercase UUID) and the key row it came from (`cid`, a `crd_` id).
  It has no `sid`, no `scope`, no `audiences` and no `act`, and lives at most 300
  seconds (`exp - iat`). The exchange never lets it outlive the key
  (`ServiceAccountTokenLifetime`).
- `leeway_seconds` is 0: valid from `nbf` inclusive until `exp` exclusive.

Regenerate with fresh keys: `go run ./internal/auth/contracts/tokencorpus/gen service-account-token`.
Everything except the keys and the signatures is fixed. `serviceAccountCorpusDigest`
in `internal/auth/contracts/tokencorpus/service_account_test.go` pins a digest over
every file of this directory, so any change to it is red until the pin moves in
the same commit (the red cell prints the new value). A consumer's own pin
(dev-health-go) is a later change.

A row with `key_set: published_and_removed` is run with the removed public keys
(`removed_public_keys` in the manifest) added to the published set, as a stale
cache would hold them; the verifier must still refuse that kid.
