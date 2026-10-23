# Signed workload-token.v1 golden corpus

The cross-language contract for every workload-token verifier: each row of
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
- The token's `typ` is `workload+jwt`, exact bytes. A case variant, a missing
  typ, `JWT`, `jwt`, `at+jwt` and an `application/`-prefixed form are refused.
- A workload token names exactly one audience, has no `org_id`, no `sid` and no
  `scope`, and lives at most 300 seconds (`exp - iat`).
- `leeway_seconds` is 0: valid from `nbf` inclusive until `exp` exclusive.

Regenerate with fresh keys: `go run ./internal/auth/contracts/tokencorpus/gen workload-token`.
Everything except the keys and the signatures is fixed.

A row with `key_set: published_and_removed` is run with the removed public keys
(`removed_public_keys` in the manifest) added to the published set, as a stale
cache would hold them; the verifier must still refuse that kid.
