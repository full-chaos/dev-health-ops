# Signed access-token.v1 golden corpus

The cross-language contract for every access-token verifier: each row of
`manifest.json` names a vector, the verifier's fixed clock (`now`) and audience,
and the decision it must reach, in `decision_order`.

- `jwks.json`: the public Ed25519 keys the vectors are verified against. Only
  public keys are committed.
- `vectors/<name>.json`: `header_json`, `payload_json` (raw text, so a repeated
  member is representable) and `signature_hex`. The signing input is
  `base64url(header_json) + "." + base64url(payload_json)`. No compact JWS
  string is stored.
- `leeway_seconds` is 0: valid from `nbf` inclusive until `exp` exclusive.

Regenerate with fresh keys: `go run ./internal/auth/contracts/tokencorpus/gen`.
Everything except the keys and the signatures is fixed.
