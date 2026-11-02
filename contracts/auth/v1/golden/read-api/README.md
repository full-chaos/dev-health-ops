# Versioned read API golden corpus

The cross-language contract for the versioned reads of principals, organizations,
memberships and grants (CHAOS-10995, contracts only: no route serves them yet).
Each row of `manifest.json` names a vector of one of three kinds:

- `request`: one request body, the schema's verdict (`request_schema`) and the
  parser's (`parse`).
- `exchange`: one valid request and the response a client parser decides on, the
  schema's verdict (`response_schema`) and the parser's (`parse`). A row whose
  schema verdict is `accept` and whose parse verdict is `reject` names, in
  `rule`, a rule only the parser can say: another id than the one asked for, an
  item of another organization or grantee, a filter that was not honoured, a page
  over the limit, out of order or not after the request cursor, a next cursor that
  is not the last item's. `response_raw` holds a response as text where the raw
  token matters (a decimal-form revision).
- `error`: one error body and the HTTP status it arrived on.

Rules of the contract the rows pin:

- Page size: `limit` 1 to 100, default 50. A limit over 100 is refused, never
  clamped.
- Cursor: keyset on the item id. `next_cursor` is `k1_` and the unpadded
  base64url of the last id of the page; absent on the last page, never null or
  empty, and never on a page with no items. Items are in ascending id order.
- A principal read is `principal_id`, `kind`, `status` and `revision`. It has no
  email, display name, password hash or identity, and the object is closed.
- A grant read carries the repository `resource_key`; an error body never does,
  and a parser refusal never repeats it (a refusal is a surface, a stage and a
  rule).
- A read holds a read scope (`principals:read`, `organizations:read`,
  `memberships:read`, `grants:read`); a write scope never satisfies one.

`readAPICorpusDigest` in `internal/auth/contracts/read_api_test.go` pins a digest
over every file of this directory: any change is red until the pin moves in the
same commit (the red cell prints the new value).
