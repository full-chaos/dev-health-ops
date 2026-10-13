# acr entitlement route contract

`GET /api/v1/internal/acr/entitlements/{org_id}` is served on the internal listener (`internal/apiservice/acr`). The acr entitlement client and `acr-projector purge` read it.

| Answer | Meaning |
| --- | --- |
| 200 `acr_entitlement.v1` body | The organization exists. |
| 404 `{"detail":"Not found","code":"acr_organization_unknown"}` | `org_id` is a well-formed UUID and no `organizations` row exists (deleted or never created). |
| 404 `{"detail":"Not found"}` (no `code`) | `org_id` is not a UUID. Says nothing about deletion. |
| 503 `{"detail":"Service unavailable"}` | The store cannot be read (database outage, no store configured). |

Rules for callers:

- Only the 404 with `code == "acr_organization_unknown"` may be read as "organization gone". Every other 404 (malformed id, unrouted path, proxy) and every 5xx or timeout is "unavailable".
- The code is stable. Changing it is a breaking contract change for acr.
- A new answer must never reuse the code for a store failure.

Code: `UnknownOrganizationCode` in `internal/apiservice/acr/handlers.go`; sentinel `ErrOrgUnknown` in `store.go`.
