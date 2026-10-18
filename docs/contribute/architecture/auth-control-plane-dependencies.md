---
page_id: con-auth-cp-dependencies
summary: The dependency and cryptography choices behind the Wave 1 Auth Control Plane - what the binaries link today, what the accepted ADRs select for later waves, the signing-key and secret rules, and how a dependency is added or replaced.
content_type: architecture
owner: engineering
source_of_truth:
  - go.mod
  - internal/auth/keystore/keystore.go
  - internal/auth/authconfig/config.go
  - ci/auth_govulncheck.py
applicability: current
lifecycle: active
---

# Auth Control Plane dependencies and cryptography

The selection record is [ACP-ADR-01](auth-control-plane-adrs.md) (Go dependency
selection) with its Wave 1 addendum (OAuth/OIDC, SAML, password hashing) and
[ACP-ADR-02](auth-control-plane-adrs.md) (signing-key custody). This page says
what that means in the tree today. The rule behind all of it: standard library
first, no hand-rolled cryptographic primitive, every security dependency
reviewed and pinned in `go.mod`.
{: .fc-page-lede }

## What the two binaries link today

The code the Auth Control Plane itself imports is the standard library,
`github.com/jackc/pgx/v5` (PostgreSQL) and OpenTelemetry. The **linked**
footprint is wider than that, because the binaries import shared platform
packages: `go list -deps ./cmd/auth-service ./cmd/auth-migrate` also shows the
Prometheus client, gRPC and protobuf (OTLP export), and the ClickHouse driver
(through `internal/storage/postgres`, which imports `internal/platform/config`).
None of these is used by the service's behaviour, but all are in the image and in
the reach of the vulnerability gate, which is why that gate scans the real
import closure (`go list -deps`) and not a hand list. Everything else in the auth
tree is library code that later waves compose:

| Module | Used by | Role |
| --- | --- | --- |
| `net/http` (`ServeMux`) | `internal/auth/httpapi` | Router with explicit method-and-path registration (ACP-ADR-01 item 3). No third-party router. |
| `github.com/jackc/pgx/v5` | `internal/auth/authstore`, `authschema`, `internal/audit` | PostgreSQL pool, readiness probe, migrations, the audit and outbox transaction. |
| `go.opentelemetry.io/otel` | `internal/audit`, the health surface | Outbox and request metrics. |
| `github.com/golang-jwt/jwt/v5` | `internal/auth/edgetoken` | Verification and minting of the existing HS256 access and refresh tokens, with parity to the Python issuer. |
| `github.com/google/jsonschema-go` | `internal/auth/contracts` | Draft 2020-12 validation of `contracts/auth/v1/`. |
| `golang.org/x/crypto/bcrypt` | `internal/auth/passwordhash` | The existing password hash format, byte for byte. |
| `github.com/valkey-io/valkey-go` | `internal/auth/ratelimitvalkey` | Shared rate-limit state. |

Versions live in `go.mod` and are not repeated here, because a copy would rot.
The OAuth/OIDC client (`golang.org/x/oauth2`, `github.com/coreos/go-oidc/v3`)
and SAML (`github.com/crewjam/saml`) libraries selected by the ADR-01 addendum
are in `go.mod` for the identity-brokerage waves; the Wave 1 binaries do not
link them.

## Cryptography rules

- **Asymmetric platform tokens are Ed25519/EdDSA**, not RS256 (ACP-ADR-01
  item 4). The Wave 1 keystore parses and proves a key but holds no minting
  material (`internal/auth/keystore` package comment), so the service issues no
  token.
- **The signing key is a file, never a value.** `AUTH_SIGNING_KEY_FILE` names a
  PKCS#8 `PRIVATE KEY` PEM. A direct `AUTH_SIGNING_KEY` environment variable is
  **rejected at load**, not ignored (ACP-ADR-02 section 4), because an operator
  who set it believes it is in effect. The file must be a regular file opened
  with `O_NOFOLLOW`, at most 8 KiB, with no group or world mode bit, hold exactly
  one PEM block, and parse as Ed25519. Each failure is a bounded reason code in
  the `signing_key` readiness check, never the path or the bytes.
- **Every key carries a `kid`** from day one (`AUTH_SIGNING_KEY_ID`, at most 64
  characters of letters, digits, `-`, `_`, `.`), so rotation overlap is
  representable before it is needed (ACP-ADR-02 section 5).
- **Platform token keys are separate from the ACR Web-assertion keys and from
  the go-api envelope keys.** The Compose profile uses its own volume and key id
  for exactly this reason; see [deployment](auth-control-plane-deployment.md).
- **Secrets never enter logs.** DSNs resolve through `internal/platform/secrets`,
  which registers the resolved value for redaction; configuration errors pass
  through the log redactor before they reach a stream.

## Adding or replacing a dependency

1. Record the choice in an ADR addendum (standard library first; why not).
2. Pin it in `go.mod`; the image build runs with `-mod=readonly`.
3. The [auth gate](auth-control-plane-go-gates.md) runs a pinned `govulncheck`
   over the auth packages with **no ignore list**: any reachable finding fails
   the gate, so a vulnerable dependency cannot merge.
4. State the update owner and the replacement plan in the ADR.

## Verified against

- `go.mod`: the modules named above are direct requirements.
- `go list -deps ./cmd/auth-service ./cmd/auth-migrate`: the direct imports and the wider linked footprint described above (run on the commit that merged this page).
- `internal/auth/authconfig/config.go` (`signingKey`, `validateKeyID`): the direct-value rejection and the `kid` rule.
- `internal/auth/keystore/keystore.go` (`MaxKeyFileBytes`, `parseEd25519Public`): size bound, single block, Ed25519 only.
