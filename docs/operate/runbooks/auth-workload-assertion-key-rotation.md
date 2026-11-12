---
page_id: op-rb-auth-workload-assertion-key-rotation
summary: Rotate the web assertion signing key with auth-migrate workload-assertion-key-set and workload-assertion-key-list, in the order that keeps every assertion accepted, name the audit event, and recover from a refused step or a lost key.
content_type: runbook
owner: platform-operations
source_of_truth:
  - cmd/auth-migrate/assertionkey.go
  - internal/auth/workload/assertion_keys.go
applicability: current
lifecycle: active
---

# Web assertion key rotation

The web service signs an assertion (`X-ACR-Web-Assertion`, EdDSA) for every call to ACR. The auth-service registry records each
public signing key of the `web` workload as public metadata: the issuer URL, the key id (`kid`), a status and two times.
The operator changes the records with `auth-migrate workload-assertion-key-set` and reads them with
`auth-migrate workload-assertion-key-list`. The registry holds no key, no key file and no digest. The verbs take none.
{: .fc-page-lede }

Two places must agree. The registry row (this runbook) is the record. The ACR verifier reads the public keys from its own JWKS file
(`ACR_WEB_ASSERTION_JWKS_FILE`), and ACR stops a key at the `retire_after` time that the JWKS entry carries
(the acr verifier, function `keyInTimeBound`). auth-service reads the same file from the same Secret
(`AUTH_AUTHORIZE_WEB_APPROVER_JWKS_FILE`) to verify the web approver reference and applies the same `retire_after` rule, so
each step below changes both readers at once. The verbs never reach the JWKS file. Change the JWKS file in the same
steps, in the order below. The web side of the same rotation (the signer config) is in the web repository,
the auth endpoint profiles guide, section "Rotating the web assertion key".

## Rules

- Run both verbs as the migration role (`AUTH_MIGRATION_DATABASE_URI`). The runtime role is refused.
- `workload-assertion-key-set` writes, so it needs `-confirm`. `workload-assertion-key-list` only reads.
- A key has one status: `active`, `retiring` or `retired`. A workload has at most one `active` key.
- A new active key while another key is active needs `-retire-previous-after` (more than 0, at most 24 h). The old key becomes
  `retiring` in the same step, with `retire_after` set. The overlap is never longer than 24 h.
- A `retiring` key stops verifying at its `retire_after`. ACR enforces the same bound and the same 24 h ceiling.
- The `active` key is never retired directly: rotate in a successor. A `retired` key never changes again and never returns.
- Put no private key, key file, digest or token on a command line, in a ticket or in a log. Names and times only.
- Every step that writes names its audit event: `auth.workload.assertion_key_set`. The audit row carries the stored key row id,
  not the key id.

## Read the current state

```sh
auth-migrate workload-assertion-key-list -workload web
```

Each line is public metadata: `workload`, `key` (row id), `issuer`, `kid`, `status`, `rotated_at`, `retire_after`. Copy `issuer`
and `rotated_at` from this line. A later step needs the old key's `rotated_at` exactly.

## Rotate

Do the steps in this order. Step 1 comes first so ACR accepts the new key before the signer uses it.

1. Add the new public key (Ed25519, new `kid`) to the ACR JWKS file, next to the old key. ACR accepts both.
2. Record the new key. `-rotated-at` is the time the key is put in service and must be later than the old key's `rotated_at`.
   `-retire-previous-after` is the overlap, at most 24 h:

   ```sh
   auth-migrate workload-assertion-key-set -workload web -issuer ISSUER_URL -kid NEW_KID -status active -rotated-at RFC3339_TIME -retire-previous-after 2h -confirm
   ```

   Audit event: `auth.workload.assertion_key_set` (status `active`). The old key is now `retiring`. Read it back with the list verb and
   copy the old key's `retire_after`.
3. Put `retire_after` (the value from step 2, RFC 3339) on the old key's entry in the ACR JWKS file. From that time ACR refuses the
   old `kid`.
4. Move web in one config change and one roll: the key id and the key file together. Never change one without the other.
5. At `retire_after`, drop the old public key from the ACR JWKS file. Then record the old key as `retired`. `-rotated-at` is the
   old key's own `rotated_at` from the list line:

   ```sh
   auth-migrate workload-assertion-key-set -workload web -issuer ISSUER_URL -kid OLD_KID -status retired -rotated-at OLD_ROTATED_AT -confirm
   ```

   Audit event: `auth.workload.assertion_key_set` (status `retired`).
6. Read the state again with the list verb: one `active` key, the old key `retired`.

The live drill of steps 1 to 5 on ACR (web pull request 1794, record `DRILL-11236-5.log`): with the old key before `retire_after`
and the new key added, an assertion signed by either key is accepted (200); after `retire_after` an assertion signed by the old key is
refused (401 `invalid_token`) and one signed by the new key is accepted (200).

## Recover

A verb refuses with one class and writes nothing. The exit code is 3 for a refusal, 2 for a bad argument line and 1 for a
configuration or store failure.

| Class (message) | Meaning | Action |
| --- | --- | --- |
| `workload not registered` | The name is not in the registry. | Run `auth-migrate workloads seed -apply`, then repeat. |
| `the workload is disabled; enable it first` | The `web` row is disabled. | Enable it (see the [bootstrap credential runbook](auth-workload-bootstrap-credential.md), "Enable a disabled workload"), then repeat. |
| `the workload has an active key; add -retire-previous-after to rotate` | A new active key needs an overlap. | Repeat with `-retire-previous-after`. |
| `retire-after is more than 24h after the active key's rotated-at` | The overlap is too long. | Use 24 h or less. |
| `the active key is retired by rotating in a successor, not directly` | The old key is still `active`. | Do step 2 first. |
| `a retired key does not change` | A retired key is terminal. | Add a new key with a new `kid`. |
| `the change is not allowed from the key's current state` | For example `-rotated-at` differs from the stored value, or the new key is not later than the old one. | Read the list line and copy the stored `rotated_at`. |
| `a field was refused` | A field is malformed (a plain https issuer URL, a printable `kid`, no value shaped like a credential). | Correct the field. |
| `store unavailable` | PostgreSQL did not answer. | Check the migration endpoint, then repeat. A repeat that changes nothing writes nothing. |

A lost or exposed signing key. Generate a new key pair and add its public key to the ACR JWKS file. Then record it as the new active key
with the shortest useful overlap (`-retire-previous-after 1m`, any value above 0): the registry has no direct retire, so a new active key
is the recovery. Drop the exposed key's entry from the ACR JWKS file at once, roll web onto the new key, and record the exposed key as
`retired` (step 5, second command). Assertions signed by the exposed key fail from the moment its entry is dropped, so roll web first
when the exposure allows it.

An overlap that is too long or too short. Move a `retiring` key's end with the same verb: `-status retiring -retire-after RFC3339_TIME`
and the stored `-rotated-at` (the new time is at most 24 h after the active key's `rotated_at`). Then put the same time on the old key's
JWKS entry. A repeat with the same values writes nothing.
