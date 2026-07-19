# Credential encryption key rotation

This runbook rotates data protected by `SETTINGS_ENCRYPTION_KEY` without breaking existing rows.

## Current format

- `v1:` ciphertexts use Fernet with a PBKDF2-HMAC-SHA256 derived key.
- PBKDF2 uses `SETTINGS_ENCRYPTION_SALT` and 600,000 iterations.
- Legacy rows without a prefix are still decrypted with the old SHA-256-derived key for backwards compatibility.
- The built-in salt is for backwards compatibility only. Production must set an explicit stable `SETTINGS_ENCRYPTION_SALT`.

## Rotate from legacy v0 rows to v1

1. Confirm the current key and salt are present in the runtime environment:
   - `SETTINGS_ENCRYPTION_KEY=<current key>`
   - `SETTINGS_ENCRYPTION_SALT=<explicit production salt>`
   - `POSTGRES_URI=<semantic database>`
2. Dry-run the upgrade:
   ```bash
   python scripts/reencrypt_settings_credentials.py
   ```
3. Apply the upgrade:
   ```bash
   python scripts/reencrypt_settings_credentials.py --apply
   ```
4. Restart API and worker processes so all writers emit `v1:` ciphertexts.
5. Re-run the dry-run command. `upgraded=0` and `failed=0` should be reported.

## Rotate `SETTINGS_ENCRYPTION_KEY`

1. Keep the old key deployed.
2. Run the v0-to-v1 upgrade above until no legacy rows remain.
3. Take an encrypted database backup.
4. In a maintenance window, deploy code configured with the new key and the same salt.
5. Re-encrypt each encrypted value by decrypting with the old key and encrypting with the new key. If the old key is no longer available to the same process, run this as a controlled one-off job that has both values as separate environment variables and calls `decrypt_value` with the old key before writing with the new key.
6. Restart all API and worker processes with only the new `SETTINGS_ENCRYPTION_KEY`.
7. Validate settings, integration credential test connections, and SSO login flows.

## Recovery

- If decryption errors appear immediately after rotation, restore the previous `SETTINGS_ENCRYPTION_KEY` and restart API/workers.
- If the re-encryption utility reports failures, stop and inspect the affected rows before retrying. Failures usually mean the row is plaintext, corrupted, or encrypted with an unexpected key.
- If a deployment accidentally omits `SETTINGS_ENCRYPTION_SALT`, restore the explicit production salt and restart before writing new credentials.

## Connection removal

Disconnecting a PagerDuty connection atomically clears its
`IntegrationCredential.credentials_encrypted` value and removes its
`ProviderOAuthCredential` row. The inactive integration descriptor remains as a
non-secret tombstone, retaining only its configuration and lifecycle metadata.

This applies to OAuth, client-credentials, and API-token authentication. After
commit, a fresh database session cannot retrieve decryptable secret material for
the disconnected credential; other provider credentials are unchanged. Remote
OAuth token revocation remains best-effort and happens only after local removal
has committed.

This contract does not backfill or migrate already-inactive descriptors. Review
those rows separately if historical credential cleanup is required.
