# Dev Health job contracts v1

This directory is the language-neutral source of truth for bounded worker job
arguments. River owns the job kind and queue columns; `encoded_args` contains
only the envelope defined by `envelope.schema.json`.

## Stability policy

- Kinds are lowercase dotted identifiers and are never renamed or reused.
- Contract versions are positive integers. A decoder rejects an unknown
  version instead of guessing.
- The only compatible in-place schema edit is adding an optional field. A
  removal, rename, new required field, type/constraint change, or semantic
  change requires a new version.
- Decoders remain strict: an implementation must explicitly add that optional
  field before advertising the new schema digest, and producers must leave it
  unset until every old digest has drained. “Optional” does not mean arbitrary
  unknown fields are ignored.
- A kind at version N supports both N and N-1 while a rolling deployment may
  contain either binary. Version 1 is the only exception because N-1 does not
  exist.
- A producer may emit a version only after every target profile reports that
  exact kind/version and schema digest in a capability report. The digest
  covers both the envelope and kind payload schemas, preventing an older
  strict decoder from falsely qualifying after an additive optional-field
  revision within the same version.
- The complete encoded envelope is limited to 16 KiB. Values contain bounded
  identifiers and safe options only. Credentials, DSNs, headers, SQL, raw
  provider/webhook payloads, rendered reports, and tenant data are forbidden.

`registry.json` defines the supported kind/version pairs and execution policy.
`migration-state.json` records producer routing and promotion state without
making queue state authoritative for product state.

## Validation

From the repository root:

```text
go run ./cmd/worker-contractcheck validate
go run ./cmd/worker-contractcheck capabilities --profile ops
go run ./cmd/worker-contractcheck compare --base <old-v1-dir> --candidate contracts/jobs/v1
```

`compare` exits non-zero for a breaking in-place edit and is designed to be
wired to CI with the merge-base contract directory as `--base`.
