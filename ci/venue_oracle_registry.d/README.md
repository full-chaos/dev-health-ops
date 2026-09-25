# Venue oracle registry (CHAOS-6584, CHAOS-6724)

One file per package: `ci/venue_oracle_registry.d/<package dir with / as __>.tsv`
(`internal/apiservice/admin` -> `internal__apiservice__admin.tsv`).

Row (TAB separated): `<package dir>	<Test function>	<run|local>`

- `run` = `ci/check_go.sh venue-oracles` runs it and requires its proof file.
- `local` = carries `//venueoracle:local-only` directly above it.
- Rows sorted (LC_ALL=C) and unique within the file; the package column must match the file name.

A PR that adds a venue test adds ONE row to ITS package's file (create the file for a
new package). Nothing else is shared between PRs. `bash ci/check_venue_oracle_registry.sh`
checks it (no Go toolchain needed). The old single file `ci/venue_oracle_registry.tsv`
must not exist: the check fails if it does.
