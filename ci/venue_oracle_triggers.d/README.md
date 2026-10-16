# Venue oracle trigger paths (CHAOS-9172)

`ci/check_go.sh venue-oracles --changed` (the pull request job) selects a venue test by
the paths of its own package. A venue test also compares what code in OTHER packages
does. A change there selects nothing unless the path is declared here.

One file per package of the test: `ci/venue_oracle_triggers.d/<package dir with / as __>.tsv`.

Row (TAB separated): `<package dir>	<Test function>	<repo path>`

- A changed path equal to, or under, `<repo path>` also selects that test.
- The test must be a `run` row of `ci/venue_oracle_registry.d/`; the path must exist and
  must lie outside the package of the test.
- Rows sorted (LC_ALL=C) by test, then path, and unique within the file.

`bash ci/check_venue_oracle_registry.sh` checks it (no Go toolchain needed).

This list is declared by hand: it holds the paths a venue test is KNOWN to depend on that are
not Go packages of its test binary (a ClickHouse migration, a fixture tree, a contract file).
Every Go package the test binary links is covered without a row: `ci/venue_oracle_closure.d/`
(CHAOS-9935) lists, per venue test package, the in-module packages `go list -test` says its test
binary links, and a changed file directly in one of them (or under its testdata) selects every
test of the package. `TestClosureFilesMatchTheTestBinaryDependencies` fails when a file is stale
and prints the regenerate command. A path that is in neither still selects nothing on a pull
request; the full run on main is the check for it. Named limit: a file embedded from a
sub-directory that is not a package (go:embed of `sql/*.sql`) lies outside every package
directory and needs a row here.

`internal/testsupport/venuechanged` (TestEveryProducerAVenueTestImportsIsDeclaredAsATrigger,
CHAOS-9578, per test since CHAOS-9614) fails when a `run` test uses a producer package
(internal/, outside testsupport) that no row of ITS OWN covers. Selection is per test, so a
row for one test of a package never covers another test of it. A test uses the producers
named in its function, in every same-package test-file declaration it reaches through
identifiers (helpers, fixtures, types, methods), and in the blank imports of the package.
`ci/venue_oracle_trigger_gaps.tsv` (`<package dir>	<Test function>	<producer package>`, sorted) is the
ratchet of producers neither a row here nor the closure covers; it is empty since CHAOS-9935 (the
closure covers every direct import) and a new line fails review.
