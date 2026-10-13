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

This list is declared by hand: it holds the paths a venue test is KNOWN to depend on.
A path that is not declared still selects nothing on a pull request; the full run on
main is the check for it. Add a row when a main run goes red after a green pull request.

`internal/testsupport/venuechanged` (TestEveryProducerAVenueTestImportsIsDeclaredAsATrigger,
CHAOS-9578) fails when a package with a `run` row imports a producer package (internal/,
outside testsupport) that no row of its file covers. Known gaps are listed in
`ci/venue_oracle_trigger_gaps.tsv` (`<package dir>	<producer package>`, sorted): a new gap
fails the test, and a line whose gap got covered must be removed. Close a gap by declaring the
producer here and deleting its line there.
