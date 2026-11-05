// Package restorefence is the fence that runs after a point-in-time restore (or
// any promote) of the auth database, before the auth store takes writes.
//
// A restore to time T drops every auth write after T. Two things follow, and
// the fence undoes both: aggregate revisions published after T would be issued
// again with other content (the floors are raised past the highest revision
// the source timeline reached), and credentials revoked after T would be live
// again (the revocations are applied again). The source of the facts is a
// manifest exported from a shadow restore of the same WAL archive to its end;
// every input is a floor or a one-way state, so applying is idempotent and a
// second run changes nothing.
//
// The auth schema work runs as the migration role; export and apply also read and write the legacy public tables, so the
// fence runs on the owner login (Secret auth-migrate-database-owner). The runtime role only reads
// whether the current timeline is fenced (Check), and the readiness check built
// on it fails closed: a database on a timeline with no fence record takes no
// traffic.
package restorefence
