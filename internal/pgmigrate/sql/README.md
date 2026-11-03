Revisions after the PostgreSQL baseline (revision 0162), applied by `dho migrate
postgres` in revision order. Name each file `<revision>_<slug>.sql`; its first line is
`-- Revision <revision> (down_revision <previous>)`, and the first file continues 0162.
Each file has its down SQL under `sql/down/` with the same name, and a line appended to
`../chain_manifest.tsv` (`<revision><TAB><sha256>`); a released file is never edited.

The whole walk (the baseline of an empty database and every pending file) runs in ONE
transaction together with the `alembic_version` updates that record it, so a failed file
rolls every file of the run back (CHAOS-7291). A file therefore cannot hold a statement
that cannot run in a transaction block (CREATE INDEX CONCURRENTLY, VACUUM, CREATE
DATABASE...) or that ends the transaction (COMMIT, BEGIN, SAVEPOINT...):
`TestEveryChainRevisionCanRunInTheWalksOneTransaction` fails on one. The tables a file
alters stay ACCESS EXCLUSIVE-locked until the run commits.

The baseline (`../baseline/head.json`) is the schema at revision 0162. It moved there
once, from 0138, by decision D6430 (CHAOS-11743): the files 0139..0162 and their down SQL
are history in `../archive/`, which nothing applies. A database that records a revision
below 0162 is refused (below_baseline); bring it to 0162 with a build from before the
squash first.
