Revisions after the PostgreSQL baseline (revision 0138), applied by `dho migrate
postgres` in revision order. Name each file `<revision>_<slug>.sql`, with the
revision number of the Alembic revision it is (or, for a release with no Alembic
script, the next number). Each file runs in one transaction together with the
`alembic_version` update that records it, so a failed file leaves no trace.

The baseline (`baseline/head.json`) is frozen at revision 0138 and never moves.
Adding an Alembic revision means adding its file here:

    alembic upgrade <previous>:<revision> --sql

renders it (drop the BEGIN/COMMIT and `alembic_version` lines: the migrator adds the
version update itself). A revision that reads data or runs Python cannot be
rendered; write the SQL by hand. `TestChainCoversEveryAlembicRevision` fails while
an Alembic revision above the baseline has no file (or a file names no revision, or
continues the wrong revision), and `TestBaselineIsTheExecutedPythonUpgrade` executes
the result: a database built by the real Python upgrade at the baseline and at every
chain revision, upgraded by dho, must equal the Python upgrade to the head.
