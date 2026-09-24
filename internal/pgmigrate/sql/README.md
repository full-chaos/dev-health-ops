Revisions after the PostgreSQL head baseline, applied by `dho migrate
postgres` in revision order. Name each file `<revision>_<slug>.sql`, with a
revision number above the baseline's application head. Each file runs in one
transaction together with the `alembic_version` update that records it, so a
failed file leaves no trace.

Until the Python alembic chain is deleted, the baseline is re-derived from that
chain in CI. Adding an alembic revision means regenerating the baseline in the
same change.
