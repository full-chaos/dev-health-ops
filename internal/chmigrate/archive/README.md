What the ClickHouse head baseline replaced when it moved to the chain head (CHAOS-11751): `baseline-098/head.json`
(the old baseline) and `sql/099..144` (the chain files after it, bytes unchanged). Read by tests only.

Nothing applies these files and the binary does not embed them (`TestNoNonTestSourceEmbedsTheArchive`).
`baseline/head.json` is the capture of a fresh database built from this archive
(`TestRegenerateTheHeadBaseline`, `CHMIGRATE_REGENERATE_HEAD=1`), and `TestBaselineEqualsTheArchivedChain` holds it
equal to the archive by execution. `SHA256SUMS` pins every file byte-for-byte;
`internal/cigates/scripts/chmigrate_archive_frozen_test.go` fails on any edit, addition or removal.
