Frozen pre-baseline SQL text (000..098); read by tests only. The live chain is baseline/head.json + sql/; the 099..144 text is in ../archive/ (CHAOS-11751).

Nothing applies these files and they are not embedded. `SHA256SUMS` pins every file byte-for-byte;
`internal/cigates/scripts/chmigrate_history_frozen_test.go` fails on any edit, addition or removal.
