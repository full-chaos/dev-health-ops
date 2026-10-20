Frozen pre-baseline SQL text; the live chain is baseline/head.json + sql/099+; read by tests only.

Nothing applies these files and they are not embedded. `SHA256SUMS` pins every file byte-for-byte;
`internal/cigates/scripts/chmigrate_history_frozen_test.go` fails on any edit, addition or removal.
