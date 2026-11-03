History, not applied. The PostgreSQL baseline moved from revision 0138 to 0162 by
decision D6430 (CHAOS-11743). This directory keeps what came before, byte for byte:

- `baseline-0138/head.json`: the old baseline (the Python upgrade to 0138, captured);
- `sql/` and `sql/down/`: the chain revisions 0139..0162 and their down SQL, as released;
- `chain_manifest.tsv`: `<revision><TAB><sha256 of sql/<file>><TAB><file>` for each one.

The binary embeds only `chain_manifest.tsv` (the revisions it must know, so a database
that records one is refused as below the baseline). `TestBaselineEqualsTheArchivedChain`
builds a database from the old baseline and these files and holds it equal to
`../baseline/head.json`. Nothing here is ever edited.
