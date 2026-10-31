package fixture

import "embed"

//go:embed archive/chain_manifest.tsv data.txt
var files embed.FS
