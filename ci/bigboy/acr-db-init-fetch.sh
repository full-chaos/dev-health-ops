#!/usr/bin/env bash
# acr-db-init-fetch.sh [out-dir]: fetch deploy/compose/acr-db-init.sh at the acr commit of acr-db-init.pin, check its sha256, write it to
# <out-dir>/acr-db-init.sh (default: the dir of ACR_DB_INIT_SCRIPT in images.env) and print the path. Overrides: ACR_DB_INIT_PIN, ACR_REPO_URL.
set -euo pipefail
here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
pin=${ACR_DB_INIT_PIN:-$here/acr-db-init.pin}
url=${ACR_REPO_URL:-https://github.com/full-chaos/dev-health-acr.git}
out=${1:-/var/lib/oci-cache/mcp-venue/acr-db-init}
commit=$(sed -n 's/^ACR_DB_INIT_COMMIT=\([0-9a-f]\{40\}\)$/\1/p' "$pin")
want=$(sed -n 's/^ACR_DB_INIT_SHA256=\([0-9a-f]\{64\}\)$/\1/p' "$pin")
[[ -n $commit && -n $want ]] || { echo "FAIL: $pin has no valid ACR_DB_INIT_COMMIT / ACR_DB_INIT_SHA256" >&2; exit 4; }
tmp=$(mktemp -d); trap 'rm -rf "${tmp:?}"' EXIT
git init -q "$tmp/r"
git -C "$tmp/r" fetch -q --depth 1 "$url" "$commit"
git -C "$tmp/r" show "$commit:deploy/compose/acr-db-init.sh" >"$tmp/script"
got=$(sha256sum "$tmp/script" | cut -d' ' -f1)
[[ $got == "$want" ]] || { echo "FAIL: acr-db-init.sh at $commit has sha256 $got, the pin says $want" >&2; exit 5; }
mkdir -p "$out"
dest=$out/acr-db-init.sh
install -m 0755 "$tmp/script" "$dest.new"
mv -f "$dest.new" "$dest"
echo "$dest"
