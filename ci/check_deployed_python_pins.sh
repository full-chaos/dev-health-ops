#!/usr/bin/env bash
# Fail unless every package in ci/requirements-deployed-python-api.txt is
# installed in the given Python environment at exactly its pinned version.
#
# The venue oracles run the Python api from the uv environment with the
# deployed image's versions overlaid (see .github/workflows/venue-oracles.yml).
# An overlay that silently did not apply would leave the oracles proving
# parity against the wrong Python, so the step verifies the mechanism, not
# just that the install command exited zero.
#
# Subset, not equality: the uv environment also holds dev/extra packages the
# deployed image does not ship, so the installed set is a superset of the
# pins. Names are compared normalised (case, "_", ".", "-" are one thing), as
# pip freeze and uv pip freeze spell them differently.
#
# Usage: ci/check_deployed_python_pins.sh [PYTHON]   (default .venv/bin/python)
set -euo pipefail

python_bin="${1:-.venv/bin/python}"
here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
requirements="${PINS_FILE:-$here/requirements-deployed-python-api.txt}"

normalise() { # NAME==VERSION -> lower-case name with runs of -_. as one "-"
  awk -F'==' '{ n = tolower($1); gsub(/[-_.]+/, "-", n); print n "==" $2 }'
}

installed="$(uv pip freeze --python "$python_bin" | normalise)"
checked=0
missing=()
while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  checked=$((checked + 1))
  want="$(printf '%s\n' "$line" | normalise)"
  if ! grep -Fxq -- "$want" <<<"$installed"; then
    name="${want%%==*}"
    have="$(grep -F -- "${name}==" <<<"$installed" | head -n1 || true)"
    missing+=("pinned ${line}, installed: ${have:-not installed}")
  fi
done <"$requirements"

if [ "$checked" -eq 0 ]; then
  echo "no pins read from $requirements" >&2
  exit 1
fi
if [ "${#missing[@]}" -gt 0 ]; then
  echo "deployed-version overlay did not apply:" >&2
  printf '  %s\n' "${missing[@]}" >&2
  exit 1
fi
echo "deployed-version overlay verified: $checked packages at the pinned versions"
