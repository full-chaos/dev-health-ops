#!/usr/bin/env bash
# THE single derivation and validation of docker/Dockerfile's PYTHON_BASE_IMAGE.
#
# WHY ONE SCRIPT (CHAOS-4922, lane-4441). This logic previously existed twice --
# once in mirror-test-images.yml's derivation, once in docker-images.yml's
# ensure-base step -- and the two disagreed on the same Dockerfile in BOTH
# directions:
#
#   ARG value                                    ensure-base   mirror
#   ghcr.io/<owner>/python:TAG@sha256:...        pass          pass
#   ghcr.io/<owner>/lib/python:TAG@sha256:...    pass          FAIL
#   ghcr.io/other-owner/python:TAG@sha256:...    FAIL          FAIL
#   ghcr.io/<owner>/python:TAG   (NO DIGEST)     FAIL          PASS   <-- the defect
#
# The last row was the dangerous one: the mirror would publish a FLOATING TAG
# (`imagetools create --tag <ghcr> python:TAG`, whatever Hub points at today),
# and only afterwards would the build fail on the unpinned ARG. Mirror succeeds
# wrongly, build fails correctly, and the ghcr tag is left pointing somewhere
# nobody chose -- recoverable only by re-mirroring, not by reverting.
#
# It also contradicted the comment directly above it, which asserted the ref
# "carries its digest". The comment stated the property; nothing enforced it.
#
# Two validators for one property will diverge, and the gap between them is
# exactly the set of silent failures. So there is now one.
#
# Usage:  ci/python_base_ref.sh <owner> [dockerfile]
# Prints exactly two lines on success:
#   ghcr <full ghcr ref, digest-pinned>
#   upstream <repo:tag@digest, the ref to mirror FROM>
set -euo pipefail

owner="${1:?owner required}"
dockerfile="${2:-docker/Dockerfile}"

[ -r "${dockerfile}" ] || { echo "::error::${dockerfile} is missing or unreadable" >&2; exit 1; }

ref=$(sed -nE 's/^ARG PYTHON_BASE_IMAGE="([^"]+)".*$/\1/p' "${dockerfile}")

# Exactly one. Two ARG lines are legal Dockerfile syntax and the SECOND wins the
# build, so "some line matched" is the wrong question -- the old guards accepted
# two and appended both to the mirror list.
n=$(printf '%s' "${ref}" | grep -c . || true)
if [ "${n:-0}" -ne 1 ]; then
  echo "::error::derived ${n:-0} PYTHON_BASE_IMAGE refs from ${dockerfile}, expected exactly 1" >&2
  exit 1
fi

# Our owner specifically. The mirror publishes under this owner, so a ref naming
# another one would be pulled from somewhere nothing ever writes to.
case "${ref}" in
  "ghcr.io/${owner}/"*) : ;;
  *) echo "::error::PYTHON_BASE_IMAGE must be under ghcr.io/${owner}/: ${ref}" >&2; exit 1 ;;
esac

# Digest-pinned, with a FULL 64-hex digest. This is the property the mirror
# exists to provide and the one that was unenforced; the length check also
# catches a truncated-digest typo, which would otherwise mirror nothing and
# fail opaquely at pull time.
if ! printf '%s' "${ref}" | grep -qE '@sha256:[0-9a-f]{64}$'; then
  echo "::error::PYTHON_BASE_IMAGE must be digest-pinned (@sha256:<64 hex>): ${ref}" >&2
  exit 1
fi

# The inner expansion is quoted: inside ${var#pattern} an unquoted expansion is
# treated as a GLOB, so an owner containing pattern characters would strip the
# wrong prefix or none at all. SC2295, caught by this repo's own pinned linter
# on the first run of this file.
#
# (This comment deliberately avoids starting a line with the linter's name: a
# comment beginning with that word parses as a DIRECTIVE, SC1072/SC1073. That
# trap has now been hit twice in this PR series.)
upstream="${ref#ghcr.io/"${owner}"/}"

# No extra path segment: the mirror maps <repo>:<tag> onto ghcr.io/<owner>/, so a
# nested path would mirror to a location the Dockerfile does not name.
if printf '%s' "${upstream}" | grep -qE '[[:space:]/]'; then
  echo "::error::PYTHON_BASE_IMAGE has an unexpected path segment: ${ref}" >&2
  exit 1
fi

printf 'ghcr %s\n' "${ref}"
printf 'upstream %s\n' "${upstream}"
