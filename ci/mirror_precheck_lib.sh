#!/usr/bin/env bash
# Shared comparison logic for mirror-test-images.yml's `precheck` job.
#
# WHY (CHAOS-4928 codex round 1, P1): precheck used to ask ghcr.io only "does
# the destination TAG exist", never "does it carry the digest this PR now
# needs". A PR that bumps a digest-pinned upstream ref (e.g. postgres:18-alpine
# @sha256:OLD -> @sha256:NEW) leaves the OLD content sitting at the
# destination tag, which still answers "yes, present" -- so precheck reported
# missing=false, `mirror`'s `if:` skipped the publish for that pull_request,
# and the NEW digest was never copied. Whatever pulled it next (a
# Testcontainers-backed job asking for the exact NEW digest) got a 404 from
# ghcr, first noticed at test time with no signal pointing back here.
#
# THE FIX is digest-aware, not just presence-aware, and ONLY for refs that
# actually carry a digest to compare against: an unpinned upstream tag
# (clickhouse/clickhouse-server:26.7, valkey/valkey:8-alpine -- see this
# workflow's own job-level `concurrency` comment for why those two stay
# presence-only, a decision already made and unchanged by this fix) has
# nothing to compare TO, so presence is still the right and only question.
#
# Sourced by mirror-test-images.yml's `precheck` job, and by
# ci/mirror_precheck_lib_test.sh directly (no docker/network needed there --
# the test stubs the `docker` function itself, see that file's header).
set -euo pipefail

# mirror_dest_ref OWNER IMAGE_REF
# Prints the ghcr.io destination ref this image mirrors to. IDENTICAL
# derivation to the one already used by both the `mirror` job's own publish
# step and its "Verify every mirrored digest matches upstream" step below in
# the workflow -- kept as one function so the three call sites cannot drift
# apart on what "the destination" even means.
mirror_dest_ref() {
  local owner="$1" image="$2" ref repo tag
  ref="${image}"
  case "${ref}" in *@sha256:*) ref="${ref%@*}" ;; esac
  case "${ref}" in
    *:*) repo="${ref%:*}"; tag="${ref##*:}" ;;
    *)   repo="${ref}";    tag="mirrored"   ;;
  esac
  printf 'ghcr.io/%s/%s:%s' "${owner}" "${repo}" "${tag}"
}

# mirror_image_missing OWNER IMAGE_REF
#
# Prints exactly one telemetry line to stdout, per CHAOS-4928's "always add
# telemetry" rule -- source ref, dest, the digest actually found at dest (or
# why none was found), and the decision:
#
#   PRECHECK image=<src> dest=<dest> dest_digest=<digest|absent|unchecked> decision=<missing|present> reason=<...>
#
# Exit status is the decision, so a caller can write `if ! mirror_image_missing ...`
# without parsing its own stdout: 0 = present (nothing to mirror), 1 = missing
# (mirror needed).
mirror_image_missing() {
  local owner="$1" image="$2"
  local dest dest_digest decision reason want

  dest="$(mirror_dest_ref "${owner}" "${image}")"

  case "${image}" in
    *@sha256:*)
      # Digest-pinned upstream: the ONLY question that matters is whether the
      # destination tag's manifest digest equals the pin. Presence with the
      # WRONG digest is exactly the CHAOS-4928 failure mode -- treated as
      # missing, same as no destination at all.
      want="${image##*@}"
      if dest_digest="$(docker buildx imagetools inspect --format '{{ .Manifest.Digest }}' "${dest}" 2>/dev/null)"; then
        if [ "${dest_digest}" = "${want}" ]; then
          decision=present; reason=digest-match
        else
          decision=missing; reason=digest-mismatch
        fi
      else
        dest_digest=absent
        decision=missing; reason=absent
      fi
      ;;
    *)
      # Unpinned upstream tag: nothing to compare a digest against, so this
      # stays presence-only -- unchanged behavior, not a regression. `--raw`,
      # not the non-raw form: the non-raw form truncates a manifest list to a
      # summary (this file's own "Verify" step documents the same trap).
      if docker buildx imagetools inspect --raw "${dest}" >/dev/null 2>&1; then
        dest_digest=unchecked
        decision=present; reason=presence-only-tag-exists
      else
        dest_digest=absent
        decision=missing; reason=presence-only-tag-absent
      fi
      ;;
  esac

  printf 'PRECHECK image=%s dest=%s dest_digest=%s decision=%s reason=%s\n' \
    "${image}" "${dest}" "${dest_digest}" "${decision}" "${reason}"

  [ "${decision}" = present ]
}
