#!/usr/bin/env bash
# CHAOS-11096: pre-pull the pinned Go build base image with bounded retries, then a second source
# with the SAME digest. Sourced by the kind proofs. Anonymous pulls only: no login, no credentials.
# After success PINNED_BASE_TAG names a run-unique local tag; build with
#   --build-arg GO_BUILD_IMAGE="${PINNED_BASE_TAG}" --pull=false
# Overrides (tests): PULL_TRIES, PULL_TIMEOUT_SECS, PULL_BACKOFF_SECS (space list, one per gap).

pull_pinned_base_image() {
  local dockerfile="$1" ref repo digest src tag_part n=0 try rc out seen start rest
  local tries="${PULL_TRIES:-3}" tmo="${PULL_TIMEOUT_SECS:-120}"
  local -a backoff sources=() failed=()
  read -r -a backoff <<< "${PULL_BACKOFF_SECS:-5 15}"

  ref="$(sed -n 's/^ARG GO_BUILD_IMAGE="\(.*\)"$/\1/p' "${dockerfile}")"
  [ -n "${ref}" ] || { printf 'FAIL: ARG GO_BUILD_IMAGE not found in %s\n' "${dockerfile}" >&2; return 1; }
  digest="${ref##*@}"
  rest="${ref%@*}"
  tag_part="${rest%%:*}"
  repo="${tag_part#mirror.gcr.io/}"
  case "${digest}" in sha256:????????????????????????????????????????????????????????????????) ;; *)
    printf 'FAIL: GO_BUILD_IMAGE in %s has no sha256 digest pin\n' "${dockerfile}" >&2; return 1 ;; esac

  sources=("mirror.gcr.io" "docker.io")
  for src in "${sources[@]}"; do
    for ((try = 1; try <= tries; try++)); do
      start="${SECONDS}"
      rc=0
      # timeout stops only the docker CLI; the daemon pull may go on. The next try pulls the same
      # digest, so a timed-out try is safe.
      out="$(timeout "${tmo}" docker pull "${src}/${repo}@${digest}" 2>&1)" || rc=$?
      seen="-"
      if [ "${rc}" -eq 0 ]; then
        if docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "${src}/${repo}@${digest}" 2>/dev/null \
          | sed 's/.*@//' | grep -qxF "${digest}"; then seen="${digest}"; else seen="mismatch"; fi
        [ "${seen}" = "${digest}" ] || { rc=65; out="digest mismatch: wanted ${digest}"; }
      fi
      printf 'base-image pull: source=%s try=%d/%d rc=%d elapsed=%ds digest=%s\n' \
        "${src}" "${try}" "${tries}" "${rc}" "$((SECONDS - start))" "${seen}"
      if [ "${rc}" -eq 0 ]; then
        PINNED_BASE_TAG="dho-go-build-base:${GITHUB_RUN_ID:-local}-$$"
        docker tag "${src}/${repo}@${digest}" "${PINNED_BASE_TAG}"
        export PINNED_BASE_TAG
        return 0
      fi
      if printf '%s' "${out}" | grep -qiE 'toomanyrequests|rate limit'; then
        printf 'base-image pull: source=%s try=%d/%d rate-limited (anonymous pull refused)\n' "${src}" "${try}" "${tries}"
      fi
      if [ "${try}" -lt "${tries}" ]; then
        n=$((try - 1))
        sleep "${backoff[${n}]:-${backoff[-1]:-0}}"
      fi
    done
    failed+=("${src} (${tries} tries)")
  done
  printf 'FAIL: base image %s@%s could not be pulled from: %s\n' "${repo##*/}" "${digest}" "$(IFS=,; printf '%s' "${failed[*]}" | sed 's/,/, /g')" >&2
  return 1
}
