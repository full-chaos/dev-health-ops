#!/usr/bin/env bash
# CHAOS-11096: pre-pull the pinned Go build base image with bounded retries, then a second source
# with the SAME digest. Sourced by the kind proofs. Anonymous pulls only: no login, no credentials.
# After success PINNED_BASE_TAG names a run-unique local tag; build with
#   --build-arg GO_BUILD_IMAGE="${PINNED_BASE_TAG}" --pull=false
# Overrides (tests): PULL_TRIES, PULL_TIMEOUT_SECS, PULL_BACKOFF_SECS (space list, one per gap).

pull_pinned_base_image() {
  local dockerfile="$1" ref repo digest src tag tag_part n=0 try rc out seen start rest listed
  local tries="${PULL_TRIES:-3}" tmo="${PULL_TIMEOUT_SECS:-120}"
  local -a backoff sources=() failed=()
  unset PINNED_BASE_TAG
  read -r -a backoff <<< "${PULL_BACKOFF_SECS:-5 15}" || backoff=(0)
  [ "${#backoff[@]}" -gt 0 ] || backoff=(0)

  ref="$(sed -n 's/^ARG GO_BUILD_IMAGE="\(.*\)"$/\1/p' "${dockerfile}")" \
    || { printf 'FAIL: cannot read %s\n' "${dockerfile}" >&2; return 1; }
  [ -n "${ref}" ] || { printf 'FAIL: ARG GO_BUILD_IMAGE not found in %s\n' "${dockerfile}" >&2; return 1; }
  case "${ref}" in mirror.gcr.io/*) ;; *)
    printf 'FAIL: GO_BUILD_IMAGE in %s is not a mirror.gcr.io ref: %s\n' "${dockerfile}" "${ref}" >&2; return 1 ;; esac
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
        rc=0
        listed="$(docker image inspect --format '{{range .RepoDigests}}{{println .}}{{end}}' "${src}/${repo}@${digest}" 2>&1)" || rc=$?
        if [ "${rc}" -ne 0 ]; then
          seen="inspect-failed"
        elif printf '%s\n' "${listed}" | sed 's/.*@//' | grep -qxF "${digest}"; then
          seen="${digest}"
        else
          seen="mismatch"
        fi
        [ "${seen}" = "${digest}" ] || { [ "${rc}" -ne 0 ] || rc=65; out="digest check failed: wanted ${digest}, ${seen}"; }
      fi
      printf 'base-image pull: source=%s try=%d/%d rc=%d elapsed=%ds digest=%s\n' \
        "${src}" "${try}" "${tries}" "${rc}" "$((SECONDS - start))" "${seen}"
      if [ "${rc}" -eq 0 ]; then
        tag="dho-go-build-base:${GITHUB_RUN_ID:-local}-$$"
        rc=0
        docker tag "${src}/${repo}@${digest}" "${tag}" || rc=$?
        if [ "${rc}" -ne 0 ]; then
          unset PINNED_BASE_TAG
          printf 'FAIL: docker tag of %s/%s@%s to %s failed rc=%d\n' "${src}" "${repo}" "${digest}" "${tag}" "${rc}" >&2
          return 1
        fi
        PINNED_BASE_TAG="${tag}"
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
  listed=""
  for src in "${failed[@]}"; do listed="${listed:+${listed}, }${src}"; done
  printf 'FAIL: base image %s@%s could not be pulled from: %s\n' "${repo##*/}" "${digest}" "${listed}" >&2
  return 1
}
