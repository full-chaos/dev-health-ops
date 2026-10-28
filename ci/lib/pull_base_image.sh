#!/usr/bin/env bash
# CHAOS-11096, CHAOS-11649: pre-pull the pinned Go build base image and the pinned runtime base image of
# docker/go-worker.Dockerfile with bounded retries (ci/lib/retry.sh). Sourced by the kind proofs. Anonymous pulls only:
# no login, no credentials.
#   pull_pinned_base_image DOCKERFILE     ARG GO_BUILD_IMAGE (mirror.gcr.io, digest pin); sources mirror.gcr.io, then docker.io
#                                         with the SAME digest; exports PINNED_BASE_TAG (build with
#                                         --build-arg GO_BUILD_IMAGE="${PINNED_BASE_TAG}" --pull=false)
#   pull_pinned_runtime_image DOCKERFILE  ARG GO_RUNTIME_IMAGE (gcr.io, digest pin); source gcr.io only (mirror.gcr.io has no
#                                         distroless, Docker Hub neither); exports PINNED_RUNTIME_TAG (build with
#                                         --build-arg GO_RUNTIME_IMAGE="${PINNED_RUNTIME_TAG}")
# After success the tag names a run-unique local image. Overrides (tests): PULL_TRIES, PULL_TIMEOUT_SECS,
# PULL_BACKOFF_SECS (space list, one per gap).

# shellcheck source=ci/lib/retry.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/retry.sh"

# _pull_attempt: one pull of ${src}/${repo}@${digest} and its digest check; prints the per-try line. Reads the variables of
# _pull_pinned_ref (dynamic scope) and sets out and try_n there.
_pull_attempt() {
  local start="${SECONDS}" rc=0 listed
  try_n=$((try_n + 1))
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
    "${src}" "${try_n}" "${tries}" "${rc}" "$((SECONDS - start))" "${seen}"
  if [ "${rc}" -ne 0 ] && printf '%s' "${out}" | grep -qiE 'toomanyrequests|rate limit'; then
    printf 'base-image pull: source=%s try=%d/%d rate-limited (anonymous pull refused)\n' "${src}" "${try_n}" "${tries}"
  fi
  return "${rc}"
}

# _pull_pinned_ref DOCKERFILE ARG REGISTRY "SOURCES" TAG_PREFIX TAG_VAR
_pull_pinned_ref() {
  local dockerfile="$1" arg="$2" registry="$3" sources_str="$4" tag_prefix="$5" tag_var="$6"
  local ref repo digest src tag tag_part rest listed rc try_n=0 out seen
  local tries="${PULL_TRIES:-3}" tmo="${PULL_TIMEOUT_SECS:-120}"
  local -a sources=() failed=()
  unset "${tag_var}"

  ref="$(sed -n "s/^ARG ${arg}=\"\(.*\)\"\$/\1/p" "${dockerfile}")" \
    || { printf 'FAIL: cannot read %s\n' "${dockerfile}" >&2; return 1; }
  [ -n "${ref}" ] || { printf 'FAIL: ARG %s not found in %s\n' "${arg}" "${dockerfile}" >&2; return 1; }
  case "${ref}" in "${registry}"/*) ;; *)
    printf 'FAIL: %s in %s is not a %s ref: %s\n' "${arg}" "${dockerfile}" "${registry}" "${ref}" >&2; return 1 ;; esac
  digest="${ref##*@}"
  rest="${ref%@*}"
  tag_part="${rest%%:*}"
  repo="${tag_part#"${registry}"/}"
  case "${digest}" in sha256:????????????????????????????????????????????????????????????????) ;; *)
    printf 'FAIL: %s in %s has no sha256 digest pin\n' "${arg}" "${dockerfile}" >&2; return 1 ;; esac

  read -r -a sources <<< "${sources_str}"
  for src in "${sources[@]}"; do
    try_n=0
    rc=0
    RETRY_QUIET=1 retry_named "pull-${src}" "${tries}" "${PULL_BACKOFF_SECS:-5 15}" -- _pull_attempt || rc=$?
    if [ "${rc}" -eq 0 ]; then
      tag="${tag_prefix}:${GITHUB_RUN_ID:-local}-$$"
      rc=0
      docker tag "${src}/${repo}@${digest}" "${tag}" || rc=$?
      if [ "${rc}" -ne 0 ]; then
        unset "${tag_var}"
        printf 'FAIL: docker tag of %s/%s@%s to %s failed rc=%d\n' "${src}" "${repo}" "${digest}" "${tag}" "${rc}" >&2
        return 1
      fi
      printf -v "${tag_var}" '%s' "${tag}"
      export "${tag_var?}"
      return 0
    fi
    failed+=("${src} (${tries} tries)")
  done
  listed=""
  for src in "${failed[@]}"; do listed="${listed:+${listed}, }${src}"; done
  printf 'FAIL: base image %s@%s could not be pulled from: %s\n' "${repo##*/}" "${digest}" "${listed}" >&2
  return 1
}

pull_pinned_base_image() {
  _pull_pinned_ref "$1" GO_BUILD_IMAGE mirror.gcr.io "mirror.gcr.io docker.io" dho-go-build-base PINNED_BASE_TAG
}

pull_pinned_runtime_image() {
  _pull_pinned_ref "$1" GO_RUNTIME_IMAGE gcr.io "gcr.io" dho-go-runtime-base PINNED_RUNTIME_TAG
}
