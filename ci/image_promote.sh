#!/usr/bin/env bash
# Hotfix image promotion for .github/workflows/docker-images.yml (the `resolve` job).
#
#   gate    pull_request: decide whether this PR build may push tree-<tree> images (label + same-repo head + write access).
#   promote push to main: when every image family was built from this exact tree by a reviewed workflow run, copy the
#           tree-<tree> digest to sha-<merge7> (no rebuild). Any doubt = promoted=false and the normal build runs.
#
# Never fails open: every refusal prints a ::warning:: and leaves promoted=false (or hotfix=false). The script exits 0
# either way; the workflow reads the outputs.
# shellcheck disable=SC2153
set -uo pipefail

: "${REPO:?}"
out="${GITHUB_OUTPUT:-/dev/stdout}"
hotfix=false
promoted=false
tree=""
sha=""

warn() { printf '::warning::%s\n' "$*"; }
emit() { printf '%s=%s\n' "$1" "$2" >>"${out}"; }

commit_json() { gh api "repos/${REPO}/git/commits/$1"; }

gate() {
  [ "${EVENT_NAME:-}" = pull_request ] || return 0
  local labels perm actor
  if [ "${HEAD_REPO:-}" != "${REPO}" ]; then
    warn "hotfix-image: head repo '${HEAD_REPO:-}' is not ${REPO}; no image push for a fork"
    return 0
  fi
  if ! labels="$(gh api "repos/${REPO}/pulls/${PR_NUMBER}")"; then
    warn "hotfix-image: cannot read the PR labels; no image push"
    return 0
  fi
  jq -e '[.labels[].name] | index("hotfix-image")' >/dev/null 2>&1 <<<"${labels}" || return 0
  for actor in ${ACTORS:-}; do
    if ! perm="$(gh api "repos/${REPO}/collaborators/${actor}/permission" | jq -r '.permission // empty')"; then
      warn "hotfix-image: cannot read the permission of ${actor}; no image push"
      return 0
    fi
    case "${perm}" in
      admin | write) ;;
      *)
        warn "hotfix-image: ${actor} has '${perm}' permission, write is required; no image push"
        return 0
        ;;
    esac
  done
  [ -n "${ACTORS:-}" ] || {
    warn "hotfix-image: no actor to check; no image push"
    return 0
  }
  if ! tree="$(commit_json "${HEAD_SHA}" | jq -r '.tree.sha // empty')" || [ -z "${tree}" ]; then
    warn "hotfix-image: cannot read the tree of ${HEAD_SHA}; no image push"
    tree=""
    return 0
  fi
  hotfix=true
}

decline() {
  warn "promote: $* -- building the images as today"
  promoted=false
}

promote() {
  [ "${EVENT_NAME:-}" = push ] && [ "${REF:-}" = refs/heads/main ] || return 0
  : "${OWNER:?}" "${SHA:?}"
  sha="${SHA}"
  local mtree prs head_sha pr_number head_tree family image json digest platform rev att stray src src_json
  local signer="${REPO}/.github/workflows/docker-images.yml"
  local families="dev-health-go-operator dev-health-go-contractcheck dev-health-go-migrate dev-health-go-dho dev-health-go-auth-service dev-health-go-auth-migrate dev-health-go-api-tools"
  local -a digests=()

  if ! mtree="$(commit_json "${sha}" | jq -r '.tree.sha // empty')" || [ -z "${mtree}" ]; then
    decline "cannot read the tree of merge commit ${sha}"
    return 0
  fi
  if ! prs="$(gh api "repos/${REPO}/commits/${sha}/pulls" | jq -c --arg sha "${sha}" '[.[] | select(.merge_commit_sha == $sha and .merged_at != null) | {n: .number, h: .head.sha}]')"; then
    decline "cannot list the pull requests of ${sha}"
    return 0
  fi
  if [ "$(jq 'length' <<<"${prs}")" != 1 ]; then
    decline "${sha} is not the merge commit of exactly one merged pull request"
    return 0
  fi
  pr_number="$(jq -r '.[0].n' <<<"${prs}")"
  head_sha="$(jq -r '.[0].h' <<<"${prs}")"
  if ! head_tree="$(commit_json "${head_sha}" | jq -r '.tree.sha // empty')" || [ "${head_tree}" != "${mtree}" ]; then
    decline "PR head ${head_sha} tree '${head_tree:-unreadable}' differs from merge tree ${mtree} (out-of-date PR)"
    return 0
  fi

  for family in ${families}; do
    image="ghcr.io/${OWNER}/${family}"
    if ! json="$(docker buildx imagetools inspect "${image}:tree-${mtree}" --format '{{json .}}' 2>/dev/null)"; then
      decline "${family}: no tree-${mtree} image"
      return 0
    fi
    digest="$(jq -r '.manifest.digest // empty' <<<"${json}")"
    if ! [[ "${digest}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
      decline "${family}: tree-${mtree} has no readable digest"
      return 0
    fi
    for platform in linux/amd64 linux/arm64; do
      rev="$(jq -r --arg p "${platform}" '.image[$p].config.Labels["org.opencontainers.image.revision"] // empty' <<<"${json}")"
      if [ "${rev}" != "${head_sha}" ]; then
        decline "${family}: ${platform} revision label '${rev}' is not the PR head ${head_sha}"
        return 0
      fi
    done
    if ! att="$(gh attestation verify "oci://${image}@${digest}" --repo "${REPO}" --signer-workflow "${signer}" --format json 2>/dev/null)"; then
      decline "${family}: no verifiable build provenance for ${digest} from ${signer}"
      return 0
    fi
    if ! jq -e 'type == "array" and length > 0' >/dev/null 2>&1 <<<"${att}"; then
      decline "${family}: provenance for ${digest} is empty or unreadable"
      return 0
    fi
    stray="$(jq -r --arg ref "refs/pull/${pr_number}/merge" '[.[].verificationResult.signature.certificate | select((.githubWorkflowTrigger == "pull_request" and .sourceRepositoryRef == $ref) | not)] | length' <<<"${att}" 2>/dev/null)" || stray=1
    if [ "${stray}" != 0 ]; then
      decline "${family}: provenance for ${digest} holds a certificate that is not a pull_request run on refs/pull/${pr_number}/merge"
      return 0
    fi
    while IFS= read -r src; do
      if ! [[ "${src}" =~ ^[0-9a-f]{40}$ ]] || ! src_json="$(commit_json "${src}")"; then
        decline "${family}: cannot read provenance source commit '${src}'"
        return 0
      fi
      if [ "$(jq -r '.tree.sha // empty' <<<"${src_json}")" != "${mtree}" ]; then
        decline "${family}: provenance source ${src} tree differs from merge tree ${mtree}"
        return 0
      fi
      if ! jq -e --arg h "${head_sha}" '[.parents[].sha] | index($h)' >/dev/null 2>&1 <<<"${src_json}"; then
        decline "${family}: provenance source ${src} does not contain PR head ${head_sha}"
        return 0
      fi
    done < <(jq -r '[.[].verificationResult.signature.certificate.sourceRepositoryDigest] | unique | .[]' <<<"${att}")
    digests+=("${digest}")
  done

  local i=0 short="${sha:0:7}"
  for family in ${families}; do
    image="ghcr.io/${OWNER}/${family}"
    if ! docker buildx imagetools create -t "${image}:sha-${short}" "${image}@${digests[${i}]}"; then
      decline "${family}: copying ${digests[${i}]} to sha-${short} failed"
      return 0
    fi
    i=$((i + 1))
  done
  echo "promote: every family copied tree-${mtree} -> sha-${short} (PR ${pr_number}, head ${head_sha})"
  promoted=true
}

case "${1:-}" in
  gate) gate ;;
  promote) promote ;;
  *)
    echo "usage: image_promote.sh gate|promote" >&2
    exit 2
    ;;
esac
emit hotfix "${hotfix}"
emit tree "${tree}"
emit promoted "${promoted}"
emit sha "${sha}"
exit 0
