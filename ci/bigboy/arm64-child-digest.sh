#!/usr/bin/env bash
# Sourced helper (CHAOS-10137). _records/images.env pins the linux/arm64 CHILD digest of each image, not the
# multi-arch index digest that `imagetools inspect --format '{{json .Manifest}}'` prints.
# arm64_child_digest <image-ref>: prints the linux/arm64 child digest and returns 0; prints nothing and returns 1
# when the ref does not resolve or its index has no linux/arm64 child.
arm64_child_digest() {
  local raw d
  raw=$(docker buildx imagetools inspect --raw "$1" 2>/dev/null) || return 1
  d=$(printf '%s' "$raw" | jq -r '[.manifests[]? | select(.platform.os == "linux" and .platform.architecture == "arm64")][0].digest // empty' 2>/dev/null) || return 1
  [[ ${d:-} == sha256:* ]] || return 1
  printf '%s\n' "$d"
}
