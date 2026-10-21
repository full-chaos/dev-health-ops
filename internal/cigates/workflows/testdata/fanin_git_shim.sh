if [ "$1" = "log" ]; then
  touch "${GIT_LOG_CALLED_FILE}"
  if [ -n "${DIGEST_WALK_CANDIDATES:-}" ]; then
    printf '%s\n' "${DIGEST_WALK_CANDIDATES}"
  fi
  exit 0
fi
if [ "$1" = "merge-base" ] && [ "$2" = "--is-ancestor" ]; then
  touch "${GIT_MERGE_BASE_CALLED_FILE}"
  if [ "${MERGE_BASE_IS_ANCESTOR}" = "true" ]; then
    exit 0
  else
    exit 1
  fi
fi
exit 0
