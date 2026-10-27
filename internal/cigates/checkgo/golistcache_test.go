package checkgo_test

import "strings"

// Every stand-in `go` of this package runs the real `go list` for check_go.sh, which lists the module several times per stage
// (about 2 s each, serialised by subprocessSlots). The answer depends only on the directory and the arguments of a tree no test
// changes, so the stand-ins keep one answer per directory and arguments in FAKE_GO_LIST_CACHE (a directory TestMain makes and
// removes; the children inherit the variable). A failed list is never kept. Unset, the stand-in runs the real go every time.
const goListCacheShell = `if [ "$1" = "list" ] && [ -n "${FAKE_GO_LIST_CACHE:-}" ]; then
  key="$({ printf '%s\0' "${PWD}" "$@"; } | sha256sum | cut -c1-64)"
  if [ -f "${FAKE_GO_LIST_CACHE}/${key}" ]; then cat "${FAKE_GO_LIST_CACHE}/${key}"; exit 0; fi
  tmp="$(mktemp "${FAKE_GO_LIST_CACHE}/.${key}.XXXXXX")" || exit 1
  "${FAKE_REAL_GO}" "$@" > "${tmp}"; rc=$?
  cat "${tmp}"
  if [ "${rc}" -eq 0 ]; then mv -f "${tmp}" "${FAKE_GO_LIST_CACHE}/${key}"; else rm -f "${tmp}"; fi
  exit "${rc}"
fi
`

// withGoListCache puts the list cache after the shebang of a stand-in go script and names the real go it runs.
func withGoListCache(script, realGo string) string {
	return strings.Replace(script, "#!/usr/bin/env bash\n", "#!/usr/bin/env bash\nFAKE_REAL_GO=\""+realGo+"\"\n"+goListCacheShell, 1)
}
