#!/usr/bin/env bash
# stack.sh [-s <set>] <verb> [args] (CHAOS-8693): the one entry point for the bigboy compose stack.
# It owns the project name, the working directory, the ordered file list, the env file and the profiles:
# all of them come from stack.files, none from the caller. Verbs (any other is refused):
#   files               print the effective file list, one per line (no docker)
#   config [--quiet|--images|--services]   no flag: NAME=<length> only (compose-config-redacted.sh); a flag: passed on
#   ps [args]           compose ps
#   logs [args]         compose logs (default --tail=200)
#   up <service>...     compose up -d --no-deps --no-build <service>...  (at least one service; never the whole stack)
#   restart <service>   compose restart <service>
#   repin-web           bigboy-repin-web.sh (WEB_REPIN as there)
# Before a mutating verb it prints the file list and the pinned image refs. After it, one line goes to
# $BIGBOY_ROOT/_records/stack-runs.log: time, verb, service, tools sha, image digests, rc.
set -euo pipefail
# the chain comes from stack.files only: no inherited compose variable may add a file, profile, project or env file
for v in $(compgen -e | grep '^COMPOSE_' || true); do unset "$v"; done
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
R=$(cd "${BIGBOY_ROOT:-/home/ubuntu/devhealth}" 2>/dev/null && pwd) || { echo "stack.sh: BIGBOY_ROOT is not a directory" >&2; exit 2; }
T=$(cd "${BIGBOY_TOOLS_DIR:-$HERE}" 2>/dev/null && pwd) || { echo "stack.sh: BIGBOY_TOOLS_DIR is not a directory" >&2; exit 2; }
LIST=$HERE/stack.files
SET=stack
die() { echo "stack.sh: $*" >&2; exit 2; }

if [ "${1:-}" = "-s" ]; then SET=${2:?set name}; shift 2; fi
VERB=${1:-}; [ -n "$VERB" ] || die "usage: stack.sh [-s set] <files|config|ps|logs|up|restart|repin-web> [args]"
shift
case $VERB in files|config|ps|logs|up|restart|repin-web) ;; *) die "unknown verb '$VERB'" ;; esac

[ -f "$LIST" ] || die "file list $LIST is missing"
grep -qE "^${SET}[[:space:]]" "$LIST" || die "unknown set '$SET' in $LIST"
PROJECT=""; ENVF=(); PROFILES=(); FILES=(); NEEDS=()
read_set() {
  local want=$1 s kind val
  while read -r s kind val _; do
    [ "$s" = "$want" ] && [ "$kind" = include ] && read_set "$val"
  done < <(grep -vE '^[[:space:]]*(#|$)' "$LIST")
  while read -r s kind val _; do
    [ "$s" = "$want" ] || continue
    case $kind in
      include) ;;
      project) PROJECT=$val ;;
      env-file) ENVF+=("$val") ;;
      profile) PROFILES+=("$val") ;;
      root) FILES+=("$R/$val") ;;
      tools) FILES+=("$T/$val") ;;
      needs) NEEDS+=("$val") ;;
      *) die "bad kind '$kind' in $LIST" ;;
    esac
  done < <(grep -vE '^[[:space:]]*(#|$)' "$LIST")
}
read_set "$SET"
[ -n "$PROJECT" ] || die "set '$SET' has no project line"
[ "${#FILES[@]}" -gt 0 ] || die "set '$SET' has no compose files"
for f in "${FILES[@]}"; do [ -f "$f" ] || die "overlay file missing: $f"; done
for e in "${ENVF[@]}"; do [ -f "$R/$e" ] || die "env file missing: $R/$e"; done

if [ "$VERB" = files ]; then printf '%s\n' "${FILES[@]}"; exit 0; fi

ARGS=(-p "$PROJECT")
for e in "${ENVF[@]}"; do ARGS+=(--env-file "$e"); done
for p in "${PROFILES[@]}"; do ARGS+=(--profile "$p"); done
for f in "${FILES[@]}"; do ARGS+=(-f "$f"); done
cd "$R"
export BIGBOY_ROOT="$R" BIGBOY_TOOLS_DIR="$T"
export DHO_API_CH_USERS_XML="${DHO_API_CH_USERS_XML:-$R/.go-api-dev/dho_api_ch.xml}"
if [ -z "${BIGBOY_OPERATOR_IMAGE:-}" ] && grep -qs 'BIGBOY_OPERATOR_IMAGE' "${FILES[@]}"; then
  BIGBOY_OPERATOR_IMAGE=$(docker compose -p "$PROJECT" ps --format '{{.Image}}' go-reconciler 2>/dev/null | head -1 || true)
  [ -n "$BIGBOY_OPERATOR_IMAGE" ] || die "BIGBOY_OPERATOR_IMAGE is unset and no running go-reconciler names it; export it"
fi
export BIGBOY_OPERATOR_IMAGE="${BIGBOY_OPERATOR_IMAGE:-}"
if [ -z "${RIVER_DHO_IMAGE:-}" ]; then
  for f in "${FILES[@]}"; do
    [ "${f##*/}" = compose.bigboy.images.yml ] && RIVER_DHO_IMAGE=$(grep -oE 'ghcr.io/full-chaos/dev-health-go-dho@sha256:[0-9a-f]{64}' "$f" | head -1 || true)
  done
fi
export RIVER_DHO_IMAGE="${RIVER_DHO_IMAGE:-}" DHO_SMOKE_DIR="${DHO_SMOKE_DIR:-$T}"
for v in "${NEEDS[@]}"; do [ -n "${!v:-}" ] || die "set '$SET' needs $v exported by its step script"; done
# an env file's COMPOSE_PROFILES would add profiles: the process value (the declared profiles only) wins over it
COMPOSE_PROFILES=$(IFS=,; echo "${PROFILES[*]}"); export COMPOSE_PROFILES

valid_service() { [[ $1 =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || die "not a service name: refused"; }
compose() { docker compose "${ARGS[@]}" "$@"; }
MUTATING=0; case $VERB in up|restart|repin-web) MUTATING=1 ;; esac
if [ "$MUTATING" = 1 ]; then
  echo "stack.sh set=$SET project=$PROJECT root=$R" >&2
  printf 'stack.sh file %s\n' "${FILES[@]}" >&2
fi
ERRF=$(mktemp); trap 'rm -f "$ERRF"' EXIT
# compose's stderr can quote a resolved value, so only the NAME of a missing required variable is surfaced
safe_config() {
  local rc=0
  compose config "$@" 2>"$ERRF" || rc=$?
  [ "$rc" -eq 0 ] || grep -oE 'required variable [A-Za-z_][A-Za-z0-9_]* is missing' "$ERRF" >&2 || true
  return "$rc"
}
IMAGES=$(safe_config --images | sort -u; exit "${PIPESTATUS[0]}") || die "compose config failed: no verb runs on a chain that does not render"
if [ "$MUTATING" = 1 ]; then while read -r img; do echo "stack.sh image $img" >&2; done <<<"$IMAGES"; fi

run_logged() {
  local rc=0 svc=${1:-} sha digests
  shift
  "$@" || rc=$?
  [ "$VERB" != repin-web ] || { IMAGES=$(safe_config --images | sort -u; exit "${PIPESTATUS[0]}") || IMAGES=unresolved; while read -r img; do echo "stack.sh image $img" >&2; done <<<"$IMAGES"; }
  sha=$(git -C "$T" rev-parse HEAD 2>/dev/null || echo unknown)
  digests=$(printf '%s\n' "$IMAGES" | sed -E 's#.*/##; s#@sha256:(.{8}).*#@\1#' | paste -sd, -)
  mkdir -p "$R/_records"
  printf '%s verb=%s set=%s service=%s sha=%s images=%s rc=%s\n' "$(date -u +%FT%TZ)" "$VERB" "$SET" "${svc:--}" "$sha" "$digests" "$rc" >> "$R/_records/stack-runs.log"
  return "$rc"
}

case $VERB in
  config)
    case ${1:-} in
      "") rm -f "$ERRF"; exec "$HERE/compose-config-redacted.sh" "${ARGS[@]}" ;;
      --quiet|--images|--services) [ $# -eq 1 ] || die "config takes one flag"; safe_config "$1" ;;
      *) die "config accepts no argument, --quiet, --images or --services" ;;
    esac ;;
  ps) compose ps "$@" ;;
  logs) [ $# -gt 0 ] || set -- --tail=200; compose logs "$@" ;;
  up) [ $# -gt 0 ] || die "up needs at least one service name"; for a in "$@"; do valid_service "$a"; done; run_logged "$*" compose up -d --no-deps --no-build "$@" ;;
  restart) [ $# -eq 1 ] || die "restart takes exactly one service name"; valid_service "$1"; run_logged "$1" compose restart "$1" ;;
  repin-web) run_logged web "$HERE/bigboy-repin-web.sh" ;;
esac
