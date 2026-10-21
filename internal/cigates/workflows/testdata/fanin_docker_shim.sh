if [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "inspect" ]; then
  ref="$4"
  case "$ref" in
    *:latest)
      case "$SCENARIO" in
        absent)
          echo "ERROR: ${ref}: not found" >&2
          exit 1
          ;;
        unknown)
          echo "ERROR: failed to do request: connection reset by peer" >&2
          exit 1
          ;;
        empty_success)
          # exit 0 but print nothing: the rc==0-yet-empty-stdout case.
          exit 0
          ;;
        unlabelled)
          # exit 0, valid JSON, but no revision label on either platform
          # -- falls through to the digest-walk fallback, same as
          # dev-hops-api during this PR's own migration.
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{}}},"linux/arm64":{"config":{"Labels":{}}}}}'
          exit 0
          ;;
        labelled)
          # BOTH platforms carry the SAME revision label -- this is the
          # only scenario that reaches the actual descendant-vs-decline
          # `git merge-base --is-ancestor` check, the thing CHAOS-4947
          # exists to make. Whether it's a descendant is controlled by
          # the `git` shim below via $MERGE_BASE_IS_ANCESTOR, not here.
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}},"linux/arm64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}}}}'
          exit 0
          ;;
        disagree)
          # codex round 6 (bigboy), P2: the two platforms carry DIFFERENT,
          # both non-empty revision labels -- a genuinely ambiguous read
          # (the workflow itself has no way to tell which platform is
          # authoritative), distinct from `unlabelled` (both empty,
          # legitimately falls through to the digest-walk fallback).
          echo '{"manifest":{"digest":"sha256:aaaa"},"image":{"linux/amd64":{"config":{"Labels":{"org.opencontainers.image.revision":"'"${LABELLED_REVISION}"'"}}},"linux/arm64":{"config":{"Labels":{"org.opencontainers.image.revision":"deadfeed00000000000000000000000000000000"}}}}}'
          exit 0
          ;;
        *)
          echo "unrecognized SCENARIO: ${SCENARIO}" >&2
          exit 99
          ;;
      esac
      ;;
    *)
      # This branch answers BOTH the just-published sha- source-tag probe
      # AND the digest-walk's per-candidate sha- probes (same ref shape,
      # `<image>:sha-<short>`) -- they're distinguished only by which
      # short-sha is being asked about, not by any other structural cue.
      #
      # codex round 2, P3 (my own least-sure item from round 2's prompt,
      # confirmed both ways): the harness's `git log` shim always returned
      # empty history, so the digest-walk's actual found-a-match path
      # (walk N candidates, find one whose digest equals :latest's,
      # `break`, tag from it) was NEVER exercised -- only "walked, found
      # nothing, declined". A regression in the match/break/found_base
      # logic passed every existing row. If $DIGEST_WALK_MATCH_SHORT is
      # set, the candidate whose short-sha matches it gets the SAME
      # digest as :latest's (sha256:aaaa, the `unlabelled` scenario's
      # fixed digest) -- every OTHER candidate gets the existing default
      # (sha256:bbbb, guaranteed != aaaa), so the walk must actually
      # iterate past real misses to find the real match, not just accept
      # the first candidate it's handed.
      candidate_short="${ref##*:sha-}"
      # 4752-go's peer read of #2167 (r3 order, team-lead): the source-tag
      # probe (this SAME ref shape, `<image>:sha-deadbee` for the fixed
      # $BUILT_SHA every test here uses) had no scenario control at all --
      # it always "found" the ref, so the source_rc==1/rc==2 branches at
      # the call site (SKIP vs `::error::` decline) were never exercised.
      # $SOURCE_TAG_SCENARIO, when set, answers ONLY for that exact
      # short-sha (never a digest-walk candidate's, which uses a
      # different, unrelated sha) -- everything else keeps the prior
      # unconditional-success behaviour.
      # codex round 4, P2: a MIXED run (some families UNKNOWN, others
      # CONFIRMED ABSENT, none actually found) needed its own per-family
      # override to construct -- $SOURCE_TAG_SCENARIO alone answers the
      # SAME way for every family (they share one short_sha). When
      # $SOURCE_TAG_OVERRIDE_FAMILY is set and this ref's image name
      # matches it, $SOURCE_TAG_OVERRIDE_SCENARIO wins for THIS family
      # only; every other family still gets the uniform
      # $SOURCE_TAG_SCENARIO.
      family_scenario="${SOURCE_TAG_SCENARIO:-}"
      if [ -n "${SOURCE_TAG_OVERRIDE_FAMILY:-}" ]; then
        case "${ref}" in
          *"/${SOURCE_TAG_OVERRIDE_FAMILY}:sha-"*)
            family_scenario="${SOURCE_TAG_OVERRIDE_SCENARIO:-}"
            ;;
        esac
      fi
      if [ -n "${family_scenario}" ] && [ "${candidate_short}" = "deadbee" ]; then
        case "${family_scenario}" in
          absent)
            echo "ERROR: ${ref}: not found" >&2
            exit 1
            ;;
          unknown)
            echo "ERROR: failed to do request: connection reset by peer" >&2
            exit 1
            ;;
          *)
            echo "unrecognized source-tag scenario: ${family_scenario}" >&2
            exit 99
            ;;
        esac
      fi
      if [ -n "${DIGEST_WALK_MATCH_SHORT:-}" ] && [ "${candidate_short}" = "${DIGEST_WALK_MATCH_SHORT}" ]; then
        echo '{"manifest":{"digest":"sha256:aaaa"}}'
      else
        echo '{"manifest":{"digest":"sha256:bbbb"}}'
      fi
      exit 0
      ;;
  esac
elif [ "$1" = "buildx" ] && [ "$2" = "imagetools" ] && [ "$3" = "create" ]; then
  echo "CREATE: $*" >> "${RECORD_FILE}"
  exit 0
fi
exit 0
