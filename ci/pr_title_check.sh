#!/usr/bin/env bash
# CHAOS-12791: the house PR-title rule (root AGENTS.md): <type>(<area>): CHAOS-<n> <description>.
# Inputs (env): EVENT, AUTHOR, TITLE. On merge_group: HEAD_REF, REPO, GH_TOKEN (the PR is read via REST).
# This file holds the one pattern of this repo.
set -euo pipefail

RE='^(feat|bug|enhancement|chore|fix|docs)\([a-z0-9][a-z0-9._/-]*\): CHAOS-[0-9]+ [^[:space:]].*$'

# "Imperative" is not machine-checkable; the checkable part: the description must not open with an article, a determiner or a
# pronoun (case-insensitive), so "the process exits" and "we need x" fail while "add", "make", "keep" pass.
STOP=' a an the this that these those it its we i our my there '
DESC_RE=': CHAOS-[0-9]+ ([^[:space:]]+)'

event=${EVENT:-pull_request}
author=${AUTHOR:-}
title=${TITLE:-}

if [ "$event" = merge_group ]; then
  if [[ "${HEAD_REF:-}" =~ /pr-([0-9]+)- ]]; then
    pr=${BASH_REMATCH[1]}
  else
    echo "cannot identify the queued pull request from ${HEAD_REF:-}" >&2
    exit 2
  fi
  pj=$(gh api "repos/${REPO:?}/pulls/$pr" --jq '[.user.login, .title] | @tsv')
  author=${pj%%$'\t'*}
  title=${pj#*$'\t'}
fi

if [ "$author" = 'dependabot[bot]' ]; then
  echo "exempt: author is dependabot[bot]"
  exit 0
fi

if ! [[ "$title" =~ $RE ]]; then
  echo "PR title does not match <type>(<area>): CHAOS-<n> <description> with type in feat|bug|enhancement|chore|fix|docs" >&2
  exit 1
fi
ids=$(grep -oE 'CHAOS-[0-9]+' <<<"$title" | wc -l)
if [ "$ids" != 1 ]; then
  echo "PR title must carry exactly one CHAOS-<n> id, found $ids" >&2
  exit 1
fi
[[ "$title" =~ $DESC_RE ]]
first=${BASH_REMATCH[1],,}
if [[ "$STOP" == *" $first "* ]]; then
  echo "PR title description must start with an imperative verb, not '$first' (no article, determiner or pronoun first)" >&2
  exit 1
fi
echo "PR title ok"
