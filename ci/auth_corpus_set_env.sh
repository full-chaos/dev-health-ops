#!/usr/bin/env bash
# PLACEHOLDER (CHAOS-10160, red stage): the hook the external auth corpus runs when a case changes the target's
# environment (ci/auth_corpus_set_env.sh NAME VALUE; an empty VALUE means unset). This stage does nothing, so the
# target keeps its environment and the corpus stops at the first case that depends on the change, "register: solo,
# auto-org off". The next commit replaces this file with the script that recreates go-api through compose.
# The real script is added by the next commit of this pull request; its tests (internal/cigates) fail on this one.
exit 0
