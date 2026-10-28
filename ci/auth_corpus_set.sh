#!/usr/bin/env bash
# CHAOS-11670: the one source of what the external auth corpus is made of. Sourced by ci/auth_corpus_external.sh (which runs
# `go test -tags integration` on the tested package) and by ci/auth_corpus_queue_relevance.sh (which lists the live
# `go list -deps` closure of the roots, and of the tested package with its tests and the integration tag).
# AUTH_CORPUS_ROOTS: the go-api auth route packages, the apiservice top level and every command the corpus images build
# (every go build of docker/go-worker.Dockerfile, docker/providerstub.Dockerfile and docker/smtpsink.Dockerfile; internal/cigates/workflows
# parses them and tests that each is a root).
# shellcheck disable=SC2034
AUTH_CORPUS_ROOTS=(./internal/api/session ./internal/api/sso ./internal/api/policy ./internal/apiservice ./cmd/dho ./cmd/auth-service ./cmd/dev-health-worker-migrate ./cmd/auth-migrate ./internal/testsupport/providerstub/cmd/providerstub ./internal/testsupport/smtpcapture/cmd/smtpsink)
# shellcheck disable=SC2034
AUTH_CORPUS_TEST_PACKAGE=./internal/apiservice/authflowvenue
