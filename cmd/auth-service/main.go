// Command auth-service is the Go Auth Control Plane's HTTP service
// (CHAOS-4881, Wave 1 of CHAOS-3274; plan ratified 2026-09-02).
//
// This file is a THIN ENTRYPOINT and must stay one: flag and environment
// parsing, dependency wiring, listener orchestration and graceful shutdown all
// live in internal/auth/authruntime, and every domain concern lives behind an
// internal/ interface with no dependency on main and no dependency on HTTP
// transport types. cmd/auth-service/main_test.go enforces the shape by parsing
// this file's AST.
//
// Design note (chris, 2026-09-02): "eventually all the binaries we have for
// servicing will likely need to be plugins to replace dev-hops" -- keeping
// main thin is what leaves that door open. It is a NOTE, not a deliverable:
// chris scoped it the same day -- "we don't need to write the shell/thin
// client replacement yet ... if it's only for the auth control plane, right
// now, that's fine" -- so there is deliberately no plugin host here, no shared
// servicing-binary framework, and no abstraction beyond what CHAOS-4881 needs.
// Do not build one on the strength of this comment.
//
// # The service is DORMANT
//
// Nothing in production calls it. No business route is mounted
// (authruntime.Routes returns an empty set), no production token is issued
// (internal/auth/keystore verifies custody and derives the PUBLIC half only;
// it holds no minting material), Auth.js and the Ops login path are untouched,
// and no resource API depends on this process. What IS live: /healthz,
// /readyz and /metrics on the operator listener, and the API listener's
// middleware stack answering every path with this service's own 404 envelope.
//
// Readiness fails CLOSED. Two required checks are registered -- "postgres"
// (the pool answers AND the auth-owned schema is visible to this role) and
// "signing_key" (the configured key file passes the ACP-ADR-02 §3 custody
// contract and parses as Ed25519) -- and both are re-run live, under a bounded
// deadline, on every /readyz request. See internal/auth/authconfig's package
// doc for the line drawn between a configuration fault, which refuses startup,
// and a dependency fault, which starts and reports not-ready.
package main

import "github.com/full-chaos/dev-health-ops/internal/auth/authruntime"

func main() { authruntime.Main() }
