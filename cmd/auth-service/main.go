// Command auth-service is the Go Auth Control Plane's HTTP service
// (CHAOS-4881, Wave 1 of CHAOS-3274; plan ratified 2026-09-02).
//
// This file is a THIN ENTRYPOINT and must stay one. chris, 2026-09-02,
// ratified alongside the Wave 1 plan: "eventually all the binaries we have for
// servicing will likely need to be plugins to replace dev-hops" -- so flag and
// environment parsing, dependency wiring, listener orchestration and graceful
// shutdown all live in internal/auth/authruntime, and every domain concern
// (identity, session, token, principal, authorization, provisioning, audit)
// lives behind an internal/ interface with no dependency on main and no
// dependency on HTTP transport types. Re-hosting this service as a subcommand
// of a future unified binary is then a call to authruntime.Execute, not a
// rewrite.
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
