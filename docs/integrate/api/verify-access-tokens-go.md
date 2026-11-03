---
page_id: int-verify-go
summary: Verify an EdDSA access token in a Go service with the dev-health-go authverify package, a JWKS client with cache, and a handler that tells a refusal (401) from an unavailable key source (503).
content_type: task-guide
owner: platform-api
source_of_truth:
  - internal/auth/accessverify/accessverify.go
  - internal/apiservice/accessverify.go
applicability: current
lifecycle: active
---

# Verify access tokens in a Go service

This guide shows a Go HTTP handler that verifies an EdDSA user access token (`access-token.v1`) with the
`authverify` package of `github.com/full-chaos/dev-health-go`, version v0.37.0. The example is the same
construction that go-api and query-api use. The guide covers Go only.

## Before you start

- You know the issuer URL of the deployment (`AUTH_TOKEN_ISSUER_URL` of auth-service) and your service can
  reach it over `https`.
- You know the audience your service accepts. The verifier takes one audience:
  `dev-health-api`, `auth-service`, `acr-api` or `acr-mcp`. See
  [Access-token claims and validation](../../reference/auth/access-token-claims.md#audiences-and-scopes).

```sh
go get github.com/full-chaos/dev-health-go@v0.37.0
```

## The example

```go
package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-go/authverify"
)

func main() {
	issuer := os.Getenv("AUTH_ACCESS_TOKEN_ISSUER")

	keys, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: issuer})
	if err != nil {
		log.Fatal(err)
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{
		Issuer:   issuer,
		Audience: "dev-health-api",
		Keys:     keys,
		Leeway:   30 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/whoami", func(w http.ResponseWriter, r *http.Request) {
		bearer, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		claims, err := verifier.Verify(bearer, time.Now())
		var unavailable *authverify.UnavailableError
		switch {
		case errors.As(err, &unavailable):
			w.Header().Set("Retry-After", "5")
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		case err != nil:
			log.Printf("token refused: reason=%s", authverify.ReasonOf(err))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		fmt.Fprintf(w, "subject=%s session=%s\n", claims.Subject, claims.SessionID)
	})

	log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Set `AUTH_ACCESS_TOKEN_ISSUER` to the issuer URL, byte for byte. Start the service, and send a request with
`Authorization: Bearer <token>`. Never print a token.

## What the code does

1. `NewJWKSClient` pins one issuer and does no network call. The first token causes the first fetch of the
   discovery document and the JWKS. The client caches the keys for 5 minutes, fetches again at most once in
   30 seconds, and keeps the last good keys for 10 more minutes if the issuer is down. See
   [JWKS and discovery](../../reference/auth/jwks-and-discovery.md#what-a-verifier-does).
2. `NewVerifier` fixes the issuer and the audience. It refuses a leeway above 60 seconds or with a fraction
   of a second. A leeway of 0 is the default.
3. `Verify` checks the token in a fixed order and returns the claims. The order is the contract, the
   signature, the issuer, the audience, `nbf` and `exp`.
4. A refusal wraps `authverify.ErrRefused`. `authverify.ReasonOf(err)` gives the reason: `contract`,
   `unknown_kid`, `signature`, `issuer`, `audience`, `not_yet_valid` or `expired`. The text of an error never
   holds a token, a claim or a key.
5. When the key source cannot be read, `Verify` returns `*authverify.UnavailableError` (it wraps
   `authverify.ErrKeySourceUnavailable`). It is not a refusal. Answer `503`, as the example does, and not `401`.
   A client that gets `401` signs in again for nothing.

## What the verifier does not check

`Verify` proves that the issuer signed the token and that the claims fit your service. It does not prove that
the session is still live, that the user is active, or that the user is a member of the organization in
`org_id`. Those are state, not token content. go-api and query-api read that state for every request. See
[how a token is checked](../../reference/auth/access-token-claims.md#how-a-token-is-checked). A service
that needs a revocation to take effect before `exp` must check the session itself.

## Test the code

- Use `authverify.StaticKeys` as the `Keys` field with a key that your test holds, and sign a test token
  with that key. This needs no network.
- Test a token with a wrong audience, an expired token, and a `kid` that the key map does not hold. Each must
  answer `401`.
- Test a key source that fails. It must answer `503`.

See [Troubleshoot token verification](../troubleshooting/token-verification.md) when a real token is refused.
