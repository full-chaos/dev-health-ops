package livee2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/envelopemint"
)

const usage = `usage: cigate live-e2e <verb>
  assert-health FILE | assert-meta FILE | assert-home FILE
  edge-request DOCUMENTS OPERATION ORG
  mint-token                             (env E2E_SESSION_ID, E2E_ISSUER_URL, E2E_ISSUER_KEY_FILE; optional E2E_USER_ID, E2E_ORG_ID, E2E_ISSUER_KID)
  valkey-ping                            (env REDIS_URL)
`

// Run is `cigate live-e2e`. Exit codes: 0 ok, 1 a check or step failed, 2 usage.
func Run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fail := func(err error) int {
		fmt.Fprintf(stderr, "live-e2e %s: %s\n", args[0], err)
		return 1
	}
	need := func(n int) bool {
		if len(args) != n+1 {
			fmt.Fprint(stderr, usage)
			return false
		}
		return true
	}
	switch args[0] {
	case "assert-health", "assert-meta", "assert-home":
		if !need(1) {
			return 2
		}
		payload, err := os.ReadFile(args[1])
		if err != nil {
			return fail(err)
		}
		switch args[0] {
		case "assert-health":
			err = AssertHealth(payload)
		case "assert-meta":
			err = AssertMeta(payload)
		default:
			err = AssertHome(payload, getenv("FIXTURE_PROVIDER"))
		}
		if err != nil {
			return fail(err)
		}
		return 0
	case "edge-request":
		if !need(3) {
			return 2
		}
		documents, err := os.ReadFile(args[1])
		if err != nil {
			return fail(err)
		}
		body, err := EdgeRequest(documents, args[2], args[3])
		if err != nil {
			return fail(err)
		}
		_, _ = stdout.Write(body)
		return 0
	case "mint-token":
		if !need(0) {
			return 2
		}
		userID, err := uuidEnv(getenv, "E2E_USER_ID", E2EUserID)
		if err != nil {
			return fail(err)
		}
		orgID, err := uuidEnv(getenv, "E2E_ORG_ID", DefaultOrgID)
		if err != nil {
			return fail(err)
		}
		sessionID, err := uuidEnv(getenv, "E2E_SESSION_ID", "")
		if err != nil {
			return fail(err)
		}
		issuer := getenv("E2E_ISSUER_URL")
		if issuer == "" {
			return fail(errors.New("E2E_ISSUER_URL is not set"))
		}
		kid := getenv("E2E_ISSUER_KID")
		if kid == "" {
			kid = DefaultIssuerKeyID
		}
		key, err := envelopemint.LoadPrivateKeyFromFile(getenv("E2E_ISSUER_KEY_FILE"))
		if err != nil {
			return fail(err)
		}
		token, err := MintToken(key, kid, issuer, userID, orgID, sessionID, time.Now().UTC(), uuid.NewString())
		if err != nil {
			return fail(err)
		}
		fmt.Fprintln(stdout, token)
		return 0
	case "valkey-ping":
		if !need(0) {
			return 2
		}
		if err := ValkeyPing(ctx, getenv("REDIS_URL")); err != nil {
			return fail(err)
		}
		return 0
	}
	fmt.Fprint(stderr, usage)
	return 2
}

// uuidEnv reads name as a UUID; an unset name takes fallback, and an empty fallback makes it required.
func uuidEnv(getenv func(string) string, name, fallback string) (uuid.UUID, error) {
	raw := getenv(name)
	if raw == "" {
		raw = fallback
	}
	if raw == "" {
		return uuid.UUID{}, fmt.Errorf("%s is not set", name)
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%s is not a UUID", name)
	}
	return id, nil
}
