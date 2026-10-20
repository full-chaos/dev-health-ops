package livee2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/google/uuid"
)

const usage = `usage: cigate live-e2e <verb>
  assert-health FILE | assert-meta FILE | assert-home FILE
  edge-request DOCUMENTS OPERATION ORG
  mint-token                             (env E2E_ORG_ID, SETTINGS_ENCRYPTION_KEY)
  jwt-secret                             (env ENC_KEY_INPUT)
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
	case "jwt-secret":
		if !need(0) {
			return 2
		}
		key := getenv("ENC_KEY_INPUT")
		if key == "" {
			return fail(fmt.Errorf("ENC_KEY_INPUT is not set"))
		}
		fmt.Fprintln(stdout, JWTSecret(key))
		return 0
	case "mint-token":
		if !need(0) {
			return 2
		}
		orgID := getenv("E2E_ORG_ID")
		if orgID == "" {
			orgID = DefaultOrgID
		}
		key := getenv("SETTINGS_ENCRYPTION_KEY")
		if key == "" {
			key = DefaultEncryptionKey
		}
		token, err := MintToken(JWTSecret(key), orgID, time.Now().UTC(), uuid.NewString())
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
