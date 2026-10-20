package livee2e

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Connector opens the Postgres connection of the fixture steps; the returned func closes it.
type Connector func(ctx context.Context, uri string) (DB, func(), error)

const usage = `usage: cigate live-e2e <verb>
  assert-health FILE | assert-meta FILE | assert-home FILE
  edge-request DOCUMENTS OPERATION ORG
  principal ACTION PRINCIPAL_ID          (env POSTGRES_URI or DATABASE_URI, E2E_ORG_ID)
  auth-token                             (env POSTGRES_URI or DATABASE_URI, E2E_ORG_ID, SETTINGS_ENCRYPTION_KEY)
  jwt-secret                             (env ENC_KEY_INPUT)
  valkey-ping                            (env REDIS_URL)
`

// Run is `cigate live-e2e`. Exit codes: 0 ok, 1 a check or step failed, 2 usage.
func Run(args []string, getenv func(string) string, connect Connector, stdout, stderr io.Writer) int {
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
	readFile := func(path string) ([]byte, error) { return os.ReadFile(path) }
	switch args[0] {
	case "assert-health", "assert-meta", "assert-home":
		if !need(1) {
			return 2
		}
		payload, err := readFile(args[1])
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
		documents, err := readFile(args[1])
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
	case "valkey-ping":
		if !need(0) {
			return 2
		}
		if err := ValkeyPing(ctx, getenv("REDIS_URL")); err != nil {
			return fail(err)
		}
		return 0
	case "principal", "auth-token":
		wantArgs := 0
		if args[0] == "principal" {
			wantArgs = 2
		}
		if !need(wantArgs) {
			return 2
		}
		uri := getenv("POSTGRES_URI")
		if uri == "" {
			uri = getenv("DATABASE_URI")
		}
		if uri == "" {
			return fail(fmt.Errorf("POSTGRES_URI or DATABASE_URI is not set"))
		}
		orgID := getenv("E2E_ORG_ID")
		if orgID == "" {
			orgID = DefaultOrgID
		}
		db, closeDB, err := connect(ctx, strings.Replace(uri, "+asyncpg", "", 1))
		if err != nil {
			return fail(fmt.Errorf("connect: %w", err))
		}
		defer closeDB()
		now := time.Now().UTC()
		if args[0] == "principal" {
			if err := Principal(ctx, db, args[1], args[2], orgID, now); err != nil {
				return fail(err)
			}
			return 0
		}
		if err := SeedE2EIdentity(ctx, db, orgID, now); err != nil {
			return fail(err)
		}
		key := getenv("SETTINGS_ENCRYPTION_KEY")
		if key == "" {
			key = DefaultEncryptionKey
		}
		token, err := MintToken(JWTSecret(key), orgID, now, uuid.NewString())
		if err != nil {
			return fail(err)
		}
		fmt.Fprintln(stdout, token)
		return 0
	}
	fmt.Fprint(stderr, usage)
	return 2
}

type pgxDB struct{ conn *pgx.Conn }

func (d pgxDB) Exec(ctx context.Context, sql string, args ...any) error {
	_, err := d.conn.Exec(ctx, sql, args...)
	return err
}

func (d pgxDB) QueryRow(ctx context.Context, sql string, args ...any) Row {
	return d.conn.QueryRow(ctx, sql, args...)
}

// ConnectPostgres opens a pgx connection to a postgresql:// URI.
func ConnectPostgres(ctx context.Context, uri string) (DB, func(), error) {
	conn, err := pgx.Connect(ctx, uri)
	if err != nil {
		return nil, nil, err
	}
	return pgxDB{conn}, func() { _ = conn.Close(context.Background()) }, nil
}
