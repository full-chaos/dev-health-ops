package ssoseed

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type recordingDB struct {
	sql  []string
	args [][]any
}

func (r *recordingDB) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	r.sql = append(r.sql, sql)
	r.args = append(r.args, args)
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

const testOrg = "00000000-0000-4000-8000-0000000000b1"
const testProvider = "00000000-0000-4000-8000-0000000000c1"

func validProvider(protocol string) Provider {
	now := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	config := `{"k":"v"}`
	return Provider{ID: testProvider, OrgID: testOrg, Name: "p", Protocol: protocol, Status: "active", DefaultRole: "member",
		Config: &config, CreatedAt: now, UpdatedAt: now}
}

// The insert names exactly the columns of the production row except the last_* columns, which stay NULL.
func TestInsertWritesTheProductionColumnsWithoutTheLastColumns(t *testing.T) {
	db := &recordingDB{}
	if err := Insert(context.Background(), db, validProvider("oidc")); err != nil {
		t.Fatal(err)
	}
	if len(db.sql) != 1 {
		t.Fatalf("%d statements, want 1", len(db.sql))
	}
	head := regexp.MustCompile(`(?s)\((.*?)\)\s*VALUES`).FindStringSubmatch(db.sql[0])
	if head == nil {
		t.Fatalf("no column list in %q", db.sql[0])
	}
	var got []string
	for _, c := range strings.Split(head[1], ",") {
		got = append(got, strings.TrimSpace(c))
	}
	var want []string
	for _, c := range Columns() {
		if !strings.HasPrefix(c, "last_") {
			want = append(want, c)
		}
	}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("insert columns %v, production columns without last_* %v", got, want)
	}
	if len(db.args[0]) != len(want) {
		t.Errorf("%d arguments for %d columns", len(db.args[0]), len(want))
	}
}

func TestInsertRefusesAnUnknownProtocolAndWritesNothing(t *testing.T) {
	for _, protocol := range []string{"", "ldap", "OIDC", "oauth_github"} {
		db := &recordingDB{}
		if err := Insert(context.Background(), db, validProvider(protocol)); err == nil {
			t.Errorf("protocol %q accepted", protocol)
		}
		if len(db.sql) != 0 {
			t.Errorf("protocol %q: %d statements written", protocol, len(db.sql))
		}
	}
	for _, protocol := range []string{"oidc", "saml"} {
		if !ValidProtocol(protocol) {
			t.Errorf("protocol %q refused", protocol)
		}
	}
}

func TestInsertRefusesANonUUIDIdOrOrg(t *testing.T) {
	for _, mutate := range []func(*Provider){func(p *Provider) { p.ID = "x" }, func(p *Provider) { p.OrgID = "x" }} {
		p := validProvider("oidc")
		mutate(&p)
		db := &recordingDB{}
		if err := Insert(context.Background(), db, p); err == nil || len(db.sql) != 0 {
			t.Errorf("err %v, %d statements", err, len(db.sql))
		}
	}
}

// The stored secret is sealed with the production cipher: not the plain text, opened by that cipher with the same key and by
// no other key.
func TestSealIsOpenedByTheProductionCipherWithTheSameKeyOnly(t *testing.T) {
	const key, secret = "ssoseed-test-key-0123456789", "client-secret-value-xyz"
	stored, err := Seal(key, "", secret)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored, secret) {
		t.Fatalf("the stored object holds the plain secret: %q", stored)
	}
	var object map[string]string
	if err := json.Unmarshal([]byte(stored), &object); err != nil || len(object) != 1 {
		t.Fatalf("stored %q is not the one-key object: %v", stored, err)
	}
	sealed := object["client_secret"]
	if !strings.HasPrefix(sealed, "v1:") {
		t.Errorf("sealed value %q has no v1 prefix", sealed)
	}
	same, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key), "")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := same.Decrypt(secrets.NewValue(sealed))
	if err != nil || string(plain) != secret {
		t.Fatalf("production cipher, same key: %q, %v; want the secret", plain, err)
	}
	other, err := providerfoundation.NewFernetDecryptor(secrets.NewValue(key+"x"), "")
	if err != nil {
		t.Fatal(err)
	}
	if plain, err := other.Decrypt(secrets.NewValue(sealed)); err == nil {
		t.Fatalf("another key opened the sealed secret: %q", plain)
	}
}

func TestConfigBuildersRefuseAnEmptyValue(t *testing.T) {
	if _, err := OIDCConfig("https://i", "c", "https://a", "https://t", "https://u", ""); err == nil {
		t.Error("an OIDC config with an empty jwks uri was built")
	}
	if _, err := SAMLConfig(testProvider, "e", "https://s", "", "sp", "https://acs"); err == nil {
		t.Error("a SAML config with an empty certificate was built")
	}
}

type noRowQuerier struct{}

func (noRowQuerier) QueryRow(context.Context, string, ...any) pgx.Row { return noRow{} }

type noRow struct{}

func (noRow) Scan(...any) error { return pgx.ErrNoRows }

func TestOrgIDForEmailRefusesAnEmailWithNoOrganization(t *testing.T) {
	if org, err := OrgIDForEmail(context.Background(), noRowQuerier{}, "nobody@example.com"); err == nil || org != "" || errors.Is(err, pgx.ErrNoRows) {
		t.Errorf("org %q err %v, want a named refusal", org, err)
	}
}
