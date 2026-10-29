package devhealth_test

import (
	"encoding/json"
	"net/url"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/jackc/pgx/v5"
)

// The chart renders connection URIs from credentials (the PgBouncer Secret, the shared and migrate Secrets). A credential
// written raw into the userinfo breaks the URI at a reserved character ('#', '@', '/', ':', '?', '%'): the userinfo or the
// authority ends early and the stored password is wrong. The chart percent-encodes the user, password and database of
// every URI it builds (RFC 3986: urlquery, then "+" -> "%20"); a credential of unreserved characters renders byte for
// byte as before. Every Go reader decodes the userinfo, so the decoded password is the credential.
// The expected strings below are LITERAL (computed once with python quote(safe="")), never re-derived by the code under test.

const (
	uriPgbouncerSecret = "a-dev-health-go-pgbouncer"
	uriSharedSecret    = "a-dev-health-secrets"
	uriMigrateSecret   = "a-dev-health-migrate-secrets"

	resDomain = "p#a@s/s:w?r%d+x[y]"
	resQueue  = "q&=;!$'()*, ~z"
	resCoord  = "c\"d\\e|f<g>h^i`j"
	resCHUser = "ch:user/x"
	resCHPass = "c#h@pw/:?%41&=+"
	resPGUser = "dev@health"
	resPGPass = "p@ss w#rd"
)

func jsonSet(path, value string) []string {
	b, _ := json.Marshal(value)
	return []string{"--set-json", path + "=" + string(b)}
}

func uriCatArgs(parts ...[]string) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func uriStringData(t *testing.T, r keepRender, secret string) map[string]any {
	t.Helper()
	doc := authDoc(r.docs, "Secret", secret)
	if doc == nil {
		t.Fatalf("Secret %s did not render (%d documents)", secret, len(r.docs))
	}
	sd, _ := doc["stringData"].(map[string]any)
	if sd == nil {
		t.Fatalf("Secret %s has no stringData", secret)
	}
	return sd
}

func uriKey(t *testing.T, sd map[string]any, secret, key string) string {
	t.Helper()
	v, ok := sd[key].(string)
	if !ok || v == "" {
		t.Fatalf("Secret %s has no %s", secret, key)
	}
	return v
}

func reservedPgbouncerArgs() []string {
	return uriCatArgs(
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD", resDomain),
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD", resQueue),
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD", resCoord),
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_KEDA_READONLY_PASSWORD", "keda-0004"),
	)
}

func TestPgbouncerURIsEncodeReservedPasswords(t *testing.T) {
	r := keepMustRender(t, "reserved pgbouncer passwords", reservedPgbouncerArgs()...)
	sd := uriStringData(t, r, uriPgbouncerSecret)
	cases := []struct {
		key, want, role, password, host, port string
	}{
		{"POSTGRES_URI", "postgresql://devhealth_domain:p%23a%40s%2Fs%3Aw%3Fr%25d%2Bx%5By%5D@a-dev-health-go-pgbouncer-transaction:6432/devhealth",
			"devhealth_domain", resDomain, "a-dev-health-go-pgbouncer-transaction", "6432"},
		{"WORKER_DATABASE_URI", "postgresql://devhealth_queue:q%26%3D%3B%21%24%27%28%29%2A%2C%20~z@a-dev-health-go-pgbouncer-queue-session:6433/devhealth",
			"devhealth_queue", resQueue, "a-dev-health-go-pgbouncer-queue-session", "6433"},
		{"COORDINATOR_DATABASE_URI", "postgresql://devhealth_coordinator:c%22d%5Ce%7Cf%3Cg%3Eh%5Ei%60j@a-dev-health-go-pgbouncer-coordinator-session:6434/devhealth",
			"devhealth_coordinator", resCoord, "a-dev-health-go-pgbouncer-coordinator-session", "6434"},
	}
	for _, c := range cases {
		got := uriKey(t, sd, uriPgbouncerSecret, c.key)
		if got != c.want {
			t.Errorf("%s is not the percent-encoded literal", c.key)
		}
		u, err := url.Parse(got)
		if err != nil {
			t.Errorf("%s does not parse with net/url", c.key)
			continue
		}
		if pw, _ := u.User.Password(); u.User.Username() != c.role || pw != c.password || u.Hostname() != c.host || u.Port() != c.port || u.Path != "/devhealth" || u.Fragment != "" {
			t.Errorf("%s: net/url does not read back the role, password, host, port and database", c.key)
		}
		cfg, err := pgx.ParseConfig(got)
		if err != nil {
			t.Errorf("%s does not parse with pgx", c.key)
			continue
		}
		if cfg.User != c.role || cfg.Password != c.password || cfg.Host != c.host || cfg.Database != "devhealth" {
			t.Errorf("%s: pgx does not read back the role, password, host and database", c.key)
		}
	}
}

func TestPgbouncerURIsAreByteIdenticalForUnreservedPasswords(t *testing.T) {
	r := keepMustRender(t, "unreserved pgbouncer passwords", keepDummyPasswords...)
	sd := uriStringData(t, r, uriPgbouncerSecret)
	want := map[string]string{
		"POSTGRES_URI":             "postgresql://devhealth_domain:dummy-domain-0001@a-dev-health-go-pgbouncer-transaction:6432/devhealth",
		"WORKER_DATABASE_URI":      "postgresql://devhealth_queue:dummy-queue-0002@a-dev-health-go-pgbouncer-queue-session:6433/devhealth",
		"COORDINATOR_DATABASE_URI": "postgresql://devhealth_coordinator:dummy-coord-0003@a-dev-health-go-pgbouncer-coordinator-session:6434/devhealth",
	}
	for k, v := range want {
		if got := uriKey(t, sd, uriPgbouncerSecret, k); got != v {
			t.Errorf("%s changed for an unreserved password", k)
		}
	}
}

func TestPgbouncerUserlistDoublesAQuoteInAPassword(t *testing.T) {
	args := uriCatArgs(
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD", `do"main`),
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_QUEUE_DATABASE_PASSWORD", "queue-0002"),
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_COORDINATOR_DATABASE_PASSWORD", "coord-0003"),
		jsonSet("goWorkers.pgbouncer.secret.data.RIVER_KEDA_READONLY_PASSWORD", `ke"da`),
	)
	r := keepMustRender(t, "quote in userlist passwords", args...)
	sd := uriStringData(t, r, uriPgbouncerSecret)
	got := uriKey(t, sd, uriPgbouncerSecret, "userlist.txt")
	want := "\"devhealth_domain\" \"do\"\"main\"\n\"devhealth_keda_readonly\" \"ke\"\"da\"\n"
	if got != want {
		t.Errorf("userlist.txt does not double an embedded quote in a password")
	}
}

func TestPgbouncerUserlistIsByteIdenticalForUnreservedPasswords(t *testing.T) {
	r := keepMustRender(t, "unreserved userlist", keepDummyPasswords...)
	sd := uriStringData(t, r, uriPgbouncerSecret)
	got := uriKey(t, sd, uriPgbouncerSecret, "userlist.txt")
	want := "\"devhealth_domain\" \"dummy-domain-0001\"\n\"devhealth_keda_readonly\" \"dummy-keda-0004\"\n"
	if got != want {
		t.Errorf("userlist.txt changed for unreserved passwords")
	}
}

func TestChartCredentialURIsEncodeReservedCredentials(t *testing.T) {
	args := uriCatArgs(
		keepDummyPasswords2(),
		jsonSet("clickhouse.credentials.user", resCHUser),
		jsonSet("clickhouse.credentials.password", resCHPass),
		[]string{"--set", "postgresql.enabled=true"},
		jsonSet("postgresql.credentials.user", resPGUser),
		jsonSet("postgresql.credentials.password", resPGPass),
	)
	r := keepMustRender(t, "reserved chart credentials", args...)
	shared := uriStringData(t, r, uriSharedSecret)
	migrate := uriStringData(t, r, uriMigrateSecret)
	const chHTTP = "clickhouse://ch%3Auser%2Fx:c%23h%40pw%2F%3A%3F%2541%26%3D%2B@a-dev-health-clickhouse:8123/default"
	const chNative = "clickhouse://ch%3Auser%2Fx:c%23h%40pw%2F%3A%3F%2541%26%3D%2B@a-dev-health-clickhouse:9000/default"
	const pg = "postgresql+asyncpg://dev%40health:p%40ss%20w%23rd@a-dev-health-postgresql:5432/devhealth"
	if got := uriKey(t, shared, uriSharedSecret, "CLICKHOUSE_URI"); got != chHTTP {
		t.Errorf("shared CLICKHOUSE_URI is not the percent-encoded literal")
	}
	if got := uriKey(t, shared, uriSharedSecret, "CLICKHOUSE_NATIVE_URI"); got != chNative {
		t.Errorf("shared CLICKHOUSE_NATIVE_URI is not the percent-encoded literal")
	}
	if got := uriKey(t, migrate, uriMigrateSecret, "CLICKHOUSE_URI"); got != chNative {
		t.Errorf("migrate CLICKHOUSE_URI is not the percent-encoded literal")
	}
	if got := uriKey(t, shared, uriSharedSecret, "DATABASE_URI"); got != pg {
		t.Errorf("DATABASE_URI is not the percent-encoded literal")
	}
	for _, c := range []struct{ uri, user, pass string }{{chNative, resCHUser, resCHPass}, {chHTTP, resCHUser, resCHPass}} {
		opts, err := clickhouse.ParseDSN(c.uri)
		if err != nil {
			t.Errorf("clickhouse.ParseDSN: %v", err)
			continue
		}
		if opts.Auth.Username != c.user || opts.Auth.Password != c.pass {
			t.Errorf("clickhouse-go does not read back the user and password")
		}
	}
	u, err := url.Parse(strings.Replace(pg, "postgresql+asyncpg", "postgresql", 1))
	if err != nil {
		t.Fatalf("net/url on DATABASE_URI: %v", err)
	}
	cfg, err := pgx.ParseConfig(u.String())
	if err != nil || cfg.User != resPGUser || cfg.Password != resPGPass || cfg.Database != "devhealth" {
		t.Errorf("pgx does not read back the DATABASE_URI user, password and database")
	}
}

func TestChartCredentialURIsAreByteIdenticalForUnreservedCredentials(t *testing.T) {
	r := keepMustRender(t, "unreserved chart credentials", uriCatArgs(keepDummyPasswords2(), []string{"--set", "postgresql.enabled=true"})...)
	shared := uriStringData(t, r, uriSharedSecret)
	migrate := uriStringData(t, r, uriMigrateSecret)
	want := map[string]string{
		"CLICKHOUSE_URI":        "clickhouse://ch:ch@a-dev-health-clickhouse:8123/default",
		"CLICKHOUSE_NATIVE_URI": "clickhouse://ch:ch@a-dev-health-clickhouse:9000/default",
	}
	for k, v := range want {
		if got := uriKey(t, shared, uriSharedSecret, k); got != v {
			t.Errorf("shared %s changed for unreserved credentials", k)
		}
	}
	if got := uriKey(t, migrate, uriMigrateSecret, "CLICKHOUSE_URI"); got != "clickhouse://ch:ch@a-dev-health-clickhouse:9000/default" {
		t.Errorf("migrate CLICKHOUSE_URI changed for unreserved credentials")
	}
	if got := uriKey(t, shared, uriSharedSecret, "DATABASE_URI"); got != "postgresql+asyncpg://devhealth:devhealth@a-dev-health-postgresql:5432/devhealth" {
		t.Errorf("DATABASE_URI changed for unreserved credentials")
	}
}

func keepDummyPasswords2() []string { return keepDummyPasswords }

// A database name is NEVER encoded in a URI: the Python readers (SQLAlchemy with asyncpg, urllib) do not decode the URI path. Instead the
// chart refuses to render a database name that is not identifier-safe ([A-Za-z0-9_]+), by name, so no reader sees an encoded path and no
// URI carries a raw reserved character.
func TestAReservedDatabaseNameFailsTheRender(t *testing.T) {
	for _, db := range []string{"my db", "my#db", "my%db", "my/db", "my?db", "my@db", "my:db", "db-name", "db.name"} {
		cases := []struct {
			label, key string
			args       []string
		}{
			{"external pgbouncer database", "goWorkers.pgbouncer.postgres.database", uriCatArgs(keepDummyPasswords2(),
				[]string{"--set", "postgresql.enabled=false", "--set", "goWorkers.pgbouncer.postgres.host=pg.example.test"},
				jsonSet("goWorkers.pgbouncer.postgres.database", db))},
			{"DATABASE_URI database", "postgresql.credentials.database", uriCatArgs(keepDummyPasswords2(), keepExternalSameName,
				[]string{"--set", "postgresql.enabled=true"}, jsonSet("postgresql.credentials.database", db))},
			{"ClickHouse database", "clickhouse.credentials.database", uriCatArgs(keepDummyPasswords2(), jsonSet("clickhouse.credentials.database", db))},
		}
		for _, c := range cases {
			r := keepHelm(t, c.label, c.args...)
			rc, out := r.rc, r.stderr
			if rc == 0 {
				t.Errorf("%s %q: the render succeeded; a database name that is not identifier-safe must fail it", c.label, db)
				continue
			}
			if !strings.Contains(out, "identifier-safe") || !strings.Contains(out, c.key) {
				t.Errorf("%s %q: the refusal does not name %s and the identifier-safe rule", c.label, db, c.key)
			}
		}
	}
}

func TestAnIdentifierSafeDatabaseNameRendersRawInEveryURI(t *testing.T) {
	args := uriCatArgs(
		keepDummyPasswords2(),
		[]string{"--set", "postgresql.enabled=true"},
		jsonSet("postgresql.credentials.database", "my_db_1"),
		jsonSet("clickhouse.credentials.database", "Ch_DB_2"),
	)
	r := keepMustRender(t, "identifier-safe database names", args...)
	pgb := uriStringData(t, r, uriPgbouncerSecret)
	shared := uriStringData(t, r, uriSharedSecret)
	migrate := uriStringData(t, r, uriMigrateSecret)
	want := []struct {
		sd          map[string]any
		secret, key string
		suffix      string
	}{
		{pgb, uriPgbouncerSecret, "POSTGRES_URI", "/my_db_1"},
		{pgb, uriPgbouncerSecret, "WORKER_DATABASE_URI", "/my_db_1"},
		{pgb, uriPgbouncerSecret, "COORDINATOR_DATABASE_URI", "/my_db_1"},
		{shared, uriSharedSecret, "DATABASE_URI", "/my_db_1"},
		{shared, uriSharedSecret, "CLICKHOUSE_URI", "/Ch_DB_2"},
		{shared, uriSharedSecret, "CLICKHOUSE_NATIVE_URI", "/Ch_DB_2"},
		{migrate, uriMigrateSecret, "CLICKHOUSE_URI", "/Ch_DB_2"},
	}
	for _, c := range want {
		if got := uriKey(t, c.sd, c.secret, c.key); !strings.HasSuffix(got, c.suffix) {
			t.Errorf("%s/%s: the database name is not written raw at the end of the URI", c.secret, c.key)
		}
	}
}

func TestPgbouncerURIsEncodeReservedRoleNames(t *testing.T) {
	args := uriCatArgs(
		keepDummyPasswords2(),
		jsonSet("config.RIVER_DOMAIN_DATABASE_ROLE", "dev@dom ain"),
		jsonSet("config.RIVER_QUEUE_DATABASE_ROLE", "q:role/x"),
		jsonSet("config.RIVER_COORDINATOR_DATABASE_ROLE", "c#role?"),
	)
	r := keepMustRender(t, "reserved role names", args...)
	sd := uriStringData(t, r, uriPgbouncerSecret)
	cases := []struct{ key, want, role, password string }{
		{"POSTGRES_URI", "postgresql://dev%40dom%20ain:dummy-domain-0001@a-dev-health-go-pgbouncer-transaction:6432/devhealth", "dev@dom ain", "dummy-domain-0001"},
		{"WORKER_DATABASE_URI", "postgresql://q%3Arole%2Fx:dummy-queue-0002@a-dev-health-go-pgbouncer-queue-session:6433/devhealth", "q:role/x", "dummy-queue-0002"},
		{"COORDINATOR_DATABASE_URI", "postgresql://c%23role%3F:dummy-coord-0003@a-dev-health-go-pgbouncer-coordinator-session:6434/devhealth", "c#role?", "dummy-coord-0003"},
	}
	for _, c := range cases {
		got := uriKey(t, sd, uriPgbouncerSecret, c.key)
		if got != c.want {
			t.Errorf("%s: the role is not the percent-encoded literal", c.key)
		}
		u, err := url.Parse(got)
		if err != nil {
			t.Errorf("%s does not parse with net/url", c.key)
			continue
		}
		if pw, _ := u.User.Password(); u.User.Username() != c.role || pw != c.password {
			t.Errorf("%s: net/url does not read back the role and the password", c.key)
		}
		cfg, err := pgx.ParseConfig(got)
		if err != nil || cfg.User != c.role || cfg.Password != c.password {
			t.Errorf("%s: pgx does not read back the role and the password", c.key)
		}
	}
	if got := uriKey(t, sd, uriPgbouncerSecret, "userlist.txt"); !strings.HasPrefix(got, "\"dev@dom ain\" \"dummy-domain-0001\"\n") {
		t.Errorf("userlist.txt does not carry the domain role verbatim")
	}
}
