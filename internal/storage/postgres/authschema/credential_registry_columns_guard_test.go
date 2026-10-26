package authschema

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// approvedRegistryColumns is the reviewed, closed column list of
// credential_registry. A new column is a reviewed edit of this list.
var approvedRegistryColumns = map[string]string{
	"id":                 "row id",
	"principal_id":       "owning principal",
	"credential_class":   "class from the credential vocabulary",
	"display_prefix":     "non-secret display fragment",
	"secret_hash":        "verifier of a platform credential; the fingerprint bytes for a domain-owned row",
	"hash_algorithm":     "names the hash, or the marker fingerprint",
	"last_used_at":       "timestamp",
	"expires_at":         "timestamp",
	"revoked_at":         "timestamp",
	"revoked_reason":     "closed reason code",
	"created_by":         "principal id",
	"created_at":         "timestamp",
	"updated_at":         "timestamp",
	"rotation_parent_id": "lineage id",
	"audiences":          "token names",
	"scopes":             "scope names",
	"domain_owner":       "closed owner name",
	"external_reference": "opaque domain id",
	"fingerprint":        "public 16-hex fingerprint",
}

var (
	registrySecretWords         = regexp.MustCompile(`(?i)(secret|token|password|passwd|bearer|api_?key|private|plaintext|credential_value|passphrase)`)
	registryApprovedSecretNames = map[string]bool{"secret_hash": true, "credential_class": true}

	lineComment   = regexp.MustCompile(`(?m)--[^\n]*`)
	dollarQuoted  = regexp.MustCompile(`(?s)\$([A-Za-z_]*)\$.*?\$([A-Za-z_]*)\$`)
	stringLiteral = regexp.MustCompile(`'(?:[^']|'')*'`)

	createRegistry = regexp.MustCompile(`(?is)^\s*CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + tableRef + `\s*\((.*)\)\s*$`)
	alterRegistry  = regexp.MustCompile(`(?is)^\s*ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:ONLY\s+)?` + tableRef + `\s+(.*)$`)
	mentionsTable  = regexp.MustCompile(`(?i)\bcredential_registry\b`)
	alterAny       = regexp.MustCompile(`(?i)^\s*ALTER\s+TABLE\b`)

	addColumn    = regexp.MustCompile(`(?is)^ADD\s+(?:COLUMN\s+)?(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)\s+\S`)
	renameColumn = regexp.MustCompile(`(?is)^RENAME\s+(?:COLUMN\s+)?([a-z_][a-z0-9_]*)\s+TO\s+([a-z_][a-z0-9_]*)\s*$`)
	dropColumn   = regexp.MustCompile(`(?is)^DROP\s+COLUMN\s+(?:IF\s+EXISTS\s+)?([a-z_][a-z0-9_]*)\s*$`)
	columnTweak  = regexp.MustCompile(`(?is)^ALTER\s+(?:COLUMN\s+)?[a-z_][a-z0-9_]*\s+(?:SET\s+DEFAULT|DROP\s+DEFAULT|SET\s+NOT\s+NULL|DROP\s+NOT\s+NULL|SET\s+DATA\s+TYPE|TYPE)\b`)
	constraintOp = regexp.MustCompile(`(?is)^(?:ADD\s+CONSTRAINT|DROP\s+CONSTRAINT(?:\s+IF\s+EXISTS)?|VALIDATE\s+CONSTRAINT|ADD\s+CHECK|ENABLE\s+(?:ALWAYS\s+|REPLICA\s+)?TRIGGER|DISABLE\s+TRIGGER|ENABLE\s+ROW|FORCE\s+ROW|OWNER\s+TO)\b`)
)

const tableRef = `(?:"?auth"?\s*\.\s*)?"?credential_registry"?`

func neutralise(sql string) string {
	sql = lineComment.ReplaceAllString(sql, "")
	sql = dollarQuoted.ReplaceAllStringFunc(sql, func(body string) string {
		if alterAny.MatchString(body) || regexp.MustCompile(`(?is)ALTER\s+TABLE[^;]*credential_registry`).MatchString(body) {
			return "$DYNAMIC_ALTER_ON_REGISTRY$"
		}
		return "$$"
	})
	sql = stringLiteral.ReplaceAllString(sql, "''")
	return strings.ReplaceAll(sql, `"`, "")
}

func splitTopLevel(s, sep string) []string {
	var parts []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth == 0 && strings.HasPrefix(s[i:], sep) {
			parts = append(parts, strings.TrimSpace(s[start:i]))
			start = i + len(sep)
		}
	}
	return append(parts, strings.TrimSpace(s[start:]))
}

// registryColumnsFrom folds every statement of the given migrations that
// creates or alters credential_registry into the final column set. A
// statement on the table it cannot classify is an error, never skipped.
func registryColumnsFrom(sqls []string) (map[string]bool, error) {
	columns := map[string]bool{}
	for index, raw := range sqls {
		for _, statement := range splitTopLevel(neutralise(raw), ";") {
			if statement == "" {
				continue
			}
			if strings.Contains(statement, "$DYNAMIC_ALTER_ON_REGISTRY$") {
				return nil, fmt.Errorf("migration #%d alters credential_registry from dynamic SQL", index+1)
			}
			if m := createRegistry.FindStringSubmatch(statement); m != nil {
				for _, entry := range splitTopLevel(m[1], ",") {
					first := strings.ToLower(strings.Fields(entry + " x")[0])
					switch first {
					case "constraint", "primary", "unique", "check", "foreign", "like", "exclude":
						continue
					}
					columns[first] = true
				}
				continue
			}
			if !alterAny.MatchString(statement) || !mentionsTable.MatchString(statement) {
				continue
			}
			m := alterRegistry.FindStringSubmatch(statement)
			if m == nil {
				return nil, fmt.Errorf("migration #%d: unclassifiable ALTER mentioning credential_registry: %.80s", index+1, statement)
			}
			for _, action := range splitTopLevel(m[1], ",") {
				if a := addColumn.FindStringSubmatch(action); a != nil && !constraintOp.MatchString(action) {
					columns[strings.ToLower(a[1])] = true
				} else if r := renameColumn.FindStringSubmatch(action); r != nil {
					delete(columns, strings.ToLower(r[1]))
					columns[strings.ToLower(r[2])] = true
				} else if d := dropColumn.FindStringSubmatch(action); d != nil {
					delete(columns, strings.ToLower(d[1]))
				} else if columnTweak.MatchString(action) || constraintOp.MatchString(action) {
					continue
				} else {
					return nil, fmt.Errorf("migration #%d: unclassifiable action on credential_registry: %.80s", index+1, action)
				}
			}
		}
	}
	return columns, nil
}

func registryColumnViolations(columns map[string]bool) []string {
	var out []string
	for column := range columns {
		if _, ok := approvedRegistryColumns[column]; !ok {
			out = append(out, "column "+column+" is not in the reviewed list")
		}
		if registrySecretWords.MatchString(column) && !registryApprovedSecretNames[column] {
			out = append(out, "column "+column+" looks like secret material")
		}
	}
	sort.Strings(out)
	return out
}

func lineageSQL(t *testing.T) []string {
	t.Helper()
	migrations, err := Migrations()
	if err != nil {
		t.Fatal(err)
	}
	var sqls []string
	for _, m := range migrations {
		sqls = append(sqls, m.SQL)
	}
	return sqls
}

// TestCredentialRegistryColumnsAcrossTheWholeLineageAreReviewed: the final
// column set after every embedded migration equals the reviewed list.
func TestCredentialRegistryColumnsAcrossTheWholeLineageAreReviewed(t *testing.T) {
	columns, err := registryColumnsFrom(lineageSQL(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(columns) < 15 {
		t.Fatalf("parsed only %d columns (%v): the parser, not the table, is probably wrong", len(columns), columns)
	}
	if v := registryColumnViolations(columns); len(v) > 0 {
		t.Fatalf("credential_registry: %v", v)
	}
	for approved := range approvedRegistryColumns {
		if !columns[approved] {
			t.Errorf("reviewed column %s is not produced by the lineage", approved)
		}
	}
}

// TestCredentialRegistryColumnGuardFailsOnPlantedMigrations: a later migration
// that adds a secret column, renames a column to a secret name, or alters the
// table in a form the parser cannot classify, fails the guard.
func TestCredentialRegistryColumnGuardFailsOnPlantedMigrations(t *testing.T) {
	cases := map[string]string{
		"add api_secret":                  `ALTER TABLE credential_registry ADD COLUMN api_secret text;`,
		"add qualified and quoted":        `ALTER TABLE auth."credential_registry" ADD COLUMN IF NOT EXISTS "Api_Secret" text;`,
		"add in a multi-column list":      `ALTER TABLE credential_registry ADD COLUMN note text, ADD COLUMN bearer_value text;`,
		"unreviewed benign column":        `ALTER TABLE credential_registry ADD COLUMN note text;`,
		"rename to token_hash":            `ALTER TABLE credential_registry RENAME COLUMN fingerprint TO token_hash;`,
		"rename after a benign add":       `ALTER TABLE credential_registry ADD COLUMN IF NOT EXISTS memo text; ALTER TABLE credential_registry RENAME COLUMN memo TO api_key;`,
		"create like a copy":              `ALTER TABLE other RENAME TO credential_registry;`,
		"unparseable action":              `ALTER TABLE credential_registry SPLIT COLUMN scopes INTO (a, b);`,
		"unparseable statement":           `ALTER TABLE ONLY auth.credential_registry ??? ;`,
		"dynamic alter inside a function": `DO $x$ BEGIN EXECUTE 'ALTER TABLE credential_registry ADD COLUMN s text'; END $x$;`,
	}
	base := lineageSQL(t)
	for name, planted := range cases {
		t.Run(name, func(t *testing.T) {
			columns, err := registryColumnsFrom(append(append([]string{}, base...), planted))
			if err != nil {
				return
			}
			if len(registryColumnViolations(columns)) == 0 {
				t.Fatalf("the planted migration %q passed the guard (columns %v)", planted, columns)
			}
		})
	}
}

// TestCredentialRegistryColumnGuardAcceptsHarmlessAlters: constraint, trigger
// and column-default alters do not trip the guard.
func TestCredentialRegistryColumnGuardAcceptsHarmlessAlters(t *testing.T) {
	harmless := `ALTER TABLE credential_registry ADD CONSTRAINT c CHECK (scopes <> '{}') NOT VALID;
ALTER TABLE auth.credential_registry VALIDATE CONSTRAINT c;
ALTER TABLE credential_registry ALTER COLUMN scopes SET DEFAULT '{}', DROP CONSTRAINT c;
ALTER TABLE credential_registry DISABLE TRIGGER USER;`
	columns, err := registryColumnsFrom(append(lineageSQL(t), harmless))
	if err != nil {
		t.Fatalf("harmless alters were refused: %v", err)
	}
	if v := registryColumnViolations(columns); len(v) > 0 {
		t.Fatalf("harmless alters changed the verdict: %v", v)
	}
}
