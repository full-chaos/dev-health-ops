package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The wire envelopes are projections of the rows their producer writes, so
// their member sets are DERIVED from the producer's schema (the 0005
// migration), never hand-picked: a column the producer adds must either become
// a wire member or be excluded here with a reason, and a wire member with no
// column behind it is an invention. Both directions are asserted.

const producerMigration = "internal/storage/postgres/authschema/migrations/0005_audit_and_outbox.sql"

// notOnTheWire names the columns that stay in the database, with the reason.
// An unlisted column that is not a schema member fails the test.
var notOnTheWire = map[string]map[string]string{
	"auth_outbox_events": {
		"id":           "surrogate bigserial key; consumers dedupe on idempotency_key",
		"available_at": "delivery scheduling bookkeeping",
		"published_at": "delivery bookkeeping",
		"attempts":     "delivery bookkeeping",
		"last_error":   "delivery bookkeeping; free text from the publisher",
	},
	"security_audit_events": {
		"id":        "surrogate bigserial key",
		"client_ip": "an address is a high-cardinality identity (TRD section 13)",
	},
}

var columnLine = regexp.MustCompile(`^\s+([a-z_]+)\s+(?:bigserial|bigint|integer|text|timestamptz|jsonb|uuid|inet)\b`)

func migrationColumns(t *testing.T, table string) []string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), producerMigration))
	if err != nil {
		t.Fatalf("reading the producer migration: %v", err)
	}
	text := string(raw)
	start := strings.Index(text, "CREATE TABLE "+table+" (")
	if start < 0 {
		t.Fatalf("table %s not found in %s", table, producerMigration)
	}
	body := text[start:]
	end := strings.Index(body, "\n);")
	if end < 0 {
		t.Fatalf("table %s has no closing paren", table)
	}
	var columns []string
	for _, line := range strings.Split(body[:end], "\n")[1:] {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		if match := columnLine.FindStringSubmatch(line); match != nil {
			columns = append(columns, match[1])
		}
	}
	if len(columns) < 6 {
		t.Fatalf("parsed only %d columns from %s: the parser, not the table, is probably wrong", len(columns), table)
	}
	return columns
}

func schemaMembers(t *testing.T, surface string) []string {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	var members []string
	for name := range schema.Properties {
		if name != "schema_version" {
			members = append(members, name)
		}
	}
	sort.Strings(members)
	return members
}

func TestWireEnvelopesMatchTheirProducersColumnsInBothDirections(t *testing.T) {
	for _, tc := range []struct{ table, surface string }{
		{"auth_outbox_events", ProvisioningEventSurface},
		{"security_audit_events", AuditEventSurface},
	} {
		t.Run(tc.table, func(t *testing.T) {
			columns := migrationColumns(t, tc.table)
			members := schemaMembers(t, tc.surface)
			memberSet := map[string]bool{}
			for _, m := range members {
				memberSet[m] = true
			}
			columnSet := map[string]bool{}
			for _, c := range columns {
				columnSet[c] = true
			}
			for _, c := range columns {
				_, excluded := notOnTheWire[tc.table][c]
				switch {
				case memberSet[c] && excluded:
					t.Errorf("column %s is both a schema member and listed as not on the wire", c)
				case !memberSet[c] && !excluded:
					t.Errorf("column %s of %s is neither a %s member nor excluded with a reason", c, tc.table, tc.surface)
				}
			}
			for _, m := range members {
				if !columnSet[m] {
					t.Errorf("%s member %q has no column in %s: the envelope invented it", tc.surface, m, tc.table)
				}
			}
			for c := range notOnTheWire[tc.table] {
				if !columnSet[c] {
					t.Errorf("exclusion %q names a column %s no longer has", c, tc.table)
				}
			}
		})
	}
}
