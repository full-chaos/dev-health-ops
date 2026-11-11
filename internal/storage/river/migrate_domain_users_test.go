package riverstore

import (
	"regexp"
	"strings"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

func TestDomainUsersGrantIsTheDomainPostureColumns(t *testing.T) {
	t.Parallel()

	var want []string
	for _, column := range postgresstore.DomainPosture().ColumnScoped {
		if column.TableName == "users" && column.Privilege == "SELECT" {
			want = append(want, column.ColumnName)
		}
	}
	const role = `"domain_runtime"`
	usersGrant := regexp.MustCompile(`GRANT ([^;]*) ON TABLE public\.users TO ` + regexp.QuoteMeta(role) + `;`)
	var grants []string
	for _, statement := range runtimeGrantStatements(MigrationOptions{
		Schema:     "river",
		DomainRole: "domain_runtime",
		QueueRole:  "queue_runtime",
	}) {
		for _, match := range usersGrant.FindAllStringSubmatch(statement, -1) {
			grants = append(grants, match[1])
		}
	}
	if len(grants) != 1 {
		t.Fatalf("domain role public.users grants = %q, want exactly one", grants)
	}
	if grants[0] == "SELECT" {
		t.Fatal("domain role holds table-wide SELECT on public.users, so it can read password_hash")
	}
	if wantGrant := "SELECT (" + strings.Join(want, ", ") + ")"; grants[0] != wantGrant {
		t.Fatalf("domain role public.users grant = %q, want %q (DomainPosture users SELECT columns)", grants[0], wantGrant)
	}
	if strings.Contains(grants[0], "password_hash") || strings.Contains(grants[0], "token_version") {
		t.Fatalf("domain role public.users grant names a credential column: %q", grants[0])
	}
}
