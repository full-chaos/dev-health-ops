package riverstore

import (
	"regexp"
	"strings"
	"testing"

	postgresstore "github.com/full-chaos/dev-health-ops/internal/storage/postgres"
)

func TestDomainMembershipsGrantIsTheDomainPostureColumns(t *testing.T) {
	t.Parallel()

	var want []string
	for _, column := range postgresstore.DomainPosture().ColumnScoped {
		if column.TableName == "memberships" && column.Privilege == "SELECT" {
			want = append(want, column.ColumnName)
		}
	}
	if len(want) == 0 {
		t.Fatal("DomainPosture declares no memberships SELECT columns; the billing owner lookup joins memberships")
	}
	const role = `"domain_runtime"`
	membershipsGrant := regexp.MustCompile(`GRANT ([^;]*) ON TABLE public\.memberships TO ` + regexp.QuoteMeta(role) + `;`)
	var grants []string
	for _, statement := range runtimeGrantStatements(MigrationOptions{
		Schema:     "river",
		DomainRole: "domain_runtime",
		QueueRole:  "queue_runtime",
	}) {
		for _, match := range membershipsGrant.FindAllStringSubmatch(statement, -1) {
			grants = append(grants, match[1])
		}
	}
	if len(grants) != 1 {
		t.Fatalf("domain role public.memberships grants = %q, want exactly one", grants)
	}
	if wantGrant := "SELECT (" + strings.Join(want, ", ") + ")"; grants[0] != wantGrant {
		t.Fatalf("domain role public.memberships grant = %q, want %q (DomainPosture memberships SELECT columns)", grants[0], wantGrant)
	}
}
