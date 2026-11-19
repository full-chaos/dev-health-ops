package postgres

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

// apiGrantInputs renders the part of an api posture that internal/rivermigrate postureGrants turns into the River
// migration's grants (rivermigrate.go postureGrants: each RequiredTables entry's name and 3 flags, each ColumnScoped
// entry, the RequiredSequences), one line each, in order.
func apiGrantInputs(posture RolePosture) string {
	var lines []string
	for _, table := range posture.RequiredTables {
		lines = append(lines, fmt.Sprintf("table %s insert=%t update=%t delete=%t", table.TableName, table.AllowInsert, table.AllowUpdate, table.AllowDelete))
	}
	for _, column := range posture.ColumnScoped {
		lines = append(lines, fmt.Sprintf("column %s.%s %s", column.TableName, column.ColumnName, column.Privilege))
	}
	for _, sequence := range posture.RequiredSequences {
		lines = append(lines, "sequence "+sequence)
	}
	return strings.Join(lines, "\n") + "\n"
}

// mainAPIGrantInputs are the sha256 digests of apiGrantInputs(APIPostureFor(sessions, platformRolesOwned)) on ops main
// ae1e234d (before CHAOS-14131), frozen: the River migration's api grants of this build are byte-equal to main's.
var mainAPIGrantInputs = map[[2]bool]string{
	{false, false}: "bae045f10be5305a862ccb5bff535bd99372b4ea710d9cf06c5e81cccf733be9",
	{true, false}:  "239c5ead9e227f57dbb9203a620e0e5ca6f7ea39933aaa90f0c34f9128f8b9e1",
	{false, true}:  "7282fe458298002b632a8abaa461e10bae53fcfebc5acffa3257c6427fbc6002",
	{true, true}:   "8b01504b4da7b37d086647ded1393e6305d77483d1c36b1e6c2d1d343405bb42",
}

// mainSSOProvidersGrant is main's sso_providers line of apiGrantInputs, under every switch.
const mainSSOProvidersGrant = "table sso_providers insert=false update=true delete=true"

// CHAOS-14131 step 1 changes no grant: the inputs the River migration reads from go-api's posture are byte-equal to
// main's frozen digests, under every switch, and its sso_providers grant is main's SELECT + UPDATE + DELETE. The
// retired UPDATE lives only in RetiredTableUpdates, which the migration does not read.
func TestAPIPostureGrantInputsAreByteEqualToMain(t *testing.T) {
	for switches, want := range mainAPIGrantInputs {
		rendered := apiGrantInputs(APIPostureFor(switches[0], switches[1]))
		sum := sha256.Sum256([]byte(rendered))
		if got := hex.EncodeToString(sum[:]); got != want {
			t.Errorf("%v: grant inputs sha256 %s, main %s:\n%s", switches, got, want, rendered)
		}
		if !strings.Contains(rendered, "\n"+mainSSOProvidersGrant+"\n") && !strings.HasPrefix(rendered, mainSSOProvidersGrant+"\n") {
			t.Errorf("%v: no %q line", switches, mainSSOProvidersGrant)
		}
	}
}
