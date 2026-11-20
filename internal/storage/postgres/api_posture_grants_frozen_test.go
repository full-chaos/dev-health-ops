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
// Re-pinned by CHAOS-14250: the only difference from main is two DELETE-only lines, sync_dispatch_bucket_signals and
// sync_dispatch_bucket_reservations (org deletion removes the bucket sweep's rows).
var mainAPIGrantInputs = map[[2]bool]string{
	{false, false}: "835a9fb2855d54ecb30c74db84fa5c353ef1c60602db24558995dd4857645fcf",
	{true, false}:  "69d374e7eca7d3e63fb34d55760eadb604542bb1c6e3f2ba8a7feee887a50f07",
	{false, true}:  "dceb9547cb9e69674dbdf51545046b999afd7ce6d9979dd19f70f9b085df24a5",
	{true, true}:   "8053a876b0f5155398d77f9cefc7565c697b1dbbde165f08c46a8bb9deb3c67d",
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
