package home

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/linktier"
)

// TestBasisQueryRanksComeFromLinktier holds the basis read to the one source of the link tier ranks: the query
// carries exactly the numbers linktier.Rank gives, in the multiIf of the rank and in the mapping of the best rank back
// to a tier, and no other rank number.
func TestBasisQueryRanksComeFromLinktier(t *testing.T) {
	query := basisQuery()
	for _, want := range []string{
		fmt.Sprintf("provenance = '%s', %d", linktier.Native, linktier.Rank(linktier.Native)),
		fmt.Sprintf("provenance = '%s', %d", linktier.ExplicitText, linktier.Rank(linktier.ExplicitText)),
		fmt.Sprintf("provenance = '%s', %d", linktier.Heuristic, linktier.Rank(linktier.Heuristic)),
		fmt.Sprintf("%d, '%s'", linktier.Rank(linktier.Native), linktier.Native),
		fmt.Sprintf("%d, '%s'", linktier.Rank(linktier.ExplicitText), linktier.ExplicitText),
	} {
		if !regexp.MustCompile(regexp.QuoteMeta(want)).MatchString(query) {
			t.Errorf("the basis query lacks %q:\n%s", want, query)
		}
	}
	if strings.Contains(query, "'explicit_text', 2, 1)") {
		t.Errorf("the basis query spells the old rank multiIf (unknown provenance ranked with heuristic): %s", query)
	}
}
