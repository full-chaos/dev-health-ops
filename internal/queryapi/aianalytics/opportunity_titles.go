package aianalytics

import (
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/scopelabel"
)

// nameTitle is a served opportunity title with its entity named, never identified: every title of this
// package is built as "<words> " + the entity id, and the id is prose the product never shows (D5428,
// D5945). The id is replaced by the entity's display name (a name equal to its own id is no name), or by
// the unnamed wording of CHAOS-9116 / D5860 when there is none. A title that does not end in the id is
// returned as it is. The id stays in the machine fields (entity id, evidence refs): they are keys.
func nameTitle(title, entityID string, name *string, unnamed string) string {
	if entityID == "" || !strings.HasSuffix(title, entityID) {
		return title
	}
	label := unnamed
	if name != nil {
		if clean, ok := scopelabel.CleanNameFor(*name, entityID); ok {
			label = clean
		}
	}
	return strings.TrimSuffix(title, entityID) + label
}
