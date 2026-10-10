package daily

import (
	"fmt"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
)

// The list of repositories of a scope read is split so that no statement
// carries more than the bound, and no repository is lost or read twice.
func TestTheRepositoryListOfAScopeReadIsSplitUnderTheStatementBound(t *testing.T) {
	if chunks := chunkRepositoryIDs(nil, maxWorkItemScopeFilterBytes); len(chunks) != 0 {
		t.Fatalf("an empty list gave %d chunk(s), want none: no statement is run for it", len(chunks))
	}
	for _, count := range []int{1, 3, 1600, 1700, 30000} {
		repoIDs := make([]uuid.UUID, count)
		for index := range repoIDs {
			repoIDs[index] = uuid.NewSHA1(uuid.NameSpaceURL, []byte(fmt.Sprintf("repository:%d", index)))
		}
		chunks := chunkRepositoryIDs(repoIDs, maxWorkItemScopeFilterBytes)
		position := 0
		for number, chunk := range chunks {
			if len(chunk) == 0 {
				t.Fatalf("%d repositories: chunk %d is empty", count, number)
			}
			rendered := 2
			for index, repoID := range chunk {
				if repoID != repoIDs[position] {
					t.Fatalf("%d repositories: chunk %d holds %s at %d, the list holds %s there", count, number, repoID, position, repoIDs[position])
				}
				position++
				rendered += querybound.RenderedStringLen(repoID.String())
				if index > 0 {
					rendered += 2
				}
			}
			if rendered > maxWorkItemScopeFilterBytes {
				t.Errorf("%d repositories: chunk %d is %d bytes as a list in a statement, the bound is %d",
					count, number, rendered, maxWorkItemScopeFilterBytes)
			}
		}
		if position != count {
			t.Errorf("%d repositories: the chunks hold %d of them", count, position)
		}
		// A partition of a few repositories is one statement, as before.
		if count <= 1600 && len(chunks) != 1 {
			t.Errorf("%d repositories: %d chunks, want one statement", count, len(chunks))
		}
		if count >= 1700 && len(chunks) < 2 {
			t.Errorf("%d repositories: %d chunk, want the list split", count, len(chunks))
		}
	}
}
