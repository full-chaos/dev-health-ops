package teamsidentity

import (
	"fmt"
	"strings"
)

// linearWalkMaxPages is the hard bound on ONE Linear connection walked by
// discovery: 50 pages in all, an embedded first page included (the bound every
// nested Linear connection has, CHAOS-8781). Past it the walk fails closed;
// nothing is truncated.
const linearWalkMaxPages = 50

// linearCursorWalk is the one place a Linear discovery loop decides whether to
// ask for another page. Every pagination loop in this package has the same three
// clauses and fails CLOSED on any of them (D4920): a page bound, a non-empty
// cursor, and a cursor that PROGRESSES (never the one just sent, never one seen
// before). An ambiguous answer is an error, never a second identical request and
// never a duplicate row returned as success.
type linearCursorWalk struct {
	label string // names the connection in an error: "teams", "members of team T"
	pages int    // pages read so far, embedded page included
	seen  map[string]struct{}
}

// newLinearCursorWalk starts a walk. pagesRead is 1 when the first page was
// embedded in an earlier query, and sentCursor is the cursor that page ended on.
func newLinearCursorWalk(label string, pagesRead int, sentCursor string) *linearCursorWalk {
	walk := &linearCursorWalk{label: label, pages: pagesRead, seen: map[string]struct{}{}}
	if sentCursor != "" {
		walk.seen[sentCursor] = struct{}{}
	}
	return walk
}

// next is called after a page was READ. It returns the cursor to send for the
// next page and true, or "", false when the connection ended. The error carries
// only the connection label and counts: no payload text, no credential.
func (walk *linearCursorWalk) next(hasNextPage bool, endCursor string) (string, bool, error) {
	walk.pages++
	if !hasNextPage {
		return "", false, nil
	}
	if walk.pages >= linearWalkMaxPages {
		return "", false, fmt.Errorf("linear %s still had a next page after %d pages (max %d pages)",
			walk.label, walk.pages, linearWalkMaxPages)
	}
	cursor := strings.TrimSpace(endCursor)
	if cursor == "" {
		return "", false, fmt.Errorf("linear %s said there was a next page but gave no cursor (after %d pages)",
			walk.label, walk.pages)
	}
	if _, repeated := walk.seen[cursor]; repeated {
		return "", false, fmt.Errorf("linear %s said there was a next page but its cursor did not advance (after %d pages)",
			walk.label, walk.pages)
	}
	walk.seen[cursor] = struct{}{}
	return cursor, true, nil
}

// requireCursor checks the cursor an EMBEDDED page ended on, before the first
// follow-up is sent: a connection that said "next page" with no cursor cannot be
// continued, and starting over would read page 1 twice.
func (walk *linearCursorWalk) requireCursor(cursor string) (string, error) {
	cursor = strings.TrimSpace(cursor)
	if cursor == "" {
		return "", fmt.Errorf("linear %s said there was a next page but gave no cursor (after %d pages)",
			walk.label, walk.pages)
	}
	return cursor, nil
}
