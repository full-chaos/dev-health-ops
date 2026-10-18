package teamsidentity

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// pageWalkMaxPages is the page bound of every GitHub, GitLab and Jira discovery walk: the same 50 the Linear walk has (CHAOS-8781,
// CHAOS-8786). Past it, or on a continuation that cannot progress, the walk fails closed: an error, never a partial list as success.
const pageWalkMaxPages = linearWalkMaxPages

// Closed vocabulary of the cause of a failed walk (the cause label of the counter and of the Warn line).
const (
	walkCauseMaxPages        = "max_pages"
	walkCauseNoContinuation  = "no_continuation"
	walkCauseRepeated        = "continuation_repeated"
	walkCauseEmptyPageToNext = "empty_page_advertises_next"
	walkCauseBadContinuation = "continuation_unreadable"
	walkCauseInconsistent    = "continuation_inconsistent"
)

var pageWalkFailures = func() metric.Int64Counter {
	const name = "teams_identity_page_walk_failed_total"
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/api/teamsidentity").Int64Counter(
		name, metric.WithDescription("Teams-identity discovery and member walks that failed closed, by provider, walk and cause."))
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}()

// pageWalk is the one place a GitHub, GitLab or Jira discovery loop decides whether to ask for another page. It has the three
// clauses of linearCursorWalk, and one more for offset and Link walks: a page bound, a continuation that exists, a continuation that
// was never sent before, and a page that carries no items cannot advertise a next page. Any of them fails the walk CLOSED with an
// error naming the provider, the walk and the owner, and writes one Warn line and one counter increment.
type pageWalk struct {
	ctx      context.Context
	provider string // "github", "gitlab", "jira"
	walk     string // "org teams", "team repos", "team members", "group members", "subgroups", "projects", "issue search"
	owner    string // the org, team, group or project the walk reads
	pages    int
	seen     map[string]struct{}
}

// newPageWalk starts a walk whose first request is identified by firstKey (the URL, page number or token it was sent with).
func newPageWalk(ctx context.Context, provider, walk, owner, firstKey string) *pageWalk {
	return &pageWalk{ctx: ctx, provider: provider, walk: walk, owner: owner, seen: map[string]struct{}{firstKey: {}}}
}

// advance is called after a page was READ with the items it held, the continuation it advertised (empty = none) and whether it
// advertised one. It returns the continuation to send, or false when the walk ended.
func (walk *pageWalk) advance(continuation string, advertised bool, items int) (string, bool, error) {
	walk.pages++
	if !advertised {
		return "", false, nil
	}
	switch {
	case walk.pages >= pageWalkMaxPages:
		return "", false, walk.fail(walkCauseMaxPages)
	case continuation == "":
		return "", false, walk.fail(walkCauseNoContinuation)
	case items == 0:
		return "", false, walk.fail(walkCauseEmptyPageToNext)
	}
	if _, repeated := walk.seen[continuation]; repeated {
		return "", false, walk.fail(walkCauseRepeated)
	}
	walk.seen[continuation] = struct{}{}
	return continuation, true, nil
}

// fail records the failure and returns the error: provider, walk, owner and counts only, no payload text.
func (walk *pageWalk) fail(cause string) error {
	slog.Default().WarnContext(walk.ctx, "teams_identity_page_walk_failed",
		slog.String("provider", walk.provider), slog.String("walk", walk.walk), slog.String("owner", walk.owner),
		slog.String("cause", cause), slog.Int("pages", walk.pages), slog.Int("max_pages", pageWalkMaxPages))
	pageWalkFailures.Add(walk.ctx, 1, metric.WithAttributes(
		attribute.String("provider", walk.provider), attribute.String("walk", walk.walk), attribute.String("cause", cause)))
	return fmt.Errorf("%s %s walk of %s failed closed: %s after %d pages (max %d pages)",
		walk.provider, walk.walk, walk.owner, cause, walk.pages, pageWalkMaxPages)
}

// gitlabNextPage reads X-Next-Page: "" is the end; a value that is not a positive integer fails the walk (it used to end it, and
// the list was returned as complete).
func (walk *pageWalk) gitlabNextPage(header string, items int) (int, bool, error) {
	if header == "" {
		_, _, err := walk.advance("", false, items)
		return 0, false, err
	}
	number, parseErr := strconv.Atoi(header)
	if parseErr != nil || number < 1 {
		walk.pages++
		return 0, false, walk.fail(walkCauseBadContinuation)
	}
	// "01" and "1" are the same page: the continuation is compared in its canonical form
	next, more, err := walk.advance(strconv.Itoa(number), true, items)
	if err != nil || !more {
		return 0, false, err
	}
	number, _ = strconv.Atoi(next)
	return number, true, nil
}

// jiraAdvance reads the continuation signal of one Jira search page (nextPageToken, isLast and the page's item count) and decides
// whether another page is asked for. It is the only check of the Jira walk: a signal that is malformed (an isLast that is not a
// boolean, a token that is not a non-empty string) or inconsistent (isLast true together with a token) fails the walk, and so does
// an empty page that advertises another one. An empty page without a continuation, and isLast true, end the list.
func (walk *pageWalk) jiraAdvance(page *pyjson.Object, startAt, items int) (token string, nextStartAt int, more bool, err error) {
	rawToken, _ := page.Get("nextPageToken")
	rawLast, hasLast := page.Get("isLast")
	last := false
	if hasLast {
		flag, isBool := rawLast.(bool)
		if !isBool {
			walk.pages++
			return "", 0, false, walk.fail(walkCauseBadContinuation)
		}
		last = flag
	}
	if truthy(rawToken) {
		text, isString := rawToken.(string)
		if !isString || text == "" {
			walk.pages++
			return "", 0, false, walk.fail(walkCauseBadContinuation)
		}
		token = text
	}
	if last && token != "" {
		walk.pages++
		return "", 0, false, walk.fail(walkCauseInconsistent)
	}
	advertised := !last
	if items == 0 {
		advertised = token != "" || (hasLast && !last)
	}
	nextStartAt = startAt + items
	continuation := fmt.Sprintf("startAt:%d", nextStartAt)
	if token != "" {
		continuation = "token:" + token
	}
	if _, more, err = walk.advance(continuation, advertised, items); err != nil || !more {
		return "", 0, false, err
	}
	return token, nextStartAt, true, nil
}

// appendNew appends the items whose key was not seen yet: a page walk that joins pages lists an item once (CHAOS-8786 class note).
func appendNew[T any](out []T, seen map[string]struct{}, page []T, key func(T) string) []T {
	for _, item := range page {
		id := key(item)
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, item)
	}
	return out
}

// memberObjectKey is the identity of a member entry for the join of pages: the first of the fields that is present. An entry
// with none of them is never merged away (its key is unique to its position in the page).
func memberObjectKey(fields ...string) func(*pyjson.Object) string {
	return func(entry *pyjson.Object) string {
		for _, field := range fields {
			if value, ok := entry.Get(field); ok && truthy(value) {
				return field + ":" + pythonStr(value)
			}
		}
		return fmt.Sprintf("anon:%p", entry)
	}
}
