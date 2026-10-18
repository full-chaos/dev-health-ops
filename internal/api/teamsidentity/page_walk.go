package teamsidentity

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
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
	walkCausePageRequest     = "page_request_failed"
)

// pageWalkFailureCounterName is the instrument of every failed walk.
const pageWalkFailureCounterName = "dev_health_teams_identity_page_walk_failed_total"

var pageWalkFailures = func() metric.Int64Counter {
	const name = pageWalkFailureCounterName
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
	// pagePrints are the fingerprints of the pages read so far (replay)
	pagePrints map[string]struct{}
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

// pageError is the error of a page request. The first page's error is the caller's own; the error of a later page is a walk that
// ended early (a continuation the client refuses, a provider that failed mid-walk): it writes the Warn line and the counter with the
// error's class, and its text names the provider, the walk and the owner. The cause stays reachable through Unwrap.
func (walk *pageWalk) pageError(err error) error {
	if err == nil || walk.pages == 0 {
		return err
	}
	slog.Default().WarnContext(walk.ctx, "teams_identity_page_walk_failed",
		slog.String("provider", walk.provider), slog.String("walk", walk.walk), slog.String("owner", walk.owner),
		slog.String("cause", walkCausePageRequest), slog.Int("pages", walk.pages), slog.Int("max_pages", pageWalkMaxPages),
		slog.String("error_class", logging.ErrorClass(err)))
	pageWalkFailures.Add(walk.ctx, 1, metric.WithAttributes(
		attribute.String("provider", walk.provider), attribute.String("walk", walk.walk), attribute.String("cause", walkCausePageRequest)))
	return fmt.Errorf("%s %s walk of %s failed closed: %s after %d pages: %w",
		walk.provider, walk.walk, walk.owner, walkCausePageRequest, walk.pages, err)
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

// gitlabNextPage reads X-Next-Page of the page just read (number current): "" is the end. GitLab's offset pagination always names the
// next page as current+1, so any other value, a non-canonical one or one that is not a positive integer fails the walk.
func (walk *pageWalk) gitlabNextPage(header string, current, items int) (int, bool, error) {
	if header == "" {
		_, _, err := walk.advance("", false, items)
		return 0, false, err
	}
	number, parseErr := strconv.Atoi(header)
	// only the canonical decimal form is a page number: "02" and "+2" name a page the walk cannot compare with the ones it sent
	if parseErr != nil || number < 1 || strconv.Itoa(number) != header {
		walk.pages++
		return 0, false, walk.fail(walkCauseBadContinuation)
	}
	if number != current+1 {
		walk.pages++
		return 0, false, walk.fail(walkCauseInconsistent)
	}
	next, more, err := walk.advance(header, true, items)
	if err != nil || !more {
		return 0, false, err
	}
	number, _ = strconv.Atoi(next)
	return number, true, nil
}

// replay fails the walk when a page of items is the same page as an earlier one (same fingerprint): a provider that ignores the
// continuation the walk sends answers with the first page again, and a fresh-looking continuation key cannot show that.
func (walk *pageWalk) replay(fingerprint string) error {
	if fingerprint == "" {
		return nil
	}
	if walk.pagePrints == nil {
		walk.pagePrints = map[string]struct{}{}
	}
	if _, again := walk.pagePrints[fingerprint]; again {
		walk.pages++
		return walk.fail(walkCauseRepeated)
	}
	walk.pagePrints[fingerprint] = struct{}{}
	return nil
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
	if rawToken != nil {
		// an empty string is no token, as Python's `if next_token:` reads it; any other non-string is a malformed signal
		text, isString := rawToken.(string)
		if !isString {
			walk.pages++
			return "", 0, false, walk.fail(walkCauseBadContinuation)
		}
		token = text
	}
	if last && token != "" {
		walk.pages++
		return "", 0, false, walk.fail(walkCauseInconsistent)
	}
	// /rest/api/3/search/jql continues by nextPageToken only and ignores startAt: a non-empty page that says isLast false and gives
	// no token cannot be continued. An absent isLast keeps the startAt continuation (the pinned Python iter_issues behaviour; the
	// replay check catches an endpoint that ignores it).
	if items > 0 && hasLast && !last && token == "" {
		walk.pages++
		return "", 0, false, walk.fail(walkCauseNoContinuation)
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

// jiraPageFingerprint identifies a page of Jira issues by the keys (or ids) of its issues in order; "" for an empty page.
func jiraPageFingerprint(list []pyjson.Value) string {
	if len(list) == 0 {
		return ""
	}
	parts := make([]string, len(list))
	for index, item := range list {
		if issue, ok := item.(*pyjson.Object); ok {
			parts[index] = memberObjectKey("key", "id")(issue)
		} else {
			parts[index] = fmt.Sprintf("anon:%d", index)
		}
	}
	return strings.Join(parts, "|")
}
