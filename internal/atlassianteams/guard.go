package atlassianteams

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// maxGuardedBody bounds the response the guard buffers to inspect.
const maxGuardedBody = 32 << 20

// CompletePagesOnly wraps a transport so a gateway answer that says another page
// exists but names no cursor to fetch it with is an error. The vendored client
// treats that answer as the end of the list, which would turn an incomplete team
// or member list into a "complete" snapshot; a snapshot is what the sync retracts
// against, so it must not be cut short silently. (The other half of the same
// guarantee is the client's Strict mode, which turns GraphQL errors next to
// partial data into failures.)
func CompletePagesOnly(next http.RoundTripper) http.RoundTripper {
	if next == nil {
		next = http.DefaultTransport
	}
	return completePages{next: next}
}

type completePages struct{ next http.RoundTripper }

func (t completePages) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.next.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxGuardedBody+1))
	_ = resp.Body.Close()
	if readErr != nil {
		// readErr is io.ReadAll's own I/O failure reading resp.Body -- never
		// the body's content itself, but the guard test treats any error from
		// a call that reads the body as provider-origin by construction, so
		// it is classified rather than returned raw.
		return nil, logging.TransportFailure(readErr)
	}
	if len(body) > maxGuardedBody {
		return nil, fmt.Errorf("atlassian gateway answer exceeds %d bytes", maxGuardedBody)
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	if resp.StatusCode != http.StatusOK {
		return resp, nil
	}
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return resp, nil // the client reports a body it cannot read
	}
	if incompletePage(decoded, "$") != "" {
		// The JSON path incompletePage found is provider-echoed structure
		// (map keys from the gateway's own response); it never enters the
		// error, only the fixed fact that the guard fired.
		return nil, errors.New("atlassian gateway answered hasNextPage without an endCursor: the list would be cut short")
	}
	return resp, nil
}

// incompletePage returns the path of the first pageInfo object that promises a
// next page without a usable cursor, "" when there is none.
func incompletePage(value any, path string) string {
	switch typed := value.(type) {
	case map[string]any:
		if next, ok := typed["hasNextPage"].(bool); ok && next {
			if cursor, _ := typed["endCursor"].(string); strings.TrimSpace(cursor) == "" {
				return path
			}
		}
		for key, child := range typed {
			if at := incompletePage(child, path+"."+key); at != "" {
				return at
			}
		}
	case []any:
		for i, child := range typed {
			if at := incompletePage(child, fmt.Sprintf("%s[%d]", path, i)); at != "" {
				return at
			}
		}
	}
	return ""
}

// RefuseRedirects is an http.Client CheckRedirect that never follows one: the stored credential rides every
// request to the gateway and the tenant, and must not be replayed to another host (a redirect to a sibling
// or nested host keeps the Authorization header under Go's own rules).
func RefuseRedirects(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// GatewayHTTPClient is the HTTP client every Atlassian gateway read uses: complete pages only, a bounded
// wait, and no redirect followed.
func GatewayHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, Transport: CompletePagesOnly(nil), CheckRedirect: RefuseRedirects}
}
