package atlassianteams

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
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
		return nil, readErr
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
	if at := incompletePage(decoded, "$"); at != "" {
		return nil, fmt.Errorf("atlassian gateway answered hasNextPage without an endCursor at %s: the list would be cut short", at)
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
