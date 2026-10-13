package logging

import (
	"context"
	"log/slog"
	"strings"
)

// RedactURLPath hides what can be a credential in the PATH of a URL (CHAOS-8034): a base URL an operator configured can
// hold one, and the base URL is the start of every request URL. The closed set of forms, read on the percent-decoded
// segments (an encoded separator or name is seen as what it stands for):
//   - a segment that is exactly a protected name ("token", "api_key") hides the segment after it;
//   - a segment that holds a protected word ("token-abc", "abc-secret") is hidden itself;
//   - a path parameter (`;name=value`) whose name is a protected key loses its value.
//
// Every other byte stays, so a plain path is returned unchanged. A raw segment that holds an encoded "/" and anything to
// hide is hidden whole. path is the path of the URL only (no scheme, authority, query or fragment). When it hid
// something it writes one debug line with the byte counts, never the text.
func RedactURLPath(ctx context.Context, logger *slog.Logger, path string) string {
	redactedPath := redactURLPath(path)
	if redactedPath != path {
		if logger == nil {
			logger = slog.Default()
		}
		logger.DebugContext(ctx, "url path credential redacted", "raw_bytes", len(path), "stored_bytes", len(redactedPath))
	}
	return redactedPath
}

func redactURLPath(path string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	if !strings.ContainsAny(path, "/;") {
		return path
	}
	segments := strings.Split(path, "/")
	hideNext := false
	for index, segment := range segments {
		decoded, _ := percentDecoded(segment)
		pieces := strings.Split(decoded, "/")
		hideSegment := false
		for _, piece := range pieces {
			name, _, _ := strings.Cut(piece, ";")
			switch {
			case hideNext && name != "":
				hideSegment = true
				hideNext = false
			case name != "" && name != redacted && protectedKeyCached(name) && protectedName(name):
				hideNext = true
			case name != "" && name != redacted && protectedKeyCached(name):
				hideSegment = true
			}
		}
		if hideSegment {
			segments[index] = redacted
			continue
		}
		if strings.Contains(decoded, ";") {
			segments[index] = redactPathParameters(segment, len(pieces) > 1)
		}
	}
	return strings.Join(segments, "/")
}

// redactPathParameters hides the value of every `;name=value` parameter of one raw segment whose name is a protected key.
// A segment that held an encoded "/" is hidden whole when a parameter has to go.
func redactPathParameters(segment string, hadEncodedSlash bool) string {
	parts := strings.Split(segment, ";")
	changed := false
	for position := 1; position < len(parts); position++ {
		name, _, hasValue := strings.Cut(parts[position], "=")
		decodedName, _ := percentDecoded(name)
		if hasValue && protectedKeyCached(decodedName) {
			parts[position] = name + "=" + redacted
			changed = true
		}
	}
	switch {
	case !changed:
		return segment
	case hadEncodedSlash:
		return redacted
	}
	return strings.Join(parts, ";")
}
