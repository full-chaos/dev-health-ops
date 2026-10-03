package logging

import (
	"regexp"
	"strings"
)

// CHAOS-8277: the userinfo pass. A credential written as `user:secret@host` with or
// without a scheme reached the log because the other passes need `scheme://user:secret@host`
// whole. The shape of the change, in every layer: the layer is userinfo_post(main_chain(x)):
// the chain of passes the layer had before is unchanged, and this pass runs LAST on its
// output and only replaces text with the redaction marker, so a layer hides at least
// everything it hid before. A form this pass cannot see after the chain (a quote that
// was percent-encoded and is decoded by the chain) is a named limit, not a pass in front.

// userinfoPattern: an optional user part (no quote, slash, bracket or `=`), a colon (or its
// percent-encoded form), then the secret to the LAST `@` (or `%40`) of the whitespace-free run.
// A quote or `<`/`>` inside a password ends the run (named limit: a JSON string or a tag
// around the text must stay intact).
var userinfoPattern = regexp.MustCompile(`[^\s/@:"'<>()\[\]{},;=\\]*(?::|%3[aA])[^\s"'<>]*(?:@|%40)`)

// RedactText removes supported DSNs, URLs containing userinfo, header and bearer
// credentials, provider tokens recognised by their prefix, and the value of every
// protected key in many forms, a credential-shaped value after a credential word in
// prose, and protected path segments from free-form text before it can reach operator
// logs (the chain in redactTextNoUserinfo), then, last, a `user:secret@` userinfo left in
// the result. A failure inside the redactor returns a fixed marker.
func RedactText(value string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	value = redactTextNoUserinfo(value)
	if mayHoldUserinfo(value) {
		value = redactUserinfo(value)
	}
	return value
}

// RedactCredentialShapes is the credential part of RedactText for text that must keep its
// other bytes (the chain in RedactCredentialShapesNoUserinfo), then, last, a `user:secret@`
// userinfo left in the result. The persisted error columns use it after their own patterns
// (CHAOS-7937).
func RedactCredentialShapes(value string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	value = RedactCredentialShapesNoUserinfo(value)
	if mayHoldUserinfo(value) {
		value = redactUserinfo(value)
	}
	return value
}

// truncationSuffix is what the persisted error sanitizers append where they cut a text.
const truncationSuffix = "...[truncated]"

// RedactUserinfoLast is the userinfo pass for a caller that runs it after its own passes
// and after its length cap (the persisted error sanitizers). The pass can add the marker, so
// the caller's own cut runs again after it (cut). A text cut by a cap may end inside a
// userinfo (its `@` lost): when the final text ends with the truncation suffix, the last
// whitespace- or delimiter-free run before it is dropped if it holds a colon (after a drop the
// body ends with a delimiter, so the loop below stops at the next turn).
// The text is final after that, so a second call over the result changes nothing; it is
// fail-closed and only at the cut. (A part of a password that itself holds a comma, a
// semicolon, a parenthesis, a bracket or an at sign can stay readable when the cut falls
// inside it: a named limit, the same class as main's own cap split.)
func RedactUserinfoLast(value string, cut func(string) string) (result string) {
	defer func() {
		if recover() != nil {
			result = redactionFailed
		}
	}()
	if mayHoldUserinfo(value) {
		value = redactUserinfo(value)
	}
	value = cut(value)
	if strings.HasSuffix(value, truncationSuffix) {
		body := strings.TrimSuffix(value, truncationSuffix)
		for body != "" {
			start := strings.LastIndexAny(body, " \t\r\n\"'<>(),;[]{}") + 1
			run := body[start:]
			if !strings.Contains(run, ":") && !strings.Contains(strings.ToLower(run), "%3a") {
				break
			}
			body = body[:start]
		}
		value = body + truncationSuffix
	}
	return value
}

// mayHoldUserinfo is the cheap gate in front of the userinfo match: it needs an `@`
// (or its percent-encoded form) to end on.
func mayHoldUserinfo(value string) bool {
	return strings.Contains(value, "@") || strings.Contains(value, "%40")
}

// redactUserinfo replaces the `user:secret` part of every `user:secret@` in value with the
// redaction marker and keeps the `@` (so the host after it stays readable).
func redactUserinfo(value string) string {
	matches := userinfoPattern.FindAllStringIndex(value, -1)
	if len(matches) == 0 {
		return value
	}
	var out strings.Builder
	written := 0
	for _, match := range matches {
		end := match[1]
		// `name:tag@sha256:<digest>` is an image reference, not a credential: the last `@` of
		// the run is the digest's. A credential before it (`user:secret@host/repo@sha256:...`)
		// is still redacted, up to the `@` before the digest; with no other `@` in the run
		// there is nothing to redact.
		if isDigestReference(value[end:]) {
			previous := strings.LastIndexByte(value[match[0]:end-1], '@')
			if previous < 0 {
				continue
			}
			end = match[0] + previous + 1
		}
		out.WriteString(value[written:match[0]])
		out.WriteString(redacted + "@")
		written = end
	}
	if written == 0 {
		return value
	}
	out.WriteString(value[written:])
	return out.String()
}

// isDigestReference reports whether text, the text right after an `@`, starts an image
// digest (`sha256:`, `sha384:`, `sha512:`).
func isDigestReference(text string) bool {
	for _, algorithm := range []string{"sha256:", "sha384:", "sha512:"} {
		if strings.HasPrefix(text, algorithm) {
			return true
		}
	}
	return false
}
