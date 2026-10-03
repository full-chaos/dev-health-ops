// Package hardened holds the two compositions of the error-text sanitizer that also run the credential-shape passes of the logging
// package (CHAOS-7937, CHAOS-8277). They live apart from errortext so that errortext stays a stdlib-only leaf (the logging package's
// own tests import errortext; an import of logging by errortext would be a cycle). One engine (errortext's matchers), two named
// compositions, each the order its entry point always had with passes only ADDED after the passes main ran (D4495).
package hardened

import (
	"github.com/full-chaos/dev-health-ops/internal/platform/errortext"
	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
)

// Parity is pythonparity.SanitizeErrorTextHardened: the Python-parity patterns, then the credential shapes, then the cap, then the
// userinfo pass LAST with a second cut (it can add the marker). maxLength <= 0 = no cap.
func Parity(text string, maxLength int) string {
	if text == "" {
		return text
	}
	shaped := logging.RedactCredentialShapesNoUserinfo(errortext.Redact(text))
	return logging.RedactUserinfoLast(errortext.Truncate(shaped, maxLength), func(text string) string { return errortext.Truncate(text, maxLength) })
}

// SyncWriters is syncdispatchruntime.SanitizeErrorText, the sync writers' path: main's chain (the credential shapes first,
// errortext.Sanitize, the userinfo pass last) with the Python-parity pass APPENDED after the userinfo pass (D4495 and the shape of
// record of D4497: a pass in front of an existing pass changes what that pass hides; a pass after it can only hide more). The
// appended pass can add markers, so it runs together with the cap INSIDE the cut that RedactUserinfoLast calls before its final
// tail step: the text the last cut leaves then goes through that step, so a second call over the result changes nothing.
func SyncWriters(text string) string {
	return logging.RedactUserinfoLast(errortext.Sanitize(logging.RedactCredentialShapesNoUserinfo(text)), func(text string) string {
		return errortext.Cap(errortext.Redact(text))
	})
}
