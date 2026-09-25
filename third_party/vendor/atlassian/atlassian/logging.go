package atlassian

import (
	"net/http"
	"strings"
)

func SanitizeHeaders(h http.Header) http.Header {
	clean := http.Header{}
	for k, vals := range h {
		switch strings.ToLower(k) {
		case "authorization", "cookie":
			clean[k] = []string{"<redacted>"}
		default:
			clean[k] = append([]string{}, vals...)
		}
	}
	return clean
}
