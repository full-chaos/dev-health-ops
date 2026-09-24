package session

import "encoding/base64"

func encodeClaims(payload string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(payload))
}
