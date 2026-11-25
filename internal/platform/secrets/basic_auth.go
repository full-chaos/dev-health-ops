package secrets

import "encoding/base64"

// BasicAuthSpellings returns the four base64 spellings (standard and URL alphabets, padded and
// not) of the exact "email:token" an HTTP Basic request sends, untrimmed. A gateway that echoes
// the Authorization header carries one of them, and no spelling of the email or of the token
// alone matches it. Pass them as values to NewBoundaryWith, which derives their escaped
// spellings too. An empty email or token has none.
func BasicAuthSpellings(email, token string) []string {
	if email == "" || token == "" {
		return nil
	}
	raw := []byte(email + ":" + token)
	return []string{
		base64.StdEncoding.EncodeToString(raw), base64.RawStdEncoding.EncodeToString(raw),
		base64.URLEncoding.EncodeToString(raw), base64.RawURLEncoding.EncodeToString(raw),
	}
}
