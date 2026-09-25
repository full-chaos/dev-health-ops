package goapiproof

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"strings"
)

// The echo guard: a response that reflects the credential its request sent must
// never be stored as evidence (artifacts linked from a receipt) or have a value
// taken from it printed (the build header, the /buildinfo commit). The guard
// refuses the leg instead, by name and never with the value.
//
// What it recognises in a response body or header value, for each secret the
// request carried in a header (Authorization's scheme prefix stripped):
//
//   - the raw value;
//   - the value base64-encoded (standard and URL alphabets, padded and not);
//   - any dot-separated part of at least echoPartMinLen bytes (a JWT's payload
//     and signature parts);
//   - any run of echoWindowLen consecutive bytes of the value, so a value split
//     into pieces of at least that length (two JSON fields, say) is still seen.
//
// What it does NOT recognise, stated so nobody reads more into it: a value cut
// into pieces shorter than echoWindowLen, or transformed some other way (hex,
// reversed, encrypted). Those need a route built to hide a credential, which no
// known route is; the guard closes the accidental reflection classes.

const (
	// echoSecretMinLen is the shortest credential value looked for: anything
	// shorter would match ordinary text.
	echoSecretMinLen = 8
	// echoPartMinLen is the shortest dot-separated part looked for.
	echoPartMinLen = 16
	// echoWindowLen is the length of the consecutive run that counts as a
	// reflected piece of a value.
	echoWindowLen = 24
)

// SentSecrets is the set of credential values a request carried.
type SentSecrets struct {
	secrets [][]byte
	// needles are whole-value forms searched by containment: the raw value,
	// its base64 forms, its long dot-separated parts.
	needles [][]byte
}

// SecretsOnRequest reads the credential values a request carries in its
// headers, AFTER Credential.Apply, so it holds exactly what was sent whatever
// the credential kind. Content-Type is not a credential.
func SecretsOnRequest(header http.Header) SentSecrets {
	var out SentSecrets
	for name, values := range header {
		if http.CanonicalHeaderKey(name) == "Content-Type" {
			continue
		}
		for _, value := range values {
			value = strings.TrimSpace(strings.TrimPrefix(value, "Bearer "))
			if len(value) < echoSecretMinLen {
				continue
			}
			out.add([]byte(value))
		}
	}
	return out
}

func (s *SentSecrets) add(secret []byte) {
	s.secrets = append(s.secrets, secret)
	s.needles = append(s.needles, secret)
	for _, encoding := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		s.needles = append(s.needles, []byte(encoding.EncodeToString(secret)))
	}
	for _, part := range bytes.Split(secret, []byte(".")) {
		if len(part) >= echoPartMinLen {
			s.needles = append(s.needles, part)
		}
	}
}

// Empty reports whether the request carried no credential worth guarding.
func (s SentSecrets) Empty() bool { return len(s.secrets) == 0 }

// ReflectedIn reports whether data carries a form of any secret.
func (s SentSecrets) ReflectedIn(data []byte) bool {
	for _, needle := range s.needles {
		if bytes.Contains(data, needle) {
			return true
		}
	}
	for _, secret := range s.secrets {
		if len(secret) >= echoWindowLen && windowReflected(data, secret) {
			return true
		}
	}
	return false
}

// HeaderReflects reports whether any value of any response header carries a
// form of any secret. Header names are not searched.
func (s SentSecrets) HeaderReflects(header http.Header) bool {
	for _, values := range header {
		for _, value := range values {
			if s.ReflectedIn([]byte(value)) {
				return true
			}
		}
	}
	return false
}

// windowReflected reports whether data holds any echoWindowLen consecutive
// bytes of secret. It slides a rolling hash over data against the set of
// secret windows' hashes and confirms a hash hit by comparing bytes.
func windowReflected(data, secret []byte) bool {
	if len(data) < echoWindowLen {
		return false
	}
	const base = 1099511628211 // FNV-64 prime, as a polynomial base
	var top uint64 = 1         // base^(echoWindowLen-1)
	for i := 0; i < echoWindowLen-1; i++ {
		top *= base
	}
	hash := func(window []byte) uint64 {
		var h uint64
		for _, b := range window {
			h = h*base + uint64(b) + 1
		}
		return h
	}
	windows := make(map[uint64]struct{}, len(secret))
	for i := 0; i+echoWindowLen <= len(secret); i++ {
		windows[hash(secret[i:i+echoWindowLen])] = struct{}{}
	}
	h := hash(data[:echoWindowLen])
	for i := 0; ; i++ {
		if _, ok := windows[h]; ok && bytes.Contains(secret, data[i:i+echoWindowLen]) {
			return true
		}
		if i+echoWindowLen >= len(data) {
			return false
		}
		h = (h-(uint64(data[i])+1)*top)*base + uint64(data[i+echoWindowLen]) + 1
	}
}
