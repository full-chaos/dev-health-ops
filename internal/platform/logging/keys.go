package logging

import (
	"strings"
	"unicode"
)

// protectedKeyWords and protectedKeyWordPairs decide which keys are
// protected: a key is protected when any of its words, or any two adjacent
// words, is listed. The same decision covers an attribute key, a group name on
// the attribute's path, an object key inside a logged value, and a key found
// inside free text. Wide on purpose: over-matching hides one value from a log
// line, while under-matching ships a credential to every log reader.
var protectedKeyWords = map[string]bool{
	"token": true, "secret": true, "password": true, "passwd": true, "credential": true,
	"authorization": true, "auth": true, "bearer": true, "cookie": true, "session": true,
	"signature": true, "apikey": true, "header": true, "ciphertext": true, "cert": true, "pem": true,
	"dsn": true, "uri": true,
}

var protectedKeyWordPairs = map[[2]string]bool{
	{"api", "key"}: true, {"private", "key"}: true, {"client", "secret"}: true, {"access", "key"}: true,
	{"raw", "payload"}: true, {"response", "body"}: true, {"request", "body"}: true,
	{"source", "metadata"}: true, {"integration", "config"}: true, {"database", "url"}: true,
}

// protectedPairWords are the words that appear in a protected pair.
var protectedPairWords = func() map[string]bool {
	words := map[string]bool{}
	if len(protectedKeyWordPairs) == 0 {
		return words
	}
	for pair := range protectedKeyWordPairs {
		words[pair[0]] = true
		words[pair[1]] = true
	}
	return words
}()

// protectedKeyFragments are matched inside the key's letters and digits run
// together, so a key whose words carry no separator ("apitoken",
// "ACCESSTOKEN", "userpassword") is still caught. Only fragments that are not
// ordinary parts of other words are listed; "auth", "uri" or "cert" stay
// word-level so "author", "security" or "concert" are not protected.
var protectedKeyFragments = []string{
	"token", "secret", "password", "passwd", "credential", "authorization", "bearer",
	"apikey", "privatekey", "clientsecret", "accesskey", "ciphertext", "cookie", "databaseurl",
}

// candidateWords is every lower-case string whose presence in a text makes a
// protected key possible there. A text containing none of them cannot hold a
// protected key, so the key/value scan is skipped for it.
var candidateWords = func() []string {
	seen := map[string]bool{}
	var words []string
	add := func(word string) {
		if !seen[word] {
			seen[word] = true
			words = append(words, word)
		}
	}
	if len(protectedKeyWords) == 0 || len(protectedKeyWordPairs) == 0 {
		return protectedKeyFragments
	}
	for word := range protectedKeyWords {
		add(word)
	}
	for pair := range protectedKeyWordPairs {
		add(pair[0])
	}
	for _, fragment := range protectedKeyFragments {
		add(fragment)
	}
	return words
}()

// keyWords splits a key into lower-case words at every separator (anything
// but a letter or digit: "_", "-", ".", spaces) and at camelCase boundaries,
// including an acronym followed by a word ("APIKey" -> "api", "key"). A
// plural of a listed word, or of a word of a listed pair, counts as that word.
func keyWords(key string) []string {
	var words []string
	var current []rune
	runes := []rune(key)
	flush := func() {
		if len(current) > 0 {
			words = append(words, strings.ToLower(string(current)))
			current = current[:0]
		}
	}
	for index, r := range runes {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()
			continue
		}
		if unicode.IsUpper(r) && len(current) > 0 {
			previous := current[len(current)-1]
			nextIsLower := index+1 < len(runes) && unicode.IsLower(runes[index+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextIsLower) {
				flush()
			}
		}
		current = append(current, r)
	}
	flush()
	for index, word := range words {
		if singular := strings.TrimSuffix(word, "s"); singular != word && (protectedKeyWords[singular] || protectedPairWords[singular]) {
			words[index] = singular
		}
	}
	return words
}

// tokenCountWords are the words that, directly before "tokens", make the pair
// a count of language-model tokens ("prompt_tokens", "completionTokens"), not
// a credential.
var tokenCountWords = map[string]bool{
	"prompt": true, "completion": true, "input": true, "output": true,
	"total": true, "max": true, "cached": true, "reasoning": true,
}

// ProtectedKey reports whether a key names a protected value in any spelling
// (snake_case, kebab-case, camelCase, dotted, run together, plural).
func ProtectedKey(key string) bool {
	words := keyWords(key)
	if count := len(words); count >= 2 && words[count-1] == "token" && tokenCountWords[words[count-2]] &&
		strings.HasSuffix(strings.ToLower(key), "tokens") {
		words = words[:count-2]
	}
	for index, word := range words {
		if protectedKeyWords[word] {
			return true
		}
		if index+1 < len(words) && protectedKeyWordPairs[[2]string{word, words[index+1]}] {
			return true
		}
	}
	joined := strings.Join(words, "")
	for _, fragment := range protectedKeyFragments {
		if strings.Contains(joined, fragment) {
			return true
		}
	}
	return false
}

// mayHoldProtectedKey is the cheap prefilter in front of the key/value scan.
func mayHoldProtectedKey(text string) bool {
	lower := strings.ToLower(text)
	for _, word := range candidateWords {
		if strings.Contains(lower, word) {
			return true
		}
	}
	return false
}
