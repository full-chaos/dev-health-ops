// Package passwordhash is the one password hash every Go route writes:
// bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()) as the Python
// api calls it (services/users.py _hash_password, register.py,
// password_reset.py).
package passwordhash

import (
	"strings"

	"golang.org/x/crypto/bcrypt"
)

// Cost is bcrypt.gensalt()'s default rounds.
const Cost = 12

// Hash returns the bcrypt hash of password with a fresh salt, in the "$2b$"
// form Python's bcrypt writes. Go's bcrypt writes "$2a$"; for any password it
// accepts (at most 72 bytes) the two identifiers name the same computation,
// so only the identifier is rewritten. A password over 72 bytes is refused,
// as bcrypt 5 raises ValueError for it.
func Hash(password string) (string, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte(password), Cost)
	if err != nil {
		return "", err
	}
	text := string(hash)
	if rest, ok := strings.CutPrefix(text, "$2a$"); ok {
		text = "$2b$" + rest
	}
	return text, nil
}
