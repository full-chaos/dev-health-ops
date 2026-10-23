package identity

// ValidBcryptHash reports whether hash parses as a bcrypt verifier.
func ValidBcryptHash(hash string) bool { return validBcrypt(hash) }
