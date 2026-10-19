package contracts

// NewSecretString wraps a credential a producer minted, so it is redacted
// everywhere it is printed from the moment it exists.
func NewSecretString(value string) SecretString { return SecretString{value: value} }
