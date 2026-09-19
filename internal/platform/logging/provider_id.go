package logging

// maxProviderAssignedIDBytes bounds an identifier a provider assigns (a
// request id, a message id).
const maxProviderAssignedIDBytes = 64

// ProviderAssignedID returns raw when it has the shape of an identifier a
// provider assigns -- 1 to 64 bytes of letters, digits, "_" and "-" -- and
// reports whether it was dropped. Any other value (longer, or holding any
// other byte) is dropped: the result is "" and dropped is true, and the log
// line records id_dropped=true. An empty raw is no id at all and is not
// dropped. This is the one way a value read from a provider response reaches
// a log line.
func ProviderAssignedID(raw string) (id string, dropped bool) {
	if raw == "" {
		return "", false
	}
	if len(raw) > maxProviderAssignedIDBytes {
		return "", true
	}
	for index := 0; index < len(raw); index++ {
		b := raw[index]
		if !(b == '_' || b == '-' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')) {
			return "", true
		}
	}
	return raw, false
}
