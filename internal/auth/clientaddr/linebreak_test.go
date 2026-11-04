package clientaddr

import "testing"

// A key with an embedded CR or LF is malformed; Go's base64 decoder skips both, so the parser must refuse them.
func TestProofKeysRefuseEmbeddedLineBreaks(t *testing.T) {
	good := testEntryA[3:]
	for name, sep := range map[string]string{"LF": "\n", "CRLF": "\r\n"} {
		broken := "k1:" + good[:20] + sep + good[20:]
		if _, err := ParseProofKeys(EnvVerifyKey, broken, MaxVerifyKeys); err == nil {
			t.Errorf("verify keys, %s: accepted", name)
		}
		if key, err := ParseSigningKey(broken); err == nil || key != nil {
			t.Errorf("signing key, %s: accepted", name)
		}
	}
}

func TestProofKeysStillAcceptAValidKeyAndTrimOuterWhitespace(t *testing.T) {
	if keys, err := ParseProofKeys(EnvVerifyKey, testEntryA, MaxVerifyKeys); err != nil || len(keys) != 1 {
		t.Fatalf("valid key: %v %v", keys, err)
	}
	if keys, err := ParseProofKeys(EnvVerifyKey, testEntryA+"\n", MaxVerifyKeys); err != nil || len(keys) != 1 {
		t.Fatalf("valid key with a trailing newline: %v %v", keys, err)
	}
	if key, err := ParseSigningKey(testEntryA); err != nil || key == nil {
		t.Fatalf("valid signing key: %v %v", key, err)
	}
}
