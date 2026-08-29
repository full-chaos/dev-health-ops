package providerfoundation

import "testing"

// TestCredentialFingerprintMatchesPythonOracle is a cross-language oracle
// proof, not just internal self-consistency: the expected digest below was
// computed independently in Python against the EXACT algorithm this ports
// (hashlib.sha256 over json.dumps(scope, sort_keys=True, default=str,
// separators=(",", ":")), scope={"api_key_sha256": sha256("linear-secret-
// token")}) -- proving this port produces byte-identical output to what a
// real sync_runs.credential_fingerprint stamped by Python at plan time would
// contain, not merely a plausible-looking hash.
func TestCredentialFingerprintMatchesPythonOracle(t *testing.T) {
	credential := testCredential("linear", map[string]string{"api_key": "linear-secret-token"})
	got := CredentialFingerprint(credential, "cred-1", "integration-1")
	want := "02540201d7df4a642cf4186cdadb423e1349df8fc27fa76563feb168d866a69b"
	if got != want {
		t.Fatalf("CredentialFingerprint=%q want=%q (Python oracle)", got, want)
	}
}

// TestCredentialFingerprintChangesWhenSecretRotates is the CHAOS-2755
// reachability proof: the whole point of this witness is that an in-place
// secret edit (same credential id, different bytes) must change the digest,
// so a stamped run can detect it. A no-op fingerprint that never changes
// would pass every mismatch check vacuously.
func TestCredentialFingerprintChangesWhenSecretRotates(t *testing.T) {
	before := CredentialFingerprint(testCredential("linear", map[string]string{"api_key": "old-token"}), "cred-1", "integration-1")
	after := CredentialFingerprint(testCredential("linear", map[string]string{"api_key": "new-token"}), "cred-1", "integration-1")
	if before == after {
		t.Fatalf("fingerprint did not change across a rotated secret: before=%q after=%q", before, after)
	}
}

// TestCredentialFingerprintStableAcrossIdenticalContent pins that hashing the
// same decrypted content twice (e.g. re-resolving for a retried run) yields
// the SAME digest -- required for the stamped-vs-resolved comparison to ever
// succeed on an unchanged credential.
func TestCredentialFingerprintStableAcrossIdenticalContent(t *testing.T) {
	first := CredentialFingerprint(testCredential("linear", map[string]string{"api_key": "stable-token"}), "cred-1", "integration-1")
	second := CredentialFingerprint(testCredential("linear", map[string]string{"api_key": "stable-token"}), "cred-1", "integration-1")
	if first != second {
		t.Fatalf("fingerprint is not stable for identical content: first=%q second=%q", first, second)
	}
}

// TestCredentialFingerprintFallsBackWhenCredentialHasNoRecognizedField pins
// safe_credential_scope's fallback branch: a credential exposing none of the
// identifier/secret keys (e.g. region-only PagerDuty config with no
// recognized field) still produces a deterministic, non-empty digest keyed
// by credential_id+integration_id, never an empty scope.
func TestCredentialFingerprintFallsBackWhenCredentialHasNoRecognizedField(t *testing.T) {
	credential := testCredential("pagerduty", map[string]string{})
	got := CredentialFingerprint(credential, "cred-1", "integration-1")
	if got == "" {
		t.Fatal("fallback scope produced an empty fingerprint")
	}
	// credential_id differing must change the fallback digest -- otherwise
	// every credential-less integration would collide on the same witness.
	other := CredentialFingerprint(credential, "cred-2", "integration-1")
	if got == other {
		t.Fatalf("fallback fingerprint did not vary with credential_id: %q", got)
	}
}
