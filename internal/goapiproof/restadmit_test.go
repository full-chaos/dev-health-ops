package goapiproof

import "testing"

func restSnapshotFromJSON(t *testing.T, body string) Snapshot {
	t.Helper()
	snap, err := DecodeRESTSnapshot([]byte(body))
	if err != nil {
		t.Fatalf("DecodeRESTSnapshot(%q): %v", body, err)
	}
	return snap
}

func TestDecodeRESTSnapshot_WholeBodyIsData(t *testing.T) {
	snap := restSnapshotFromJSON(t, `{"teams":["a","b"],"repos":[]}`)
	if !snap.DataPresent {
		t.Fatal("DataPresent = false, want true: a decoded REST body always carries Data")
	}
	body, ok := snap.Data.(map[string]any)
	if !ok {
		t.Fatalf("Data = %#v (%T), want a map", snap.Data, snap.Data)
	}
	if _, ok := body["teams"]; !ok {
		t.Fatalf("Data has no \"teams\" key: %#v", body)
	}
}

func TestDecodeRESTSnapshot_RefusesNonFiniteLiteral(t *testing.T) {
	if _, err := DecodeRESTSnapshot([]byte(`{"x":NaN}`)); err != ErrNonFiniteNumber {
		t.Fatalf("err = %v, want ErrNonFiniteNumber", err)
	}
}

func TestDecodeRESTSnapshot_RefusesInvalidUTF8(t *testing.T) {
	if _, err := DecodeRESTSnapshot([]byte("{\"x\":\"\xff\"}")); err == nil {
		t.Fatal("want a refusal for invalid UTF-8, got nil")
	}
}

func TestDecodeRESTSnapshot_TrailingBytes(t *testing.T) {
	snap := restSnapshotFromJSON(t, `{"x":1}garbage`)
	if !snap.TrailingBytes {
		t.Fatal("TrailingBytes = false, want true")
	}
}

// TestDecodeRESTSnapshot_AdmitsInsignificantTrailingWhitespace covers
// RFC 8259 insignificant whitespace (space, tab, CR, LF) after the first
// JSON value: admissible on either leg, so a Go response's trailing
// encoder newline must not read as different from a Python response
// carrying none.
func TestDecodeRESTSnapshot_AdmitsInsignificantTrailingWhitespace(t *testing.T) {
	cases := map[string]string{
		"trailing newline":       "{\"x\":1}\n",
		"trailing spaces":        "{\"x\":1}   ",
		"trailing tabs":          "{\"x\":1}\t\t",
		"trailing CRLF":          "{\"x\":1}\r\n",
		"trailing mixed run":     "{\"x\":1} \t\r\n \t",
		"no trailing bytes":      `{"x":1}`,
		"trailing newline array": "[1,2,3]\n",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			snap := restSnapshotFromJSON(t, body)
			if snap.TrailingBytes {
				t.Fatalf("%s: TrailingBytes = true, want false (insignificant whitespace is admissible)", name)
			}
		})
	}
}

// TestDecodeRESTSnapshot_RefusesNonWhitespaceTrailingByte covers the
// other half of the rule: a trailing byte that is not RFC 8259
// whitespace still refuses, even a single one right after an otherwise
// clean whitespace run.
func TestDecodeRESTSnapshot_RefusesNonWhitespaceTrailingByte(t *testing.T) {
	cases := map[string]string{
		"bare non-whitespace byte":       "{\"x\":1}x",
		"whitespace then non-whitespace": "{\"x\":1}\n x",
		"trailing comma":                 "{\"x\":1},",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			snap := restSnapshotFromJSON(t, body)
			if !snap.TrailingBytes {
				t.Fatalf("%s: TrailingBytes = false, want true", name)
			}
		})
	}
}

// TestDecodeRESTSnapshot_RefusesSecondJSONValue covers the explicit
// carve-out: a second JSON value after the first still refuses, whether
// or not RFC 8259 whitespace separates the two.
func TestDecodeRESTSnapshot_RefusesSecondJSONValue(t *testing.T) {
	cases := map[string]string{
		"adjacent second object":       `{"x":1}{"y":2}`,
		"whitespace-separated objects": "{\"x\":1}\n{\"y\":2}",
		"second scalar":                "{\"x\":1} 2",
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			snap := restSnapshotFromJSON(t, body)
			if !snap.TrailingBytes {
				t.Fatalf("%s: TrailingBytes = false, want true", name)
			}
		})
	}
}

func TestDecodeRESTSnapshot_BareNullIsDataPresent(t *testing.T) {
	snap := restSnapshotFromJSON(t, `null`)
	if !snap.DataPresent {
		t.Fatal("DataPresent = false for a bare null body, want true: the body decoded to a real (if null) JSON value")
	}
	if snap.Data != nil {
		t.Fatalf("Data = %#v, want nil", snap.Data)
	}
}

func restLeg(status int, body string, build string) RESTLeg {
	return RESTLeg{StatusCode: status, Body: []byte(body), Build: build}
}

func TestRESTAdmit_RefusesUnexpectedCandidateStatus(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(503, `{}`, "abc123"),
		Baseline:  restLeg(200, `{}`, ""),
	}
	got := RESTAdmit(in, true)
	if got.Admitted {
		t.Fatal("Admitted = true, want a refusal for an unexpected candidate status")
	}
	if got.Reason != RESTRefusalUnexpectedStatus {
		t.Fatalf("Reason = %q, want %q", got.Reason, RESTRefusalUnexpectedStatus)
	}
}

func TestRESTAdmit_RefusesUnexpectedBaselineStatus(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(200, `{}`, "abc123"),
		Baseline:  restLeg(500, `{}`, ""),
	}
	got := RESTAdmit(in, true)
	if got.Admitted || got.Reason != RESTRefusalUnexpectedStatus {
		t.Fatalf("got %+v, want a refusal for an unexpected baseline status", got)
	}
}

func TestRESTAdmit_RefusesAbsentBuildHeader(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(200, `{}`, ""),
		Baseline:  restLeg(200, `{}`, ""),
	}
	got := RESTAdmit(in, true)
	if got.Admitted || got.Reason != RESTRefusalBuildUnbound {
		t.Fatalf("got %+v, want RESTRefusalBuildUnbound", got)
	}
}

func TestRESTAdmit_RefusesMismatchedBuildHeader(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(200, `{}`, "someone-elses-build"),
		Baseline:  restLeg(200, `{}`, ""),
	}
	got := RESTAdmit(in, true)
	if got.Admitted || got.Reason != RESTRefusalBuildUnbound {
		t.Fatalf("got %+v, want RESTRefusalBuildUnbound", got)
	}
}

func TestRESTAdmit_RefusesTrailingBytes(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(200, `{"x":1}trailer`, "abc123"),
		Baseline:  restLeg(200, `{"x":1}`, ""),
	}
	got := RESTAdmit(in, true)
	if got.Admitted || got.Reason != RESTRefusalTrailingBytes {
		t.Fatalf("got %+v, want RESTRefusalTrailingBytes", got)
	}
}

func TestRESTAdmit_RefusesUndecodableBody(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(200, `{not json`, "abc123"),
		Baseline:  restLeg(200, `{}`, ""),
	}
	got := RESTAdmit(in, true)
	if got.Admitted || got.Reason != RESTRefusalBodyNotJSON {
		t.Fatalf("got %+v, want RESTRefusalBodyNotJSON", got)
	}
}

func TestRESTAdmit_AdmitsAndDecodesOnSuccess(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(200, `{"x":1}`, "abc123"),
		Baseline:  restLeg(200, `{"x":1}`, ""),
	}
	got := RESTAdmit(in, true)
	if !got.Admitted {
		t.Fatalf("got %+v, want admitted", got)
	}
	if !got.CandidateSnap.DataPresent || !got.BaselineSnap.DataPresent {
		t.Fatalf("snapshots not decoded: %+v", got)
	}
}

// TestRESTAdmit_AdmitsTrailingWhitespaceOnEitherLeg exercises the rule
// at the RESTAdmit level, not just DecodeRESTSnapshot in isolation: a
// trailing encoder newline on the candidate leg with none on the
// baseline leg, or the reverse, must still admit.
func TestRESTAdmit_AdmitsTrailingWhitespaceOnEitherLeg(t *testing.T) {
	t.Run("candidate carries the trailing newline", func(t *testing.T) {
		in := RESTAdmissionInput{
			NamedBuild:          "abc123",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			Candidate: restLeg(200, "{\"x\":1}\n", "abc123"),
			Baseline:  restLeg(200, `{"x":1}`, ""),
		}
		got := RESTAdmit(in, true)
		if !got.Admitted {
			t.Fatalf("got %+v, want admitted", got)
		}
	})
	t.Run("baseline carries the trailing whitespace", func(t *testing.T) {
		in := RESTAdmissionInput{
			NamedBuild:          "abc123",
			WantCandidateStatus: 200, WantBaselineStatus: 200,
			Candidate: restLeg(200, `{"x":1}`, "abc123"),
			Baseline:  restLeg(200, "{\"x\":1}  \t", ""),
		}
		got := RESTAdmit(in, true)
		if !got.Admitted {
			t.Fatalf("got %+v, want admitted", got)
		}
	})
}

// TestRESTAdmit_EmptyBodyOnOneLegOnlyStillRefusesBodyNotJSON pins the
// existing contract for an empty body on one leg only: an empty body
// does not decode to any JSON value, so it refuses as
// RESTRefusalBodyNotJSON, unchanged by the whitespace-admission rule.
func TestRESTAdmit_EmptyBodyOnOneLegOnlyStillRefusesBodyNotJSON(t *testing.T) {
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 200, WantBaselineStatus: 200,
		Candidate: restLeg(200, "", "abc123"),
		Baseline:  restLeg(200, `{"x":1}`, ""),
	}
	got := RESTAdmit(in, true)
	if got.Admitted || got.Reason != RESTRefusalBodyNotJSON {
		t.Fatalf("got %+v, want RESTRefusalBodyNotJSON", got)
	}
}

func TestRESTAdmit_StatusOnlySkipsDecode(t *testing.T) {
	// StatusOnly must never fail on a non-JSON body (a plain-text
	// http.Error), and must never populate either snapshot.
	in := RESTAdmissionInput{
		NamedBuild:          "abc123",
		WantCandidateStatus: 400, WantBaselineStatus: 422,
		Candidate: restLeg(400, "invalid start_date\n", "abc123"),
		Baseline:  restLeg(422, `{"detail":[{"type":"date_from_datetime_parsing"}]}`, ""),
	}
	got := RESTAdmit(in, false)
	if !got.Admitted {
		t.Fatalf("got %+v, want admitted (status-only mode ignores body shape)", got)
	}
	if got.CandidateSnap.DataPresent || got.BaselineSnap.DataPresent {
		t.Fatalf("status-only admission decoded a body: %+v", got)
	}
}

// TestRESTAdmit_StatusDivergentCorpusEntriesRefuseBothReversalsByName is
// corpus-derived, not a fixed list: it walks every REST corpus request
// whose two Want statuses differ and proves RESTAdmit refuses by name, in
// BOTH directions a real-world reversal could take -- the baseline
// recovering to the candidate's own declared status, and the candidate
// regressing to the baseline's own declared status -- so a FUTURE
// divergent entry is covered by this test with no edit to it.
func TestRESTAdmit_StatusDivergentCorpusEntriesRefuseBothReversalsByName(t *testing.T) {
	for operation, spec := range restEndpointSpecs {
		for _, req := range spec.Requests {
			if req.WantCandidateStatus == req.WantBaselineStatus {
				continue
			}
			decodeBody := req.BodyMode == RESTBodyModeJSON

			t.Run(operation+"/"+req.Name+"/baseline_recovers_to_candidates_status", func(t *testing.T) {
				in := RESTAdmissionInput{
					NamedBuild:          "abc123",
					WantCandidateStatus: req.WantCandidateStatus,
					WantBaselineStatus:  req.WantBaselineStatus,
					Candidate:           restLeg(req.WantCandidateStatus, `{}`, "abc123"),
					Baseline:            restLeg(req.WantCandidateStatus, `{}`, ""),
				}
				got := RESTAdmit(in, decodeBody)
				if got.Admitted || got.Reason != RESTRefusalUnexpectedStatus {
					t.Fatalf("%s/%s: baseline answering %d (the candidate's own declared status) got %+v, want a named RESTRefusalUnexpectedStatus refusal, not a silent pass", operation, req.Name, req.WantCandidateStatus, got)
				}
			})

			t.Run(operation+"/"+req.Name+"/candidate_regresses_to_baselines_status", func(t *testing.T) {
				in := RESTAdmissionInput{
					NamedBuild:          "abc123",
					WantCandidateStatus: req.WantCandidateStatus,
					WantBaselineStatus:  req.WantBaselineStatus,
					Candidate:           restLeg(req.WantBaselineStatus, `{}`, "abc123"),
					Baseline:            restLeg(req.WantBaselineStatus, `{}`, ""),
				}
				got := RESTAdmit(in, decodeBody)
				if got.Admitted || got.Reason != RESTRefusalUnexpectedStatus {
					t.Fatalf("%s/%s: candidate answering %d (the baseline's own declared status) got %+v, want a named RESTRefusalUnexpectedStatus refusal, not a silent pass", operation, req.Name, req.WantBaselineStatus, got)
				}
			})
		}
	}
}
