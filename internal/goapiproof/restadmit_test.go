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
