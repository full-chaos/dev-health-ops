package apiservice

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/google/uuid"
)

// CHAOS-12402 (pm 11-03): the token `sub` of a workload is the session wire form (prn_ + base64url of the 16 bytes).
// A UUID-text form in AUTH_ACR_WORKLOAD_SUBJECT can never match a token. It must not stop the start (D6465), so it
// is an ERROR line and a gauge, and the chart refuses it at render time.
const (
	acrSubjectUUIDForm = "prn_00000000-0000-4000-8000-000000000000"
	acrSubjectBaseForm = "prn_AAAAAAAAAAAAAAAAAAAAAA"
)

func captureACRSubjectState(t *testing.T) (*bytes.Buffer, *[]bool) {
	t.Helper()
	var states []bool
	previous := recordACRWorkloadSubjectInvalid
	recordACRWorkloadSubjectInvalid = func(_ context.Context, invalid bool) { states = append(states, invalid) }
	t.Cleanup(func() { recordACRWorkloadSubjectInvalid = previous })
	var buf bytes.Buffer
	return &buf, &states
}

func TestACRWorkloadSubjectUUIDFormLogsErrorAndSetsGaugeAndStillStarts(t *testing.T) {
	buf, states := captureACRSubjectState(t)
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	lookup := acrWorkloadLookup(map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: acrSubjectUUIDForm})
	workload, err := buildACRWorkload(lookup, authverify.StaticKeys{}, logger)
	if err != nil || workload == nil {
		t.Fatalf("a UUID-form subject must not stop the start (D6465): workload %v err %v", workload, err)
	}
	out := buf.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, envACRWorkloadSubject) {
		t.Fatalf("no ERROR line naming %s: %q", envACRWorkloadSubject, out)
	}
	if strings.Contains(out, acrSubjectUUIDForm) || strings.Contains(out, strings.TrimPrefix(acrSubjectUUIDForm, "prn_")) {
		t.Fatalf("the log line carries the value: %q", out)
	}
	if len(*states) != 1 || !(*states)[0] {
		t.Fatalf("gauge states %v, want one set of 1", *states)
	}
}

func TestACRWorkloadSubjectBaseFormLogsNothingAndClearsGauge(t *testing.T) {
	buf, states := captureACRSubjectState(t)
	logger := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	lookup := acrWorkloadLookup(map[string]string{envAccessTokenIssuer: acrWorkloadIssuer, envACRWorkloadSubject: acrSubjectBaseForm})
	if workload, err := buildACRWorkload(lookup, authverify.StaticKeys{}, logger); err != nil || workload == nil {
		t.Fatalf("workload %v err %v", workload, err)
	}
	if buf.Len() != 0 {
		t.Fatalf("a base64-form subject logged %q", buf.String())
	}
	if len(*states) != 1 || (*states)[0] {
		t.Fatalf("gauge states %v, want one set of 0", *states)
	}
}

func TestACRWorkloadSubjectFormIsTheCanonicalSessionForm(t *testing.T) {
	id := uuid.MustParse("00112233-4455-6677-8899-aabbccddeeff")
	for name, tc := range map[string]struct {
		subject string
		want    bool
	}{
		"session form":                {"prn_ABEiM0RVZneImaq7zN3u_w", true},
		"uuid text form":              {"prn_" + id.String(), false},
		"21 chars":                    {"prn_ABEiM0RVZneImaq7zN3u_", false},
		"23 chars":                    {"prn_ABEiM0RVZneImaq7zN3u_wA", false},
		"non canonical trailing bits": {"prn_ABEiM0RVZneImaq7zN3u_x", false},
	} {
		if got := isACRWorkloadSubjectSessionForm(tc.subject); got != tc.want {
			t.Errorf("%s: session form = %v, want %v", name, got, tc.want)
		}
	}
}
