package chquery

import (
	"context"
	"testing"
	"time"
)

func TestTeamRepoDonorReadFailureIsNotMissingOwnership(t *testing.T) {
	fake := &capturingConn{}
	reader, err := NewReader(fake)
	if err != nil {
		t.Fatal(err)
	}
	if donors, err := reader.FetchTeamRepoDonors(context.Background(), []string{"issue"}, "org", time.Now()); err == nil || donors != nil {
		t.Fatalf("read failure became success: donors=%v err=%v", donors, err)
	}
	fake.query = ""
	if donors, err := reader.FetchTeamRepoDonors(context.Background(), nil, "org", time.Now()); err != nil || len(donors) != 0 || fake.query != "" {
		t.Fatalf("empty request must not query: donors=%v err=%v query=%s", donors, err, fake.query)
	}
	for _, org := range []string{"", " "} {
		if _, err := reader.FetchTeamRepoDonors(context.Background(), []string{"issue"}, org, time.Now()); err == nil {
			t.Fatal("unscoped query accepted")
		}
	}
}
