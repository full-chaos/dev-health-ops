package loadharness

import (
	"testing"
	"time"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestSummarizeNearestRank(t *testing.T) {
	var lat []time.Duration
	for i := 1; i <= 100; i++ {
		lat = append(lat, ms(i))
	}
	s := Summarize(lat, 0, 0)
	if s.Count != 100 || s.P50 != ms(50) || s.P95 != ms(95) || s.P99 != ms(99) || s.Max != ms(100) {
		t.Fatalf("got %+v", s)
	}
}

func TestSummarizeUnsortedInputKept(t *testing.T) {
	lat := []time.Duration{ms(30), ms(10), ms(20)}
	s := Summarize(lat, 0, 0)
	if s.P50 != ms(20) || s.Max != ms(30) {
		t.Fatalf("got %+v", s)
	}
	if lat[0] != ms(30) {
		t.Fatal("input was reordered")
	}
}

func TestSummarizeEdgeCases(t *testing.T) {
	if s := Summarize(nil, 0, 0); s.Count != 0 || s.P99 != 0 || s.ErrorRate != 0 {
		t.Fatalf("empty: %+v", s)
	}
	s := Summarize([]time.Duration{ms(7)}, 0, 0)
	if s.P50 != ms(7) || s.P95 != ms(7) || s.P99 != ms(7) {
		t.Fatalf("one sample: %+v", s)
	}
	s = Summarize([]time.Duration{ms(5), ms(5), ms(5), ms(5)}, 0, 0)
	if s.P99 != ms(5) {
		t.Fatalf("ties: %+v", s)
	}
}

func TestErrorRateCountsDroppedInDenominator(t *testing.T) {
	lat := []time.Duration{ms(1), ms(1), ms(1), ms(1), ms(1), ms(1)}
	s := Summarize(lat, 2, 2)
	if s.Count != 6 || s.Errors != 2 || s.Dropped != 2 {
		t.Fatalf("got %+v", s)
	}
	want := 4.0 / 8.0
	if s.ErrorRate != want {
		t.Fatalf("rate %v want %v", s.ErrorRate, want)
	}
}
