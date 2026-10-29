package restorefence

import (
	"context"
	"errors"
	"testing"
)

func TestApplyNeedsAManifestOrABlindMargin(t *testing.T) {
	if _, err := Apply(context.Background(), nil, testSchema(t), nil, Options{}); !errors.Is(err, ErrApply) {
		t.Fatalf("apply with no input = %v, want ErrApply", err)
	}
}

func TestApplyRefusesAMarginOutsideTheRevisionRange(t *testing.T) {
	for _, margin := range []int64{-1, maxRevision, maxRevision + 5} {
		if _, err := Apply(context.Background(), nil, testSchema(t), nil, Options{BlindMargin: margin}); !errors.Is(err, ErrApply) {
			t.Errorf("margin %d = %v, want ErrApply", margin, err)
		}
	}
}

func TestApplyRefusesAManifestThatLacksASection(t *testing.T) {
	m := sampleManifest(t)
	m.Sections = m.Sections[1:]
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	_, err := Apply(context.Background(), nil, testSchema(t), &m, Options{})
	if !errors.Is(err, ErrManifest) {
		t.Fatalf("a manifest with a missing section = %v, want ErrManifest", err)
	}
}

func TestApplyRefusesAManifestWhoseDigestIsStale(t *testing.T) {
	m := sampleManifest(t)
	m.Floors[0].Revision++
	_, err := Apply(context.Background(), nil, testSchema(t), &m, Options{})
	if !errors.Is(err, ErrManifest) {
		t.Fatalf("a stale digest = %v, want ErrManifest", err)
	}
}
