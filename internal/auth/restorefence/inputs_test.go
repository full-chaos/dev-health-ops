package restorefence

import (
	"context"
	"errors"
	"testing"
)

type fakeInput struct {
	name   string
	floors map[string]int64
	status error
}

func (f fakeInput) Name() string                                     { return f.name }
func (f fakeInput) Status(context.Context) error                     { return f.status }
func (f fakeInput) Floors(context.Context) (map[string]int64, error) { return f.floors, nil }

func floorOf(floors []Floor, typ, id string) int64 {
	for _, f := range floors {
		if f.AggregateType == typ && f.AggregateID == id {
			return f.Revision
		}
	}
	return 0
}

func TestTheFloorIsTheMaximumOverEveryNamedInput(t *testing.T) {
	m := sampleManifest(t)
	got, err := resolveInputs(context.Background(), &m, Options{}, []FloorInput{
		fakeInput{name: "consumer_a", floors: map[string]int64{"organization:org_a": 9}},
		fakeInput{name: "consumer_b", floors: map[string]int64{"organization:org_b": 2, "membership:mbr_x": 5}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if floorOf(got.floors, "organization", "org_a") != 9 || floorOf(got.floors, "organization", "org_b") != 7 || floorOf(got.floors, "membership", "mbr_x") != 5 {
		t.Fatalf("floors = %+v", got.floors)
	}
	names := []string{}
	for _, r := range got.reports {
		names = append(names, r.Name)
		if !r.Used {
			t.Errorf("input %s was not used: %+v", r.Name, r)
		}
	}
	if len(names) != 3 || names[0] != "wal_manifest" {
		t.Fatalf("per-input report = %v, want wal_manifest first then both consumers", names)
	}
	if got.source != "manifest" {
		t.Fatalf("source = %s", got.source)
	}
}

func TestAnInputWhoseStatusFailsIsReportedAndNotUsed(t *testing.T) {
	m := sampleManifest(t)
	got, err := resolveInputs(context.Background(), &m, Options{}, []FloorInput{
		fakeInput{name: "advisory", floors: map[string]int64{"organization:org_a": 99}, status: errors.New("not durable")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if floorOf(got.floors, "organization", "org_a") != 3 {
		t.Fatal("a floor from an input whose status failed was used")
	}
	last := got.reports[len(got.reports)-1]
	if last.Name != "advisory" || last.Used || last.Reason == "" {
		t.Fatalf("the skipped input is not reported: %+v", last)
	}
	if last.Reason == "not durable" {
		t.Fatal("the status error text reached the report")
	}
}

func TestTheBlindMarginAppliesOnlyWhenNoOtherInputExists(t *testing.T) {
	m := sampleManifest(t)
	if _, err := resolveInputs(context.Background(), &m, Options{BlindMargin: 5}, nil); !errors.Is(err, ErrApply) {
		t.Fatalf("margin with a manifest = %v, want ErrApply", err)
	}
	usable := []FloorInput{fakeInput{name: "consumer", floors: map[string]int64{"organization:org_a": 1}}}
	if _, err := resolveInputs(context.Background(), nil, Options{BlindMargin: 5}, usable); !errors.Is(err, ErrApply) {
		t.Fatalf("margin with a usable consumer input = %v, want ErrApply", err)
	}
	unusable := []FloorInput{fakeInput{name: "consumer", status: errors.New("down")}}
	got, err := resolveInputs(context.Background(), nil, Options{BlindMargin: 5}, unusable)
	if err != nil {
		t.Fatalf("margin with only an unusable input: %v", err)
	}
	if got.source != "blind_margin" || got.margin != 5 {
		t.Fatalf("resolved = %+v", got)
	}
	if last := got.reports[len(got.reports)-1]; last.Name != "blind_margin" || !last.Used {
		t.Fatalf("the margin is not reported as used: %+v", last)
	}
}

func TestWithNoManifestAndNoMarginAUsableInputAloneIsEnoughAndNothingIsNot(t *testing.T) {
	usable := []FloorInput{fakeInput{name: "consumer", floors: map[string]int64{"organization:org_a": 4}}}
	got, err := resolveInputs(context.Background(), nil, Options{}, usable)
	if err != nil || got.source != "inputs" || floorOf(got.floors, "organization", "org_a") != 4 {
		t.Fatalf("inputs alone = %+v, %v", got, err)
	}
	if _, err := resolveInputs(context.Background(), nil, Options{}, nil); !errors.Is(err, ErrApply) {
		t.Fatalf("no input at all = %v, want ErrApply", err)
	}
	if _, err := resolveInputs(context.Background(), nil, Options{}, []FloorInput{fakeInput{name: "c", status: errors.New("x")}}); !errors.Is(err, ErrApply) {
		t.Fatalf("only an unusable input = %v, want ErrApply", err)
	}
}
