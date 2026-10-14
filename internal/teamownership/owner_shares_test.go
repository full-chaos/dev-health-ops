package teamownership

import (
	"math"
	"testing"
)

func share(v float64) *float64 { return &v }

// CHAOS-9410, D5988/D5990/D5991: the main owner and every lesser owner, each at
// its own stored share renormalised over all owners; equal only where an owner
// has no measure.
func TestOwnerSharesOfRepo(t *testing.T) {
	tests := []struct {
		name   string
		order  []string
		mains  []string
		stored map[string]*float64
		want   []OwnerShare
	}{
		{
			name: "main owner and two lesser owners keep their own shares", order: []string{"a", "b", "c"}, mains: []string{"a"},
			stored: map[string]*float64{"a": share(0.5), "b": share(0.3), "c": share(0.2)},
			want:   []OwnerShare{{"a", true, 0.5}, {"b", false, 0.3}, {"c", false, 0.2}},
		},
		{
			name: "stored shares that do not sum to one are renormalised", order: []string{"a", "b"}, mains: []string{"a"},
			stored: map[string]*float64{"a": share(6), "b": share(2)},
			want:   []OwnerShare{{"a", true, 0.75}, {"b", false, 0.25}},
		},
		{
			name: "a full tie is every tied team main at an equal share", order: []string{"b", "a"}, mains: []string{"b", "a"},
			stored: map[string]*float64{"a": share(0.5), "b": share(0.5)},
			want:   []OwnerShare{{"b", true, 0.5}, {"a", true, 0.5}},
		},
		{
			name: "lesser owners are listed by share, then team id", order: []string{"m", "z", "x", "y"}, mains: []string{"m"},
			stored: map[string]*float64{"m": share(0.4), "z": share(0.2), "x": share(0.2), "y": share(0.2)},
			want:   []OwnerShare{{"m", true, 0.4}, {"x", false, 0.2}, {"y", false, 0.2}, {"z", false, 0.2}},
		},
		{
			name: "an owner with no measured share splits the repository equally", order: []string{"a", "b"}, mains: []string{"a"},
			stored: map[string]*float64{"a": share(0.9), "b": nil},
			want:   []OwnerShare{{"a", true, 0.5}, {"b", false, 0.5}},
		},
		{
			name: "shares that sum to nothing split equally", order: []string{"a", "b"}, mains: []string{"a"},
			stored: map[string]*float64{"a": share(0), "b": share(0)},
			want:   []OwnerShare{{"a", true, 0.5}, {"b", false, 0.5}},
		},
		{
			name: "a sole owner holds the repository whole", order: []string{"a"}, mains: []string{"a"},
			stored: map[string]*float64{"a": nil},
			want:   []OwnerShare{{"a", true, 1}},
		},
		{
			name: "co-owners with no main owner are lesser owners", order: []string{"a", "b"}, mains: nil,
			stored: map[string]*float64{"a": share(0.7), "b": share(0.3)},
			want:   []OwnerShare{{"a", false, 0.7}, {"b", false, 0.3}},
		},
		{
			name: "a drifted share is a real share, no rounding", order: []string{"a", "b"}, mains: []string{"a"},
			stored: map[string]*float64{"a": share(0.5000001), "b": share(0.4999999)},
			want:   []OwnerShare{{"a", true, 0.5000001}, {"b", false, 0.4999999}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ownerSharesOfRepo(test.order, test.mains, func(id string) *float64 { return test.stored[id] })
			if len(got) != len(test.want) {
				t.Fatalf("got %v, want %v", got, test.want)
			}
			for i := range got {
				if got[i].TeamID != test.want[i].TeamID || got[i].Main != test.want[i].Main || math.Abs(got[i].Share-test.want[i].Share) > 1e-12 {
					t.Fatalf("owner %d = %+v, want %+v", i, got[i], test.want[i])
				}
			}
		})
	}
}
