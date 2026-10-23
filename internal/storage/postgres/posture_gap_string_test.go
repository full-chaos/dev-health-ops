package postgres

import "testing"

func TestPostureGapStringNamesTheColumnOfAColumnExcess(t *testing.T) {
	for _, test := range []struct {
		gap  PostureGap
		want string
	}{
		{PostureGap{TableName: "t", Excess: []string{"SELECT WITH GRANT OPTION"}},
			"t: excess privileges [SELECT WITH GRANT OPTION] beyond the declared posture"},
		{PostureGap{TableName: "t", ColumnName: "c", Excess: []string{"UPDATE WITH GRANT OPTION"}},
			"t.c: excess privileges [UPDATE WITH GRANT OPTION] beyond the declared posture"},
		{PostureGap{TableName: "s", Excess: []string{"USAGE WITH GRANT OPTION"}},
			"s: excess privileges [USAGE WITH GRANT OPTION] beyond the declared posture"},
	} {
		if got := test.gap.String(); got != test.want {
			t.Errorf("String() = %q, want %q", got, test.want)
		}
	}
}
