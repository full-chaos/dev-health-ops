package pybody

import "testing"

// TestEmailStrShapes pins RequiredEmailStr and OptionalEmailStr against
// FastAPI's own answers for one input of each shape (captured from the live
// app that TestEmailStrMatchesLiveFastAPI runs).
func TestEmailStrShapes(t *testing.T) {
	const period = `The part after the @-sign is not valid. It should have a period.`
	invalid := `{"detail":[{"type":"value_error","loc":["body","email"],"msg":"value is not a valid email address: ` + period +
		`","input":"a@b","ctx":{"reason":"` + period + `"}}]}`
	stringType := func(input string) string {
		return `{"detail":[{"type":"string_type","loc":["body","email"],"msg":"Input should be a valid string","input":` + input + `}]}`
	}
	cases := []struct {
		path, body string
		status     int
		want       string
	}{
		{"/required", `{}`, 422, `{"detail":[{"type":"missing","loc":["body","email"],"msg":"Field required","input":{}}]}`},
		{"/required", `{"email": null}`, 422, stringType("null")},
		{"/required", `{"email": 5}`, 422, stringType("5")},
		{"/required", `{"email": "Admin@Example.COM"}`, 200, `{"email":"Admin@example.com"}`},
		{"/required", `{"email": "a@b"}`, 422, invalid},
		{"/optional", `{}`, 200, `{"email":null}`},
		{"/optional", `{"email": null}`, 200, `{"email":null}`},
		{"/optional", `{"email": 5}`, 422, stringType("5")},
		{"/optional", `{"email": "Admin@Example.COM"}`, 200, `{"email":"Admin@example.com"}`},
		{"/optional", `{"email": "a@b"}`, 422, invalid},
	}
	for _, c := range cases {
		status, got := serveEmailStr(t, c.path, []byte(c.body))
		if status != c.status || got != c.want {
			t.Errorf("%s %s: got %d %s, want %d %s", c.path, c.body, status, got, c.status, c.want)
		}
	}
}
