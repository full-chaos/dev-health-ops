package investment

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/llmbudget"
)

type settleRecorder struct {
	mu     sync.Mutex
	output []int64
}

func (store *settleRecorder) Admit(context.Context, llmbudget.AdmitRequest) (llmbudget.Admission, error) {
	return llmbudget.Admission{ID: "r1", BudgetConfigured: true}, nil
}

func (store *settleRecorder) Settle(_ context.Context, settle llmbudget.SettleRequest) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	var out int64
	if settle.Usage.OutputTokens != nil {
		out = *settle.Usage.OutputTokens
	}
	store.output = append(store.output, out)
	return nil
}

// The output count is computed only for an answers OBJECT. Any other shape of
// answers (which the decision parser refuses), an absent answers, a count the
// provider reported, an absent usage and a rejected usage keep their value
// and their body.
func TestAClefOutputCountIsComputedOnlyForAnAnswersObject(t *testing.T) {
	const object = `{"q":{"type":"noul","noul":0.5}}`
	cases := []struct {
		name     string
		body     string
		want     int64
		computed bool
	}{
		{"object", `{"model":"M","answers":` + object + `,"usage":{"input_tokens":100}}`, int64(len(object)), true},
		{"array", `{"model":"M","answers":[1,2,3],"usage":{"input_tokens":100}}`, 0, false},
		{"string", `{"model":"M","answers":"abc","usage":{"input_tokens":100}}`, 0, false},
		{"null", `{"model":"M","answers":null,"usage":{"input_tokens":100}}`, 0, false},
		{"number", `{"model":"M","answers":7,"usage":{"input_tokens":100}}`, 0, false},
		{"boolean", `{"model":"M","answers":true,"usage":{"input_tokens":100}}`, 0, false},
		{"answers absent", `{"model":"M","usage":{"input_tokens":100}}`, 0, false},
		{"provider count kept", `{"model":"M","answers":` + object + `,"usage":{"input_tokens":100,"output_tokens":23}}`, 23, false},
		{"usage absent", `{"model":"M","answers":` + object + `}`, 0, false},
		{"usage rejected", `{"model":"M","answers":` + object + `,"usage":{"input_tokens":-1}}`, 0, false},
	}
	for _, model := range []string{"clef", "clef-flash"} {
		for _, tc := range cases {
			t.Run(model+"/"+tc.name, func(t *testing.T) {
				payload := bytes.ReplaceAll([]byte(tc.body), []byte(`"M"`), []byte(`"`+model+`"`))
				stub := newClefBYO(func(int, []byte) (int, []byte) { return http.StatusOK, payload })
				stub.cfg.Model = model
				recorder := &settleRecorder{}
				stub.store = recorder
				byo := stub.build()

				client, err := byo.newClef(categorize.ClefClientConfig{Token: stub.cfg.APIKey, BaseURL: stub.cfg.BaseURL, Model: model})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = client.Close() }()
				result, err := client.PostSystemOneDetailed(context.Background(), []byte(`{}`))
				if err != nil {
					t.Fatalf("post: %v", err)
				}
				if result.Usage.OutputTokens != tc.want || result.Usage.OutputComputed != tc.computed {
					t.Errorf("usage output %d computed %v, want %d %v", result.Usage.OutputTokens, result.Usage.OutputComputed, tc.want, tc.computed)
				}
				if !tc.computed && !bytes.Equal(result.Body, payload) {
					t.Errorf("the body was changed: %s", result.Body)
				}

				served, err := byo.Build(context.Background(), "org-1")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = served.Close() }()
				m := &Materializer{}
				m.SetServed(served)
				_, _ = m.categorizeEntry(context.Background(), shadowTestConfig(), shadowEntry(0, "u1", shadowTestBundle(t, "u1", 6)))
				rows := servedAttempts(t, served)
				if len(rows) != 1 {
					t.Fatalf("%d attempt rows", len(rows))
				}
				if int64(rows[0].OutputTokens) != tc.want {
					t.Errorf("attempt row output_tokens = %d, want %d", rows[0].OutputTokens, tc.want)
				}
				if len(recorder.output) != 1 || recorder.output[0] != tc.want {
					t.Errorf("settle output_tokens = %v, want [%d]", recorder.output, tc.want)
				}
			})
		}
	}
}
