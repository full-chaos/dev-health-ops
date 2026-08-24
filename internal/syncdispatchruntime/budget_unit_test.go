package syncdispatchruntime

import "testing"

func TestDecodeProcessorFlags(t *testing.T) {
	t.Run("nil bytes decode to an empty non-nil map", func(t *testing.T) {
		got := decodeProcessorFlags(nil)
		if got == nil {
			t.Fatal("want non-nil map")
		}
		if len(got) != 0 {
			t.Fatalf("got=%v want empty", got)
		}
	})
	t.Run("a real flag set decodes correctly", func(t *testing.T) {
		got := decodeProcessorFlags([]byte(`{"family_dataset_work_items":true,"family_dataset_work_item_labels":true}`))
		if !got["family_dataset_work_items"] || !got["family_dataset_work_item_labels"] {
			t.Fatalf("got=%v, want both flags true", got)
		}
	})
	t.Run("malformed JSON decodes to an empty non-nil map, not a panic", func(t *testing.T) {
		got := decodeProcessorFlags([]byte(`not json`))
		if got == nil || len(got) != 0 {
			t.Fatalf("got=%v want empty non-nil map", got)
		}
	})
}
