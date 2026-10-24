package syncdispatchruntime

// unitPageSize is the most sync_run_units rows one keyset page holds. A run may carry far more units than this; no
// statement, buffer or slice of a stage that pages grows with that count.
const unitPageSize = 500

// forEachUnitPage walks an id-ordered set in keyset pages: load(afterID, limit) returns at most limit rows with id
// greater than afterID, in id order. visit sees each page once, in order, after the page's query is closed, so it may
// run statements on the same transaction.
func forEachUnitPage[T any](limit int, load func(afterID string, limit int) ([]T, error), idOf func(T) string, visit func([]T) error) error {
	after := ""
	for {
		page, err := load(after, limit)
		if err != nil {
			return err
		}
		if len(page) == 0 {
			return nil
		}
		if err := visit(page); err != nil {
			return err
		}
		if len(page) < limit {
			return nil
		}
		after = idOf(page[len(page)-1])
	}
}
