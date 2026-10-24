package syncdispatchruntime

// dispatchReadPageSize is K: the most rows one paged dispatch read returns per query.
const dispatchReadPageSize = 500

// forEachKeysetPage folds rows read in id order, at most limit per fetch. fetch returns the rows whose id is greater
// than after (nil for the first page), in id order, at most limit of them. A page shorter than limit ends the walk.
func forEachKeysetPage[T any](
	limit int, idOf func(T) string,
	fetch func(after *string, limit int) ([]T, error), fold func(T) error,
) error {
	var after *string
	for {
		rows, err := fetch(after, limit)
		if err != nil {
			return err
		}
		for _, row := range rows {
			if err := fold(row); err != nil {
				return err
			}
		}
		if len(rows) < limit {
			return nil
		}
		last := idOf(rows[len(rows)-1])
		after = &last
	}
}
