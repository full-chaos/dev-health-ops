package explain

func pctp(v float64) *float64 { return &v }

// deltaOf reads a nullable delta as the number it served, 0 for null.
func deltaOf(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}
