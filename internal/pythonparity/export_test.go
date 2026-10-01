package pythonparity

// The frozen Python oracles of this package live in the external test package:
// the golden harness imports this package, so an in-package test cannot import
// the harness. They read the corpora and constants the in-package tests own
// through the names below.

// CapitalSigma is capitalSigma.
const CapitalSigma = capitalSigma

// FnMatchOracleCases is fnMatchCases as (pattern, name) pairs, in order.
func FnMatchOracleCases() [][2]string {
	pairs := make([][2]string, 0, len(fnMatchCases))
	for _, testCase := range fnMatchCases {
		pairs = append(pairs, [2]string{testCase.pattern, testCase.name})
	}
	return pairs
}

// IsoformatOracleCases is isoformatCases as (unix seconds, nanoseconds) pairs,
// in order.
func IsoformatOracleCases() [][2]int64 {
	moments := make([][2]int64, 0, len(isoformatCases))
	for _, testCase := range isoformatCases {
		moments = append(moments, [2]int64{testCase.unix, int64(testCase.nanos)})
	}
	return moments
}
