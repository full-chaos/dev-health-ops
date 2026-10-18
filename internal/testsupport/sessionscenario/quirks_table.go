package sessionscenario

// Quirks classes every recorded behaviour of the corpus goldens that differs from a clean contract
// (CHAOS-10157). A reimplementation (auth-service) keeps Contractual entries, may answer Accidental ones
// cleanly, and must not keep Unsafe ones. TestQuirkTable fails on a golden quirk with no entry here.
var Quirks = []Quirk{}
