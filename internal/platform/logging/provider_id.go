package logging

import "log/slog"

// maxProviderAssignedIDBytes bounds an identifier a provider assigns (a
// request id, a message id).
const maxProviderAssignedIDBytes = 64

// ProviderAssignedID returns raw when it has the shape of an identifier a
// provider assigns -- 1 to 64 bytes of letters, digits, "_" and "-" -- and
// reports whether it was dropped. Any other value (longer, or holding any
// other byte) is dropped: the result is "" and dropped is true, and the log
// line records id_dropped=true. An empty raw is no id at all and is not
// dropped. This is the one way a value read from a provider response reaches
// a log line.
func ProviderAssignedID(raw string) (id string, dropped bool) {
	if raw == "" {
		return "", false
	}
	if len(raw) > maxProviderAssignedIDBytes {
		return "", true
	}
	for index := 0; index < len(raw); index++ {
		b := raw[index]
		if !(b == '_' || b == '-' || (b >= '0' && b <= '9') || (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')) {
			return "", true
		}
	}
	return raw, false
}

// droppedProviderID replaces, in a logged list, an id that did not pass
// ProviderAssignedID.
const droppedProviderID = "[id_dropped]"

// ProviderIDAttr is the log attribute for one provider-assigned id: key=id
// when the id passes ProviderAssignedID, otherwise <key>_dropped=true and no
// id. It is how a worker log line carries an id read from a provider.
func ProviderIDAttr(key, raw string) slog.Attr {
	id, dropped := ProviderAssignedID(raw)
	if dropped {
		return slog.Bool(key+"_dropped", true)
	}
	return slog.String(key, id)
}

// ProviderIDsAttr is the log attribute for a list of provider-assigned ids:
// each id passes ProviderAssignedID or is logged as "[id_dropped]".
func ProviderIDsAttr(key string, raw []string) slog.Attr {
	ids := make([]string, 0, len(raw))
	for _, value := range raw {
		id, dropped := ProviderAssignedID(value)
		if dropped {
			id = droppedProviderID
		}
		ids = append(ids, id)
	}
	return slog.Any(key, ids)
}
