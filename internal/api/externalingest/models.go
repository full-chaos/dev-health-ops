package externalingest

// entityFamilyForRecordKinds is entity_family_for_record_kinds: the single
// family every submitted kind implies, or "" when the batch mixes families
// (in which case _check_entity_family_or_400 always 400s, since
// source.entityFamily can only equal one of them).
func entityFamilyForRecordKinds(kinds []string) string {
	families := map[string]bool{}
	for _, kind := range kinds {
		if operationalRecordKinds[kind] {
			families["operational"] = true
		} else {
			families["legacy"] = true
		}
	}
	if len(families) != 1 {
		return ""
	}
	for family := range families {
		return family
	}
	return ""
}
