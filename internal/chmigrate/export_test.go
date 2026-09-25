package chmigrate

// SetRepairBeforeDelete installs a hook that runs before each stale row's
// mutation (where a concurrent writer could add a newer row of the same key) and
// returns the function that removes it.
func SetRepairBeforeDelete(hook func(id, org string)) (restore func()) {
	previous := beforeDelete
	beforeDelete = func(row staleRow) { hook(row.ID, row.StaleOrg) }
	return func() { beforeDelete = previous }
}
