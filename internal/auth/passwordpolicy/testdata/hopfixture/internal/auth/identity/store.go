// Package identity holds the fixture's two password writers.
package identity

type Exec func(sql string, args ...any)

func InsertUser(x Exec, hash string) {
	x(`INSERT INTO public.users (id, password_hash) VALUES ($1, $2)`, hash)
}

func SetPassword(x Exec, hash string) {
	x(`UPDATE public.users SET password_hash = $2, token_version = token_version + 1 WHERE id = $1`, hash)
}
