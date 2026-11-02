package tokencorpus

import (
	"errors"
	"io"
)

const (
	TypServiceAccount     = "sa+jwt"
	ServiceAccountCeiling = 0
)

func GenerateServiceAccount(random io.Reader) (*Corpus, error) {
	return nil, errors.New("not implemented")
}

func CorpusDigest(dir string) (string, error) {
	return "", errors.New("not implemented")
}
