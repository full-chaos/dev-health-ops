package providerfoundation

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
)

type withinCountingReader struct {
	reader io.Reader
	read   int
}

func (counter *withinCountingReader) Read(p []byte) (int, error) {
	n, err := counter.reader.Read(p)
	counter.read += n
	return n, err
}

func withinResponse(contentLength int64, size int) (*http.Response, *withinCountingReader) {
	counter := &withinCountingReader{reader: bytes.NewReader(bytes.Repeat([]byte{'a'}, size))}
	return &http.Response{StatusCode: http.StatusOK, ContentLength: contentLength, Body: io.NopCloser(counter)}, counter
}

func TestReadResponseBodyWithin(t *testing.T) {
	const limit = 1000
	for _, row := range []struct {
		name          string
		contentLength int64
		size          int
		wantLen       int
		wantOver      int64
		maxRead       int
		maxCap        int
	}{
		{"declared above limit reads nothing", limit + 7, limit + 7, 0, limit + 7, 0, 0},
		{"declared at limit is read whole", limit, limit, limit, 0, limit, limit + 1},
		{"declared below limit, no doubling", 10, 10, 10, 0, 10, 11},
		{"declared zero", 0, 0, 0, 0, 0, 1},
		{"unknown length within limit", -1, 700, 700, 0, 700, -1},
		{"unknown length above limit stops at limit+1", -1, limit * 5, 0, limit + 1, limit + 1, -1},
		{"under-reported length stops at limit+1", 10, limit * 5, 0, limit + 1, limit + 1, -1},
		{"under-reported length within limit grows", 10, 600, 600, 0, 600, -1},
	} {
		t.Run(row.name, func(t *testing.T) {
			response, counter := withinResponse(row.contentLength, row.size)
			body, over, err := ReadResponseBodyWithin(response, limit, func(err error) error { return err })
			if err != nil {
				t.Fatal(err)
			}
			if len(body) != row.wantLen || over != row.wantOver {
				t.Fatalf("len=%d over=%d, want %d/%d", len(body), over, row.wantLen, row.wantOver)
			}
			if counter.read > row.maxRead {
				t.Fatalf("read %d bytes, want <= %d", counter.read, row.maxRead)
			}
			if row.maxCap >= 0 && cap(body) > row.maxCap {
				t.Fatalf("cap=%d, want <= %d", cap(body), row.maxCap)
			}
		})
	}
}

func TestReadResponseBodyWithinNeverAllocatesFromAHugeDeclaredLength(t *testing.T) {
	response, _ := withinResponse(1<<40, 0)
	body, over, err := ReadResponseBodyWithin(response, 1000, func(err error) error { return err })
	if err != nil || body != nil || over != 1<<40 {
		t.Fatalf("body=%d over=%d err=%v, want nil/2^40/nil", len(body), over, err)
	}
}

type withinFailingReader struct{}

func (withinFailingReader) Read([]byte) (int, error) { return 0, errors.New("reset") }

func TestReadResponseBodyWithinWrapsReadFailures(t *testing.T) {
	cause := errors.New("transport")
	response := &http.Response{StatusCode: http.StatusOK, ContentLength: -1, Body: io.NopCloser(withinFailingReader{})}
	_, _, err := ReadResponseBodyWithin(response, 1000, func(error) error { return cause })
	var providerErr *ProviderError
	if !errors.As(err, &providerErr) || providerErr.Reason != ReasonBodyRead || !errors.Is(err, cause) {
		t.Fatalf("err=%v, want a body_read ProviderError wrapping the cause", err)
	}
	if _, _, err := ReadResponseBodyWithin(nil, 1000, func(err error) error { return err }); !errors.Is(err, ErrNormalizationInvalid) {
		t.Fatalf("nil response err=%v", err)
	}
}
