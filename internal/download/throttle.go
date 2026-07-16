package download

import (
	"io"
	"sync"
	"time"
)

// throttledReader is a token-bucket reader that caps the byte rate pulled
// from the wrapped reader.
type throttledReader struct {
	r    io.Reader
	rate int64

	mu     sync.Mutex
	last   time.Time
	bucket int64
}

func newThrottledReader(r io.Reader, rate int64) *throttledReader {
	return &throttledReader{r: r, rate: rate, last: time.Now(), bucket: rate}
}

func (t *throttledReader) Read(p []byte) (int, error) {
	t.mu.Lock()
	t.refill()
	for t.bucket <= 0 {
		t.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		t.mu.Lock()
		t.refill()
	}
	n := min(int64(len(p)), t.bucket)
	t.mu.Unlock()
	read, err := t.r.Read(p[:n])
	t.mu.Lock()
	t.bucket -= int64(read)
	t.mu.Unlock()
	return read, err
}

// refill adds tokens for the time elapsed since the last refill; caller must
// hold t.mu.
func (t *throttledReader) refill() {
	now := time.Now()
	t.bucket += int64(now.Sub(t.last).Seconds() * float64(t.rate))
	t.last = now
	if t.bucket > t.rate {
		t.bucket = t.rate
	}
}
