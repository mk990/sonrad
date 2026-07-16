package download

import (
	"io"
	"sync"
	"time"
)

// throttle is a token bucket shared by every concurrent download, so the
// configured rate is a true aggregate cap rather than a per-file one.
type throttle struct {
	rate int64 // bytes/sec

	mu     sync.Mutex
	last   time.Time
	bucket int64
}

func newThrottle(rate int64) *throttle {
	return &throttle{rate: rate, last: time.Now(), bucket: rate}
}

// take blocks until at least one token is available, reserves up to `want`
// tokens and returns how many were reserved. Unused tokens must be handed
// back via refund.
func (t *throttle) take(want int64) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	for {
		t.refill()
		if t.bucket > 0 {
			n := min(want, t.bucket)
			t.bucket -= n
			return n
		}
		t.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		t.mu.Lock()
	}
}

func (t *throttle) refund(n int64) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	t.bucket = min(t.bucket+n, t.rate)
	t.mu.Unlock()
}

// refill adds tokens for the time elapsed since the last refill; caller must
// hold t.mu.
func (t *throttle) refill() {
	now := time.Now()
	t.bucket += int64(now.Sub(t.last).Seconds() * float64(t.rate))
	t.last = now
	if t.bucket > t.rate {
		t.bucket = t.rate
	}
}

// reader wraps r so its reads draw from the shared bucket.
func (t *throttle) reader(r io.Reader) io.Reader {
	return &throttledReader{r: r, t: t}
}

type throttledReader struct {
	r io.Reader
	t *throttle
}

func (tr *throttledReader) Read(p []byte) (int, error) {
	n := tr.t.take(int64(len(p)))
	read, err := tr.r.Read(p[:n])
	tr.t.refund(n - int64(read))
	return read, err
}
