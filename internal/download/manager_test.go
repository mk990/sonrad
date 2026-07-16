package download

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mk990/sonrad/internal/upstream"
)

// slowServer serves a fixed-size body and tracks how many requests are being
// served at once. The active counter is decremented BEFORE the final byte is
// written, so a client can never observe fewer concurrent transfers than the
// server counted — the peak is an exact upper bound, not a racy estimate.
func slowServer(t *testing.T, active, peak *int32) *httptest.Server {
	t.Helper()
	const size = 4096
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt32(active, 1)
		for {
			old := atomic.LoadInt32(peak)
			if cur <= old || atomic.CompareAndSwapInt32(peak, old, cur) {
				break
			}
		}
		w.Header().Set("Content-Length", fmt.Sprint(size))
		w.WriteHeader(200)
		w.Write(make([]byte, size-1))
		w.(http.Flusher).Flush()
		time.Sleep(30 * time.Millisecond) // hold the slot so transfers overlap
		atomic.AddInt32(active, -1)
		w.Write([]byte{0})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func waitDone(t *testing.T, m *Manager, jobs int) []View {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		queue, history := m.Snapshot()
		if len(queue) == 0 && len(history) == jobs {
			return history
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("jobs did not finish in time")
	return nil
}

// TestMaxConcurrentWithinJob verifies that a single job with many files (a
// season pack) downloads at most MaxConcurrent files at a time — not all of
// them at once.
func TestMaxConcurrentWithinJob(t *testing.T) {
	var active, peak int32
	srv := slowServer(t, &active, &peak)

	m := NewManager(context.Background(), upstream.New("test", "", false), Options{MaxConcurrent: 3})
	var files []FileSpec
	for i := range 10 {
		files = append(files, FileSpec{
			URL:      fmt.Sprintf("%s/ep%d.mkv", srv.URL, i),
			Filename: fmt.Sprintf("ep%d.mkv", i),
		})
	}
	m.Add(NewJob("pack", "tv", filepath.Join(t.TempDir(), "pack"), files))

	history := waitDone(t, m, 1)
	if history[0].Status != "Completed" {
		t.Fatalf("job status = %q (%s), want Completed", history[0].Status, history[0].FailMessage)
	}
	if p := atomic.LoadInt32(&peak); p > 3 {
		t.Errorf("peak concurrent downloads = %d, want <= 3", p)
	}
	if p := atomic.LoadInt32(&peak); p < 2 {
		t.Errorf("peak concurrent downloads = %d — downloads never overlapped, gating is too strict", p)
	}
}

// TestMaxConcurrentAcrossJobs verifies the cap is global: many jobs added at
// once still share the same MaxConcurrent download slots.
func TestMaxConcurrentAcrossJobs(t *testing.T) {
	var active, peak int32
	srv := slowServer(t, &active, &peak)

	m := NewManager(context.Background(), upstream.New("test", "", false), Options{MaxConcurrent: 3})
	dir := t.TempDir()
	for job := range 5 {
		var files []FileSpec
		for i := range 2 {
			files = append(files, FileSpec{
				URL:      fmt.Sprintf("%s/j%de%d.mkv", srv.URL, job, i),
				Filename: fmt.Sprintf("j%de%d.mkv", job, i),
			})
		}
		m.Add(NewJob(fmt.Sprintf("job%d", job), "tv", filepath.Join(dir, fmt.Sprintf("job%d", job)), files))
	}

	history := waitDone(t, m, 5)
	for _, j := range history {
		if j.Status != "Completed" {
			t.Errorf("job %s status = %q (%s), want Completed", j.Name, j.Status, j.FailMessage)
		}
	}
	if p := atomic.LoadInt32(&peak); p > 3 {
		t.Errorf("peak concurrent downloads = %d, want <= 3", p)
	}
}

// TestAggregateRateLimit verifies the rate limit caps the TOTAL transfer
// speed, not each file's — 3 concurrent files must not download 3x faster
// than the configured limit.
func TestAggregateRateLimit(t *testing.T) {
	const fileSize = 64 * 1024
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(fileSize))
		w.Write(make([]byte, fileSize))
	}))
	t.Cleanup(srv.Close)

	// 3 files x 64 KiB = 192 KiB total at 128 KiB/s. The bucket starts full
	// (128 KiB burst), so the remaining 64 KiB must take at least ~0.5 s.
	m := NewManager(context.Background(), upstream.New("test", "", false), Options{
		MaxConcurrent: 3,
		RateLimit:     128 * 1024,
	})
	var files []FileSpec
	for i := range 3 {
		files = append(files, FileSpec{
			URL:      fmt.Sprintf("%s/f%d.mkv", srv.URL, i),
			Filename: fmt.Sprintf("f%d.mkv", i),
		})
	}
	start := time.Now()
	m.Add(NewJob("limited", "movies", filepath.Join(t.TempDir(), "limited"), files))
	history := waitDone(t, m, 1)
	elapsed := time.Since(start)

	if history[0].Status != "Completed" {
		t.Fatalf("job status = %q (%s), want Completed", history[0].Status, history[0].FailMessage)
	}
	if elapsed < 400*time.Millisecond {
		t.Errorf("3 files finished in %v — rate limit is per-file, not aggregate", elapsed)
	}
}
