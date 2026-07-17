package download

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mk990/sonrad/internal/upstream"
)

func TestPermanentDownloadError(t *testing.T) {
	cases := []struct {
		err       error
		permanent bool
	}{
		{&httpStatusError{code: 404, status: "404 Not Found"}, true},
		{&httpStatusError{code: 403, status: "403 Forbidden"}, true},
		{&httpStatusError{code: 429, status: "429 Too Many Requests"}, false},
		{&httpStatusError{code: 408, status: "408 Request Timeout"}, false},
		{&httpStatusError{code: 503, status: "503 Service Unavailable"}, false},
		{errors.New("connection reset"), false},
		{fmt.Errorf("wrapped: %w", &httpStatusError{code: 410, status: "410 Gone"}), true},
		// Message text mentioning a code must NOT trip the check (the old
		// string-matching bug).
		{errors.New("GET http://x/404-page: weird"), false},
		{nil, false},
	}
	for _, c := range cases {
		if got := permanentDownloadError(c.err); got != c.permanent {
			t.Errorf("permanentDownloadError(%v) = %v, want %v", c.err, got, c.permanent)
		}
	}
}

// TestTruncatedDownloadRetriesAndResumes: the CDN advertises 1000 bytes but
// cuts the first transfer at 500. The manager must not mark the file done;
// it must retry with a Range request and finish the remaining bytes.
func TestTruncatedDownloadRetriesAndResumes(t *testing.T) {
	const size = 1000
	data := []byte(strings.Repeat("x", size))
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n == 1 {
			// Declare the full size, deliver half, then bail — a truncated body.
			w.Header().Set("Content-Length", fmt.Sprint(size))
			w.WriteHeader(200)
			w.Write(data[:size/2])
			return
		}
		start := 0
		if rng := r.Header.Get("Range"); strings.HasPrefix(rng, "bytes=") {
			fmt.Sscanf(rng, "bytes=%d-", &start)
		}
		if start > 0 {
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, size-1, size))
			w.Header().Set("Content-Length", fmt.Sprint(size-start))
			w.WriteHeader(http.StatusPartialContent)
		}
		w.Write(data[start:])
	}))
	t.Cleanup(srv.Close)

	m := NewManager(context.Background(), upstream.New("test", "", false), Options{MaxConcurrent: 1, Retries: 3})
	dir := t.TempDir()
	m.Add(NewJob("movie", "movies", filepath.Join(dir, "movie"), []FileSpec{
		{URL: srv.URL + "/movie.mkv", Filename: "movie.mkv"},
	}))
	history := waitDone(t, m, 1)
	if history[0].Status != "Completed" {
		t.Fatalf("job status = %q (%s), want Completed", history[0].Status, history[0].FailMessage)
	}
	if atomic.LoadInt32(&calls) < 2 {
		t.Errorf("server saw %d request(s), want a retry after the truncated body", calls)
	}
	got, err := os.ReadFile(filepath.Join(dir, "movie", "movie.mkv"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Errorf("file content wrong: %d bytes, want %d", len(got), size)
	}
}

// TestPauseResume: while paused no transfer starts; on resume the job runs to
// completion.
func TestPauseResume(t *testing.T) {
	var served atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		w.Write([]byte("data"))
	}))
	t.Cleanup(srv.Close)

	m := NewManager(context.Background(), upstream.New("test", "", false), Options{MaxConcurrent: 1})
	m.Pause()
	if !m.Paused() {
		t.Fatal("Paused() = false after Pause()")
	}
	m.Add(NewJob("p", "tv", filepath.Join(t.TempDir(), "p"), []FileSpec{
		{URL: srv.URL + "/e1.mkv", Filename: "e1.mkv"},
	}))
	time.Sleep(200 * time.Millisecond)
	if n := served.Load(); n != 0 {
		t.Errorf("server saw %d request(s) while paused, want 0", n)
	}
	queue, history := m.Snapshot()
	if len(queue) != 1 || len(history) != 0 {
		t.Fatalf("queue=%d history=%d while paused, want 1/0", len(queue), len(history))
	}
	m.Resume()
	hist := waitDone(t, m, 1)
	if hist[0].Status != "Completed" {
		t.Fatalf("job status = %q, want Completed", hist[0].Status)
	}
}

// TestRetryFailedJob: a permanently failed job can be re-queued from history
// with Retry and then succeeds.
func TestRetryFailedJob(t *testing.T) {
	var broken atomic.Bool
	broken.Store(true)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if broken.Load() {
			http.NotFound(w, r)
			return
		}
		w.Write([]byte("data"))
	}))
	t.Cleanup(srv.Close)

	m := NewManager(context.Background(), upstream.New("test", "", false), Options{MaxConcurrent: 1, Retries: 2})
	m.Add(NewJob("r", "tv", filepath.Join(t.TempDir(), "r"), []FileSpec{
		{URL: srv.URL + "/e1.mkv", Filename: "e1.mkv"},
	}))
	history := waitDone(t, m, 1)
	if history[0].Status != "Failed" {
		t.Fatalf("job status = %q, want Failed", history[0].Status)
	}

	broken.Store(false)
	if !m.Retry(history[0].ID) {
		t.Fatal("Retry returned false for a history job")
	}
	if m.Retry("SABnzbd_nzo_nope") {
		t.Error("Retry returned true for an unknown id")
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		_, h := m.Snapshot()
		if len(h) == 1 && h[0].Status == "Completed" {
			if h[0].FailMessage != "" {
				t.Errorf("FailMessage = %q after successful retry, want empty", h[0].FailMessage)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("retried job never completed")
}
