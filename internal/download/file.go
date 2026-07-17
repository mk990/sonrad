package download

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// httpStatusError carries the status code of a non-2xx download response so
// retry logic can decide on the code itself instead of matching message text.
type httpStatusError struct {
	code   int
	status string // e.g. "404 Not Found"
}

func (e *httpStatusError) Error() string { return "HTTP " + e.status }

// permanentDownloadError reports whether a download error is NOT worth
// retrying. Only 4xx responses (except 408/429) are permanent. Anything else
// — network errors, 5xx, timeouts, truncated bodies — gets retried.
func permanentDownloadError(err error) bool {
	var he *httpStatusError
	if !errors.As(err, &he) {
		return false
	}
	if he.code == http.StatusRequestTimeout || he.code == http.StatusTooManyRequests {
		return false
	}
	return he.code >= 400 && he.code < 500
}

// downloadWithRetry calls downloadFile up to Options.Retries times with
// exponential backoff. Resume via HTTP Range means each retry continues from
// the bytes already on disk, so retries are cheap.
func (m *Manager) downloadWithRetry(ctx context.Context, urlStr, dest string, onProgress, onSize func(int64)) error {
	attempts := m.opts.Retries
	backoff := time.Second
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := m.downloadFile(ctx, urlStr, dest, onProgress, onSize)
		if err == nil {
			return nil
		}
		lastErr = err
		if permanentDownloadError(err) || ctx.Err() != nil {
			return err
		}
		if attempt == attempts {
			break
		}
		slog.Warn("download attempt failed, retrying", "dest", dest, "attempt", attempt, "of", attempts, "backoff", backoff, "err", err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return lastErr
}

// downloadFile fetches urlStr into dest, resuming from any bytes already on
// disk via HTTP Range and honoring the aggregate rate limit and pause gate.
func (m *Manager) downloadFile(ctx context.Context, urlStr, dest string, onProgress, onSize func(int64)) error {
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	var startAt int64
	if info, err := os.Stat(dest); err == nil {
		startAt = info.Size()
	}
	req, err := http.NewRequestWithContext(ctx, "GET", urlStr, nil)
	if err != nil {
		return err
	}
	if startAt > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", startAt))
	}
	resp, err := m.up.DoStream(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		// presumably already complete — the bytes already on disk are the size
		if onSize != nil && startAt > 0 {
			onSize(startAt)
		}
		if onProgress != nil && startAt > 0 {
			onProgress(0)
		}
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &httpStatusError{code: resp.StatusCode, status: resp.Status}
	}
	// Report the file's true total size as soon as the CDN reveals it. For a
	// 206 the Content-Length covers only the remaining range, so add the bytes
	// we resumed from; a 200 carries the whole file.
	if onSize != nil && resp.ContentLength > 0 {
		if resp.StatusCode == http.StatusPartialContent {
			onSize(startAt + resp.ContentLength)
		} else {
			onSize(resp.ContentLength)
		}
	}
	flag := os.O_CREATE | os.O_WRONLY
	if startAt > 0 && resp.StatusCode == http.StatusPartialContent {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
	}
	f, err := os.OpenFile(dest, flag, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var reader io.Reader = resp.Body
	if m.throttle != nil {
		reader = m.throttle.reader(resp.Body)
	}
	var written int64
	buf := make([]byte, 256*1024)
	for {
		n, er := reader.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				return werr
			}
			written += int64(n)
			if onProgress != nil {
				onProgress(int64(n))
			}
		}
		if er == io.EOF {
			// A clean EOF short of the advertised length means the CDN cut the
			// transfer — without this check the partial file would be marked done
			// and imported corrupt. Retry resumes from the bytes on disk.
			if resp.ContentLength > 0 && written < resp.ContentLength {
				return fmt.Errorf("truncated: got %d of %d bytes", written, resp.ContentLength)
			}
			return nil
		}
		if er != nil {
			return er
		}
		if err := m.waitResumed(ctx); err != nil {
			return err
		}
	}
}
