package download

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// permanentDownloadError reports whether a download error is worth retrying.
// We only treat 4xx (except 408/429) as permanent. Anything else — network
// errors, 5xx, timeouts — gets retried.
func permanentDownloadError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	for _, code := range []string{"400", "401", "403", "404", "405", "410", "451"} {
		if strings.Contains(s, "HTTP "+code) {
			return true
		}
	}
	return false
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
		log.Printf("download %s attempt %d/%d failed: %v (retrying in %s)", dest, attempt, attempts, err, backoff)
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
// disk via HTTP Range and honoring the aggregate rate limit.
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
		return fmt.Errorf("HTTP %s", resp.Status)
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
	if m.opts.RateLimit > 0 {
		reader = newThrottledReader(resp.Body, m.opts.RateLimit)
	}
	buf := make([]byte, 256*1024)
	for {
		n, er := reader.Read(buf)
		if n > 0 {
			if _, werr := f.Write(buf[:n]); werr != nil {
				return werr
			}
			if onProgress != nil {
				onProgress(int64(n))
			}
		}
		if er == io.EOF {
			return nil
		}
		if er != nil {
			return er
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}
