package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mk990/sonrad/internal/config"
	"github.com/mk990/sonrad/internal/download"
	"github.com/mk990/sonrad/internal/film2"
	"github.com/mk990/sonrad/internal/upstream"
)

func testServer(t *testing.T, cfg *config.Config) *Server {
	t.Helper()
	up := upstream.New("test", "", false)
	mgr := download.NewManager(context.Background(), up, download.Options{MaxConcurrent: 1})
	if cfg.DownloadDir == "" {
		cfg.DownloadDir = t.TempDir()
	}
	return New(cfg, "test", mgr, nil, up)
}

func get(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
	return rec
}

func TestIndexerAuth(t *testing.T) {
	s := testServer(t, &config.Config{APIKey: "sekret"})
	h := s.Handler()
	if rec := get(t, h, "/api?t=caps"); rec.Code != http.StatusUnauthorized {
		t.Errorf("no key: status %d, want 401", rec.Code)
	}
	if rec := get(t, h, "/api?t=caps&apikey=wrong"); rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong key: status %d, want 401", rec.Code)
	}
	rec := get(t, h, "/api?t=caps&apikey=sekret")
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), "<caps>") {
		t.Errorf("good key: status %d, body %q", rec.Code, rec.Body.String()[:50])
	}
}

func TestIndexMasksAPIKey(t *testing.T) {
	s := testServer(t, &config.Config{APIKey: "supersecretkey123"})
	h := s.Handler()

	rec := get(t, h, "/")
	body := rec.Body.String()
	if strings.Contains(body, "supersecretkey123") {
		t.Error("unauthenticated status page leaks the full API key")
	}
	if !strings.Contains(body, "supe••••") {
		t.Error("masked key not shown on unauthenticated page")
	}
	if strings.Contains(body, "/ui/action") {
		t.Error("action buttons shown without auth")
	}

	rec = get(t, h, "/?apikey=supersecretkey123")
	body = rec.Body.String()
	if !strings.Contains(body, "supersecretkey123") {
		t.Error("authenticated page should show the full key")
	}
	if !strings.Contains(body, "/ui/action") {
		t.Error("authenticated page missing action buttons")
	}
}

func TestSabPauseResume(t *testing.T) {
	s := testServer(t, &config.Config{APIKey: "k"})
	h := s.Handler()

	get(t, h, "/sabnzbd/api?mode=pause&apikey=k")
	if !s.mgr.Paused() {
		t.Fatal("mode=pause did not pause the queue")
	}
	rec := get(t, h, "/sabnzbd/api?mode=queue&apikey=k")
	var q struct {
		Queue struct {
			Paused bool   `json:"paused"`
			Status string `json:"status"`
		} `json:"queue"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}
	if !q.Queue.Paused || q.Queue.Status != "Paused" {
		t.Errorf("queue reports paused=%v status=%q, want true/Paused", q.Queue.Paused, q.Queue.Status)
	}
	get(t, h, "/sabnzbd/api?mode=resume&apikey=k")
	if s.mgr.Paused() {
		t.Fatal("mode=resume did not resume the queue")
	}
}

func TestSabRetryUnknownJob(t *testing.T) {
	s := testServer(t, &config.Config{APIKey: "k"})
	rec := get(t, s.Handler(), "/sabnzbd/api?mode=retry&value=SABnzbd_nzo_missing&apikey=k")
	var resp struct {
		Status bool `json:"status"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Status {
		t.Error("retry of unknown job reported status true")
	}
}

func TestSabQueueRealDiskspace(t *testing.T) {
	s := testServer(t, &config.Config{APIKey: "k"})
	rec := get(t, s.Handler(), "/sabnzbd/api?mode=queue&apikey=k")
	var q struct {
		Queue struct {
			DiskspaceTotal string `json:"diskspacetotal1"`
		} `json:"queue"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &q); err != nil {
		t.Fatal(err)
	}
	// The old fake value was exactly "1000.0"; a real statfs answer won't be.
	if q.Queue.DiskspaceTotal == "1000.0" || q.Queue.DiskspaceTotal == "0.00" {
		t.Errorf("diskspacetotal1 = %q — looks fake, want real filesystem size", q.Queue.DiskspaceTotal)
	}
}

func TestUIAction(t *testing.T) {
	s := testServer(t, &config.Config{APIKey: "k"})
	h := s.Handler()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/ui/action", strings.NewReader("op=pause&apikey=k"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Errorf("pause action: status %d, want 303", rec.Code)
	}
	if !s.mgr.Paused() {
		t.Error("UI pause action did not pause the queue")
	}

	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/ui/action", strings.NewReader("op=resume"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("keyless action: status %d, want 401", rec.Code)
	}
	if !s.mgr.Paused() {
		t.Error("unauthorized action still executed")
	}
}

func TestTVItems(t *testing.T) {
	files := []film2.FileEntry{
		{Name: "Show.S01E01.1080p.x265.mkv", URL: "https://cdn.x/s1/Show.S01E01.1080p.x265.mkv", Season: 1, Episode: 1},
		{Name: "Show.S01E02.1080p.x265.mkv", URL: "https://cdn.x/s1/Show.S01E02.1080p.x265.mkv", Season: 1, Episode: 2},
		// bare no-quality variant of E01 — must be dropped since a 1080p exists
		{Name: "Show.S01E01.mkv", URL: "https://cdn.x/s1/Show.S01E01.mkv", Season: 1, Episode: 1},
		// other season — must be dropped when wantSeason=1
		{Name: "Show.S02E01.720p.mkv", URL: "https://cdn.x/s2/Show.S02E01.720p.mkv", Season: 2, Episode: 1},
		// dubbed variant for the NoDubbed test
		{Name: "Show.S01E01.1080p.Dubbed.mkv", URL: "https://cdn.x/s1/Show.S01E01.1080p.Dubbed.mkv", Season: 1, Episode: 1},
	}
	hit := film2.SearchHit{IMDB: "tt1", Title: "Show", IsTV: true, URL: "https://site/show", Year: 2020}

	s := testServer(t, &config.Config{})
	items := s.tvItems(hit, files, 1, 0, "key", "http://pub")
	var packs, s2, unknownQ, dubbed int
	for _, it := range items {
		if it.Season == 2 {
			s2++
		}
		if strings.Contains(it.Title, "S01.") {
			packs++
		}
		if !strings.Contains(it.Title, "p.") { // no 1080p/720p token
			unknownQ++
		}
		if strings.Contains(it.Title, "DUBBED") {
			dubbed++
		}
	}
	if s2 != 0 {
		t.Errorf("wantSeason=1 leaked %d season-2 item(s)", s2)
	}
	if unknownQ != 0 {
		t.Errorf("%d bare no-quality item(s) emitted despite a real-quality sibling", unknownQ)
	}
	if packs != 1 {
		t.Errorf("season packs = %d, want 1", packs)
	}
	if dubbed == 0 {
		t.Error("dubbed variant missing without NoDubbed")
	}

	// pinned episode: no packs, only E02
	items = s.tvItems(hit, files, 1, 2, "key", "http://pub")
	for _, it := range items {
		if it.Episode != 2 {
			t.Errorf("wantEp=2 returned item %q (ep %d)", it.Title, it.Episode)
		}
	}
	if len(items) == 0 {
		t.Error("wantEp=2 returned no items")
	}

	// NoDubbed drops the dubbed variant
	s.cfg.NoDubbed = true
	items = s.tvItems(hit, files, 1, 0, "key", "http://pub")
	for _, it := range items {
		if strings.Contains(it.Title, "DUBBED") {
			t.Errorf("NoDubbed leaked %q", it.Title)
		}
	}
}
