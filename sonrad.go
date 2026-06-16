// sonrad — Sonarr/Radarr bridge for film2mz.top
//
// Acts as BOTH a Newznab/Torznab indexer and a SABnzbd-compatible download
// client. Single static Go file (stdlib only).
//
//   go run sonrad.go -addr :8910 -download-dir /downloads -api-key MYKEY
//
// Sonarr / Radarr config:
//   Indexer  → Newznab
//     URL:     http://HOST:8910
//     API key: MYKEY
//   Download client → SABnzbd
//     Host:     HOST
//     Port:     8910
//     URL base: /sabnzbd
//     API key:  MYKEY
//     Category: movies (Radarr) / tv (Sonarr)
//
// Search flow:
//   Sonarr → /api?t=tvsearch&imdbid=tt..&season=1&ep=2  → RSS with results
//   Sonarr → /getnzb?token=…                            → fake-NZB carrying token
//   Sonarr → /sabnzbd/api?mode=addfile                  → enqueues job
//   Worker → fetches files into <download-dir>/<cat>/<name>/
//   Sonarr polls /sabnzbd/api?mode=history → sees Completed → imports.

package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var version = "dev" // overridden at build time via -ldflags "-X main.version=..."

// ------------------------------------------------------------------
// Flags
// ------------------------------------------------------------------

var (
	flagAddr            = flag.String("addr", ":8910", "HTTP listen address")
	flagDownloadDir     = flag.String("download-dir", "./downloads", "directory finished files end up in")
	flagAPIKey          = flag.String("api-key", "", "API key Sonarr/Radarr must present (auto-generated if empty)")
	flagMaxConc         = flag.Int("max-concurrent", 3, "max concurrent file downloads")
	flagRateLimit       = flag.Int64("rate-limit", 0, "aggregate bytes/sec cap (0 = unlimited)")
	flagUA              = flag.String("user-agent", "Mozilla/5.0 (X11; Linux x86_64) sonrad/"+version, "HTTP User-Agent for upstream")
	flagCookies         = flag.String("cookies", "", "raw Cookie header for upstream requests")
	flagBase            = flag.String("base-url", "https://www.film2mz.top", "main site base URL (env: SONRAD_BASE_URL)")
	flagCacheTTL        = flag.Duration("cache-ttl", 10*time.Minute, "indexer scrape cache TTL")
	flagPubHost         = flag.String("public-host", "", "host[:port] used in indexer callback links (default: from request Host header)")
	flagDebug           = flag.Bool("debug", false, "verbose logging")
	flagTestIMDB        = flag.String("test", "", "search this title on the site, print results, exit")
	flagStateFile       = flag.String("state-file", "", "path to JSON state file for queue/history persistence (default: <download-dir>/sonrad.state.json)")
	flagInsecure        = flag.Bool("insecure-skip-verify", false, "skip TLS verification on upstream requests (for mirrors with bad certs)")
	flagShutdownTimeout = flag.Duration("shutdown-timeout", 30*time.Second, "how long to wait for in-flight requests during shutdown")
	flagRetries         = flag.Int("download-retries", 3, "attempts per file before marking it failed (1 = no retry)")
	flagSearchConc      = flag.Int("search-concurrency", 4, "parallel upstream fetches per indexer search")
	flagNoDubbed        = flag.Bool("no-dubbed", false, "exclude Dubbed audio variants from indexer results")
)

var (
	httpClient *http.Client
	mgr        *Manager
	scrapeC    = &cache{m: map[string]cacheEntry{}}
)

// ------------------------------------------------------------------
// Job / Manager
// ------------------------------------------------------------------

type Job struct {
	mu sync.Mutex

	ID          string
	Name        string
	Category    string
	Status      string
	Bytes       int64
	BytesDone   int64
	Added       time.Time
	Completed   time.Time
	StoragePath string
	FailMessage string
	Files       []*JobFile

	// transient (not persisted — lowercase keeps json.Marshal away from them)
	speedBPS        float64
	lastSampleAt    time.Time
	lastSampleBytes int64
}

// recordProgress is called whenever `n` more bytes have been pulled for `f`.
// Updates the job and file counters and refreshes an EWMA byte-rate that
// drives the SAB queue's speed / timeleft fields.
func (j *Job) recordProgress(f *JobFile, n int64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if f != nil {
		f.BytesDone += n
	}
	j.BytesDone += n

	now := time.Now()
	if j.lastSampleAt.IsZero() {
		j.lastSampleAt = now
		j.lastSampleBytes = j.BytesDone
		return
	}
	elapsed := now.Sub(j.lastSampleAt).Seconds()
	if elapsed < 0.5 {
		return
	}
	instant := float64(j.BytesDone-j.lastSampleBytes) / elapsed
	const alpha = 0.3
	j.speedBPS = alpha*instant + (1-alpha)*j.speedBPS
	j.lastSampleAt = now
	j.lastSampleBytes = j.BytesDone
}

type JobFile struct {
	URL       string
	Filename  string
	Bytes     int64
	BytesDone int64
	Status    string // pending|downloading|done|failed
	Error     string
}

type Manager struct {
	mu        sync.RWMutex
	queue     []*Job
	history   []*Job
	sem       chan struct{}
	rateLimit int64
	ctx       context.Context

	stateFile string
	dirty     int32 // accessed via sync/atomic via a small helper; using int32 not atomic.Bool to avoid bumping go-version requirements
	dirtyMu   sync.Mutex
	wg        sync.WaitGroup // counts in-flight runJob goroutines
}

func NewManager(ctx context.Context, maxConc int, rateLimit int64, stateFile string) *Manager {
	if maxConc < 1 {
		maxConc = 1
	}
	return &Manager{
		sem:       make(chan struct{}, maxConc),
		rateLimit: rateLimit,
		ctx:       ctx,
		stateFile: stateFile,
	}
}

func (m *Manager) markDirty() {
	m.dirtyMu.Lock()
	m.dirty = 1
	m.dirtyMu.Unlock()
}

func (m *Manager) Add(j *Job) {
	m.mu.Lock()
	m.queue = append(m.queue, j)
	m.mu.Unlock()
	m.markDirty()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runJob(j)
	}()
}

func (m *Manager) Delete(id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, j := range m.queue {
		if j.ID == id {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			m.markDirty()
			return true
		}
	}
	for i, j := range m.history {
		if j.ID == id {
			m.history = append(m.history[:i], m.history[i+1:]...)
			m.markDirty()
			return true
		}
	}
	return false
}

func (m *Manager) Snapshot() (queue, history []*Job) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	queue = append([]*Job(nil), m.queue...)
	history = append([]*Job(nil), m.history...)
	return
}

func (m *Manager) finalize(j *Job, ok bool, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, q := range m.queue {
		if q.ID == j.ID {
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			break
		}
	}
	j.mu.Lock()
	j.Completed = time.Now()
	if ok {
		j.Status = "Completed"
	} else {
		j.Status = "Failed"
		j.FailMessage = errMsg
	}
	j.mu.Unlock()
	m.history = append([]*Job{j}, m.history...)
	if len(m.history) > 500 {
		m.history = m.history[:500]
	}
	m.markDirty()
}

// savedState is the on-disk JSON layout. We deliberately keep it tiny and
// flat — humans should be able to read and edit it.
type savedState struct {
	Queue   []*Job `json:"queue"`
	History []*Job `json:"history"`
}

func (m *Manager) saveStateNow() {
	if m.stateFile == "" {
		return
	}
	q, h := m.Snapshot()
	data, err := json.MarshalIndent(savedState{Queue: q, History: h}, "", "  ")
	if err != nil {
		log.Printf("state save: marshal: %v", err)
		return
	}
	tmp := m.stateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		log.Printf("state save: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, m.stateFile); err != nil {
		log.Printf("state save: rename: %v", err)
	}
}

func (m *Manager) saveLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.saveStateNow()
			return
		case <-t.C:
			m.dirtyMu.Lock()
			if m.dirty == 0 {
				m.dirtyMu.Unlock()
				continue
			}
			m.dirty = 0
			m.dirtyMu.Unlock()
			m.saveStateNow()
		}
	}
}

// loadState reads any persisted state, restores history, and re-queues any
// jobs that were in flight (resume picks up via HTTP Range on next attempt).
func (m *Manager) loadState() {
	if m.stateFile == "" {
		return
	}
	data, err := os.ReadFile(m.stateFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("state load: %v", err)
		}
		return
	}
	var st savedState
	if err := json.Unmarshal(data, &st); err != nil {
		log.Printf("state load: corrupt %s: %v (ignoring)", m.stateFile, err)
		return
	}
	m.mu.Lock()
	m.history = append(m.history, st.History...)
	if len(m.history) > 500 {
		m.history = m.history[:500]
	}
	m.mu.Unlock()
	for _, j := range st.Queue {
		// reset transient fields — speed restarts from 0, statuses re-driven by worker
		j.speedBPS = 0
		j.lastSampleAt = time.Time{}
		j.lastSampleBytes = 0
		j.Status = "Queued"
		for _, f := range j.Files {
			if f.Status == "downloading" {
				f.Status = "pending"
			}
		}
		m.Add(j)
	}
	log.Printf("state load: %d queued (resuming), %d history", len(st.Queue), len(st.History))
}

func (m *Manager) runJob(j *Job) {
	j.mu.Lock()
	j.Status = "Downloading"
	storage := j.StoragePath
	files := append([]*JobFile(nil), j.Files...)
	j.mu.Unlock()

	if err := os.MkdirAll(storage, 0o755); err != nil {
		m.finalize(j, false, "mkdir: "+err.Error())
		return
	}

	failedAny := false
	failMsg := ""
	for _, f := range files {
		select {
		case <-m.ctx.Done():
			m.finalize(j, false, "shutting down")
			return
		case m.sem <- struct{}{}:
		}
		f.Status = "downloading"
		dest := filepath.Join(storage, sanitizeFilename(f.Filename))
		err := downloadFileWithRetry(m.ctx, f.URL, dest, m.rateLimit, *flagRetries, func(n int64) {
			j.recordProgress(f, n)
		})
		<-m.sem
		if err != nil {
			f.Status = "failed"
			f.Error = err.Error()
			failedAny = true
			failMsg = err.Error()
			log.Printf("job %s: file %q failed: %v", j.ID, f.Filename, err)
			continue
		}
		f.Status = "done"
		// Reconcile size if upstream didn't advertise it
		if info, e := os.Stat(dest); e == nil {
			j.mu.Lock()
			diff := info.Size() - f.Bytes
			f.Bytes = info.Size()
			if f.BytesDone != info.Size() {
				j.BytesDone += info.Size() - f.BytesDone
				f.BytesDone = info.Size()
			}
			j.Bytes += diff
			j.mu.Unlock()
		}
	}
	m.finalize(j, !failedAny, failMsg)
}

// ------------------------------------------------------------------
// Scrape cache (tiny TTL map)
// ------------------------------------------------------------------

type cacheEntry struct {
	val any
	exp time.Time
}

type cache struct {
	mu sync.Mutex
	m  map[string]cacheEntry
}

func (c *cache) Get(k string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.m[k]
	if !ok || time.Now().After(e.exp) {
		return nil, false
	}
	return e.val, true
}

func (c *cache) Set(k string, v any, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[k] = cacheEntry{v, time.Now().Add(ttl)}
}

// ------------------------------------------------------------------
// Scraper — pulls from film2mz.top
// ------------------------------------------------------------------

// Directory carries the release metadata parsed out of a download filename
// (quality/codec/audio/source/season). film2mz serves flat per-file links
// rather than browsable directories, so this is no longer a real directory —
// it's kept as the unit the indexer formatters/category logic operate on.
type Directory struct {
	Quality string // 480p / 720p / 1080p / 2160p
	Codec   string // x264 / x265
	Audio   string // SoftSub / Dubbed
	Source  string // Web-DL / BluRay
	Season  int    // 0 for movie
}

type FileEntry struct {
	Name    string
	URL     string
	Size    int64
	Season  int
	Episode int
}

var (
	reTitle = regexp.MustCompile(`(?is)<title>(.*?)</title>`)
	// Absolute CDN download links on a post/series page. The play/online and
	// player-launch anchors don't point at a media file, so this skips them.
	reMediaURL = regexp.MustCompile(`(?i)href="(https?://[^"]+?\.(?:mkv|mp4|avi|m4v|mov|ts|wmv))"`)
	reSE       = regexp.MustCompile(`(?i)S(\d{1,2})E(\d{1,4})`)
	reIMDB     = regexp.MustCompile(`tt\d{6,9}`)
	// Strip Sonarr/Radarr release-style noise from a free-text query before
	// shipping it to film2mz's search. Order matters: episode tokens first,
	// then years.
	reQueryEpisode = regexp.MustCompile(`(?i)\bs\d{1,2}(?:e\d{1,4})?\b`)
	reQuerySeason  = regexp.MustCompile(`(?i)\bseason\s*\d{1,2}\b`)
	reQueryYear    = regexp.MustCompile(`\b(19|20)\d{2}\b`)
	reMultiSpace   = regexp.MustCompile(`\s+`)
)

func httpGetBytes(rawurl string) ([]byte, error) {
	req, err := http.NewRequest("GET", rawurl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", *flagUA)
	if *flagCookies != "" {
		req.Header.Set("Cookie", *flagCookies)
	}
	req.Header.Set("Accept", "*/*")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: %s", rawurl, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
}

// httpPostForm posts a urlencoded form, mirroring the headers film2mz's
// /quick-search endpoint expects (X-Requested-With, Origin, Referer).
func httpPostForm(rawurl string, form url.Values) ([]byte, error) {
	req, err := http.NewRequest("POST", rawurl, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", *flagUA)
	if *flagCookies != "" {
		req.Header.Set("Cookie", *flagCookies)
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	base := strings.TrimRight(*flagBase, "/")
	req.Header.Set("Origin", base)
	req.Header.Set("Referer", base+"/")
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("POST %s: %s", rawurl, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 32*1024*1024))
}

// scrapePageFiles fetches a film2mz post/series page and returns every direct
// download link on it. Quality/codec/audio/season are parsed per-file from the
// filename; film2mz doesn't expose per-file sizes so Size stays 0 and callers
// fall back to defaultSize().
func scrapePageFiles(pageURL string) ([]FileEntry, error) {
	key := "page:" + pageURL
	if v, ok := scrapeC.Get(key); ok {
		return v.([]FileEntry), nil
	}
	body, err := httpGetBytes(pageURL)
	if err != nil {
		return nil, err
	}
	s := string(body)
	var files []FileEntry
	seen := map[string]bool{}
	for _, m := range reMediaURL.FindAllStringSubmatch(s, -1) {
		u := htmlUnescape(m[1])
		if seen[u] {
			continue
		}
		seen[u] = true
		files = append(files, fileEntryFromURL(u))
	}
	if len(files) > 0 { // never cache a no-result scrape — likely transient
		scrapeC.Set(key, files, *flagCacheTTL)
	}
	return files, nil
}

func fileEntryFromURL(u string) FileEntry {
	f := FileEntry{Name: fileNameFromURL(u), URL: u}
	if m := reSE.FindStringSubmatch(f.Name); len(m) >= 3 {
		f.Season, _ = strconv.Atoi(m[1])
		f.Episode, _ = strconv.Atoi(m[2])
	}
	return f
}

// SearchHit is one result from film2mz's /quick-search endpoint.
type SearchHit struct {
	IMDB  string
	Title string
	IsTV  bool
	URL   string // absolute page URL
	Year  int
}

// film2Result is the subset of /quick-search's JSON we consume. The response is
// a flat array; each element also carries a "_formatted" object with <em>-tagged
// titles which we ignore in favour of the clean top-level fields.
type film2Result struct {
	IMDB  string  `json:"imdb_id"`
	Title string  `json:"title"`
	Type  string  `json:"type"` // "series" = TV, "post" = movie
	URL   string  `json:"url"`
	Year  flexInt `json:"year"`
}

// flexInt decodes a value that may arrive as a JSON number or a (possibly
// empty) JSON string — film2mz sends "year" both ways. Unparseable values
// decode to 0 rather than failing the whole response.
type flexInt int

func (n *flexInt) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*n = 0
		return nil
	}
	v, _ := strconv.Atoi(s)
	*n = flexInt(v)
	return nil
}

// searchFilm2 queries film2mz's free-text search and returns one hit per result.
func searchFilm2(q string) ([]SearchHit, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, nil
	}
	key := "search:" + q
	if v, ok := scrapeC.Get(key); ok {
		return v.([]SearchHit), nil
	}
	form := url.Values{}
	form.Set("q", q)
	form.Set("sort", "modified_at:desc")
	body, err := httpPostForm(strings.TrimRight(*flagBase, "/")+"/quick-search", form)
	if err != nil {
		return nil, err
	}
	var raw []film2Result
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("decode search: %w", err)
	}
	var hits []SearchHit
	seen := map[string]bool{}
	for _, r := range raw {
		if r.URL == "" || r.IMDB == "" || seen[r.URL] {
			continue
		}
		seen[r.URL] = true
		hits = append(hits, SearchHit{
			IMDB:  r.IMDB,
			Title: htmlUnescape(strings.TrimSpace(r.Title)),
			IsTV:  strings.EqualFold(r.Type, "series"),
			URL:   absURL(r.URL),
			Year:  int(r.Year),
		})
	}
	if len(hits) > 0 {
		scrapeC.Set(key, hits, *flagCacheTTL)
	}
	return hits, nil
}

func absURL(u string) string {
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return u
	}
	return strings.TrimRight(*flagBase, "/") + "/" + strings.TrimLeft(u, "/")
}

// cleanQuery strips Sonarr/Radarr release-style noise so film2mz's
// natural-language search can match. e.g.
//
//	"Alice.in.Borderland.S01E05" → "Alice in Borderland"
//	"The.Matrix.1999"            → "The Matrix"
func cleanQuery(q string) string {
	q = strings.ReplaceAll(q, ".", " ")
	q = strings.ReplaceAll(q, "_", " ")
	q = reQueryEpisode.ReplaceAllString(q, "")
	q = reQuerySeason.ReplaceAllString(q, "")
	q = reQueryYear.ReplaceAllString(q, "")
	q = reMultiSpace.ReplaceAllString(q, " ")
	return strings.TrimSpace(q)
}

// parseRelease extracts quality/codec/audio/source (+ season for episodes)
// from a release filename, e.g.
//
//	"V.for.Vendetta.2005.1080p.BluRay.x265.Farsi.Sub.mkv"
//	"Alice.in.Borderland.S02E01.1080p.WEB-DL.Farsi.Dubbed.mkv"
func parseRelease(name string) Directory {
	var d Directory
	if m := reSE.FindStringSubmatch(name); len(m) >= 2 {
		d.Season, _ = strconv.Atoi(m[1])
	}
	low := strings.ToLower(name)
	switch {
	case strings.Contains(name, "2160p"), strings.Contains(low, "4k"):
		d.Quality = "2160p"
	case strings.Contains(name, "1080p"):
		d.Quality = "1080p"
	case strings.Contains(name, "720p"):
		d.Quality = "720p"
	case strings.Contains(name, "480p"):
		d.Quality = "480p"
	}
	if strings.Contains(name, "x265") || strings.Contains(low, "hevc") || strings.Contains(low, "10bit") {
		d.Codec = "x265"
	} else {
		d.Codec = "x264"
	}
	switch {
	case strings.Contains(low, "bluray"):
		d.Source = "BluRay"
	case strings.Contains(low, "webrip"):
		d.Source = "WEBRip"
	default:
		d.Source = "Web-DL"
	}
	if strings.Contains(low, "dubbed") {
		d.Audio = "Dubbed"
	} else {
		d.Audio = "SoftSub"
	}
	return d
}

// fileNameFromURL returns the decoded basename of a download URL, dropping any
// query string or fragment.
func fileNameFromURL(u string) string {
	name := u
	if i := strings.IndexAny(name, "?#"); i >= 0 {
		name = name[:i]
	}
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if dec, err := url.PathUnescape(name); err == nil && dec != "" {
		name = dec
	}
	return name
}

func htmlUnescape(s string) string {
	r := strings.NewReplacer(
		"&amp;", "&",
		"&lt;", "<",
		"&gt;", ">",
		"&quot;", "\"",
		"&#39;", "'",
		"&apos;", "'",
	)
	return r.Replace(s)
}

// ------------------------------------------------------------------
// Token (carried through the indexer→download-client round trip)
// ------------------------------------------------------------------

type Token struct {
	Title    string   `json:"t"`
	Category string   `json:"c"`
	URLs     []string `json:"u"` // absolute CDN file URL(s) to download
}

func encodeToken(t Token) string {
	b, _ := json.Marshal(t)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeToken(s string) (Token, error) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return Token{}, err
	}
	var t Token
	err = json.Unmarshal(b, &t)
	return t, err
}

// ------------------------------------------------------------------
// Auth + URL helpers
// ------------------------------------------------------------------

func authOK(r *http.Request) bool {
	if *flagAPIKey == "" {
		return true
	}
	k := r.URL.Query().Get("apikey")
	if k == "" {
		k = r.URL.Query().Get("api_key")
	}
	if k == "" {
		k = r.Header.Get("X-Api-Key")
	}
	if k == "" {
		// Some SABnzbd clients send the key in the (multipart) form body rather
		// than the URL, notably on addfile POSTs. Parse as a fallback; harmless
		// for query-only GET requests.
		k = r.FormValue("apikey")
		if k == "" {
			k = r.FormValue("api_key")
		}
	}
	return k == *flagAPIKey
}

func publicBase(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if *flagPubHost != "" {
		return scheme + "://" + *flagPubHost
	}
	return scheme + "://" + r.Host
}

// ------------------------------------------------------------------
// Newznab indexer
// ------------------------------------------------------------------

func handleAPI(w http.ResponseWriter, r *http.Request) {
	if !authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	t := r.URL.Query().Get("t")
	switch t {
	case "", "caps":
		respondXML(w, capsXML())
	case "search", "movie", "tvsearch":
		handleSearch(w, r, t)
	default:
		respondXML(w, capsXML())
	}
}

func capsXML() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="` + version + `" title="sonrad" strapline="film2mz.top bridge" email="" url="" image=""/>
  <limits max="100" default="100"/>
  <retention days="9999"/>
  <registration available="no" open="no"/>
  <searching>
    <search available="yes" supportedParams="q"/>
    <tv-search available="yes" supportedParams="q,season,ep"/>
    <movie-search available="yes" supportedParams="q"/>
    <audio-search available="no"/>
    <book-search available="no"/>
  </searching>
  <categories>
    <category id="2000" name="Movies">
      <subcat id="2030" name="SD"/>
      <subcat id="2040" name="HD"/>
      <subcat id="2050" name="UHD"/>
      <subcat id="2060" name="3D"/>
    </category>
    <category id="5000" name="TV">
      <subcat id="5030" name="SD"/>
      <subcat id="5040" name="HD"/>
      <subcat id="5050" name="UHD"/>
      <subcat id="5070" name="Anime"/>
    </category>
  </categories>
</caps>`
}

type indexerItem struct {
	Title    string
	GUID     string
	Link     string
	PubDate  time.Time
	Size     int64
	Category int
	IMDB     string
	Season   int
	Episode  int
}

// maxTitleSearchCandidates caps how many result pages we scrape per free-text
// search. Each candidate triggers one page fetch, so this protects film2mz from
// a thundering herd when a query is ambiguous.
const maxTitleSearchCandidates = 5

func handleSearch(w http.ResponseWriter, r *http.Request, mode string) {
	q := r.URL.Query()
	apikey := q.Get("apikey")
	if apikey == "" {
		apikey = r.Header.Get("X-Api-Key")
	}
	pub := publicBase(r)
	wantSeason, _ := strconv.Atoi(q.Get("season"))
	wantEp, _ := strconv.Atoi(q.Get("ep"))

	// film2mz has no imdb→page endpoint, so we resolve candidate pages via its
	// free-text search and (when supplied) keep only those whose imdb matches.
	imdb := q.Get("imdbid")
	if imdb == "" {
		if m := reIMDB.FindString(q.Get("q")); m != "" {
			imdb = m
		}
	}
	if imdb != "" && !strings.HasPrefix(imdb, "tt") {
		imdb = "tt" + imdb
	}

	qstr := strings.TrimSpace(q.Get("q"))
	log.Printf("search: t=%s q=%q season=%q ep=%q imdb=%q", mode, qstr, q.Get("season"), q.Get("ep"), imdb)
	var hits []SearchHit
	if qstr != "" {
		var err error
		hits, err = searchFilm2(cleanQuery(qstr))
		if err != nil {
			log.Printf("search %q: %v", qstr, err)
		}
	}

	var candidates []SearchHit
	for _, h := range hits {
		switch mode {
		case "movie":
			if h.IsTV {
				continue
			}
		case "tvsearch":
			if !h.IsTV {
				continue
			}
		}
		if imdb != "" && !strings.EqualFold(h.IMDB, imdb) {
			continue
		}
		candidates = append(candidates, h)
		if len(candidates) >= maxTitleSearchCandidates {
			break
		}
	}

	if len(candidates) == 0 {
		// Sonarr/Radarr's indexer Test sends an empty query; an empty feed trips
		// the "no results in configured categories" warning that blocks Save in
		// some versions. Emit a single placeholder in the right top-level
		// category so the test sees a result. The title won't match Sonarr's
		// release parser, so RSS sync skips it.
		cat := 5000
		if mode == "movie" {
			cat = 2000
		}
		placeholder := indexerItem{
			Title:    "sonrad bridge ready — searches require a title query",
			GUID:     "sonrad-placeholder",
			Link:     pub + "/getnzb?token=placeholder&apikey=" + url.QueryEscape(apikey),
			PubDate:  time.Unix(0, 0),
			Size:     1,
			Category: cat,
		}
		respondXML(w, renderFeed("sonrad", []indexerItem{placeholder}))
		return
	}

	// Fan out one page scrape per candidate; these are the expensive HTTP calls
	// and are independent of each other.
	type result struct {
		title string
		items []indexerItem
	}
	results := make([]result, len(candidates))
	sem := make(chan struct{}, max(1, *flagSearchConc))
	var wg sync.WaitGroup
	for i, h := range candidates {
		wg.Add(1)
		go func(i int, h SearchHit) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			files, err := scrapePageFiles(h.URL)
			if err != nil {
				if *flagDebug {
					log.Printf("scrape %s: %v", h.URL, err)
				}
				return
			}
			results[i] = result{
				title: h.Title,
				items: emitItemsForHit(h, files, wantSeason, wantEp, apikey, pub),
			}
		}(i, h)
	}
	wg.Wait()

	var items []indexerItem
	feedTitle := "sonrad"
	for _, r := range results {
		if feedTitle == "sonrad" && r.title != "" {
			feedTitle = r.title
		}
		items = append(items, r.items...)
	}
	log.Printf("search: t=%s q=%q → %d candidate(s), %d item(s)", mode, qstr, len(candidates), len(items))
	respondXML(w, renderFeed(feedTitle, items))
}

// emitItemsForHit turns the download links scraped from one film2mz page into
// per-episode (+ season-pack) or per-movie indexer items.
func emitItemsForHit(h SearchHit, files []FileEntry, wantSeason, wantEp int, apikey, pub string) []indexerItem {
	title := h.Title
	imdb := h.IMDB
	var items []indexerItem

	if h.IsTV {
		// Group episodes by (season, quality, codec, audio, source) so we can
		// also offer season packs alongside the per-episode releases.
		type packKey struct {
			season                        int
			quality, codec, audio, source string
		}
		packs := map[packKey][]FileEntry{}
		var order []packKey

		// Pre-pass: note which (season, episode) pairs ship with a real
		// resolution. film2mz sometimes serves a bare ".mkv" with no quality
		// token alongside the 1080p/720p files; emitted as-is those parse as
		// "Unknown" quality and Sonarr ignores them. Drop the bare variant when
		// a proper one exists, keeping it only when it's the sole copy.
		type seKey struct{ season, episode int }
		hasQuality := map[seKey]bool{}
		for _, f := range files {
			if f.Episode == 0 {
				continue
			}
			d := parseRelease(f.Name)
			season := f.Season
			if season == 0 {
				season = d.Season
			}
			if d.Quality != "" {
				hasQuality[seKey{season, f.Episode}] = true
			}
		}

		for _, f := range files {
			if f.Episode == 0 {
				continue
			}
			d := parseRelease(f.Name)
			if f.Season == 0 {
				f.Season = d.Season
			}
			d.Season = f.Season
			if *flagNoDubbed && d.Audio == "Dubbed" {
				continue
			}
			if wantSeason > 0 && f.Season != wantSeason {
				continue
			}
			if d.Quality == "" && hasQuality[seKey{f.Season, f.Episode}] {
				continue
			}
			if wantEp == 0 || f.Episode == wantEp {
				tk := Token{Title: formatTVName(title, h.Year, f, d), Category: "tv", URLs: []string{f.URL}}
				items = append(items, indexerItem{
					Title:    tk.Title,
					GUID:     "sonrad-" + hashStr(f.URL),
					Link:     pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey),
					PubDate:  time.Now().Add(-time.Hour),
					Size:     fileSize(f, d),
					Category: categoryFor("tv", d),
					IMDB:     imdb,
					Season:   f.Season,
					Episode:  f.Episode,
				})
			}
			k := packKey{f.Season, d.Quality, d.Codec, d.Audio, d.Source}
			if _, ok := packs[k]; !ok {
				order = append(order, k)
			}
			packs[k] = append(packs[k], f)
		}

		// Season packs — only meaningful when the query isn't pinned to one ep.
		if wantEp == 0 {
			for _, k := range order {
				grp := packs[k]
				if len(grp) < 2 {
					continue
				}
				d := Directory{Season: k.season, Quality: k.quality, Codec: k.codec, Audio: k.audio, Source: k.source}
				var urls []string
				var packSize int64
				for _, f := range grp {
					urls = append(urls, f.URL)
					packSize += fileSize(f, d)
				}
				tk := Token{Title: formatSeasonPackName(title, h.Year, d), Category: "tv", URLs: urls}
				items = append(items, indexerItem{
					Title:    tk.Title,
					GUID:     "sonrad-" + hashStr(fmt.Sprintf("%s:S%dpack:%s.%s.%s.%s", h.URL, k.season, k.quality, k.codec, k.audio, k.source)),
					Link:     pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey),
					PubDate:  time.Now().Add(-time.Hour),
					Size:     packSize,
					Category: categoryFor("tv", d),
					IMDB:     imdb,
					Season:   k.season,
				})
			}
		}
		return items
	}

	// Movie — one item per download link.
	for _, f := range files {
		d := parseRelease(f.Name)
		if *flagNoDubbed && d.Audio == "Dubbed" {
			continue
		}
		tk := Token{Title: formatMovieName(title, h.Year, d), Category: "movies", URLs: []string{f.URL}}
		items = append(items, indexerItem{
			Title:    tk.Title,
			GUID:     "sonrad-" + hashStr(f.URL),
			Link:     pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey),
			PubDate:  time.Now().Add(-time.Hour),
			Size:     fileSize(f, d),
			Category: categoryFor("movies", d),
			IMDB:     imdb,
		})
	}
	return items
}

func categoryFor(kind string, d Directory) int {
	if kind == "tv" {
		switch d.Quality {
		case "480p":
			return 5030
		case "720p", "1080p":
			return 5040
		case "2160p":
			return 5050
		}
		return 5000
	}
	switch d.Quality {
	case "480p":
		return 2030
	case "720p", "1080p":
		return 2040
	case "2160p":
		return 2050
	}
	return 2000
}

func defaultSize(d Directory, movie bool) int64 {
	var base int64
	switch d.Quality {
	case "480p":
		base = 500 * 1024 * 1024
	case "720p":
		base = 1500 * 1024 * 1024
	case "1080p":
		base = 3000 * 1024 * 1024
	case "2160p":
		base = 8000 * 1024 * 1024
	default:
		base = 1500 * 1024 * 1024
	}
	if d.Codec == "x265" {
		base = base * 6 / 10
	}
	if movie {
		base = base * 5 / 4
	}
	return base
}

func fileSize(f FileEntry, d Directory) int64 {
	if f.Size > 0 {
		return f.Size
	}
	return defaultSize(d, false)
}

func formatMovieName(title string, year int, d Directory) string {
	parts := []string{stripTitle(title)}
	if year > 0 {
		parts = append(parts, strconv.Itoa(year))
	}
	if d.Quality != "" {
		parts = append(parts, d.Quality)
	}
	if d.Source != "" {
		parts = append(parts, d.Source)
	}
	if d.Codec != "" {
		parts = append(parts, d.Codec)
	}
	if d.Audio == "Dubbed" {
		parts = append(parts, "DUBBED")
	}
	parts = append(parts, "FILM2MZ")
	return strings.Join(parts, ".")
}

func formatSeasonPackName(title string, year int, d Directory) string {
	parts := []string{stripTitle(title)}
	if year > 0 {
		parts = append(parts, strconv.Itoa(year))
	}
	parts = append(parts, fmt.Sprintf("S%02d", d.Season))
	if d.Quality != "" {
		parts = append(parts, d.Quality)
	}
	if d.Source != "" {
		parts = append(parts, d.Source)
	}
	if d.Codec != "" {
		parts = append(parts, d.Codec)
	}
	if d.Audio == "Dubbed" {
		parts = append(parts, "DUBBED")
	}
	parts = append(parts, "FILM2MZ")
	return strings.Join(parts, ".")
}

func formatTVName(title string, year int, f FileEntry, d Directory) string {
	parts := []string{stripTitle(title)}
	if year > 0 {
		parts = append(parts, strconv.Itoa(year))
	}
	parts = append(parts, fmt.Sprintf("S%02dE%02d", f.Season, f.Episode))
	if d.Quality != "" {
		parts = append(parts, d.Quality)
	}
	if d.Source != "" {
		parts = append(parts, d.Source)
	}
	if d.Codec != "" {
		parts = append(parts, d.Codec)
	}
	if d.Audio == "Dubbed" {
		parts = append(parts, "DUBBED")
	}
	parts = append(parts, "FILM2MZ")
	return strings.Join(parts, ".")
}

var reTitleChars = regexp.MustCompile(`[^A-Za-z0-9 \-]+`)
var reTitleSpaces = regexp.MustCompile(`\s+`)

func stripTitle(t string) string {
	t = strings.TrimSpace(reTitleChars.ReplaceAllString(t, " "))
	t = reTitleSpaces.ReplaceAllString(t, ".")
	return t
}

// ------------------------------------------------------------------
// RSS feed renderer
// ------------------------------------------------------------------

func renderFeed(title string, items []indexerItem) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/" xmlns:atom="http://www.w3.org/2005/Atom">` + "\n")
	b.WriteString(`<channel>` + "\n")
	b.WriteString(`<title>` + xmlEsc(title) + `</title>` + "\n")
	b.WriteString(`<description>sonrad bridge for film2mz.top</description>` + "\n")
	b.WriteString(`<link>` + xmlEsc(*flagBase) + `</link>` + "\n")
	b.WriteString(`<language>en-US</language>` + "\n")
	b.WriteString(`<newznab:response offset="0" total="` + strconv.Itoa(len(items)) + `"/>` + "\n")
	for _, it := range items {
		b.WriteString(`<item>` + "\n")
		b.WriteString(`  <title>` + xmlEsc(it.Title) + `</title>` + "\n")
		b.WriteString(`  <guid isPermaLink="false">` + xmlEsc(it.GUID) + `</guid>` + "\n")
		b.WriteString(`  <link>` + xmlEsc(it.Link) + `</link>` + "\n")
		b.WriteString(`  <pubDate>` + it.PubDate.Format(time.RFC1123Z) + `</pubDate>` + "\n")
		b.WriteString(`  <category>` + strconv.Itoa(it.Category) + `</category>` + "\n")
		b.WriteString(fmt.Sprintf(`  <enclosure url="%s" length="%d" type="application/x-nzb"/>`+"\n", xmlEsc(it.Link), it.Size))
		b.WriteString(fmt.Sprintf(`  <size>%d</size>`+"\n", it.Size))
		b.WriteString(fmt.Sprintf(`  <newznab:attr name="category" value="%d"/>`+"\n", it.Category))
		b.WriteString(fmt.Sprintf(`  <newznab:attr name="size" value="%d"/>`+"\n", it.Size))
		if it.IMDB != "" {
			id := strings.TrimPrefix(it.IMDB, "tt")
			b.WriteString(`  <newznab:attr name="imdb" value="` + xmlEsc(id) + `"/>` + "\n")
			b.WriteString(`  <newznab:attr name="imdbid" value="` + xmlEsc(id) + `"/>` + "\n")
		}
		if it.Season > 0 {
			b.WriteString(fmt.Sprintf(`  <newznab:attr name="season" value="S%02d"/>`+"\n", it.Season))
		}
		if it.Episode > 0 {
			b.WriteString(fmt.Sprintf(`  <newznab:attr name="episode" value="E%02d"/>`+"\n", it.Episode))
		}
		b.WriteString(`</item>` + "\n")
	}
	b.WriteString(`</channel></rss>`)
	return b.String()
}

func xmlEsc(s string) string {
	var buf bytes.Buffer
	xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

// ------------------------------------------------------------------
// /getnzb — returns a fake-NZB file carrying our token
// ------------------------------------------------------------------

func handleGetNZB(w http.ResponseWriter, r *http.Request) {
	if !authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	tokStr := r.URL.Query().Get("token")
	if tokStr == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}
	tok, err := decodeToken(tokStr)
	if err != nil {
		http.Error(w, "bad token", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-nzb")
	w.Header().Set("Content-Disposition", `attachment; filename="`+sanitizeFilename(tok.Title)+`.nzb"`)
	w.Write([]byte(buildFakeNZB(tok)))
}

func buildFakeNZB(t Token) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">` + "\n")
	b.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	b.WriteString(`  <head>` + "\n")
	b.WriteString(`    <meta type="title">` + xmlEsc(t.Title) + `</meta>` + "\n")
	b.WriteString(`    <meta type="category">` + xmlEsc(t.Category) + `</meta>` + "\n")
	b.WriteString(`    <meta type="sonrad-token">` + xmlEsc(encodeToken(t)) + `</meta>` + "\n")
	b.WriteString(`  </head>` + "\n")
	b.WriteString(`  <file poster="sonrad@local" date="0" subject="` + xmlEsc(t.Title) + `">` + "\n")
	b.WriteString(`    <groups><group>alt.binaries.sonrad</group></groups>` + "\n")
	b.WriteString(`    <segments><segment bytes="1" number="1">sonrad@local</segment></segments>` + "\n")
	b.WriteString(`  </file>` + "\n")
	b.WriteString(`</nzb>` + "\n")
	return b.String()
}

var reNZBToken = regexp.MustCompile(`(?is)<meta\s+type="sonrad-token">([^<]+)</meta>`)

func extractToken(b []byte) (Token, error) {
	if m := reNZBToken.FindSubmatch(b); len(m) > 1 {
		return decodeToken(strings.TrimSpace(string(m[1])))
	}
	if t, err := decodeToken(string(bytes.TrimSpace(b))); err == nil && len(t.URLs) > 0 {
		return t, nil
	}
	return Token{}, errors.New("no sonrad token found")
}

// ------------------------------------------------------------------
// SABnzbd API
// ------------------------------------------------------------------

func handleSABnzbd(w http.ResponseWriter, r *http.Request) {
	if !authOK(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false, "error": "API Key Incorrect"})
		return
	}
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = r.FormValue("mode")
	}
	switch mode {
	case "version":
		writeJSON(w, 200, map[string]any{"version": "4.1.0"})
	case "auth":
		writeJSON(w, 200, map[string]any{"auth": "apikey"})
	case "get_config":
		sabGetConfig(w, r)
	case "set_config", "set_config_default":
		writeJSON(w, 200, map[string]any{"status": true})
	case "fullstatus", "status":
		sabFullStatus(w, r)
	case "queue":
		sabQueue(w, r)
	case "history":
		sabHistory(w, r)
	case "addurl", "addid":
		sabAddURL(w, r)
	case "addfile", "addlocalfile":
		sabAddFile(w, r)
	case "delete":
		sabDelete(w, r)
	case "get_cats":
		writeJSON(w, 200, map[string]any{"categories": []string{"*", "movies", "tv"}})
	case "get_scripts":
		writeJSON(w, 200, map[string]any{"scripts": []string{"None"}})
	case "qstatus":
		sabQStatus(w, r)
	case "warnings":
		writeJSON(w, 200, map[string]any{"warnings": []any{}})
	case "server_stats":
		writeJSON(w, 200, map[string]any{"total": 0, "month": 0, "week": 0, "day": 0})
	case "shutdown", "restart", "pause", "resume":
		writeJSON(w, 200, map[string]any{"status": true})
	default:
		writeJSON(w, 200, map[string]any{"status": true})
	}
}

func sabGetConfig(w http.ResponseWriter, r *http.Request) {
	abs, _ := filepath.Abs(*flagDownloadDir)
	writeJSON(w, 200, map[string]any{
		"config": map[string]any{
			"misc": map[string]any{
				"complete_dir":          abs,
				"download_dir":          abs,
				"complete_dir_writable": true,
				"history_retention":     "",
				"queue_complete":        "",
				"pre_check":             false,
				"enable_meta":           true,
				"sample_match":          false,
			},
			"categories": []map[string]any{
				{"name": "*", "dir": "", "pp": 3, "script": "None", "priority": -100},
				{"name": "movies", "dir": "movies", "pp": 3, "script": "None", "priority": 0},
				{"name": "tv", "dir": "tv", "pp": 3, "script": "None", "priority": 0},
			},
			"sorters":    []any{},
			"servers":    []any{},
			"scheduling": []any{},
		},
	})
}

func sabFullStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{
		"status": map[string]any{
			"paused":          false,
			"pause_int":       "0",
			"diskspace1":      "1000.0",
			"diskspace2":      "1000.0",
			"diskspacetotal1": "1000.0",
			"diskspacetotal2": "1000.0",
			"speedlimit":      "0",
			"speedlimit_abs":  "0",
			"have_warnings":   "0",
			"version":         "4.1.0",
			"uptime":          "0",
		},
	})
}

func sabQueue(w http.ResponseWriter, r *http.Request) {
	queue, _ := mgr.Snapshot()
	slots := make([]map[string]any, 0, len(queue))
	var totalBytes, totalDone int64
	var totalSpeed float64
	for i, j := range queue {
		j.mu.Lock()
		left := j.Bytes - j.BytesDone
		if left < 0 {
			left = 0
		}
		jobLeft := j.Bytes - j.BytesDone
		if jobLeft < 0 {
			jobLeft = 0
		}
		var jobETA string = "0:00:00"
		if j.speedBPS > 0 {
			jobETA = formatHMS(int64(float64(jobLeft) / j.speedBPS))
		}
		slot := map[string]any{
			"status":        j.Status,
			"index":         i,
			"nzo_id":        j.ID,
			"filename":      j.Name,
			"name":          j.Name,
			"cat":           j.Category,
			"mb":            bytesToMB(j.Bytes),
			"mbleft":        bytesToMB(left),
			"mbmissing":     "0.0",
			"size":          bytesString(j.Bytes),
			"sizeleft":      bytesString(left),
			"percentage":    percentage(j.BytesDone, j.Bytes),
			"timeleft":      jobETA,
			"kbpersec":      fmt.Sprintf("%.1f", j.speedBPS/1024),
			"mbpersec":      fmt.Sprintf("%.3f", j.speedBPS/(1024*1024)),
			"priority":      "Normal",
			"script":        "None",
			"labels":        []string{},
			"missing":       0,
			"direct_unpack": "",
			"avg_age":       "0d",
		}
		totalBytes += j.Bytes
		totalDone += j.BytesDone
		totalSpeed += j.speedBPS
		j.mu.Unlock()
		slots = append(slots, slot)
	}
	left := totalBytes - totalDone
	if left < 0 {
		left = 0
	}
	timeLeft := "0:00:00"
	if totalSpeed > 0 {
		timeLeft = formatHMS(int64(float64(left) / totalSpeed))
	}
	writeJSON(w, 200, map[string]any{
		"queue": map[string]any{
			"status":          "Downloading",
			"paused":          false,
			"noofslots":       len(slots),
			"noofslots_total": len(slots),
			"limit":           100,
			"start":           0,
			"mb":              bytesToMB(totalBytes),
			"mbleft":          bytesToMB(left),
			"speed":           fmt.Sprintf("%.1f K", totalSpeed/1024),
			"kbpersec":        fmt.Sprintf("%.1f", totalSpeed/1024),
			"timeleft":        timeLeft,
			"slots":           slots,
			"diskspace1":      "1000.0",
			"diskspace2":      "1000.0",
			"diskspacetotal1": "1000.0",
			"diskspacetotal2": "1000.0",
			"version":         "4.1.0",
		},
	})
}

// formatHMS renders seconds as H:MM:SS for SAB queue/history fields.
func formatHMS(secs int64) string {
	if secs < 0 {
		secs = 0
	}
	if secs > 99*3600 { // SAB caps display at "99:59:59" — bigger values look broken in arr UIs
		secs = 99 * 3600
	}
	h := secs / 3600
	m := (secs / 60) % 60
	s := secs % 60
	return fmt.Sprintf("%d:%02d:%02d", h, m, s)
}

func sabHistory(w http.ResponseWriter, r *http.Request) {
	_, history := mgr.Snapshot()
	slots := make([]map[string]any, 0, len(history))
	for _, j := range history {
		j.mu.Lock()
		slot := map[string]any{
			"nzo_id":        j.ID,
			"name":          j.Name,
			"title":         j.Name,
			"nzb_name":      j.Name + ".nzb",
			"category":      j.Category,
			"status":        j.Status,
			"bytes":         j.Bytes,
			"size":          bytesString(j.Bytes),
			"completed":     j.Completed.Unix(),
			"completeness":  100,
			"fail_message":  j.FailMessage,
			"storage":       j.StoragePath,
			"path":          j.StoragePath,
			"download_time": 0,
			"postproc_time": 0,
			"action_line":   "",
			"pp":            "",
			"script":        "None",
			"report":        "",
			"downloaded":    j.BytesDone,
			"stage_log":     []any{},
		}
		j.mu.Unlock()
		slots = append(slots, slot)
	}
	writeJSON(w, 200, map[string]any{
		"history": map[string]any{
			"noofslots":           len(slots),
			"total_size":          "0",
			"month_size":          "0",
			"week_size":           "0",
			"day_size":            "0",
			"slots":               slots,
			"last_history_update": time.Now().Unix(),
			"version":             "4.1.0",
		},
	})
}

func sabAddURL(w http.ResponseWriter, r *http.Request) {
	rawURL := firstNonEmpty(r.URL.Query().Get("name"), r.FormValue("name"))
	if rawURL == "" {
		writeJSON(w, 200, map[string]any{"status": false, "error": "no url"})
		return
	}
	body, err := httpGetBytes(rawURL)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": err.Error()})
		return
	}
	tok, err := extractToken(body)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": "not a sonrad nzb: " + err.Error()})
		return
	}
	if cat := chooseCat(r); cat != "" {
		tok.Category = cat
	}
	j, err := startJob(tok)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"status": true, "nzo_ids": []string{j.ID}})
}

func sabAddFile(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseMultipartForm(64 * 1024 * 1024)
	var data []byte
	if r.MultipartForm != nil {
		for _, fhs := range r.MultipartForm.File {
			for _, fh := range fhs {
				f, err := fh.Open()
				if err != nil {
					continue
				}
				data, _ = io.ReadAll(f)
				f.Close()
				if len(data) > 0 {
					break
				}
			}
			if len(data) > 0 {
				break
			}
		}
	}
	if len(data) == 0 {
		data, _ = io.ReadAll(r.Body)
	}
	tok, err := extractToken(data)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": "not a sonrad nzb: " + err.Error()})
		return
	}
	if cat := chooseCat(r); cat != "" {
		tok.Category = cat
	}
	j, err := startJob(tok)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"status": true, "nzo_ids": []string{j.ID}})
}

func sabDelete(w http.ResponseWriter, r *http.Request) {
	val := firstNonEmpty(r.URL.Query().Get("value"), r.FormValue("value"))
	for _, id := range strings.Split(val, ",") {
		id = strings.TrimSpace(id)
		if id != "" {
			mgr.Delete(id)
		}
	}
	writeJSON(w, 200, map[string]any{"status": true})
}

func sabQStatus(w http.ResponseWriter, r *http.Request) {
	queue, _ := mgr.Snapshot()
	writeJSON(w, 200, map[string]any{
		"paused":    false,
		"kbpersec":  0.0,
		"mb":        0.0,
		"mbleft":    0.0,
		"noofslots": len(queue),
		"timeleft":  "0:00:00",
	})
}

func chooseCat(r *http.Request) string {
	return firstNonEmpty(r.URL.Query().Get("cat"), r.FormValue("cat"))
}

// ------------------------------------------------------------------
// Job creation
// ------------------------------------------------------------------

func startJob(t Token) (*Job, error) {
	if len(t.URLs) == 0 {
		return nil, errors.New("token has no urls")
	}
	cat := t.Category
	if cat == "" {
		cat = "*"
	}
	sub := ""
	switch cat {
	case "movies":
		sub = "movies"
	case "tv":
		sub = "tv"
	}
	storage := filepath.Join(*flagDownloadDir, sub, sanitizeFilename(t.Title))
	j := &Job{
		ID:          newID(),
		Name:        t.Title,
		Category:    cat,
		Status:      "Queued",
		Added:       time.Now(),
		StoragePath: storage,
	}
	for _, u := range t.URLs {
		j.Files = append(j.Files, &JobFile{
			URL:      u,
			Filename: fileNameFromURL(u),
			Status:   "pending",
		})
	}
	log.Printf("queued %s (%s) → %s [%d file(s)]", j.Name, cat, storage, len(j.Files))
	mgr.Add(j)
	return j, nil
}

// ------------------------------------------------------------------
// File downloader (resume + rate-limit)
// ------------------------------------------------------------------

// permanentDownloadError reports whether a downloadFile error is worth
// retrying. We only treat 4xx (except 408/429) as permanent. Anything else
// — network errors, 5xx, timeouts — gets retried.
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

// downloadFileWithRetry calls downloadFile up to `attempts` times with
// exponential backoff. Resume via HTTP Range means each retry continues from
// the bytes already on disk, so retries are cheap.
func downloadFileWithRetry(ctx context.Context, urlStr, dest string, rateLimit int64, attempts int, onProgress func(int64)) error {
	if attempts < 1 {
		attempts = 1
	}
	backoff := time.Second
	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := downloadFile(ctx, urlStr, dest, rateLimit, onProgress)
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

func downloadFile(ctx context.Context, urlStr, dest string, rateLimit int64, onProgress func(int64)) error {
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
	req.Header.Set("User-Agent", *flagUA)
	if *flagCookies != "" {
		req.Header.Set("Cookie", *flagCookies)
	}
	if startAt > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", startAt))
	}
	// reuse the shared transport (honors -insecure-skip-verify) but bypass the
	// short timeout we set for scrape requests — downloads can take hours.
	client := &http.Client{Transport: httpClient.Transport}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		// presumably already complete
		if onProgress != nil && startAt > 0 {
			onProgress(0)
		}
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("HTTP %s", resp.Status)
	}
	flag := os.O_CREATE | os.O_WRONLY
	if startAt > 0 && resp.StatusCode == http.StatusPartialContent {
		flag |= os.O_APPEND
	} else {
		flag |= os.O_TRUNC
		startAt = 0
	}
	f, err := os.OpenFile(dest, flag, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	var reader io.Reader = resp.Body
	if rateLimit > 0 {
		reader = &throttledReader{r: resp.Body, rate: rateLimit, last: time.Now(), bucket: rateLimit}
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
			break
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
	return nil
}

type throttledReader struct {
	r    io.Reader
	rate int64

	mu     sync.Mutex
	last   time.Time
	bucket int64
}

func (t *throttledReader) Read(p []byte) (int, error) {
	t.mu.Lock()
	now := time.Now()
	elapsed := now.Sub(t.last).Seconds()
	t.last = now
	t.bucket += int64(elapsed * float64(t.rate))
	if t.bucket > t.rate {
		t.bucket = t.rate
	}
	for t.bucket <= 0 {
		t.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
		t.mu.Lock()
		now = time.Now()
		elapsed = now.Sub(t.last).Seconds()
		t.last = now
		t.bucket += int64(elapsed * float64(t.rate))
		if t.bucket > t.rate {
			t.bucket = t.rate
		}
	}
	max := int64(len(p))
	if max > t.bucket {
		max = t.bucket
	}
	t.mu.Unlock()
	n, err := t.r.Read(p[:max])
	t.mu.Lock()
	t.bucket -= int64(n)
	t.mu.Unlock()
	return n, err
}

// ------------------------------------------------------------------
// Misc helpers
// ------------------------------------------------------------------

func bytesToMB(b int64) string { return fmt.Sprintf("%.2f", float64(b)/(1024*1024)) }

func bytesString(b int64) string {
	if b >= 1024*1024*1024 {
		return fmt.Sprintf("%.2f GB", float64(b)/(1024*1024*1024))
	}
	return fmt.Sprintf("%.2f MB", float64(b)/(1024*1024))
}

func percentage(done, total int64) string {
	if total <= 0 {
		return "0"
	}
	return strconv.Itoa(int(done * 100 / total))
}

var reUnsafe = regexp.MustCompile(`[\\/:*?"<>|]+`)

func sanitizeFilename(s string) string {
	s = reUnsafe.ReplaceAllString(s, "_")
	s = strings.TrimSpace(s)
	if s == "" {
		s = "untitled"
	}
	return s
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "SABnzbd_nzo_" + hex.EncodeToString(b)
}

func hashStr(s string) string {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return strconv.FormatUint(h, 16)
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

func respondXML(w http.ResponseWriter, s string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Write([]byte(s))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

// ------------------------------------------------------------------
// Status page (minimal HTML at /)
// ------------------------------------------------------------------

func indexPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	queue, history := mgr.Snapshot()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!doctype html><meta charset=utf-8><title>sonrad</title>
<style>body{font-family:-apple-system,system-ui,sans-serif;margin:24px;color:#222}
table{border-collapse:collapse;margin-bottom:24px;width:100%%}
td,th{border:1px solid #e4e4e4;padding:6px 10px;font-size:13px;vertical-align:top}
th{background:#fafafa;text-align:left}
h1{font-size:20px;margin-bottom:4px}h2{font-size:14px;margin-top:24px}
code{background:#f4f4f4;padding:1px 5px;border-radius:3px;font-size:12px}
small{color:#888}
.ok{color:#16a34a}.fail{color:#dc2626}.run{color:#2563eb}</style>
<h1>sonrad %s</h1>
<p><small>Newznab: <code>%s/api</code> &nbsp;·&nbsp; SABnzbd: <code>%s/sabnzbd</code> &nbsp;·&nbsp; API key: <code>%s</code></small></p>
<h2>Queue (%d)</h2><table><tr><th>Name</th><th>Cat</th><th>Status</th><th>Progress</th></tr>`,
		version, publicBase(r), publicBase(r), *flagAPIKey, len(queue))
	for _, j := range queue {
		j.mu.Lock()
		cls := "run"
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td class="%s">%s</td><td>%s / %s (%s%%)</td></tr>`,
			htmlEscMin(j.Name), j.Category, cls, j.Status,
			bytesString(j.BytesDone), bytesString(j.Bytes), percentage(j.BytesDone, j.Bytes))
		j.mu.Unlock()
	}
	fmt.Fprintf(w, `</table><h2>History (%d)</h2><table><tr><th>Name</th><th>Cat</th><th>Status</th><th>Size</th><th>Completed</th><th>Storage</th></tr>`, len(history))
	for _, j := range history {
		j.mu.Lock()
		cls := "ok"
		if j.Status != "Completed" {
			cls = "fail"
		}
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td class="%s">%s</td><td>%s</td><td>%s</td><td><code>%s</code></td></tr>`,
			htmlEscMin(j.Name), j.Category, cls, j.Status,
			bytesString(j.Bytes), j.Completed.Format("2006-01-02 15:04"), htmlEscMin(j.StoragePath))
		j.mu.Unlock()
	}
	w.Write([]byte(`</table>`))
}

func htmlEscMin(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

// ------------------------------------------------------------------
// Main
// ------------------------------------------------------------------

// handleHealthz is a tiny liveness/readiness probe for Docker HEALTHCHECK,
// kubernetes probes, and uptime monitors.
func handleHealthz(w http.ResponseWriter, r *http.Request) {
	queue, history := mgr.Snapshot()
	writeJSON(w, 200, map[string]any{
		"status":         "ok",
		"version":        version,
		"queue_length":   len(queue),
		"history_length": len(history),
	})
}

// initHTTPClient builds the shared upstream client. Done in main() rather
// than at package init time so the flags are parsed first.
func initHTTPClient() {
	tr := &http.Transport{
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if *flagInsecure {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		log.Printf("WARNING: TLS verification disabled for upstream requests")
	}
	httpClient = &http.Client{Timeout: 90 * time.Second, Transport: tr}
}

func main() {
	flag.Parse()

	initHTTPClient()

	if *flagTestIMDB != "" {
		testScrape(*flagTestIMDB)
		return
	}

	if *flagAPIKey == "" {
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		*flagAPIKey = hex.EncodeToString(b)
		log.Printf("no -api-key supplied; generated ephemeral key: %s", *flagAPIKey)
	}
	if err := os.MkdirAll(*flagDownloadDir, 0o755); err != nil {
		log.Fatalf("download dir: %v", err)
	}
	if abs, err := filepath.Abs(*flagDownloadDir); err == nil {
		*flagDownloadDir = abs
	}
	if *flagStateFile == "" {
		*flagStateFile = filepath.Join(*flagDownloadDir, "sonrad.state.json")
	}

	// Root context cancelled on SIGINT/SIGTERM — workers honor it.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	mgr = NewManager(ctx, *flagMaxConc, *flagRateLimit, *flagStateFile)
	mgr.loadState()
	go mgr.saveLoop(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/api", handleAPI)
	mux.HandleFunc("/api/", handleAPI)
	mux.HandleFunc("/getnzb", handleGetNZB)
	mux.HandleFunc("/sabnzbd/api", handleSABnzbd)
	mux.HandleFunc("/sabnzbd/api/", handleSABnzbd)
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/", indexPage)

	srv := &http.Server{
		Addr:              *flagAddr,
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}

	log.Printf("sonrad %s listening on %s", version, *flagAddr)
	log.Printf("download dir: %s", *flagDownloadDir)
	log.Printf("state file:   %s", *flagStateFile)
	log.Printf("api key:      %s", *flagAPIKey)
	log.Printf("concurrency:  %d files, %d search, rate-limit %d B/s, retries %d",
		*flagMaxConc, *flagSearchConc, *flagRateLimit, *flagRetries)
	log.Printf("Newznab indexer URL: http://<host>%s/api  (apikey: %s)", *flagAddr, *flagAPIKey)
	log.Printf("SABnzbd base URL   : http://<host>%s/sabnzbd  (apikey: %s)", *flagAddr, *flagAPIKey)

	// Run the server in a goroutine so we can react to ctx cancellation.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining (timeout %s)…", *flagShutdownTimeout)
	case err := <-errCh:
		log.Printf("server error: %v", err)
	}

	// 1. stop accepting new HTTP requests, drain in-flight ones
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), *flagShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	// 2. wait for download workers (ctx is already cancelled, they're aborting)
	done := make(chan struct{})
	go func() {
		mgr.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		log.Printf("workers didn't drain in time, forcing exit")
	}

	// 3. final state flush so a restart picks up where we left off
	mgr.saveStateNow()
	log.Printf("bye")
}

// testScrape runs a free-text search against film2mz and, for each hit, scrapes
// its page and prints the download links + parsed release metadata.
func testScrape(query string) {
	hits, err := searchFilm2(cleanQuery(query))
	if err != nil {
		log.Fatalf("search: %v", err)
	}
	fmt.Printf("Query: %q → %d hit(s)\n\n", query, len(hits))
	for _, h := range hits {
		kind := "movie"
		if h.IsTV {
			kind = "tv"
		}
		fmt.Printf("%s [%s] (%s, %d)\n  %s\n", h.Title, kind, h.IMDB, h.Year, h.URL)
		files, err := scrapePageFiles(h.URL)
		if err != nil {
			fmt.Printf("    error: %v\n", err)
			continue
		}
		for _, f := range files {
			d := parseRelease(f.Name)
			fmt.Printf("    - %s (S%02dE%02d, q=%s codec=%s src=%s audio=%s)\n",
				f.Name, f.Season, f.Episode, d.Quality, d.Codec, d.Source, d.Audio)
			fmt.Printf("        %s\n", f.URL)
		}
		fmt.Println()
	}
}
