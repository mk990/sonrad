// sonrad — Sonarr/Radarr bridge for azfilm.theazizi.ir
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
	flagBase            = flag.String("base-url", "https://azfilm.theazizi.ir", "azfilm base URL")
	flagCacheTTL        = flag.Duration("cache-ttl", 10*time.Minute, "indexer scrape cache TTL")
	flagPubHost         = flag.String("public-host", "", "host[:port] used in indexer callback links (default: from request Host header)")
	flagDebug           = flag.Bool("debug", false, "verbose logging")
	flagTestIMDB        = flag.String("test", "", "scrape this IMDB id, print results, exit")
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
// Scraper — pulls from azfilm.theazizi.ir
// ------------------------------------------------------------------

type Directory struct {
	URL     string
	Label   string // e.g. "SoftSub/S01/720p.Web-DL"
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
	reDirLink    = regexp.MustCompile(`(?is)class="[^"]*dl-quality-btn-dir[^"]*"\s+href="([^"]+)"`)
	reTitle      = regexp.MustCompile(`(?is)<title>(.*?)</title>`)
	reFile       = regexp.MustCompile(`(?is)<code><i[^>]*>([^<]+?\.(?:mkv|mp4|avi|m4v|mov|ts|wmv))</i></code>`)
	reFileA      = regexp.MustCompile(`(?is)<a[^>]+href="([^"]+?\.(?:mkv|mp4|avi|m4v|mov|ts|wmv))"`)
	reSizeCell   = regexp.MustCompile(`(?is)class="[^"]*\bs\b[^"]*"[^>]*>\s*<code[^>]*>([^<]+?)</code>`)
	reSizeAny    = regexp.MustCompile(`(?i)^\s*([\d.]+)\s*(B|KB|MB|GB|TB|KiB|MiB|GiB|TiB|K|M|G|T)?\s*$`)
	reSE         = regexp.MustCompile(`(?i)S(\d{1,2})E(\d{1,4})`)
	reSeason     = regexp.MustCompile(`(?i)/S(\d{1,2})/`)
	reIMDB       = regexp.MustCompile(`tt\d{6,9}`)
	reSearchCard = regexp.MustCompile(`(?is)<a\s+class="card"\s+href="movie\.php\?imdb=(tt\d+)"[^>]*>(.*?)</a>`)
	reCardType   = regexp.MustCompile(`(?is)class="ctype\s+(\w+)"`)
	reCardTitle  = regexp.MustCompile(`(?is)<h2\s+class="ctitle">([^<]+)</h2>`)
	// Strip Sonarr/Radarr release-style noise from a free-text query before
	// shipping it to azfilm. Order matters: episode tokens first, then years.
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

func scrapeMoviePage(imdb string) (title string, dirs []Directory, err error) {
	if !strings.HasPrefix(imdb, "tt") {
		imdb = "tt" + imdb
	}
	key := "movie:" + imdb
	if v, ok := scrapeC.Get(key); ok {
		cached := v.(struct {
			T string
			D []Directory
		})
		return cached.T, cached.D, nil
	}
	body, err := httpGetBytes(*flagBase + "/movie.php?imdb=" + url.QueryEscape(imdb))
	if err != nil {
		return "", nil, err
	}
	s := string(body)
	if m := reTitle.FindStringSubmatch(s); len(m) > 1 {
		title = strings.TrimSpace(htmlUnescape(m[1]))
		// strip site/brand suffix: "Title | Site", "Title - Site", "Title — Site", "Title – Site"
		for _, sep := range []string{"|", " — ", " – ", " - "} {
			if i := strings.LastIndex(title, sep); i > 0 && len(title)-i < 48 {
				title = strings.TrimSpace(title[:i])
			}
		}
	}
	if title == "" {
		title = imdb
	}
	seen := map[string]bool{}
	for _, m := range reDirLink.FindAllStringSubmatch(s, -1) {
		u := htmlUnescape(m[1])
		if seen[u] {
			continue
		}
		seen[u] = true
		dirs = append(dirs, parseDirURL(u))
	}
	if len(dirs) > 0 { // never cache a no-result scrape — likely transient
		scrapeC.Set(key, struct {
			T string
			D []Directory
		}{title, dirs}, *flagCacheTTL)
	}
	return title, dirs, nil
}

// SearchHit is one card from azfilm's /index.php?q= search results.
type SearchHit struct {
	IMDB  string
	Title string
	IsTV  bool
}

// searchByTitle queries azfilm's free-text search and returns one hit per card.
func searchByTitle(q string) ([]SearchHit, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, nil
	}
	key := "search:" + q
	if v, ok := scrapeC.Get(key); ok {
		return v.([]SearchHit), nil
	}
	body, err := httpGetBytes(*flagBase + "/index.php?q=" + url.QueryEscape(q))
	if err != nil {
		return nil, err
	}
	s := string(body)
	var hits []SearchHit
	seen := map[string]bool{}
	for _, m := range reSearchCard.FindAllStringSubmatch(s, -1) {
		imdb := m[1]
		if seen[imdb] {
			continue
		}
		seen[imdb] = true
		h := SearchHit{IMDB: imdb}
		block := m[2]
		if mm := reCardTitle.FindStringSubmatch(block); len(mm) > 1 {
			h.Title = htmlUnescape(strings.TrimSpace(mm[1]))
		}
		if mm := reCardType.FindStringSubmatch(block); len(mm) > 1 {
			h.IsTV = strings.EqualFold(mm[1], "tv")
		}
		hits = append(hits, h)
	}
	if len(hits) > 0 {
		scrapeC.Set(key, hits, *flagCacheTTL)
	}
	return hits, nil
}

// cleanQuery strips Sonarr/Radarr release-style noise so azfilm's
// natural-language search can match. e.g.
//   "Alice.in.Borderland.S01E05" → "Alice in Borderland"
//   "The.Matrix.1999"            → "The Matrix"
func cleanQuery(q string) string {
	q = strings.ReplaceAll(q, ".", " ")
	q = strings.ReplaceAll(q, "_", " ")
	q = reQueryEpisode.ReplaceAllString(q, "")
	q = reQuerySeason.ReplaceAllString(q, "")
	q = reQueryYear.ReplaceAllString(q, "")
	q = reMultiSpace.ReplaceAllString(q, " ")
	return strings.TrimSpace(q)
}

func parseDirURL(rawurl string) Directory {
	d := Directory{URL: rawurl}
	label := strings.TrimSuffix(rawurl, "/")
	if i := strings.Index(label, "/tt"); i >= 0 {
		rest := label[i+1:]
		parts := strings.Split(rest, "/")
		if len(parts) > 1 {
			d.Label = strings.Join(parts[1:], "/")
		}
	}
	L := d.Label
	if m := reSeason.FindStringSubmatch("/" + L + "/"); len(m) > 1 {
		n, _ := strconv.Atoi(m[1])
		d.Season = n
	}
	switch {
	case strings.Contains(L, "2160p"), strings.Contains(L, "4K"):
		d.Quality = "2160p"
	case strings.Contains(L, "1080p"):
		d.Quality = "1080p"
	case strings.Contains(L, "720p"):
		d.Quality = "720p"
	case strings.Contains(L, "480p"):
		d.Quality = "480p"
	}
	if strings.Contains(L, "x265") || strings.Contains(strings.ToLower(L), "hevc") {
		d.Codec = "x265"
	} else {
		d.Codec = "x264"
	}
	switch {
	case strings.Contains(strings.ToLower(L), "bluray"):
		d.Source = "BluRay"
	case strings.Contains(strings.ToLower(L), "webrip"):
		d.Source = "WEBRip"
	default:
		d.Source = "Web-DL"
	}
	if strings.Contains(L, "Dubbed") {
		d.Audio = "Dubbed"
	} else {
		d.Audio = "SoftSub"
	}
	return d
}

func scrapeDirectory(dirURL string) ([]FileEntry, error) {
	key := "dir:" + dirURL
	if v, ok := scrapeC.Get(key); ok {
		return v.([]FileEntry), nil
	}
	body, err := httpGetBytes(dirURL)
	if err != nil {
		return nil, err
	}
	s := string(body)
	var files []FileEntry
	seen := map[string]bool{}

	// 1. Anchor tags (preferred — gives us the actual URL)
	for _, m := range reFileA.FindAllStringSubmatch(s, -1) {
		name := htmlUnescape(m[1])
		// strip directory components if any
		if i := strings.LastIndex(name, "/"); i >= 0 {
			name = name[i+1:]
		}
		if seen[name] || name == "" {
			continue
		}
		seen[name] = true
		files = append(files, fileEntryFromName(dirURL, name))
	}
	// 2. <code><i>filename</i></code> fallback
	for _, m := range reFile.FindAllStringSubmatch(s, -1) {
		name := strings.TrimSpace(htmlUnescape(m[1]))
		if seen[name] || name == "" {
			continue
		}
		seen[name] = true
		files = append(files, fileEntryFromName(dirURL, name))
	}

	// Sizes live in cells like <td class="s"><code>1.2 G</code></td> —
	// gather them in document order and pair each filename with the next
	// size cell that appears after it.
	type posVal struct {
		pos int
		val string
	}
	var sizes []posVal
	for _, m := range reSizeCell.FindAllStringSubmatchIndex(s, -1) {
		sizes = append(sizes, posVal{pos: m[0], val: strings.TrimSpace(s[m[2]:m[3]])})
	}
	for i := range files {
		filePos := strings.Index(s, files[i].Name)
		if filePos < 0 {
			continue
		}
		for _, sz := range sizes {
			if sz.pos > filePos {
				files[i].Size = parseSizeFlexible(sz.val)
				break
			}
		}
	}

	if len(files) > 0 {
		scrapeC.Set(key, files, *flagCacheTTL)
	}
	return files, nil
}

func fileEntryFromName(dirURL, name string) FileEntry {
	f := FileEntry{Name: name, URL: joinURL(dirURL, name)}
	if m := reSE.FindStringSubmatch(name); len(m) >= 3 {
		f.Season, _ = strconv.Atoi(m[1])
		f.Episode, _ = strconv.Atoi(m[2])
	}
	return f
}

func joinURL(base, name string) string {
	if !strings.HasSuffix(base, "/") {
		base += "/"
	}
	return base + url.PathEscape(name)
}

// parseSizeFlexible accepts "1.2 GB", "1.2GB", "1.2G", "523823104", "-", "" etc.
// Single-letter suffixes K/M/G/T are treated as KB/MB/GB/TB (binary units).
func parseSizeFlexible(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return 0
	}
	m := reSizeAny.FindStringSubmatch(s)
	if len(m) < 2 {
		return 0
	}
	f, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0
	}
	unit := ""
	if len(m) > 2 {
		unit = strings.ToUpper(m[2])
	}
	switch unit {
	case "", "B":
		return int64(f)
	case "K", "KB", "KIB":
		return int64(f * 1024)
	case "M", "MB", "MIB":
		return int64(f * 1024 * 1024)
	case "G", "GB", "GIB":
		return int64(f * 1024 * 1024 * 1024)
	case "T", "TB", "TIB":
		return int64(f * 1024 * 1024 * 1024 * 1024)
	}
	return 0
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
	DirURL   string   `json:"d"`
	Files    []string `json:"f,omitempty"` // empty = download whole dir
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
  <server version="` + version + `" title="sonrad" strapline="azfilm.theazizi.ir bridge" email="" url="" image=""/>
  <limits max="100" default="100"/>
  <retention days="9999"/>
  <registration available="no" open="no"/>
  <searching>
    <search available="yes" supportedParams="q"/>
    <tv-search available="yes" supportedParams="q,imdbid,season,ep"/>
    <movie-search available="yes" supportedParams="q,imdbid"/>
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

// maxTitleSearchCandidates caps how many movie pages we scrape per free-text
// search. Each candidate triggers one movie-page fetch plus N directory-listing
// fetches, so this protects azfilm from a thundering herd.
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

	// Resolve which IMDB ids to emit results for.
	imdb := q.Get("imdbid")
	if imdb == "" {
		if m := reIMDB.FindString(q.Get("q")); m != "" {
			imdb = m
		}
	}
	if imdb != "" && !strings.HasPrefix(imdb, "tt") {
		imdb = "tt" + imdb
	}

	var candidateIMDBs []string
	if imdb != "" {
		candidateIMDBs = []string{imdb}
	} else if qstr := strings.TrimSpace(q.Get("q")); qstr != "" {
		hits, err := searchByTitle(cleanQuery(qstr))
		if err != nil {
			if *flagDebug {
				log.Printf("title search %q: %v", qstr, err)
			}
		}
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
			candidateIMDBs = append(candidateIMDBs, h.IMDB)
			if len(candidateIMDBs) >= maxTitleSearchCandidates {
				break
			}
		}
	}

	if len(candidateIMDBs) == 0 {
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
			Title:    "sonrad bridge ready — searches require IMDB id",
			GUID:     "sonrad-placeholder",
			Link:     pub + "/getnzb?token=placeholder&apikey=" + url.QueryEscape(apikey),
			PubDate:  time.Unix(0, 0),
			Size:     1,
			Category: cat,
		}
		respondXML(w, renderFeed("sonrad", []indexerItem{placeholder}))
		return
	}

	// Fan out one scrape per candidate IMDB. With up to 5 candidates and ~16
	// directories each that's 80+ HTTPs; serial would take many seconds.
	type result struct {
		title string
		items []indexerItem
	}
	results := make([]result, len(candidateIMDBs))
	sem := make(chan struct{}, max(1, *flagSearchConc))
	var wg sync.WaitGroup
	for i, im := range candidateIMDBs {
		wg.Add(1)
		go func(i int, im string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			title, dirs, err := scrapeMoviePage(im)
			if err != nil {
				if *flagDebug {
					log.Printf("scrape %s: %v", im, err)
				}
				return
			}
			results[i] = result{
				title: title,
				items: emitItemsForMovie(title, im, dirs, mode, wantSeason, wantEp, apikey, pub),
			}
		}(i, im)
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
	respondXML(w, renderFeed(feedTitle, items))
}

// emitItemsForMovie expands the directory list scraped from one IMDB page
// into per-episode (+ season-pack) or per-movie indexer items.
func emitItemsForMovie(title, imdb string, dirs []Directory, mode string, wantSeason, wantEp int, apikey, pub string) []indexerItem {
	siteHasTV := false
	for _, d := range dirs {
		if d.Season > 0 {
			siteHasTV = true
			break
		}
	}

	// Determine which directories survive the mode/season filter so we only
	// pay for listings we'll actually use.
	type relevantDir struct {
		idx int
		dir Directory
	}
	var relevant []relevantDir
	for i, d := range dirs {
		dirIsTV := d.Season > 0
		switch mode {
		case "movie":
			if dirIsTV {
				continue
			}
		case "tvsearch":
			if !dirIsTV {
				continue
			}
		}
		if wantSeason > 0 && d.Season != wantSeason {
			continue
		}
		if *flagNoDubbed && d.Audio == "Dubbed" {
			continue
		}
		relevant = append(relevant, relevantDir{i, d})
	}

	// Fetch all surviving directory listings in parallel — these are the
	// expensive HTTP calls and are independent of each other.
	listings := make([][]FileEntry, len(relevant))
	{
		sem := make(chan struct{}, max(1, *flagSearchConc))
		var wg sync.WaitGroup
		for i, rd := range relevant {
			wg.Add(1)
			go func(i int, d Directory) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				files, err := scrapeDirectory(d.URL)
				if err != nil {
					if *flagDebug {
						log.Printf("scrape dir %s: %v", d.URL, err)
					}
					return
				}
				listings[i] = files
			}(i, rd.dir)
		}
		wg.Wait()
	}

	var items []indexerItem
	for i, rd := range relevant {
		d := rd.dir
		dirIsTV := d.Season > 0
		files := listings[i]
		if files == nil {
			continue
		}

		if dirIsTV {
			// per-episode results
			for _, f := range files {
				if f.Episode == 0 {
					continue
				}
				if wantEp > 0 && f.Episode != wantEp {
					continue
				}
				if f.Season == 0 {
					f.Season = d.Season
				}
				tk := Token{
					Title:    formatTVName(title, f, d),
					Category: "tv",
					DirURL:   d.URL,
					Files:    []string{f.Name},
				}
				items = append(items, indexerItem{
					Title:    tk.Title,
					GUID:     "sonrad-" + hashStr(d.URL+":"+f.Name),
					Link:     pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey),
					PubDate:  time.Now().Add(-time.Hour),
					Size:     fileSize(f, d),
					Category: categoryFor("tv", d),
					IMDB:     imdb,
					Season:   f.Season,
					Episode:  f.Episode,
				})
			}
			// season-pack result (whole dir)
			if wantEp == 0 && len(files) > 0 {
				tk := Token{
					Title:    formatSeasonPackName(title, d),
					Category: "tv",
					DirURL:   d.URL,
				}
				var packSize int64
				for _, f := range files {
					if f.Size > 0 {
						packSize += f.Size
					} else {
						packSize += defaultSize(d, false)
					}
				}
				items = append(items, indexerItem{
					Title:    tk.Title,
					GUID:     "sonrad-" + hashStr(d.URL+":PACK"),
					Link:     pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey),
					PubDate:  time.Now().Add(-time.Hour),
					Size:     packSize,
					Category: categoryFor("tv", d),
					IMDB:     imdb,
					Season:   d.Season,
				})
			}
		} else {
			// Movie. Some hosts also serve series under non-season dirs — skip
			// those when site has clear TV structure.
			if siteHasTV && mode == "" {
				continue
			}
			if len(files) == 0 {
				tk := Token{Title: formatMovieName(title, d), Category: "movies", DirURL: d.URL}
				items = append(items, indexerItem{
					Title:    tk.Title,
					GUID:     "sonrad-" + hashStr(d.URL),
					Link:     pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey),
					PubDate:  time.Now().Add(-time.Hour),
					Size:     defaultSize(d, true),
					Category: categoryFor("movies", d),
					IMDB:     imdb,
				})
				continue
			}
			for _, f := range files {
				tk := Token{
					Title:    formatMovieName(title, d),
					Category: "movies",
					DirURL:   d.URL,
					Files:    []string{f.Name},
				}
				items = append(items, indexerItem{
					Title:    tk.Title,
					GUID:     "sonrad-" + hashStr(d.URL+":"+f.Name),
					Link:     pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey),
					PubDate:  time.Now().Add(-time.Hour),
					Size:     fileSize(f, d),
					Category: categoryFor("movies", d),
					IMDB:     imdb,
				})
			}
		}
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

func formatMovieName(title string, d Directory) string {
	parts := []string{stripTitle(title)}
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
	parts = append(parts, "AZFILM")
	return strings.Join(parts, ".")
}

func formatSeasonPackName(title string, d Directory) string {
	parts := []string{stripTitle(title), fmt.Sprintf("S%02d", d.Season)}
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
	parts = append(parts, "AZFILM")
	return strings.Join(parts, ".")
}

func formatTVName(title string, f FileEntry, d Directory) string {
	parts := []string{stripTitle(title), fmt.Sprintf("S%02dE%02d", f.Season, f.Episode)}
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
	parts = append(parts, "AZFILM")
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
	b.WriteString(`<description>sonrad bridge for azfilm.theazizi.ir</description>` + "\n")
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
	if t, err := decodeToken(string(bytes.TrimSpace(b))); err == nil && t.DirURL != "" {
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
			"sorters":  []any{},
			"servers":  []any{},
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
	if t.DirURL == "" {
		return nil, errors.New("token has no dir_url")
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
	if len(t.Files) > 0 {
		for _, fn := range t.Files {
			f := &JobFile{
				URL:      joinURL(t.DirURL, fn),
				Filename: fn,
				Status:   "pending",
			}
			j.Files = append(j.Files, f)
		}
	} else {
		entries, err := scrapeDirectory(t.DirURL)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return nil, fmt.Errorf("no files in %s", t.DirURL)
		}
		for _, e := range entries {
			j.Files = append(j.Files, &JobFile{
				URL:      e.URL,
				Filename: e.Name,
				Bytes:    e.Size,
				Status:   "pending",
			})
			j.Bytes += e.Size
		}
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
		"status":          "ok",
		"version":         version,
		"queue_length":    len(queue),
		"history_length":  len(history),
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

func testScrape(imdb string) {
	title, dirs, err := scrapeMoviePage(imdb)
	if err != nil {
		log.Fatalf("scrape: %v", err)
	}
	fmt.Printf("Title: %s\n", title)
	fmt.Printf("Directories: %d\n\n", len(dirs))
	for _, d := range dirs {
		fmt.Printf("  %s\n", d.URL)
		fmt.Printf("    season=%d quality=%s codec=%s source=%s audio=%s\n",
			d.Season, d.Quality, d.Codec, d.Source, d.Audio)
		files, err := scrapeDirectory(d.URL)
		if err != nil {
			fmt.Printf("    error: %v\n", err)
			continue
		}
		for _, f := range files {
			fmt.Printf("    - %s (S%02dE%02d, %s)\n", f.Name, f.Season, f.Episode, bytesString(f.Size))
		}
		fmt.Println()
	}
}
