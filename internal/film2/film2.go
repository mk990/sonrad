// Package film2 is the scraper for film2mz.top: free-text search via the
// site's /quick-search endpoint and direct-download link extraction from
// post/series pages. Results are cached with a small TTL map.
package film2

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/mk990/sonrad/internal/naming"
	"github.com/mk990/sonrad/internal/upstream"
)

// SearchHit is one result from film2mz's /quick-search endpoint.
type SearchHit struct {
	IMDB  string
	Title string
	IsTV  bool
	URL   string // absolute page URL
	Year  int
}

// FileEntry is one direct download link scraped from a post/series page.
// film2mz doesn't expose per-file sizes, so Size stays 0 and callers fall
// back to an estimate.
type FileEntry struct {
	Name    string
	URL     string
	Size    int64
	Season  int
	Episode int
}

var (
	// Absolute CDN download links on a post/series page. The play/online and
	// player-launch anchors don't point at a media file, so this skips them.
	reMediaURL = regexp.MustCompile(`(?i)href="(https?://[^"]+?\.(?:mkv|mp4|avi|m4v|mov|ts|wmv))"`)
	reSE       = regexp.MustCompile(`(?i)S(\d{1,2})E(\d{1,4})`)
	// Anime/series often name files with a bare episode token and no season
	// prefix, e.g. "Solo.Leveling.E1.1080p…mkv" or "…EP01…". Used only when
	// reSE doesn't match; the season is recovered from the URL path instead.
	reEpOnly = regexp.MustCompile(`(?i)(?:^|[._ -])E(?:P|pisode)?[._ -]?(\d{1,4})(?:[._ -]|$)`)
	// Season folder in a CDN path, e.g. ".../Series/Solo.Leveling/S01/file.mkv".
	reSeasonPath = regexp.MustCompile(`(?i)/S(\d{1,2})/`)
)

type Client struct {
	up    *upstream.Client
	base  string // base URL without trailing slash
	ttl   time.Duration
	cache *cache
}

func New(up *upstream.Client, baseURL string, cacheTTL time.Duration) *Client {
	return &Client{
		up:    up,
		base:  strings.TrimRight(baseURL, "/"),
		ttl:   cacheTTL,
		cache: newCache(),
	}
}

// Base returns the site base URL (no trailing slash).
func (c *Client) Base() string { return c.base }

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

// Search queries film2mz's free-text search and returns one hit per result.
func (c *Client) Search(q string) ([]SearchHit, error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return nil, nil
	}
	key := "search:" + q
	if v, ok := c.cache.Get(key); ok {
		return v.([]SearchHit), nil
	}
	form := url.Values{}
	form.Set("q", q)
	form.Set("sort", "modified_at:desc")
	body, err := c.postForm(c.base+"/quick-search", form)
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
			URL:   c.absURL(r.URL),
			Year:  int(r.Year),
		})
	}
	if len(hits) > 0 {
		c.cache.Set(key, hits, c.ttl)
	}
	return hits, nil
}

// PageFiles fetches a film2mz post/series page and returns every direct
// download link on it. Quality/codec/audio/season are parsed per-file from
// the filename.
func (c *Client) PageFiles(pageURL string) ([]FileEntry, error) {
	key := "page:" + pageURL
	if v, ok := c.cache.Get(key); ok {
		return v.([]FileEntry), nil
	}
	body, err := c.up.GetBytes(pageURL)
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
		c.cache.Set(key, files, c.ttl)
	}
	return files, nil
}

// postForm posts a urlencoded form, mirroring the headers film2mz's
// /quick-search endpoint expects (X-Requested-With, Origin, Referer).
func (c *Client) postForm(rawurl string, form url.Values) ([]byte, error) {
	req, err := http.NewRequest("POST", rawurl, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	req.Header.Set("Origin", c.base)
	req.Header.Set("Referer", c.base+"/")
	resp, err := c.up.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("POST %s: %s", rawurl, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 32<<20))
}

func (c *Client) absURL(u string) string {
	if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
		return u
	}
	return c.base + "/" + strings.TrimLeft(u, "/")
}

func fileEntryFromURL(u string) FileEntry {
	f := FileEntry{Name: naming.URLBaseName(u), URL: u}
	if m := reSE.FindStringSubmatch(f.Name); len(m) >= 3 {
		f.Season, _ = strconv.Atoi(m[1])
		f.Episode, _ = strconv.Atoi(m[2])
		return f
	}
	// No SxxExx token: recover a bare episode token ("E1", "EP01") from the
	// filename and the season from the URL path (".../S01/..."), defaulting to
	// season 1 when the path carries no season folder.
	if m := reEpOnly.FindStringSubmatch(f.Name); len(m) >= 2 {
		f.Episode, _ = strconv.Atoi(m[1])
		if m := reSeasonPath.FindStringSubmatch(u); len(m) >= 2 {
			f.Season, _ = strconv.Atoi(m[1])
		}
		if f.Season == 0 {
			f.Season = 1
		}
	}
	return f
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
