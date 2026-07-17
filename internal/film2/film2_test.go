package film2

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mk990/sonrad/internal/upstream"
)

const searchJSON = `[
  {"imdb_id":"tt0111161","title":"The Shawshank &amp; Redemption","type":"post","url":"/movie/1","year":"1994"},
  {"imdb_id":"tt0903747","title":" Breaking Bad ","type":"series","url":"https://other.example.com/series/2","year":2008},
  {"imdb_id":"","title":"No IMDB","type":"post","url":"/movie/3","year":""},
  {"imdb_id":"tt0111161","title":"Duplicate URL","type":"post","url":"/movie/1","year":"1994"},
  {"imdb_id":"tt0000001","title":"Bad Year","type":"post","url":"/movie/4","year":"n/a"}
]`

// pageHTML mimics a film2mz post page: real CDN links, a play link that must
// be skipped, a duplicate, an HTML-escaped URL, and an episode-only filename
// whose season lives in the URL path.
const pageHTML = `<html><body>
<a href="https://cdn.example.com/Series/Solo.Leveling/S02/Solo.Leveling.E5.1080p.x265.mkv">download</a>
<a href="https://cdn.example.com/tv/Breaking.Bad.S01E02.720p.WEB-DL.mkv">download</a>
<a href="https://cdn.example.com/tv/Breaking.Bad.S01E02.720p.WEB-DL.mkv">duplicate</a>
<a href="https://cdn.example.com/player/launch.php">play online</a>
<a href="https://cdn.example.com/movies/a&amp;b/Some.Movie.2005.1080p.BluRay.mkv">download</a>
</body></html>`

func newTestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return New(upstream.New("test", "", false), srv.URL, time.Minute)
}

func TestSearch(t *testing.T) {
	var calls atomic.Int32
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/quick-search" || r.Method != "POST" {
			http.NotFound(w, r)
			return
		}
		calls.Add(1)
		w.Write([]byte(searchJSON))
	}))

	hits, err := c.Search("shawshank")
	if err != nil {
		t.Fatal(err)
	}
	// entry 3 (no imdb) and entry 4 (duplicate URL) are dropped
	if len(hits) != 3 {
		t.Fatalf("got %d hits, want 3: %+v", len(hits), hits)
	}
	h := hits[0]
	if h.Title != "The Shawshank & Redemption" {
		t.Errorf("title = %q — HTML entities not unescaped", h.Title)
	}
	if h.IsTV || h.Year != 1994 || h.IMDB != "tt0111161" {
		t.Errorf("hit 0 = %+v", h)
	}
	if h.URL != c.Base()+"/movie/1" {
		t.Errorf("relative URL not absolutized: %q", h.URL)
	}
	if !hits[1].IsTV || hits[1].Title != "Breaking Bad" || hits[1].URL != "https://other.example.com/series/2" {
		t.Errorf("hit 1 = %+v", hits[1])
	}
	if hits[2].Year != 0 {
		t.Errorf("unparseable year should decode to 0, got %d", hits[2].Year)
	}

	// second identical query must be served from cache
	if _, err := c.Search("shawshank"); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("upstream called %d times, want 1 (cache miss only)", n)
	}
}

func TestPageFiles(t *testing.T) {
	c := newTestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(pageHTML))
	}))
	files, err := c.PageFiles(c.Base() + "/series/solo-leveling")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 3 {
		t.Fatalf("got %d files, want 3 (dup + play link dropped): %+v", len(files), files)
	}
	if f := files[0]; f.Season != 2 || f.Episode != 5 {
		t.Errorf("episode-only name: got S%dE%d, want S2E5 (season from URL path)", f.Season, f.Episode)
	}
	if f := files[1]; f.Season != 1 || f.Episode != 2 {
		t.Errorf("SxxExx name: got S%dE%d, want S1E2", f.Season, f.Episode)
	}
	if f := files[2]; f.URL != "https://cdn.example.com/movies/a&b/Some.Movie.2005.1080p.BluRay.mkv" {
		t.Errorf("escaped URL not unescaped: %q", f.URL)
	}
	if f := files[2]; f.Season != 0 || f.Episode != 0 {
		t.Errorf("movie parsed as episode: S%dE%d", f.Season, f.Episode)
	}
}

func TestFileEntryFromURL(t *testing.T) {
	cases := []struct {
		url             string
		season, episode int
	}{
		{"https://cdn.x/tv/Show.S03E07.1080p.mkv", 3, 7},
		{"https://cdn.x/Series/Show/S02/Show.E12.720p.mkv", 2, 12},
		{"https://cdn.x/Series/Show/Show.EP01.720p.mkv", 1, 1}, // no season folder → season 1
		{"https://cdn.x/movies/Movie.2020.1080p.mkv", 0, 0},
	}
	for _, c := range cases {
		f := fileEntryFromURL(c.url)
		if f.Season != c.season || f.Episode != c.episode {
			t.Errorf("%s: got S%dE%d, want S%dE%d", c.url, f.Season, f.Episode, c.season, c.episode)
		}
	}
}
