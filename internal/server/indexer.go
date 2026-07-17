package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mk990/sonrad/internal/film2"
	"github.com/mk990/sonrad/internal/naming"
	"github.com/mk990/sonrad/internal/release"
)

// maxTitleSearchCandidates caps how many result pages we scrape per free-text
// search. Each candidate triggers one page fetch, so this protects film2mz
// from a thundering herd when a query is ambiguous.
const maxTitleSearchCandidates = 5

var reIMDB = regexp.MustCompile(`tt\d{6,9}`)

// handleAPI is the Newznab entry point (t=caps|search|movie|tvsearch).
func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	if !s.authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	switch t := r.URL.Query().Get("t"); t {
	case "search", "movie", "tvsearch":
		s.handleSearch(w, r, t)
	default: // "", "caps", anything unknown
		respondXML(w, s.capsXML())
	}
}

func (s *Server) capsXML() string {
	return `<?xml version="1.0" encoding="UTF-8"?>
<caps>
  <server version="` + s.version + `" title="sonrad" strapline="film2mz.top bridge" email="" url="" image=""/>
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

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request, mode string) {
	q := r.URL.Query()
	apikey := q.Get("apikey")
	if apikey == "" {
		apikey = r.Header.Get("X-Api-Key")
	}
	pub := s.publicBase(r)
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
	slog.Info("search", "t", mode, "q", qstr, "season", q.Get("season"), "ep", q.Get("ep"), "imdb", imdb)
	var hits []film2.SearchHit
	if qstr != "" {
		var err error
		hits, err = s.site.Search(release.CleanQuery(qstr))
		if err != nil {
			slog.Warn("search failed", "q", qstr, "err", err)
		}
	}

	var candidates, imdbMatched []film2.SearchHit
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
		candidates = append(candidates, h)
		if imdb != "" && strings.EqualFold(h.IMDB, imdb) {
			imdbMatched = append(imdbMatched, h)
		}
	}
	// Prefer the imdb-exact hit, but only when there is one. film2mz's imdb
	// data is frequently wrong or missing (especially for anime), so an imdb
	// that doesn't line up must not zero out an otherwise-good title match.
	if len(imdbMatched) > 0 {
		candidates = imdbMatched
	} else if imdb != "" {
		slog.Info("search: imdb matched no hit; falling back to title matches", "imdb", imdb)
	}
	if len(candidates) > maxTitleSearchCandidates {
		candidates = candidates[:maxTitleSearchCandidates]
	}

	if len(candidates) == 0 {
		s.respondPlaceholderFeed(w, mode, apikey, pub)
		return
	}

	// Fan out one page scrape per candidate; these are the expensive HTTP
	// calls and are independent of each other.
	type result struct {
		title string
		items []indexerItem
	}
	results := make([]result, len(candidates))
	sem := make(chan struct{}, max(1, s.cfg.SearchConcurrency))
	var wg sync.WaitGroup
	for i, h := range candidates {
		wg.Add(1)
		go func(i int, h film2.SearchHit) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			files, err := s.site.PageFiles(h.URL)
			if err != nil {
				slog.Debug("scrape failed", "url", h.URL, "err", err)
				return
			}
			its := s.itemsForHit(h, files, wantSeason, wantEp, apikey, pub)
			if len(its) == 0 {
				slog.Info("search: scraped files but none matched — likely an episode-numbering mismatch",
					"title", h.Title, "url", h.URL, "files", len(files), "season", wantSeason, "ep", wantEp)
			}
			results[i] = result{title: h.Title, items: its}
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
	slog.Info("search done", "t", mode, "q", qstr, "candidates", len(candidates), "items", len(items))
	respondXML(w, s.renderFeed(feedTitle, items))
}

// respondPlaceholderFeed emits a single placeholder result. Sonarr/Radarr's
// indexer Test sends an empty query; an empty feed trips the "no results in
// configured categories" warning that blocks Save in some versions. The title
// won't match Sonarr's release parser, so RSS sync skips it.
func (s *Server) respondPlaceholderFeed(w http.ResponseWriter, mode, apikey, pub string) {
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
	respondXML(w, s.renderFeed("sonrad", []indexerItem{placeholder}))
}

// itemsForHit turns the download links scraped from one film2mz page into
// per-episode (+ season-pack) or per-movie indexer items.
func (s *Server) itemsForHit(h film2.SearchHit, files []film2.FileEntry, wantSeason, wantEp int, apikey, pub string) []indexerItem {
	if h.IsTV {
		return s.tvItems(h, files, wantSeason, wantEp, apikey, pub)
	}
	return s.movieItems(h, files, apikey, pub)
}

// itemLink builds the /getnzb callback link carrying the encoded token.
func itemLink(pub, apikey string, tk Token) string {
	return pub + "/getnzb?token=" + encodeToken(tk) + "&apikey=" + url.QueryEscape(apikey)
}

func (s *Server) movieItems(h film2.SearchHit, files []film2.FileEntry, apikey, pub string) []indexerItem {
	var items []indexerItem
	for _, f := range files {
		inf := release.Parse(f.Name)
		if s.cfg.NoDubbed && inf.Audio == "Dubbed" {
			continue
		}
		name := release.MovieName(h.Title, h.Year, inf)
		size := release.SizeOrDefault(f.Size, inf)
		tk := Token{
			Title:    name,
			Category: "movies",
			URLs:     []string{f.URL},
			Sizes:    []int64{size},
			Names:    []string{naming.ReleaseFileName(name, f.URL)},
		}
		items = append(items, indexerItem{
			Title:    name,
			GUID:     "sonrad-" + hashStr(f.URL),
			Link:     itemLink(pub, apikey, tk),
			PubDate:  time.Now().Add(-time.Hour),
			Size:     size,
			Category: release.Category("movies", inf),
			IMDB:     h.IMDB,
		})
	}
	return items
}

func (s *Server) tvItems(h film2.SearchHit, files []film2.FileEntry, wantSeason, wantEp int, apikey, pub string) []indexerItem {
	var items []indexerItem

	// Group episodes by (season, quality, codec, audio, source) so we can
	// also offer season packs alongside the per-episode releases.
	type packKey struct {
		season                        int
		quality, codec, audio, source string
	}
	packs := map[packKey][]film2.FileEntry{}
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
		inf := release.Parse(f.Name)
		season := f.Season
		if season == 0 {
			season = inf.Season
		}
		if inf.Quality != "" {
			hasQuality[seKey{season, f.Episode}] = true
		}
	}

	for _, f := range files {
		if f.Episode == 0 {
			continue
		}
		inf := release.Parse(f.Name)
		if f.Season == 0 {
			f.Season = inf.Season
		}
		inf.Season = f.Season
		if s.cfg.NoDubbed && inf.Audio == "Dubbed" {
			continue
		}
		if wantSeason > 0 && f.Season != wantSeason {
			continue
		}
		if inf.Quality == "" && hasQuality[seKey{f.Season, f.Episode}] {
			continue
		}
		if wantEp == 0 || f.Episode == wantEp {
			name := release.TVName(h.Title, h.Year, f.Season, f.Episode, inf)
			size := release.SizeOrDefault(f.Size, inf)
			tk := Token{
				Title:    name,
				Category: "tv",
				URLs:     []string{f.URL},
				Sizes:    []int64{size},
				Names:    []string{naming.ReleaseFileName(name, f.URL)},
			}
			items = append(items, indexerItem{
				Title:    name,
				GUID:     "sonrad-" + hashStr(f.URL),
				Link:     itemLink(pub, apikey, tk),
				PubDate:  time.Now().Add(-time.Hour),
				Size:     size,
				Category: release.Category("tv", inf),
				IMDB:     h.IMDB,
				Season:   f.Season,
				Episode:  f.Episode,
			})
		}
		k := packKey{f.Season, inf.Quality, inf.Codec, inf.Audio, inf.Source}
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
			inf := release.Info{Season: k.season, Quality: k.quality, Codec: k.codec, Audio: k.audio, Source: k.source}
			var urls []string
			var sizes []int64
			var names []string
			var packSize int64
			for _, f := range grp {
				sz := release.SizeOrDefault(f.Size, inf)
				urls = append(urls, f.URL)
				sizes = append(sizes, sz)
				names = append(names, naming.ReleaseFileName(release.TVName(h.Title, h.Year, f.Season, f.Episode, inf), f.URL))
				packSize += sz
			}
			tk := Token{Title: release.SeasonPackName(h.Title, h.Year, inf), Category: "tv", URLs: urls, Sizes: sizes, Names: names}
			items = append(items, indexerItem{
				Title:    tk.Title,
				GUID:     "sonrad-" + hashStr(fmt.Sprintf("%s:S%dpack:%s.%s.%s.%s", h.URL, k.season, k.quality, k.codec, k.audio, k.source)),
				Link:     itemLink(pub, apikey, tk),
				PubDate:  time.Now().Add(-time.Hour),
				Size:     packSize,
				Category: release.Category("tv", inf),
				IMDB:     h.IMDB,
				Season:   k.season,
			})
		}
	}
	return items
}

// hashStr is a tiny FNV-1a used to build stable GUIDs from URLs.
func hashStr(s string) string {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= 1099511628211
	}
	return strconv.FormatUint(h, 16)
}
