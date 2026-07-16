// Package release parses release metadata (quality/codec/audio/source/season)
// out of film2mz filenames and builds Sonarr/Radarr-parseable release names,
// Newznab categories and size estimates from it.
package release

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Info carries the release metadata parsed out of a download filename.
type Info struct {
	Quality string // 480p / 720p / 1080p / 2160p
	Codec   string // x264 / x265
	Audio   string // SoftSub / Dubbed
	Source  string // Web-DL / BluRay / WEBRip
	Season  int    // 0 for movie
}

var (
	reSE = regexp.MustCompile(`(?i)S(\d{1,2})E(\d{1,4})`)
	// Strip Sonarr/Radarr release-style noise from a free-text query before
	// shipping it to film2mz's search. Order matters: episode tokens first,
	// then years.
	reQueryEpisode = regexp.MustCompile(`(?i)\bs\d{1,2}(?:e\d{1,4})?\b`)
	reQuerySeason  = regexp.MustCompile(`(?i)\bseason\s*\d{1,2}\b`)
	reQueryYear    = regexp.MustCompile(`\b(19|20)\d{2}\b`)
	reMultiSpace   = regexp.MustCompile(`\s+`)
	reTitleChars   = regexp.MustCompile(`[^A-Za-z0-9 \-]+`)
	reTitleSpaces  = regexp.MustCompile(`\s+`)
)

// Parse extracts quality/codec/audio/source (+ season for episodes) from a
// release filename, e.g.
//
//	"V.for.Vendetta.2005.1080p.BluRay.x265.Farsi.Sub.mkv"
//	"Alice.in.Borderland.S02E01.1080p.WEB-DL.Farsi.Dubbed.mkv"
func Parse(name string) Info {
	var inf Info
	if m := reSE.FindStringSubmatch(name); len(m) >= 2 {
		inf.Season, _ = strconv.Atoi(m[1])
	}
	low := strings.ToLower(name)
	switch {
	case strings.Contains(name, "2160p"), strings.Contains(low, "4k"):
		inf.Quality = "2160p"
	case strings.Contains(name, "1080p"):
		inf.Quality = "1080p"
	case strings.Contains(name, "720p"):
		inf.Quality = "720p"
	case strings.Contains(name, "480p"):
		inf.Quality = "480p"
	}
	if strings.Contains(name, "x265") || strings.Contains(low, "hevc") || strings.Contains(low, "10bit") {
		inf.Codec = "x265"
	} else {
		inf.Codec = "x264"
	}
	switch {
	case strings.Contains(low, "bluray"):
		inf.Source = "BluRay"
	case strings.Contains(low, "webrip"):
		inf.Source = "WEBRip"
	default:
		inf.Source = "Web-DL"
	}
	if strings.Contains(low, "dubbed") {
		inf.Audio = "Dubbed"
	} else {
		inf.Audio = "SoftSub"
	}
	return inf
}

// CleanQuery strips Sonarr/Radarr release-style noise so film2mz's
// natural-language search can match. e.g.
//
//	"Alice.in.Borderland.S01E05" → "Alice in Borderland"
//	"The.Matrix.1999"            → "The Matrix"
func CleanQuery(q string) string {
	q = strings.ReplaceAll(q, ".", " ")
	q = strings.ReplaceAll(q, "_", " ")
	q = reQueryEpisode.ReplaceAllString(q, "")
	q = reQuerySeason.ReplaceAllString(q, "")
	q = reQueryYear.ReplaceAllString(q, "")
	q = reMultiSpace.ReplaceAllString(q, " ")
	return strings.TrimSpace(q)
}

// MovieName builds a Radarr-parseable release name.
func MovieName(title string, year int, inf Info) string {
	return buildName(title, year, "", inf)
}

// TVName builds a Sonarr-parseable per-episode release name.
func TVName(title string, year, season, episode int, inf Info) string {
	return buildName(title, year, fmt.Sprintf("S%02dE%02d", season, episode), inf)
}

// SeasonPackName builds a Sonarr-parseable season-pack release name.
func SeasonPackName(title string, year int, inf Info) string {
	return buildName(title, year, fmt.Sprintf("S%02d", inf.Season), inf)
}

func buildName(title string, year int, seToken string, inf Info) string {
	parts := []string{stripTitle(title)}
	if year > 0 {
		parts = append(parts, strconv.Itoa(year))
	}
	if seToken != "" {
		parts = append(parts, seToken)
	}
	if inf.Quality != "" {
		parts = append(parts, inf.Quality)
	}
	if inf.Source != "" {
		parts = append(parts, inf.Source)
	}
	if inf.Codec != "" {
		parts = append(parts, inf.Codec)
	}
	if inf.Audio == "Dubbed" {
		parts = append(parts, "DUBBED")
	}
	parts = append(parts, "FILM2MZ")
	return strings.Join(parts, ".")
}

func stripTitle(t string) string {
	t = strings.TrimSpace(reTitleChars.ReplaceAllString(t, " "))
	t = reTitleSpaces.ReplaceAllString(t, ".")
	return t
}

// Category maps a release to a Newznab category id. kind is "tv" or "movies".
func Category(kind string, inf Info) int {
	if kind == "tv" {
		switch inf.Quality {
		case "480p":
			return 5030
		case "720p", "1080p":
			return 5040
		case "2160p":
			return 5050
		}
		return 5000
	}
	switch inf.Quality {
	case "480p":
		return 2030
	case "720p", "1080p":
		return 2040
	case "2160p":
		return 2050
	}
	return 2000
}

// SizeOrDefault returns size when known, otherwise a quality/codec-based
// estimate. film2mz doesn't expose file sizes, and without a non-zero size
// the SAB queue reports 0 and Sonarr renders progress as "NaN%".
func SizeOrDefault(size int64, inf Info) int64 {
	if size > 0 {
		return size
	}
	var base int64
	switch inf.Quality {
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
	if inf.Codec == "x265" {
		base = base * 6 / 10
	}
	return base
}
