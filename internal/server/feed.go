package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// indexerItem is one Newznab RSS result.
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

// renderFeed renders indexer items as a Newznab RSS feed.
func (s *Server) renderFeed(title string, items []indexerItem) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<rss version="2.0" xmlns:newznab="http://www.newznab.com/DTD/2010/feeds/attributes/" xmlns:atom="http://www.w3.org/2005/Atom">` + "\n")
	b.WriteString(`<channel>` + "\n")
	b.WriteString(`<title>` + xmlEsc(title) + `</title>` + "\n")
	b.WriteString(`<description>sonrad bridge for film2mz.top</description>` + "\n")
	b.WriteString(`<link>` + xmlEsc(s.site.Base()) + `</link>` + "\n")
	b.WriteString(`<language>en-US</language>` + "\n")
	b.WriteString(`<newznab:response offset="0" total="` + strconv.Itoa(len(items)) + `"/>` + "\n")
	for _, it := range items {
		b.WriteString(`<item>` + "\n")
		b.WriteString(`  <title>` + xmlEsc(it.Title) + `</title>` + "\n")
		b.WriteString(`  <guid isPermaLink="false">` + xmlEsc(it.GUID) + `</guid>` + "\n")
		b.WriteString(`  <link>` + xmlEsc(it.Link) + `</link>` + "\n")
		b.WriteString(`  <pubDate>` + it.PubDate.Format(time.RFC1123Z) + `</pubDate>` + "\n")
		b.WriteString(`  <category>` + strconv.Itoa(it.Category) + `</category>` + "\n")
		fmt.Fprintf(&b, `  <enclosure url="%s" length="%d" type="application/x-nzb"/>`+"\n", xmlEsc(it.Link), it.Size)
		fmt.Fprintf(&b, `  <size>%d</size>`+"\n", it.Size)
		fmt.Fprintf(&b, `  <newznab:attr name="category" value="%d"/>`+"\n", it.Category)
		fmt.Fprintf(&b, `  <newznab:attr name="size" value="%d"/>`+"\n", it.Size)
		if it.IMDB != "" {
			id := strings.TrimPrefix(it.IMDB, "tt")
			b.WriteString(`  <newznab:attr name="imdb" value="` + xmlEsc(id) + `"/>` + "\n")
			b.WriteString(`  <newznab:attr name="imdbid" value="` + xmlEsc(id) + `"/>` + "\n")
		}
		if it.Season > 0 {
			fmt.Fprintf(&b, `  <newznab:attr name="season" value="S%02d"/>`+"\n", it.Season)
		}
		if it.Episode > 0 {
			fmt.Fprintf(&b, `  <newznab:attr name="episode" value="E%02d"/>`+"\n", it.Episode)
		}
		b.WriteString(`</item>` + "\n")
	}
	b.WriteString(`</channel></rss>`)
	return b.String()
}
