package release

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		name string
		want Info
	}{
		{"V.for.Vendetta.2005.1080p.BluRay.x265.Farsi.Sub.mkv",
			Info{Quality: "1080p", Codec: "x265", Audio: "SoftSub", Source: "BluRay"}},
		{"Alice.in.Borderland.S02E01.1080p.WEB-DL.Farsi.Dubbed.mkv",
			Info{Quality: "1080p", Codec: "x264", Audio: "Dubbed", Source: "Web-DL", Season: 2}},
		{"Some.Show.S01E05.720p.WEBRip.mkv",
			Info{Quality: "720p", Codec: "x264", Audio: "SoftSub", Source: "WEBRip", Season: 1}},
		{"Movie.2160p.HEVC.mkv",
			Info{Quality: "2160p", Codec: "x265", Audio: "SoftSub", Source: "Web-DL"}},
		{"Movie.4K.10bit.mkv",
			Info{Quality: "2160p", Codec: "x265", Audio: "SoftSub", Source: "Web-DL"}},
		{"Bare.File.mkv",
			Info{Quality: "", Codec: "x264", Audio: "SoftSub", Source: "Web-DL"}},
	}
	for _, c := range cases {
		if got := Parse(c.name); got != c.want {
			t.Errorf("Parse(%q) = %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestCleanQuery(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Alice.in.Borderland.S01E05", "Alice in Borderland"},
		{"The.Matrix.1999", "The Matrix"},
		{"Breaking_Bad_Season 2", "Breaking Bad"},
		{"Show S01", "Show"},
		{"plain title", "plain title"},
	}
	for _, c := range cases {
		if got := CleanQuery(c.in); got != c.want {
			t.Errorf("CleanQuery(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestNames(t *testing.T) {
	inf := Info{Quality: "1080p", Codec: "x265", Audio: "Dubbed", Source: "BluRay", Season: 2}
	if got, want := MovieName("V for Vendetta!", 2005, inf), "V.for.Vendetta.2005.1080p.BluRay.x265.DUBBED.FILM2MZ"; got != want {
		t.Errorf("MovieName = %q, want %q", got, want)
	}
	if got, want := TVName("Alice in Borderland", 2020, 2, 1, inf), "Alice.in.Borderland.2020.S02E01.1080p.BluRay.x265.DUBBED.FILM2MZ"; got != want {
		t.Errorf("TVName = %q, want %q", got, want)
	}
	if got, want := SeasonPackName("Alice in Borderland", 2020, inf), "Alice.in.Borderland.2020.S02.1080p.BluRay.x265.DUBBED.FILM2MZ"; got != want {
		t.Errorf("SeasonPackName = %q, want %q", got, want)
	}
}

func TestCategory(t *testing.T) {
	cases := []struct {
		kind    string
		quality string
		want    int
	}{
		{"tv", "480p", 5030}, {"tv", "720p", 5040}, {"tv", "1080p", 5040}, {"tv", "2160p", 5050}, {"tv", "", 5000},
		{"movies", "480p", 2030}, {"movies", "1080p", 2040}, {"movies", "2160p", 2050}, {"movies", "", 2000},
	}
	for _, c := range cases {
		if got := Category(c.kind, Info{Quality: c.quality}); got != c.want {
			t.Errorf("Category(%s, %s) = %d, want %d", c.kind, c.quality, got, c.want)
		}
	}
}

func TestSizeOrDefault(t *testing.T) {
	if got := SizeOrDefault(42, Info{Quality: "1080p"}); got != 42 {
		t.Errorf("known size not passed through: %d", got)
	}
	x264 := SizeOrDefault(0, Info{Quality: "1080p", Codec: "x264"})
	x265 := SizeOrDefault(0, Info{Quality: "1080p", Codec: "x265"})
	if x264 <= 0 || x265 <= 0 || x265 >= x264 {
		t.Errorf("estimates wrong: x264=%d x265=%d (x265 should be smaller)", x264, x265)
	}
}
