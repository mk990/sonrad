# sonrad

A single-file Go bridge that lets **Sonarr** and **Radarr** download from
`azfilm.theazizi.ir` direct links.

It speaks two protocols at once:

- **Newznab indexer** — Sonarr/Radarr can search azfilm by IMDB ID, see every
  available quality / codec / audio variant as a separate result, and let their
  own quality profiles pick.
- **SABnzbd download client** — when Sonarr/Radarr send a release back, sonrad
  scrapes the listing, downloads the files into the right folder, and reports
  status back so the import works automatically.

No external dependencies (Go stdlib only). One file.

---

## Quick start

### From source

```sh
go build -o sonrad sonrad.go

./sonrad \
  -addr :8910 \
  -download-dir /downloads \
  -api-key MYKEY
```

### Docker

```sh
docker build -t sonrad .

docker run -d --name sonrad \
  -p 8910:8910 \
  -e SONRAD_API_KEY=MYKEY \
  -v /your/host/downloads:/downloads \
  sonrad
```

Then open `http://HOST:8910/` for a live queue/history view.

---

## Sonarr / Radarr setup

### 1. Add as an indexer

`Settings → Indexers → Add → Newznab (custom)`

| field   | value                          |
|---------|--------------------------------|
| Name    | sonrad                         |
| URL     | `http://HOST:8910`             |
| API Key | `MYKEY` (same value you used)  |
| Categories | Movies → `2000`, TV → `5000` |

Test should succeed.

### 2. Add as a download client

`Settings → Download Clients → Add → SABnzbd`

| field        | value         |
|--------------|---------------|
| Host         | `HOST`        |
| Port         | `8910`        |
| URL Base     | `/sabnzbd`    |
| API Key      | `MYKEY`       |
| Username     | *(empty)*     |
| Password     | *(empty)*     |
| Category     | `tv` for Sonarr, `movies` for Radarr |
| Use SSL      | off           |

Test should succeed.

### 3. Path mapping (if needed)

Files land at `<download-dir>/<cat>/<sanitized-title>/...` inside sonrad. If
Sonarr/Radarr sees that volume at a different path, add a remote path mapping
in their Download Client settings so they can find the completed files.

---

## How it works

```
                        IMDB id
                           │
   Sonarr/Radarr           ▼                       azfilm.theazizi.ir
        │      ┌──────────────────────┐  GET            │
        │  ◀── │   /api (Newznab)     │ ─────────────▶  │ movie.php?imdb=…
        │      │  scrapes + emits RSS │   parses           directory links
        │      └──────────────────────┘                   │
        │                                                 │
        │  picks a result, fetches the .nzb               │
        │      ┌──────────────────────┐                   │
        │  ◀── │  /getnzb?token=…     │                   │
        │      │ fake NZB w/ token    │                   │
        │      └──────────────────────┘                   │
        │                                                 │
        │  POST the .nzb to download client               │
        │      ┌──────────────────────┐                   │
        ├────▶ │ /sabnzbd/api         │                   │
        │      │ addfile → queue job  │                   │
        │      └──────────────────────┘                   │
        │                  │                              │
        │                  ▼                              │
        │      ┌──────────────────────┐  GET dir + files  │
        │      │  worker downloads    │ ────────────────▶ │
        │      │  → /downloads/cat/…  │                   │
        │      └──────────────────────┘                   │
        │                  │
        │  polls /sabnzbd/api?mode=history
        │  sees Completed → imports from `storage` path
        ▼
   library
```

Sonarr/Radarr never know they aren't talking to a real Newznab indexer and a
real SABnzbd. The "NZB" is a tiny XML doc that carries a base64-encoded JSON
token describing what to fetch.

---

## Flags

| flag                | env var                  | default                              | meaning |
|---------------------|--------------------------|--------------------------------------|---------|
| `-addr`             | `SONRAD_ADDR`            | `:8910`                              | HTTP listen address |
| `-download-dir`     | `SONRAD_DOWNLOAD_DIR`    | `./downloads`                        | where completed files end up |
| `-api-key`          | `SONRAD_API_KEY`         | *(auto-generated, logged)*           | required by Sonarr/Radarr |
| `-max-concurrent`   | `SONRAD_MAX_CONCURRENT`  | `3`                                  | parallel file downloads |
| `-rate-limit`       | `SONRAD_RATE_LIMIT`      | `0` (unlimited)                      | aggregate bytes/sec cap |
| `-user-agent`       | `SONRAD_USER_AGENT`      | `Mozilla/5.0 … sonrad/1.0`           | UA for upstream requests |
| `-cookies`          | `SONRAD_COOKIES`         | *(empty)*                            | raw `Cookie:` header for upstream |
| `-base-url`         | `SONRAD_BASE_URL`        | `https://azfilm.theazizi.ir`         | azfilm base |
| `-cache-ttl`        | `SONRAD_CACHE_TTL`       | `10m`                                | indexer scrape cache TTL |
| `-public-host`      | `SONRAD_PUBLIC_HOST`     | *(derived from request)*             | host:port used in callback links — set this behind a reverse proxy |
| `-debug`            | —                        | off                                  | verbose logging |
| `-test IMDB`        | —                        | —                                    | scrape that IMDB id, print results, exit |

Env vars are only honored by the Docker entrypoint; in the binary they map 1:1
to the flags above.

---

## Testing the scraper directly

Before touching Sonarr/Radarr, confirm parsing works on the show you care
about:

```sh
./sonrad -test tt10795658
```

You should see the title, every directory variant, and every file with its
season/episode and size. Example:

```
Title: Alice in Borderland
Directories: 16

  https://…/SoftSub/S01/1080p.x265.10bit.Web-DL/
    season=1 quality=1080p codec=x265 source=Web-DL audio=SoftSub
    - Alice.in.Borderland.S01E01.…x265.HEVC.PSA.SoftSub.…mkv  (S01E01, 748.60 MB)
    …
```

If sizes show `0.00 MB`, that directory's HTML probably uses a different
layout — open an issue or tweak `reSizeCell` / `reFile` regexes.

---

## What gets emitted as indexer results

For a given IMDB query, sonrad emits **one result per file plus one season
pack per directory** (for TV). Each result includes:

- a release-style name: `Show.Name.S01E05.720p.Web-DL.x264.AZFILM`
- the right Newznab category (`5040` for HD TV, `2040` for HD movies, etc.)
- accurate size (from the listing's `.s code` cell)
- the IMDB id

Sonarr/Radarr's quality profiles then choose which one to grab — no need for
a `-quality` flag.

---

## File layout

```
<download-dir>/
├── movies/
│   └── Some.Movie.1080p.Web-DL.x264.AZFILM/
│       └── Some.Movie.original.filename.mkv
└── tv/
    └── Alice.in.Borderland.S01E05.720p.Web-DL.x264.AZFILM/
        └── Alice.in.Borderland.S01E05.720p.NF.…mkv
```

This matches SAB's standard `complete_dir/category/jobname/files` layout,
which is what Sonarr/Radarr expect to import from.

---

## Limitations

- **IMDB only** — azfilm pages are indexed by IMDB id, so searches without one
  return empty. Sonarr passes IMDB for TV when it knows it; Radarr almost
  always does.
- **No state persistence** — queue/history live in memory. Restarting drops
  in-flight jobs (resume is per-file via HTTP `Range`, so they pick up if
  re-queued).
- **HTML scraping** — if azfilm or its directory hosts change their markup,
  the regexes (`reDirLink`, `reFile`, `reFileA`, `reSizeCell`) are the only
  things to update.
- **No subtitle sidecar download** — only video files are fetched.

---

## Endpoints (reference)

| path                  | who calls it           | purpose |
|-----------------------|------------------------|---------|
| `GET /api?t=caps`     | Sonarr/Radarr          | indexer capabilities |
| `GET /api?t=movie&imdbid=…`     | Radarr      | movie search |
| `GET /api?t=tvsearch&imdbid=…&season=…&ep=…` | Sonarr | TV search |
| `GET /getnzb?token=…&apikey=…`  | Sonarr/Radarr | fetch fake NZB |
| `/sabnzbd/api?mode=…` | Sonarr/Radarr          | SABnzbd-compatible client API |
| `GET /`               | you                    | live status page |

---

## License

Do whatever you want with it.
