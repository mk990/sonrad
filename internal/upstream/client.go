// Package upstream provides the shared HTTP client used for every request to
// the film2mz site and its CDN, applying the configured User-Agent and Cookie
// header uniformly.
package upstream

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// maxBodyBytes caps scrape/search response bodies; media downloads stream and
// are unaffected.
const maxBodyBytes = 32 << 20

type Client struct {
	userAgent string
	cookies   string
	scrape    *http.Client // short timeout — page scrapes and search calls
	stream    *http.Client // no timeout — media downloads can take hours
	lastOK    atomic.Int64 // unix time of the last successful upstream response
}

func New(userAgent, cookies string, insecureSkipVerify bool) *Client {
	tr := &http.Transport{
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if insecureSkipVerify {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &Client{
		userAgent: userAgent,
		cookies:   cookies,
		scrape:    &http.Client{Timeout: 90 * time.Second, Transport: tr},
		stream:    &http.Client{Transport: tr},
	}
}

func (c *Client) decorate(req *http.Request) {
	req.Header.Set("User-Agent", c.userAgent)
	if c.cookies != "" {
		req.Header.Set("Cookie", c.cookies)
	}
}

// Do sends a scrape-class request (bounded by the client timeout).
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	c.decorate(req)
	resp, err := c.scrape.Do(req)
	c.noteResult(resp, err)
	return resp, err
}

// DoStream sends a request on the timeout-free client, for long downloads.
// Cancellation comes from the request context instead.
func (c *Client) DoStream(req *http.Request) (*http.Response, error) {
	c.decorate(req)
	resp, err := c.stream.Do(req)
	c.noteResult(resp, err)
	return resp, err
}

func (c *Client) noteResult(resp *http.Response, err error) {
	if err == nil && resp.StatusCode < 400 {
		c.lastOK.Store(time.Now().Unix())
	}
}

// LastSuccess returns when the site/CDN last answered successfully (zero time
// if never). Health checks use it to spot "site blocked us / cookies expired"
// while the process itself is still fine.
func (c *Client) LastSuccess() time.Time {
	if v := c.lastOK.Load(); v > 0 {
		return time.Unix(v, 0)
	}
	return time.Time{}
}

// GetBytes fetches a URL and returns its body (capped at 32 MiB).
func (c *Client) GetBytes(rawurl string) ([]byte, error) {
	req, err := http.NewRequest("GET", rawurl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "*/*")
	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: %s", rawurl, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
}
