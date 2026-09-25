// gocrawler - a lightweight, concurrent web crawler / spider for recon.
//
// Given a seed URL and a depth, it crawls same-host pages, extracting:
//   - <a href> links
//   - <script src> JS files
//   - <link href> resources (CSS, etc.)
//   - <form action> endpoints
//   - inline JS "endpoint-looking" strings (a la LinkFinder-style regex)
//
// Stdlib only - no external dependencies, so `go build` works anywhere.
package main

import (
	"bufio"
	"crypto/tls"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ---------- config ----------

type config struct {
	seed        string
	maxDepth    int
	concurrency int
	timeout     time.Duration
	sameHost    bool
	userAgent   string
	outFile     string
	verbose     bool
	insecure    bool
}

// ---------- crawl state ----------

type result struct {
	url        string
	depth      int
	status     int
	links      []string
	jsFiles    []string
	forms      []string
	jsEndpoints []string
}

type crawler struct {
	cfg      config
	client   *http.Client
	seedHost string

	visited   map[string]bool
	visitedMu sync.Mutex

	sem  chan struct{} // concurrency limiter
	wg   sync.WaitGroup
	resM sync.Mutex
	results []result
}

// ---------- regexes ----------

var (
	hrefRe   = regexp.MustCompile(`(?i)<a[^>]+href=["']([^"'#>]+)["']`)
	scriptRe = regexp.MustCompile(`(?i)<script[^>]+src=["']([^"'>]+)["']`)
	linkRe   = regexp.MustCompile(`(?i)<link[^>]+href=["']([^"'>]+)["']`)
	formRe   = regexp.MustCompile(`(?i)<form[^>]+action=["']([^"'>]+)["']`)

	// Rough LinkFinder-style regex for endpoint-looking strings inside JS blobs.
	// Catches things like "/api/v1/users", "/admin/config.json", relative paths in quotes, etc.
	jsEndpointRe = regexp.MustCompile(`(?i)["'\x60](/[a-zA-Z0-9_\-./{}?=&%#]{2,}?)["'\x60]`)
)

func main() {
	cfg := parseFlags()

	transport := &http.Transport{}
	if cfg.insecure {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}

	c := &crawler{
		cfg: cfg,
		client: &http.Client{
			Timeout:   cfg.timeout,
			Transport: transport,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 10 {
					return fmt.Errorf("too many redirects")
				}
				return nil
			},
		},
		visited: make(map[string]bool),
		sem:     make(chan struct{}, cfg.concurrency),
	}

	seedURL, err := url.Parse(cfg.seed)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid seed URL: %v\n", err)
		os.Exit(1)
	}
	c.seedHost = seedURL.Host

	fmt.Printf("[*] Starting crawl: %s (depth=%d, concurrency=%d)\n\n", cfg.seed, cfg.maxDepth, cfg.concurrency)

	c.wg.Add(1)
	go c.crawl(cfg.seed, 0)
	c.wg.Wait()

	c.report()
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.seed, "u", "", "seed URL to start crawling (required)")
	flag.IntVar(&cfg.maxDepth, "d", 2, "max crawl depth")
	flag.IntVar(&cfg.concurrency, "c", 10, "concurrent workers")
	flag.DurationVar(&cfg.timeout, "timeout", 8*time.Second, "per-request timeout")
	flag.BoolVar(&cfg.sameHost, "same-host", true, "restrict crawl to seed host")
	flag.StringVar(&cfg.userAgent, "ua", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36", "User-Agent header")
	flag.BoolVar(&cfg.insecure, "k", false, "skip TLS certificate verification (self-signed / internal CA)")
	flag.StringVar(&cfg.outFile, "o", "", "output file for discovered URLs (optional)")
	flag.BoolVar(&cfg.verbose, "v", false, "verbose output")
	flag.Parse()

	if cfg.seed == "" {
		fmt.Println("usage: gocrawler -u https://target.com -d 3 -c 20")
		os.Exit(1)
	}
	return cfg
}

// crawl fetches a URL, extracts endpoints, and recurses into discovered links
// up to cfg.maxDepth. Each call runs in its own goroutine, gated by c.sem.
func (c *crawler) crawl(target string, depth int) {
	defer c.wg.Done()

	if depth > c.cfg.maxDepth {
		return
	}

	norm := normalizeURL(target)
	c.visitedMu.Lock()
	if c.visited[norm] {
		c.visitedMu.Unlock()
		return
	}
	c.visited[norm] = true
	c.visitedMu.Unlock()

	c.sem <- struct{}{}        // acquire
	defer func() { <-c.sem }() // release

	body, status, err := c.fetch(target)
	if err != nil {
		if c.cfg.verbose || depth == 0 {
			fmt.Fprintf(os.Stderr, "[!] %s -> %v\n", target, err)
		}
		return
	}

	links := extractAll(hrefRe, body)
	jsFiles := extractAll(scriptRe, body)
	cssLinks := extractAll(linkRe, body)
	forms := extractAll(formRe, body)
	jsEndpoints := extractJSEndpoints(jsEndpointRe, body)

	r := result{
		url:         target,
		depth:       depth,
		status:      status,
		links:       links,
		jsFiles:     jsFiles,
		forms:       forms,
		jsEndpoints: jsEndpoints,
	}

	c.resM.Lock()
	c.results = append(c.results, r)
	c.resM.Unlock()

	fmt.Printf("[%d] %-3d %s  (links:%d js:%d forms:%d)\n", depth, status, target, len(links), len(jsFiles), len(forms))

	if depth == c.cfg.maxDepth {
		return
	}

	// resolve + recurse into same-host links
	all := append(append([]string{}, links...), cssLinks...)
	for _, raw := range all {
		abs := resolveURL(target, raw)
		if abs == "" {
			continue
		}
		if c.cfg.sameHost && !sameHost(abs, c.seedHost) {
			continue
		}
		c.wg.Add(1)
		go c.crawl(abs, depth+1)
	}
}

func (c *crawler) fetch(target string) (string, int, error) {
	req, err := http.NewRequest("GET", target, nil)
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("User-Agent", c.cfg.userAgent)

	resp, err := c.client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	// cap body read to avoid huge files blowing up memory
	limited := io.LimitReader(resp.Body, 5<<20) // 5MB
	buf, err := io.ReadAll(limited)
	if err != nil {
		return "", resp.StatusCode, err
	}
	return string(buf), resp.StatusCode, nil
}

// ---------- helpers ----------

func extractAll(re *regexp.Regexp, body string) []string {
	matches := re.FindAllStringSubmatch(body, -1)
	out := make([]string, 0, len(matches))
	seen := map[string]bool{}
	for _, m := range matches {
		if len(m) > 1 && !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	return out
}

// extractJSEndpoints pulls quoted path-like strings out of inline <script> blocks
// and full response bodies - a cheap LinkFinder-style pass, good for spotting
// API routes hardcoded in JS.
func extractJSEndpoints(re *regexp.Regexp, body string) []string {
	matches := re.FindAllStringSubmatch(body, -1)
	seen := map[string]bool{}
	out := []string{}
	skip := []string{".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff", ".css"}
	for _, m := range matches {
		if len(m) < 2 {
			continue
		}
		p := m[1]
		if seen[p] {
			continue
		}
		lower := strings.ToLower(p)
		isAsset := false
		for _, ext := range skip {
			if strings.HasSuffix(lower, ext) {
				isAsset = true
				break
			}
		}
		if isAsset {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	return out
}

func resolveURL(base, ref string) string {
	if strings.HasPrefix(ref, "javascript:") || strings.HasPrefix(ref, "mailto:") || strings.HasPrefix(ref, "tel:") {
		return ""
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return ""
	}
	refURL, err := url.Parse(ref)
	if err != nil {
		return ""
	}
	resolved := baseURL.ResolveReference(refURL)
	resolved.Fragment = ""
	return resolved.String()
}

func sameHost(target, host string) bool {
	u, err := url.Parse(target)
	if err != nil {
		return false
	}
	return u.Host == host
}

func normalizeURL(u string) string {
	parsed, err := url.Parse(u)
	if err != nil {
		return u
	}
	parsed.Fragment = ""
	return parsed.String()
}

// report prints a summary and optionally writes discovered URLs/JS/endpoints to file.
func (c *crawler) report() {
	fmt.Printf("\n[*] Crawl complete. %d pages visited.\n\n", len(c.results))

	allJS := map[string]bool{}
	allEndpoints := map[string]bool{}
	allForms := map[string]bool{}

	for _, r := range c.results {
		for _, j := range r.jsFiles {
			allJS[resolveURL(r.url, j)] = true
		}
		for _, e := range r.jsEndpoints {
			allEndpoints[e] = true
		}
		for _, f := range r.forms {
			allForms[resolveURL(r.url, f)] = true
		}
	}

	fmt.Printf("=== JavaScript files (%d) ===\n", len(allJS))
	for j := range allJS {
		fmt.Println(" ", j)
	}

	fmt.Printf("\n=== Form action endpoints (%d) ===\n", len(allForms))
	for f := range allForms {
		fmt.Println(" ", f)
	}

	fmt.Printf("\n=== Path-like strings found in responses (%d) ===\n", len(allEndpoints))
	for e := range allEndpoints {
		fmt.Println(" ", e)
	}

	if c.cfg.outFile != "" {
		f, err := os.Create(c.cfg.outFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[!] could not write output file: %v\n", err)
			return
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		defer w.Flush()
		for _, r := range c.results {
			fmt.Fprintln(w, r.url)
		}
		for j := range allJS {
			fmt.Fprintln(w, j)
		}
		for e := range allEndpoints {
			fmt.Fprintln(w, e)
		}
		fmt.Printf("\n[*] Results written to %s\n", c.cfg.outFile)
	}
}
