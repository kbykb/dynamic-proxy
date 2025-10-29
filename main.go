package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/armon/go-socks5"
	"golang.org/x/net/proxy"
	"gopkg.in/yaml.v3"
)

// Config represents the application configuration
type Config struct {
	ProxyListURLs              []string `yaml:"proxy_list_urls"`
	HealthCheckConcurrency     int      `yaml:"health_check_concurrency"`
	UpdateIntervalMinutes      int      `yaml:"update_interval_minutes"`
	HealthCheck                struct {
		TotalTimeoutSeconds           int `yaml:"total_timeout_seconds"`
		TLSHandshakeThresholdSeconds  int `yaml:"tls_handshake_threshold_seconds"`
	} `yaml:"health_check"`
	Ports struct {
		SOCKS5Strict   string `yaml:"socks5_strict"`
		SOCKS5Relaxed  string `yaml:"socks5_relaxed"`
		HTTPStrict     string `yaml:"http_strict"`
		HTTPRelaxed    string `yaml:"http_relaxed"`
	} `yaml:"ports"`
}

// Global config variable
var config Config

// loadConfig loads configuration from config.yaml
func loadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Validate config
	if len(cfg.ProxyListURLs) == 0 {
		return nil, fmt.Errorf("at least one proxy_list_url must be specified")
	}
	if cfg.HealthCheckConcurrency <= 0 {
		cfg.HealthCheckConcurrency = 200
	}
	if cfg.UpdateIntervalMinutes <= 0 {
		cfg.UpdateIntervalMinutes = 5
	}
	if cfg.HealthCheck.TotalTimeoutSeconds <= 0 {
		cfg.HealthCheck.TotalTimeoutSeconds = 8
	}
	if cfg.HealthCheck.TLSHandshakeThresholdSeconds <= 0 {
		cfg.HealthCheck.TLSHandshakeThresholdSeconds = 5
	}
	if cfg.Ports.SOCKS5Strict == "" {
		cfg.Ports.SOCKS5Strict = ":1080"
	}
	if cfg.Ports.SOCKS5Relaxed == "" {
		cfg.Ports.SOCKS5Relaxed = ":1082"
	}
	if cfg.Ports.HTTPStrict == "" {
		cfg.Ports.HTTPStrict = ":8080"
	}
	if cfg.Ports.HTTPRelaxed == "" {
		cfg.Ports.HTTPRelaxed = ":8082"
	}

	return &cfg, nil
}

type ProxyPool struct {
	proxies   []string
	mu        sync.RWMutex
	index     uint64
	updating  int32 // atomic flag to prevent concurrent updates
}

func NewProxyPool() *ProxyPool {
	return &ProxyPool{
		proxies: make([]string, 0),
	}
}

func (p *ProxyPool) Update(proxies []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	oldCount := len(p.proxies)
	p.proxies = proxies
	// Reset index to 0 to avoid out-of-bounds issues
	atomic.StoreUint64(&p.index, 0)

	log.Printf("Proxy pool updated: %d -> %d active proxies", oldCount, len(proxies))
}

func (p *ProxyPool) GetNext() (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.proxies) == 0 {
		return "", fmt.Errorf("no available proxies")
	}

	idx := atomic.AddUint64(&p.index, 1) % uint64(len(p.proxies))
	return p.proxies[idx], nil
}

func (p *ProxyPool) GetAll() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]string, len(p.proxies))
	copy(result, p.proxies)
	return result
}

func fetchProxyList() ([]string, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // Disable certificate verification
			},
		},
	}

	allProxies := make([]string, 0)
	proxySet := make(map[string]bool) // 用于去重

	// 遍历所有配置的URL
	for _, url := range config.ProxyListURLs {
		log.Printf("Fetching proxy list from: %s", url)

		resp, err := client.Get(url)
		if err != nil {
			log.Printf("Warning: Failed to fetch from %s: %v", url, err)
			continue // 继续尝试其他URL
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Warning: Unexpected status code %d from %s", resp.StatusCode, url)
			resp.Body.Close()
			continue
		}

		scanner := bufio.NewScanner(resp.Body)
		count := 0
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			// Support formats: ip:port or socks5://ip:port
			if strings.HasPrefix(line, "socks5://") {
				line = strings.TrimPrefix(line, "socks5://")
			}

			// 去重
			if !proxySet[line] {
				proxySet[line] = true
				allProxies = append(allProxies, line)
				count++
			}
		}

		if err := scanner.Err(); err != nil {
			log.Printf("Warning: Error reading from %s: %v", url, err)
		}

		resp.Body.Close()
		log.Printf("Fetched %d proxies from %s", count, url)
	}

	if len(allProxies) == 0 {
		return nil, fmt.Errorf("no proxies fetched from any source")
	}

	log.Printf("Total unique proxies fetched: %d", len(allProxies))
	return allProxies, nil
}

func checkProxyHealth(proxyAddr string, strictMode bool) bool {
	// Create a context with timeout from config
	totalTimeout := time.Duration(config.HealthCheck.TotalTimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		return false
	}

	// Use a channel to handle timeout
	done := make(chan bool, 1)
	go func() {
		// Test HTTPS connection to verify TLS handshake works and is fast
		start := time.Now()

		conn, err := dialer.Dial("tcp", "www.google.com:443")
		if err != nil {
			done <- false
			return
		}
		defer conn.Close()

		// Perform TLS handshake to test SSL performance
		tlsConn := tls.Client(conn, &tls.Config{
			ServerName:         "www.google.com",
			InsecureSkipVerify: !strictMode, // Strict mode: verify certificate
		})

		err = tlsConn.Handshake()
		if err != nil {
			done <- false
			return
		}
		tlsConn.Close()

		// Check if TLS handshake was fast enough (from config)
		elapsed := time.Since(start)
		threshold := time.Duration(config.HealthCheck.TLSHandshakeThresholdSeconds) * time.Second
		if elapsed > threshold {
			// Too slow, reject this proxy
			done <- false
			return
		}

		done <- true
	}()

	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		return false
	}
}

// HealthCheckResult holds the results of health check for both modes
type HealthCheckResult struct {
	Strict  []string
	Relaxed []string
}

func healthCheckProxies(proxies []string) HealthCheckResult {
	var wg sync.WaitGroup
	var mu sync.Mutex
	strictHealthy := make([]string, 0)
	relaxedHealthy := make([]string, 0)

	total := len(proxies)
	var checked int64
	var strictCount int64
	var relaxedCount int64

	// Use worker pool to limit concurrent checks (from config)
	semaphore := make(chan struct{}, config.HealthCheckConcurrency)

	// Progress reporter goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()

		lastChecked := int64(0)

		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				current := atomic.LoadInt64(&checked)
				strictCurrent := atomic.LoadInt64(&strictCount)
				relaxedCurrent := atomic.LoadInt64(&relaxedCount)

				// Only print if progress has changed
				if current != lastChecked {
					percentage := float64(current) / float64(total) * 100

					// Progress bar
					barWidth := 40
					filled := int(float64(barWidth) * float64(current) / float64(total))
					bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

					log.Printf("[%s] %d/%d (%.1f%%) | Strict: %d | Relaxed: %d",
						bar, current, total, percentage, strictCurrent, relaxedCurrent)

					lastChecked = current
				}
			}
		}
	}()

	for _, proxyAddr := range proxies {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// Optimized: check strict mode first
			strictOK := checkProxyHealth(addr, true)

			if strictOK {
				// If strict mode passes, relaxed mode must pass too
				mu.Lock()
				strictHealthy = append(strictHealthy, addr)
				relaxedHealthy = append(relaxedHealthy, addr)
				mu.Unlock()
				atomic.AddInt64(&strictCount, 1)
				atomic.AddInt64(&relaxedCount, 1)
			} else {
				// Strict mode failed, try relaxed mode
				relaxedOK := checkProxyHealth(addr, false)
				if relaxedOK {
					mu.Lock()
					relaxedHealthy = append(relaxedHealthy, addr)
					mu.Unlock()
					atomic.AddInt64(&relaxedCount, 1)
				}
			}
			atomic.AddInt64(&checked, 1)
		}(proxyAddr)
	}

	wg.Wait()
	close(done)

	// Final progress update
	log.Printf("[%s] %d/%d (100.0%%) | Strict: %d | Relaxed: %d",
		strings.Repeat("█", 40), total, total, len(strictHealthy), len(relaxedHealthy))

	return HealthCheckResult{
		Strict:  strictHealthy,
		Relaxed: relaxedHealthy,
	}
}

func updateProxyPool(strictPool *ProxyPool, relaxedPool *ProxyPool) {
	// Check if an update is already in progress
	if !atomic.CompareAndSwapInt32(&strictPool.updating, 0, 1) {
		log.Println("Proxy update already in progress, skipping...")
		return
	}
	defer atomic.StoreInt32(&strictPool.updating, 0)

	log.Println("Fetching proxy list...")
	proxies, err := fetchProxyList()
	if err != nil {
		log.Printf("Error fetching proxy list: %v", err)
		return
	}

	log.Printf("Fetched %d proxies, starting health check...", len(proxies))
	result := healthCheckProxies(proxies)

	// Update strict pool
	if len(result.Strict) > 0 {
		strictPool.Update(result.Strict)
		log.Printf("[STRICT] Pool updated with %d healthy proxies", len(result.Strict))
	} else {
		log.Println("[STRICT] Warning: No healthy proxies found, keeping existing pool")
	}

	// Update relaxed pool
	if len(result.Relaxed) > 0 {
		relaxedPool.Update(result.Relaxed)
		log.Printf("[RELAXED] Pool updated with %d healthy proxies", len(result.Relaxed))
	} else {
		log.Println("[RELAXED] Warning: No healthy proxies found, keeping existing pool")
	}
}

func startProxyUpdater(strictPool *ProxyPool, relaxedPool *ProxyPool, initialSync bool) {
	if initialSync {
		// Initial update synchronously to ensure we have proxies before starting servers
		log.Println("Performing initial proxy update...")
		updateProxyPool(strictPool, relaxedPool)
	}

	// Periodic updates - each update runs in its own goroutine to avoid blocking
	updateInterval := time.Duration(config.UpdateIntervalMinutes) * time.Minute
	ticker := time.NewTicker(updateInterval)
	go func() {
		for range ticker.C {
			go updateProxyPool(strictPool, relaxedPool)
		}
	}()
}

// SOCKS5 Proxy Server
type CustomDialer struct {
	pool *ProxyPool
	mode string // "STRICT" or "RELAXED"
}

// LoggedConn wraps a net.Conn to log when it's closed
type LoggedConn struct {
	net.Conn
	addr       string
	proxyAddr  string
	closed     bool
	bytesRead  int64
	bytesWrite int64
}

func (c *LoggedConn) Close() error {
	if !c.closed {
		c.closed = true
		log.Printf("[SOCKS5] Connection closed: %s via proxy %s (read: %d bytes, wrote: %d bytes)",
			c.addr, c.proxyAddr, c.bytesRead, c.bytesWrite)
	}
	return c.Conn.Close()
}

func (c *LoggedConn) Read(b []byte) (n int, err error) {
	n, err = c.Conn.Read(b)
	if n > 0 {
		atomic.AddInt64(&c.bytesRead, int64(n))
	}
	if err != nil && err != io.EOF {
		log.Printf("[SOCKS5] Read error for %s via proxy %s after %d bytes: %v",
			c.addr, c.proxyAddr, c.bytesRead, err)
	}
	return n, err
}

func (c *LoggedConn) Write(b []byte) (n int, err error) {
	n, err = c.Conn.Write(b)
	if n > 0 {
		atomic.AddInt64(&c.bytesWrite, int64(n))
	}
	if err != nil {
		log.Printf("[SOCKS5] Write error for %s via proxy %s after %d bytes: %v",
			c.addr, c.proxyAddr, c.bytesWrite, err)
	}
	return n, err
}

func (d *CustomDialer) Dial(ctx context.Context, network, addr string) (net.Conn, error) {
	log.Printf("[SOCKS5-%s] Incoming request: %s -> %s", d.mode, network, addr)

	proxyAddr, err := d.pool.GetNext()
	if err != nil {
		log.Printf("[SOCKS5-%s] ERROR: No proxy available for %s: %v", d.mode, addr, err)
		return nil, err
	}

	log.Printf("[SOCKS5-%s] Using proxy %s for %s", d.mode, proxyAddr, addr)

	dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		log.Printf("[SOCKS5-%s] ERROR: Failed to create dialer for proxy %s: %v", d.mode, proxyAddr, err)
		return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
	}

	conn, err := dialer.Dial(network, addr)
	if err != nil {
		log.Printf("[SOCKS5-%s] ERROR: Failed to connect to %s via proxy %s: %v", d.mode, addr, proxyAddr, err)
		return nil, fmt.Errorf("failed to dial through proxy %s: %w", proxyAddr, err)
	}

	log.Printf("[SOCKS5-%s] SUCCESS: Connected to %s via proxy %s", d.mode, addr, proxyAddr)

	// Wrap the connection to log read/write errors and close events
	loggedConn := &LoggedConn{
		Conn:      conn,
		addr:      addr,
		proxyAddr: proxyAddr,
		closed:    false,
	}

	return loggedConn, nil
}

func startSOCKS5Server(pool *ProxyPool, port string, mode string) error {
	// Create a custom logger with mode-specific prefix
	socks5Logger := log.New(log.Writer(), fmt.Sprintf("[SOCKS5-%s-LIB] ", mode), log.LstdFlags)

	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &CustomDialer{pool: pool, mode: mode}
			return dialer.Dial(ctx, network, addr)
		},
		Logger: socks5Logger,
	}

	server, err := socks5.New(conf)
	if err != nil {
		return fmt.Errorf("failed to create SOCKS5 server: %w", err)
	}

	log.Printf("[%s] SOCKS5 proxy server listening on %s", mode, port)
	return server.ListenAndServe("tcp", port)
}

// HTTP Proxy Server
func handleHTTPProxy(w http.ResponseWriter, r *http.Request, pool *ProxyPool, mode string) {
	log.Printf("[HTTP-%s] Incoming request: %s %s from %s", mode, r.Method, r.URL.String(), r.RemoteAddr)

	proxyAddr, err := pool.GetNext()
	if err != nil {
		log.Printf("[HTTP-%s] ERROR: No proxy available for %s %s: %v", mode, r.Method, r.URL.String(), err)
		http.Error(w, "No available proxies", http.StatusServiceUnavailable)
		return
	}

	log.Printf("[HTTP-%s] Using proxy %s for %s %s", mode, proxyAddr, r.Method, r.URL.String())

	// Create SOCKS5 dialer
	dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		log.Printf("[HTTP-%s] ERROR: Failed to create dialer for proxy %s: %v", mode, proxyAddr, err)
		http.Error(w, "Failed to create proxy dialer", http.StatusInternalServerError)
		return
	}

	// Handle CONNECT method for HTTPS
	if r.Method == http.MethodConnect {
		handleHTTPSProxy(w, r, dialer, proxyAddr, mode)
		return
	}

	// Handle regular HTTP requests
	transport := &http.Transport{
		Dial: dialer.Dial,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Disable certificate verification
		},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   30 * time.Second,
	}

	// Create new request
	proxyReq, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
	if err != nil {
		http.Error(w, "Failed to create proxy request", http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Send request
	resp, err := client.Do(proxyReq)
	if err != nil {
		log.Printf("[HTTP-%s] ERROR: Request failed for %s: %v", mode, r.URL.String(), err)
		http.Error(w, fmt.Sprintf("Proxy request failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	log.Printf("[HTTP-%s] SUCCESS: Got response %d for %s", mode, resp.StatusCode, r.URL.String())

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func handleHTTPSProxy(w http.ResponseWriter, r *http.Request, dialer proxy.Dialer, proxyAddr string, mode string) {
	log.Printf("[HTTPS-%s] Connecting to %s via proxy %s", mode, r.Host, proxyAddr)

	// Connect to target through SOCKS5 proxy
	targetConn, err := dialer.Dial("tcp", r.Host)
	if err != nil {
		log.Printf("[HTTPS-%s] ERROR: Failed to connect to %s via proxy %s: %v", mode, r.Host, proxyAddr, err)
		http.Error(w, "Failed to connect to target", http.StatusBadGateway)
		return
	}
	defer targetConn.Close()

	// Hijack the connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		log.Printf("[HTTPS-%s] ERROR: Hijacking not supported for %s", mode, r.Host)
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		log.Printf("[HTTPS-%s] ERROR: Failed to hijack connection for %s: %v", mode, r.Host, err)
		http.Error(w, "Failed to hijack connection", http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	// Send 200 Connection Established
	clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	log.Printf("[HTTPS-%s] SUCCESS: Tunnel established to %s via proxy %s", mode, r.Host, proxyAddr)

	// Bidirectional copy
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		io.Copy(targetConn, clientConn)
	}()

	go func() {
		defer wg.Done()
		io.Copy(clientConn, targetConn)
	}()

	wg.Wait()
}

func startHTTPServer(pool *ProxyPool, port string, mode string) error {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleHTTPProxy(w, r, pool, mode)
	})

	server := &http.Server{
		Addr:    port,
		Handler: handler,
	}

	log.Printf("[%s] HTTP proxy server listening on %s", mode, port)
	return server.ListenAndServe()
}

func main() {
	log.Println("Starting Dynamic Proxy Server...")

	// Load configuration
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	config = *cfg

	// Log configuration
	log.Printf("Configuration loaded:")
	log.Printf("  - Proxy sources: %d", len(config.ProxyListURLs))
	for i, url := range config.ProxyListURLs {
		log.Printf("    [%d] %s", i+1, url)
	}
	log.Printf("  - Health check concurrency: %d", config.HealthCheckConcurrency)
	log.Printf("  - Update interval: %d minutes", config.UpdateIntervalMinutes)
	log.Printf("  - Health check timeout: %ds (TLS threshold: %ds)",
		config.HealthCheck.TotalTimeoutSeconds,
		config.HealthCheck.TLSHandshakeThresholdSeconds)
	log.Printf("  - SOCKS5 Strict port: %s", config.Ports.SOCKS5Strict)
	log.Printf("  - SOCKS5 Relaxed port: %s", config.Ports.SOCKS5Relaxed)
	log.Printf("  - HTTP Strict port: %s", config.Ports.HTTPStrict)
	log.Printf("  - HTTP Relaxed port: %s", config.Ports.HTTPRelaxed)

	// Create two proxy pools
	strictPool := NewProxyPool()
	relaxedPool := NewProxyPool()

	// Start proxy updater with initial synchronous update
	startProxyUpdater(strictPool, relaxedPool, true)

	// Check proxy pool status
	strictCount := len(strictPool.GetAll())
	relaxedCount := len(relaxedPool.GetAll())

	if strictCount == 0 {
		log.Println("[STRICT] Warning: No healthy proxies available")
		log.Println("[STRICT] Strict mode servers will return errors until proxies become available")
	} else {
		log.Printf("[STRICT] Successfully loaded %d healthy proxies", strictCount)
	}

	if relaxedCount == 0 {
		log.Println("[RELAXED] Warning: No healthy proxies available")
		log.Println("[RELAXED] Relaxed mode servers will return errors until proxies become available")
	} else {
		log.Printf("[RELAXED] Successfully loaded %d healthy proxies", relaxedCount)
	}

	// Start servers (4 servers total)
	var wg sync.WaitGroup
	wg.Add(4)

	// SOCKS5 Strict
	go func() {
		defer wg.Done()
		if err := startSOCKS5Server(strictPool, config.Ports.SOCKS5Strict, "STRICT"); err != nil {
			log.Fatalf("[STRICT] SOCKS5 server error: %v", err)
		}
	}()

	// SOCKS5 Relaxed
	go func() {
		defer wg.Done()
		if err := startSOCKS5Server(relaxedPool, config.Ports.SOCKS5Relaxed, "RELAXED"); err != nil {
			log.Fatalf("[RELAXED] SOCKS5 server error: %v", err)
		}
	}()

	// HTTP Strict
	go func() {
		defer wg.Done()
		if err := startHTTPServer(strictPool, config.Ports.HTTPStrict, "STRICT"); err != nil {
			log.Fatalf("[STRICT] HTTP server error: %v", err)
		}
	}()

	// HTTP Relaxed
	go func() {
		defer wg.Done()
		if err := startHTTPServer(relaxedPool, config.Ports.HTTPRelaxed, "RELAXED"); err != nil {
			log.Fatalf("[RELAXED] HTTP server error: %v", err)
		}
	}()

	log.Println("All servers started successfully")
	log.Println("  [STRICT] SOCKS5: " + config.Ports.SOCKS5Strict + " | HTTP: " + config.Ports.HTTPStrict)
	log.Println("  [RELAXED] SOCKS5: " + config.Ports.SOCKS5Relaxed + " | HTTP: " + config.Ports.HTTPRelaxed)
	log.Printf("Proxy pools will update every %d minutes in background...", config.UpdateIntervalMinutes)
	wg.Wait()
}
