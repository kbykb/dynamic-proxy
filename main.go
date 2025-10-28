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
		SOCKS5 string `yaml:"socks5"`
		HTTP   string `yaml:"http"`
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
	if cfg.Ports.SOCKS5 == "" {
		cfg.Ports.SOCKS5 = ":1080"
	}
	if cfg.Ports.HTTP == "" {
		cfg.Ports.HTTP = ":8080"
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

func checkProxyHealth(proxyAddr string) bool {
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
			InsecureSkipVerify: true,
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

func healthCheckProxies(proxies []string) []string {
	var wg sync.WaitGroup
	var mu sync.Mutex
	healthy := make([]string, 0)

	total := len(proxies)
	var checked int64
	var healthyCount int64

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
				healthyCurrent := atomic.LoadInt64(&healthyCount)

				// Only print if progress has changed
				if current != lastChecked {
					percentage := float64(current) / float64(total) * 100

					// Progress bar
					barWidth := 40
					filled := int(float64(barWidth) * float64(current) / float64(total))
					bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)

					log.Printf("[%s] %d/%d (%.1f%%) | Healthy: %d",
						bar, current, total, percentage, healthyCurrent)

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

			if checkProxyHealth(addr) {
				mu.Lock()
				healthy = append(healthy, addr)
				mu.Unlock()
				atomic.AddInt64(&healthyCount, 1)
			}
			atomic.AddInt64(&checked, 1)
		}(proxyAddr)
	}

	wg.Wait()
	close(done)

	// Final progress update
	log.Printf("[%s] %d/%d (100.0%%) | Healthy: %d",
		strings.Repeat("█", 40), total, total, len(healthy))

	return healthy
}

func updateProxyPool(pool *ProxyPool) {
	// Check if an update is already in progress
	if !atomic.CompareAndSwapInt32(&pool.updating, 0, 1) {
		log.Println("Proxy update already in progress, skipping...")
		return
	}
	defer atomic.StoreInt32(&pool.updating, 0)

	log.Println("Fetching proxy list...")
	proxies, err := fetchProxyList()
	if err != nil {
		log.Printf("Error fetching proxy list: %v", err)
		return
	}

	log.Printf("Fetched %d proxies, starting health check...", len(proxies))
	healthy := healthCheckProxies(proxies)

	if len(healthy) > 0 {
		pool.Update(healthy)
	} else {
		log.Println("Warning: No healthy proxies found, keeping existing pool")
	}
}

func startProxyUpdater(pool *ProxyPool, initialSync bool) {
	if initialSync {
		// Initial update synchronously to ensure we have proxies before starting servers
		log.Println("Performing initial proxy update...")
		updateProxyPool(pool)
	}

	// Periodic updates - each update runs in its own goroutine to avoid blocking
	updateInterval := time.Duration(config.UpdateIntervalMinutes) * time.Minute
	ticker := time.NewTicker(updateInterval)
	go func() {
		for range ticker.C {
			go updateProxyPool(pool)
		}
	}()
}

// SOCKS5 Proxy Server
type CustomDialer struct {
	pool *ProxyPool
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
	log.Printf("[SOCKS5] Incoming request: %s -> %s", network, addr)

	proxyAddr, err := d.pool.GetNext()
	if err != nil {
		log.Printf("[SOCKS5] ERROR: No proxy available for %s: %v", addr, err)
		return nil, err
	}

	log.Printf("[SOCKS5] Using proxy %s for %s", proxyAddr, addr)

	dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		log.Printf("[SOCKS5] ERROR: Failed to create dialer for proxy %s: %v", proxyAddr, err)
		return nil, fmt.Errorf("failed to create SOCKS5 dialer: %w", err)
	}

	conn, err := dialer.Dial(network, addr)
	if err != nil {
		log.Printf("[SOCKS5] ERROR: Failed to connect to %s via proxy %s: %v", addr, proxyAddr, err)
		return nil, fmt.Errorf("failed to dial through proxy %s: %w", proxyAddr, err)
	}

	log.Printf("[SOCKS5] SUCCESS: Connected to %s via proxy %s", addr, proxyAddr)

	// Wrap the connection to log read/write errors and close events
	loggedConn := &LoggedConn{
		Conn:      conn,
		addr:      addr,
		proxyAddr: proxyAddr,
		closed:    false,
	}

	return loggedConn, nil
}

func startSOCKS5Server(pool *ProxyPool, port string) error {
	// Create a custom logger with [SOCKS5-LIB] prefix
	socks5Logger := log.New(log.Writer(), "[SOCKS5-LIB] ", log.LstdFlags)

	conf := &socks5.Config{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dialer := &CustomDialer{pool: pool}
			return dialer.Dial(ctx, network, addr)
		},
		Logger: socks5Logger,
	}

	server, err := socks5.New(conf)
	if err != nil {
		return fmt.Errorf("failed to create SOCKS5 server: %w", err)
	}

	log.Printf("SOCKS5 proxy server listening on %s", port)
	return server.ListenAndServe("tcp", port)
}

// HTTP Proxy Server
func handleHTTPProxy(w http.ResponseWriter, r *http.Request, pool *ProxyPool) {
	log.Printf("[HTTP] Incoming request: %s %s from %s", r.Method, r.URL.String(), r.RemoteAddr)

	proxyAddr, err := pool.GetNext()
	if err != nil {
		log.Printf("[HTTP] ERROR: No proxy available for %s %s: %v", r.Method, r.URL.String(), err)
		http.Error(w, "No available proxies", http.StatusServiceUnavailable)
		return
	}

	log.Printf("[HTTP] Using proxy %s for %s %s", proxyAddr, r.Method, r.URL.String())

	// Create SOCKS5 dialer
	dialer, err := proxy.SOCKS5("tcp", proxyAddr, nil, proxy.Direct)
	if err != nil {
		log.Printf("[HTTP] ERROR: Failed to create dialer for proxy %s: %v", proxyAddr, err)
		http.Error(w, "Failed to create proxy dialer", http.StatusInternalServerError)
		return
	}

	// Handle CONNECT method for HTTPS
	if r.Method == http.MethodConnect {
		handleHTTPSProxy(w, r, dialer, proxyAddr)
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
		log.Printf("[HTTP] ERROR: Request failed for %s: %v", r.URL.String(), err)
		http.Error(w, fmt.Sprintf("Proxy request failed: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	log.Printf("[HTTP] SUCCESS: Got response %d for %s", resp.StatusCode, r.URL.String())

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

func handleHTTPSProxy(w http.ResponseWriter, r *http.Request, dialer proxy.Dialer, proxyAddr string) {
	log.Printf("[HTTPS] Connecting to %s via proxy %s", r.Host, proxyAddr)

	// Connect to target through SOCKS5 proxy
	targetConn, err := dialer.Dial("tcp", r.Host)
	if err != nil {
		log.Printf("[HTTPS] ERROR: Failed to connect to %s via proxy %s: %v", r.Host, proxyAddr, err)
		http.Error(w, "Failed to connect to target", http.StatusBadGateway)
		return
	}
	defer targetConn.Close()

	// Hijack the connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		log.Printf("[HTTPS] ERROR: Hijacking not supported for %s", r.Host)
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}

	clientConn, _, err := hijacker.Hijack()
	if err != nil {
		log.Printf("[HTTPS] ERROR: Failed to hijack connection for %s: %v", r.Host, err)
		http.Error(w, "Failed to hijack connection", http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	// Send 200 Connection Established
	clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))
	log.Printf("[HTTPS] SUCCESS: Tunnel established to %s via proxy %s", r.Host, proxyAddr)

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

func startHTTPServer(pool *ProxyPool, port string) error {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleHTTPProxy(w, r, pool)
	})

	server := &http.Server{
		Addr:    port,
		Handler: handler,
	}

	log.Printf("HTTP proxy server listening on %s", port)
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
	log.Printf("  - SOCKS5 port: %s", config.Ports.SOCKS5)
	log.Printf("  - HTTP port: %s", config.Ports.HTTP)

	pool := NewProxyPool()

	// Start proxy updater with initial synchronous update
	startProxyUpdater(pool, true)

	// Check if we have any proxies
	if len(pool.GetAll()) == 0 {
		log.Println("Warning: No healthy proxies available, but starting servers anyway...")
		log.Println("Servers will return errors until proxies become available")
	} else {
		log.Printf("Successfully loaded %d healthy proxies", len(pool.GetAll()))
	}

	// Start servers
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := startSOCKS5Server(pool, config.Ports.SOCKS5); err != nil {
			log.Fatalf("SOCKS5 server error: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := startHTTPServer(pool, config.Ports.HTTP); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	log.Println("All servers started successfully")
	log.Println("  - SOCKS5 proxy: " + config.Ports.SOCKS5)
	log.Println("  - HTTP proxy: " + config.Ports.HTTP)
	log.Printf("Proxy pool will update every %d minutes in background...", config.UpdateIntervalMinutes)
	wg.Wait()
}
