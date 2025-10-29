# Dynamic Proxy Server

[English](#english) | [中文](#中文)

---

## English

A high-performance SOCKS5/HTTP dynamic proxy server that automatically fetches, health-checks, and rotates through proxy lists.

### Features

- 🚀 **Multi-source Support**: Fetch proxies from multiple URLs simultaneously
- 🔄 **Auto Rotation**: Round-robin algorithm for automatic proxy switching
- 💪 **High Concurrency**: Configurable concurrent health checks (default: 200)
- ⚡ **Fast Health Check**: TLS handshake verification with performance filtering
- 🔧 **Flexible Configuration**: YAML-based configuration file
- 🌐 **Dual Protocol**: SOCKS5 and HTTP proxy servers
- 🔒 **HTTPS Support**: Full CONNECT tunnel support
- 📊 **Real-time Progress**: Live progress bar during health checks
- 🎯 **Smart Filtering**: Automatically removes slow and unreliable proxies
- 🔁 **Auto Update**: Periodic proxy pool refresh (configurable interval)
- 🔐 **Dual Mode**: Strict mode (SSL verification enabled) and Relaxed mode (SSL verification disabled)

### Quick Start

#### Download Pre-built Binaries

Download the latest release for your platform:

- **Linux (amd64)**: `dynamic-proxy-linux-amd64`
- **macOS (Intel)**: `dynamic-proxy-darwin-amd64`
- **macOS (Apple Silicon)**: `dynamic-proxy-darwin-arm64`
- **Windows**: `dynamic-proxy.exe`

```bash
# Linux / macOS
chmod +x dynamic-proxy-linux-amd64
./dynamic-proxy-linux-amd64

# Windows
dynamic-proxy.exe
```

#### Build from Source

```bash
# Clone the repository
git clone https://github.com/kbykb/dynamic-proxy.git
cd dynamic-proxy

# Download dependencies
go mod download

# Build
go build -o dynamic-proxy

# Run
./dynamic-proxy
```

#### Docker Deployment

**Using Docker:**

```bash
# Build the image
docker build -t dynamic-proxy .

# Run the container
docker run -d \
  --name dynamic-proxy \
  -p 17283:17283 \
  -p 17284:17284 \
  -p 17285:17285 \
  -p 17286:17286 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  --restart unless-stopped \
  dynamic-proxy
```

**Using Docker Compose:**

```bash
# Start the service
docker-compose up -d

# View logs
docker-compose logs -f

# Stop the service
docker-compose down
```

**Docker Configuration:**

The Docker image is built using multi-stage builds for minimal size:
- Base image: Alpine Linux
- Includes CA certificates for HTTPS
- Exposes ports:
  - 17283 (SOCKS5 Strict - SSL verification enabled)
  - 17284 (SOCKS5 Relaxed - SSL verification disabled)
  - 17285 (HTTP Strict - SSL verification enabled)
  - 17286 (HTTP Relaxed - SSL verification disabled)
- Config file can be mounted as a volume for easy updates

### Configuration

Edit `config.yaml` to customize settings:

```yaml
# Proxy list URLs (supports multiple sources)
proxy_list_urls:
  - "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks5.txt"
  - "https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/main/socks5/raw/all.txt"
  # Add more sources
  # - "https://example.com/proxy-list.txt"

# Health check concurrency (simultaneous tests)
health_check_concurrency: 200

# Update interval (minutes)
update_interval_minutes: 5

# Health check timeout settings
health_check:
  total_timeout_seconds: 8              # Total timeout
  tls_handshake_threshold_seconds: 5    # TLS handshake threshold

# Server ports
ports:
  socks5_strict: ":17283"    # SOCKS5 with SSL verification
  socks5_relaxed: ":17284"   # SOCKS5 without SSL verification
  http_strict: ":17285"      # HTTP with SSL verification
  http_relaxed: ":17286"     # HTTP without SSL verification
```

#### Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `proxy_list_urls` | List of proxy source URLs | 2 sources |
| `health_check_concurrency` | Concurrent health checks | 200 |
| `update_interval_minutes` | Proxy pool refresh interval | 5 minutes |
| `total_timeout_seconds` | Health check total timeout | 8 seconds |
| `tls_handshake_threshold_seconds` | Max TLS handshake time | 5 seconds |
| `ports.socks5_strict` | SOCKS5 server port (SSL verification enabled) | :17283 |
| `ports.socks5_relaxed` | SOCKS5 server port (SSL verification disabled) | :17284 |
| `ports.http_strict` | HTTP proxy server port (SSL verification enabled) | :17285 |
| `ports.http_relaxed` | HTTP proxy server port (SSL verification disabled) | :17286 |

### Usage

#### Command Line

```bash
# Test with curl (SOCKS5 Strict - SSL verification enabled)
curl --socks5 127.0.0.1:17283 https://api.ipify.org

# Test with curl (SOCKS5 Relaxed - SSL verification disabled)
curl --socks5 127.0.0.1:17284 https://api.ipify.org

# Test with curl (HTTP Strict - SSL verification enabled)
curl -x http://127.0.0.1:17285 https://api.ipify.org

# Test with curl (HTTP Relaxed - SSL verification disabled)
curl -x http://127.0.0.1:17286 https://api.ipify.org
```

#### Browser Configuration

**SOCKS5 Proxy (Strict Mode - Recommended):**
- Host: `127.0.0.1`
- Port: `17283`

**SOCKS5 Proxy (Relaxed Mode - For compatibility):**
- Host: `127.0.0.1`
- Port: `17284`

**HTTP Proxy (Strict Mode - Recommended):**
- Host: `127.0.0.1`
- Port: `17285`

**HTTP Proxy (Relaxed Mode - For compatibility):**
- Host: `127.0.0.1`
- Port: `17286`

#### Programming Examples

**Python:**

```python
import requests

# HTTP Proxy (Strict Mode - Recommended)
proxies = {
    'http': 'http://127.0.0.1:17285',
    'https': 'http://127.0.0.1:17285'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)

# HTTP Proxy (Relaxed Mode - For compatibility)
proxies = {
    'http': 'http://127.0.0.1:17286',
    'https': 'http://127.0.0.1:17286'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)

# SOCKS5 Proxy (Strict Mode - Recommended)
proxies = {
    'http': 'socks5://127.0.0.1:17283',
    'https': 'socks5://127.0.0.1:17283'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)

# SOCKS5 Proxy (Relaxed Mode - For compatibility)
proxies = {
    'http': 'socks5://127.0.0.1:17284',
    'https': 'socks5://127.0.0.1:17284'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)
```

**Node.js:**

```javascript
const axios = require('axios');
const { SocksProxyAgent } = require('socks-proxy-agent');

// SOCKS5 Proxy (Strict Mode - Recommended)
const strictAgent = new SocksProxyAgent('socks5://127.0.0.1:17283');
axios.get('https://api.ipify.org', { httpAgent: strictAgent, httpsAgent: strictAgent })
  .then(response => console.log(response.data));

// SOCKS5 Proxy (Relaxed Mode - For compatibility)
const relaxedAgent = new SocksProxyAgent('socks5://127.0.0.1:17284');
axios.get('https://api.ipify.org', { httpAgent: relaxedAgent, httpsAgent: relaxedAgent })
  .then(response => console.log(response.data));

// HTTP Proxy (Strict Mode - Recommended)
axios.get('https://api.ipify.org', {
  proxy: {
    host: '127.0.0.1',
    port: 17285
  }
}).then(response => console.log(response.data));

// HTTP Proxy (Relaxed Mode - For compatibility)
axios.get('https://api.ipify.org', {
  proxy: {
    host: '127.0.0.1',
    port: 17286
  }
}).then(response => console.log(response.data));
```

### How It Works

1. **Proxy Fetching**: Fetches proxy lists from configured URLs at startup
2. **Health Check**: Concurrent health checks with TLS handshake verification
   - **Strict Mode**: Tests with SSL certificate verification enabled
   - **Relaxed Mode**: Tests with SSL certificate verification disabled
   - **Optimization**: If a proxy passes strict mode, it's automatically added to both pools
3. **Dual Proxy Pools**: Maintains two separate pools (strict and relaxed) of healthy, fast proxies
4. **Auto Update**: Refreshes both proxy pools at configured intervals
5. **Round-Robin**: Distributes requests across proxies using round-robin algorithm
6. **Dual Protocol**: Serves both SOCKS5 and HTTP proxy protocols in both modes (4 servers total)

### Architecture

```
┌─────────────────┐
│  Proxy Sources  │
│  (Multiple URLs)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Fetch & Merge  │
│  (Deduplication)│
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│         Health Check                │
│        (200 concurrent)             │
│  ┌──────────────┐  ┌──────────────┐│
│  │ Strict Mode  │  │Relaxed Mode  ││
│  │ (SSL verify) │  │(No SSL verify)││
│  │ - TCP Connect│  │- TCP Connect ││
│  │ - TLS + Cert │  │- TLS Only    ││
│  │ - Speed Test │  │- Speed Test  ││
│  └──────────────┘  └──────────────┘│
└────────┬────────────────┬───────────┘
         │                │
         ▼                ▼
┌─────────────────┐ ┌─────────────────┐
│  Strict Pool    │ │  Relaxed Pool   │
│(SSL Verified)   │ │(More Compatible)│
└────────┬────────┘ └────────┬────────┘
         │                   │
    ┌────┴────┐         ┌────┴────┐
    ▼         ▼         ▼         ▼
┌────────┐┌────────┐┌────────┐┌────────┐
│SOCKS5  ││  HTTP  ││SOCKS5  ││  HTTP  │
│Strict  ││ Strict ││Relaxed ││Relaxed │
│:17283  ││ :17285 ││:17284  ││ :17286 │
└────────┘└────────┘└────────┘└────────┘
```

### Performance

- **Concurrent Health Checks**: Configurable worker pool (default: 200)
- **Lock-free Rotation**: Atomic operations for proxy selection
- **Minimal Lock Contention**: RWMutex for proxy pool updates
- **Connection Reuse**: HTTP transport connection pooling
- **Fast Filtering**: Rejects proxies with TLS handshake > 5s

### Troubleshooting

**Issue: "No available proxies"**

- Check network connectivity
- Verify proxy source URLs are accessible
- Wait for health check to complete
- Check firewall settings

**Issue: Connection failures**

- Proxies may be temporarily unavailable
- Target website may block proxy IPs
- Try multiple requests to use different proxies
- Adjust `health_check_concurrency` if network is limited

**Issue: Slow performance**

- Decrease `tls_handshake_threshold_seconds` to filter slower proxies
- Increase `health_check_concurrency` for faster updates
- Add more reliable proxy sources

### Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### License

MIT License

---

## 中文

高性能的 SOCKS5/HTTP 动态代理服务器，自动从代理列表获取、测活并轮询使用代理。

### 功能特性

- 🚀 **多源支持**: 同时从多个URL获取代理
- 🔄 **自动轮换**: 轮询算法自动切换代理
- 💪 **高并发**: 可配置的并发健康检查（默认200）
- ⚡ **快速测活**: TLS握手验证和性能过滤
- 🔧 **灵活配置**: 基于YAML的配置文件
- 🌐 **双协议**: SOCKS5和HTTP代理服务器
- 🔒 **HTTPS支持**: 完整的CONNECT隧道支持
- 📊 **实时进度**: 健康检查时的实时进度条
- 🎯 **智能过滤**: 自动移除慢速和不可靠的代理
- 🔁 **自动更新**: 定期刷新代理池（可配置间隔）
- 🔐 **双模式**: 严格模式（启用SSL验证）和宽松模式（禁用SSL验证）

### 快速开始

#### 下载预编译版本

下载适合您平台的最新版本：

- **Linux (amd64)**: `dynamic-proxy-linux-amd64`
- **macOS (Intel)**: `dynamic-proxy-darwin-amd64`
- **macOS (Apple Silicon)**: `dynamic-proxy-darwin-arm64`
- **Windows**: `dynamic-proxy.exe`

```bash
# Linux / macOS
chmod +x dynamic-proxy-linux-amd64
./dynamic-proxy-linux-amd64

# Windows
dynamic-proxy.exe
```

#### 从源码编译

```bash
# 克隆仓库
git clone https://github.com/kbykb/dynamic-proxy.git
cd dynamic-proxy

# 下载依赖
go mod download

# 编译
go build -o dynamic-proxy

# 运行
./dynamic-proxy
```

#### Docker 部署

**使用 Docker:**

```bash
# 构建镜像
docker build -t dynamic-proxy .

# 运行容器
docker run -d \
  --name dynamic-proxy \
  -p 17283:17283 \
  -p 17284:17284 \
  -p 17285:17285 \
  -p 17286:17286 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  --restart unless-stopped \
  dynamic-proxy
```

**使用 Docker Compose:**

```bash
# 启动服务
docker-compose up -d

# 查看日志
docker-compose logs -f

# 停止服务
docker-compose down
```

**Docker 配置说明:**

Docker 镜像使用多阶段构建，体积最小化：
- 基础镜像: Alpine Linux
- 包含 CA 证书支持 HTTPS
- 暴露端口:
  - 17283 (SOCKS5 严格模式 - 启用SSL验证)
  - 17284 (SOCKS5 宽松模式 - 禁用SSL验证)
  - 17285 (HTTP 严格模式 - 启用SSL验证)
  - 17286 (HTTP 宽松模式 - 禁用SSL验证)
- 配置文件可通过卷挂载，方便更新

### 配置说明

编辑 `config.yaml` 自定义设置：

```yaml
# 代理列表URL（支持多个源）
proxy_list_urls:
  - "https://raw.githubusercontent.com/r00tee/Proxy-List/main/Socks5.txt"
  - "https://raw.githubusercontent.com/ClearProxy/checked-proxy-list/main/socks5/raw/all.txt"
  # 添加更多源
  # - "https://example.com/proxy-list.txt"

# 健康检查并发数（同时测试数量）
health_check_concurrency: 200

# 更新间隔（分钟）
update_interval_minutes: 5

# 健康检查超时设置
health_check:
  total_timeout_seconds: 8              # 总超时时间
  tls_handshake_threshold_seconds: 5    # TLS握手阈值

# 服务器端口
ports:
  socks5_strict: ":17283"    # SOCKS5 严格模式（启用SSL验证）
  socks5_relaxed: ":17284"   # SOCKS5 宽松模式（禁用SSL验证）
  http_strict: ":17285"      # HTTP 严格模式（启用SSL验证）
  http_relaxed: ":17286"     # HTTP 宽松模式（禁用SSL验证）
```

#### 配置选项

| 选项 | 说明 | 默认值 |
|------|------|--------|
| `proxy_list_urls` | 代理源URL列表 | 2个源 |
| `health_check_concurrency` | 并发健康检查数 | 200 |
| `update_interval_minutes` | 代理池刷新间隔 | 5分钟 |
| `total_timeout_seconds` | 健康检查总超时 | 8秒 |
| `tls_handshake_threshold_seconds` | 最大TLS握手时间 | 5秒 |
| `ports.socks5_strict` | SOCKS5服务器端口（启用SSL验证） | :17283 |
| `ports.socks5_relaxed` | SOCKS5服务器端口（禁用SSL验证） | :17284 |
| `ports.http_strict` | HTTP代理服务器端口（启用SSL验证） | :17285 |
| `ports.http_relaxed` | HTTP代理服务器端口（禁用SSL验证） | :17286 |

### 使用方法

#### 命令行

```bash
# 使用curl测试（SOCKS5 严格模式 - 启用SSL验证）
curl --socks5 127.0.0.1:17283 https://api.ipify.org

# 使用curl测试（SOCKS5 宽松模式 - 禁用SSL验证）
curl --socks5 127.0.0.1:17284 https://api.ipify.org

# 使用curl测试（HTTP 严格模式 - 启用SSL验证）
curl -x http://127.0.0.1:17285 https://api.ipify.org

# 使用curl测试（HTTP 宽松模式 - 禁用SSL验证）
curl -x http://127.0.0.1:17286 https://api.ipify.org
```

#### 浏览器配置

**SOCKS5代理（严格模式 - 推荐）：**
- 主机: `127.0.0.1`
- 端口: `17283`

**SOCKS5代理（宽松模式 - 兼容性）：**
- 主机: `127.0.0.1`
- 端口: `17284`

**HTTP代理（严格模式 - 推荐）：**
- 主机: `127.0.0.1`
- 端口: `17285`

**HTTP代理（宽松模式 - 兼容性）：**
- 主机: `127.0.0.1`
- 端口: `17286`

#### 编程示例

**Python:**

```python
import requests

# HTTP代理（严格模式 - 推荐）
proxies = {
    'http': 'http://127.0.0.1:17285',
    'https': 'http://127.0.0.1:17285'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)

# HTTP代理（宽松模式 - 兼容性）
proxies = {
    'http': 'http://127.0.0.1:17286',
    'https': 'http://127.0.0.1:17286'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)

# SOCKS5代理（严格模式 - 推荐）
proxies = {
    'http': 'socks5://127.0.0.1:17283',
    'https': 'socks5://127.0.0.1:17283'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)

# SOCKS5代理（宽松模式 - 兼容性）
proxies = {
    'http': 'socks5://127.0.0.1:17284',
    'https': 'socks5://127.0.0.1:17284'
}
response = requests.get('https://api.ipify.org', proxies=proxies)
print(response.text)
```

**Node.js:**

```javascript
const axios = require('axios');
const { SocksProxyAgent } = require('socks-proxy-agent');

// SOCKS5代理（严格模式 - 推荐）
const strictAgent = new SocksProxyAgent('socks5://127.0.0.1:17283');
axios.get('https://api.ipify.org', { httpAgent: strictAgent, httpsAgent: strictAgent })
  .then(response => console.log(response.data));

// SOCKS5代理（宽松模式 - 兼容性）
const relaxedAgent = new SocksProxyAgent('socks5://127.0.0.1:17284');
axios.get('https://api.ipify.org', { httpAgent: relaxedAgent, httpsAgent: relaxedAgent })
  .then(response => console.log(response.data));

// HTTP代理（严格模式 - 推荐）
axios.get('https://api.ipify.org', {
  proxy: {
    host: '127.0.0.1',
    port: 17285
  }
}).then(response => console.log(response.data));

// HTTP代理（宽松模式 - 兼容性）
axios.get('https://api.ipify.org', {
  proxy: {
    host: '127.0.0.1',
    port: 17286
  }
}).then(response => console.log(response.data));
```

### 工作原理

1. **代理获取**: 启动时从配置的URL获取代理列表
2. **健康检查**: 并发健康检查，包含TLS握手验证
   - **严格模式**: 启用SSL证书验证进行测试
   - **宽松模式**: 禁用SSL证书验证进行测试
   - **优化策略**: 如果代理通过严格模式测试，自动添加到两个池
3. **双代理池**: 维护两个独立的代理池（严格和宽松）
4. **自动更新**: 按配置间隔刷新两个代理池
5. **轮询分配**: 使用轮询算法分配请求到代理
6. **双协议**: 同时提供SOCKS5和HTTP代理协议，每种协议都有两种模式（共4个服务器）

### 架构图

```
┌─────────────────┐
│   代理源列表    │
│  (支持多个URL)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  获取并合并     │
│   (自动去重)    │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────┐
│          健康检查                   │
│         (200并发)                   │
│  ┌──────────────┐  ┌──────────────┐│
│  │  严格模式    │  │  宽松模式    ││
│  │ (SSL验证)    │  │(无SSL验证)   ││
│  │ - TCP连接    │  │- TCP连接     ││
│  │ - TLS+证书   │  │- 仅TLS       ││
│  │ - 速度测试   │  │- 速度测试    ││
│  └──────────────┘  └──────────────┘│
└────────┬────────────────┬───────────┘
         │                │
         ▼                ▼
┌─────────────────┐ ┌─────────────────┐
│   严格代理池    │ │   宽松代理池    │
│  (SSL已验证)    │ │  (更高兼容性)   │
└────────┬────────┘ └────────┬────────┘
         │                   │
    ┌────┴────┐         ┌────┴────┐
    ▼         ▼         ▼         ▼
┌────────┐┌────────┐┌────────┐┌────────┐
│SOCKS5  ││  HTTP  ││SOCKS5  ││  HTTP  │
│严格    ││ 严格   ││宽松    ││ 宽松   │
│:17283  ││ :17285 ││:17284  ││ :17286 │
└────────┘└────────┘└────────┘└────────┘
```

### 性能特性

- **并发健康检查**: 可配置的工作池（默认200）
- **无锁轮换**: 原子操作实现代理选择
- **最小锁竞争**: 读写锁保护代理池更新
- **连接复用**: HTTP传输连接池
- **快速过滤**: 拒绝TLS握手>5秒的代理

### 故障排除

**问题："No available proxies"**

- 检查网络连接
- 验证代理源URL可访问
- 等待健康检查完成
- 检查防火墙设置

**问题：连接失败**

- 代理可能暂时不可用
- 目标网站可能屏蔽代理IP
- 尝试多次请求使用不同代理
- 如果网络受限，调整 `health_check_concurrency`

**问题：性能慢**

- 降低 `tls_handshake_threshold_seconds` 过滤慢速代理
- 增加 `health_check_concurrency` 加快更新
- 添加更可靠的代理源

### 贡献

欢迎贡献！请随时提交Pull Request。

### 许可证

MIT License
