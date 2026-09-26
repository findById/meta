# Meta

[English](README.en.md)

Meta 是一个自托管 MQTT broker 和多协议网关。当前支持 MQTT 3.1.1、MQTT 5.0、MQTT over WebSocket、轻量级独立管理服务、基于 API 的认证和 ACL 授权、内存运行态数据、expvar 指标以及 pprof。

## 功能

- MQTT 3.1.1 over TCP
- MQTT 5.0 over TCP
- MQTT over WebSocket
- 基础 QoS0 / QoS1 / QoS2 状态处理
- Retained message、will message、persistent session、offline message
- MQTT 5 session expiry、receive maximum、maximum packet size、topic alias
- 独立管理服务，提供设备认证和 topic ACL 授权
- 内存运行态存储
- 用于网关认证和授权的 Admin HTTP API
- expvar / pprof 指标
- Benchmark 压测工具

## 目录

```text
meta/
├── admin/                     # 基于配置文件的轻量认证/授权服务
├── benchmark/                 # MQTT 压测工具
├── cmd/
│   └── meta-broker/           # 网关启动入口
├── internal/
│   ├── cache/                 # 内存缓存，后续可替换为 Redis
│   ├── core/                  # Broker 核心、client、store、topic index、stats
│   ├── gateway/               # 监听器生命周期和协议绑定
│   ├── protocol/
│   │   ├── mqtt3/             # MQTT 3.1.1 协议处理
│   │   └── mqtt5/             # MQTT 5.0 协议处理
│   ├── security/              # 认证和 ACL 抽象
│   └── transport/
│       ├── tcp/
│       └── websocket/
```

## 快速开始

启动轻量管理服务：

```bash
go run ./admin \
  -addr :18080 \
  -config admin/config.example.json
```

启动 broker：

```bash
go run ./cmd/meta-broker \
  -host tcp://0.0.0.0:1883 \
  -mqtt5 tcp://0.0.0.0:1885 \
  -ws :8083 \
  -ws-path /mqtt \
  -ws-protocol mqtt-3.1.1 \
  -auth api \
  -auth-api http://127.0.0.1:18080 \
  -auth-api-token change-me \
  -metrics :18081
```

本地开发时，如果不需要管理服务，可以临时使用 `allow-all`：

```bash
go run ./cmd/meta-broker \
  -host tcp://127.0.0.1:1883 \
  -auth allow-all
```

验证 demo client：

```bash
go run ./benchmark \
  -broker tcp://127.0.0.1:1883 \
  -n 1 \
  -client-id demo-mqtt3 \
  -username demo-user \
  -password demo-password
```

## Broker 参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `-host` | `tcp://0.0.0.0:1883` | MQTT 3.1.1 TCP 监听 URI |
| `-mqtt5` | 空 | MQTT 5.0 TCP 监听 URI |
| `-ws` | 空 | MQTT over WebSocket 监听地址 |
| `-ws-path` | `/mqtt` | WebSocket path |
| `-ws-protocol` | `mqtt-3.1.1` | WebSocket listener 使用的 MQTT 协议 |
| `-auth` | `allow-all` | 网关鉴权模式：`allow-all` 或 `api` |
| `-auth-api` | 空 | 网关调用的管理服务地址 |
| `-auth-api-token` | 空 | 网关调用管理服务时携带的 token |
| `-metrics` | 空 | expvar / pprof 监听地址 |
| `-workers` | `0` | Broker worker 数量；`0` 表示 `runtime.NumCPU()` |
| `-task-queue` | `4096` | Broker publish task 队列大小 |
| `-enqueue-wait` | `200ms` | publish task 入队最大等待时间 |
| `-outbound-queue` | `1024` | 单 client 出站队列大小 |

## Admin 参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `-addr` | `:18080` | Admin HTTP 监听地址 |
| `-config` | `admin/config.example.json` | 设备和 ACL 配置文件 |

## 数据模型

Broker 和 admin 服务是两个独立进程。当前 `./admin` 是轻量支持服务，不使用数据库，设备和 ACL 保存在 JSON 配置文件中。

`admin/config.example.json` 示例：

```json
{
  "token": "change-me",
  "devices": [
    {"id":"demo-mqtt3","protocol":"mqtt-3.1.1","username":"demo-user","password":"demo-password","enabled":true}
  ],
  "acls": [
    {"clientId":"demo-mqtt3","topicFilter":"#","action":"*"}
  ]
}
```

后续可以用完整的管理系统替换 `./admin`，只要保持相同的认证和授权 API 契约即可。Broker 不连接任何管理数据库，认证和授权都通过 Admin API 完成，因此多台 broker 可以共享同一个管理服务并横向扩容。

## Admin API

如果 `admin/config.example.json` 配置了 `token`，所有 `/api/*` 请求都需要携带以下任意一种 header：

```http
Authorization: Bearer <token>
```

或：

```http
X-Admin-Token: <token>
```

接口：

| Method | Path | 说明 |
|---|---|---|
| `GET` | `/health` | 健康检查 |
| `POST` | `/api/security/authenticate` | Broker 使用的设备认证接口 |
| `POST` | `/api/security/authorize` | Broker 使用的 topic 授权接口 |

认证请求：

```json
{"protocol":"mqtt-3.1.1","clientId":"demo-mqtt3","username":"demo-user","password":"demo-password"}
```

授权请求：

```json
{"protocol":"mqtt-3.1.1","clientId":"demo-mqtt3","topic":"device/1/status","action":"publish"}
```

响应：

```json
{"allowed":true}
```

## 指标

启动 broker 时开启 metrics：

```bash
go run ./cmd/meta-broker -host tcp://0.0.0.0:1883 -metrics :18081
```

Metrics 和 pprof 地址：

- `http://127.0.0.1:18081/debug/vars`
- `http://127.0.0.1:18081/debug/pprof/`

核心指标位于 `meta_broker`：

- `accepted_connections`
- `online_clients`
- `subscriptions`
- `sessions`
- `retained_messages`
- `task_queue_len`
- `task_queue_cap`
- `task_dropped`
- `publish_rejected`
- `publish_messages`
- `deliveries`
- `delivery_failures`

建议重点监控 `task_dropped` 和 `publish_rejected`。对于 QoS1 / QoS2，如果消息无法入队，broker 不会提前返回 ACK，避免客户端误以为内部已经丢弃的消息发布成功。

## Benchmark

基础压测：

```bash
go run ./benchmark \
  -broker tcp://127.0.0.1:1883 \
  -n 100 \
  -topics 100 \
  -duration 30s \
  -interval 100ms \
  -qos 1 \
  -payload 128
```

带连接爬坡的压测：

```bash
go run ./benchmark \
  -broker tcp://127.0.0.1:1883 \
  -n 2000 \
  -topics 2000 \
  -duration 30s \
  -connect-ramp 10s \
  -connect-timeout 15s \
  -interval 10ms \
  -qos 1 \
  -payload 128
```

Benchmark 参数：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `-broker` | `tcp://127.0.0.1:1883` | Broker URI |
| `-n` | `100` | Client 数量 |
| `-topics` | `1` | Topic 数量 |
| `-duration` | `30s` | 压测时长 |
| `-connect-ramp` | `0` | 将 client 建连分散到指定时间窗口 |
| `-connect-timeout` | `10s` | 单 client 连接超时时间 |
| `-interval` | `2s` | 单 client 发布间隔 |
| `-qos` | `0` | publish 和 subscribe 使用的 QoS |
| `-payload` | `64` | payload 字节数 |
| `-username` | `admin` | MQTT username |
| `-password` | `admin` | MQTT password |
| `-client-id` | 空 | 固定 client ID；当 `n > 1` 时会自动追加索引后缀 |

## 验证

```bash
go test ./...
go test -race ./...
```
