# Meta

[简体中文](README.md)

Meta is a self-hosted MQTT broker and multi-protocol gateway. It currently supports MQTT 3.1.1, MQTT 5.0, MQTT over WebSocket, a lightweight standalone admin service, API-based authentication and ACL checks, in-memory runtime state, expvar metrics, and pprof.

## Features

- MQTT 3.1.1 over TCP
- MQTT 5.0 over TCP
- MQTT over WebSocket
- Basic QoS0 / QoS1 / QoS2 state handling
- Retained messages, will messages, persistent sessions, and offline messages
- MQTT 5 session expiry, receive maximum, maximum packet size, and topic alias handling
- Standalone admin service for device authentication and topic ACL authorization
- In-memory runtime stores
- Admin HTTP API for gateway authentication and authorization
- expvar / pprof metrics
- Benchmark tool

## Layout

```text
meta/
├── admin/                     # Lightweight config-file based auth/admin service
├── benchmark/                 # MQTT benchmark tool
├── cmd/
│   └── meta-broker/           # Gateway entrypoint
├── internal/
│   ├── cache/                 # In-memory cache; can be replaced by Redis later
│   ├── core/                  # Broker core, client, stores, topic index, stats
│   ├── gateway/               # Listener lifecycle and protocol binding
│   ├── protocol/
│   │   ├── mqtt3/             # MQTT 3.1.1 protocol handling
│   │   └── mqtt5/             # MQTT 5.0 protocol handling
│   ├── security/              # Authentication and ACL abstractions
│   └── transport/
│       ├── tcp/
│       └── websocket/
```

## Quick Start

Start the lightweight admin service:

```bash
go run ./admin \
  -addr :18080 \
  -config admin/config.example.json
```

Start the broker:

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

For local development without the admin service, use `allow-all`:

```bash
go run ./cmd/meta-broker \
  -host tcp://127.0.0.1:1883 \
  -auth allow-all
```

Verify the demo client:

```bash
go run ./benchmark \
  -broker tcp://127.0.0.1:1883 \
  -n 1 \
  -client-id demo-mqtt3 \
  -username demo-user \
  -password demo-password
```

## Broker Flags

| Flag | Default | Description |
|---|---:|---|
| `-host` | `tcp://0.0.0.0:1883` | MQTT 3.1.1 TCP listener URI |
| `-mqtt5` | empty | MQTT 5.0 TCP listener URI |
| `-ws` | empty | MQTT over WebSocket listen address |
| `-ws-path` | `/mqtt` | WebSocket path |
| `-ws-protocol` | `mqtt-3.1.1` | MQTT protocol used by the WebSocket listener |
| `-auth` | `allow-all` | Gateway auth mode: `allow-all` or `api` |
| `-auth-api` | empty | Admin service base URL used by the gateway |
| `-auth-api-token` | empty | Token sent by the gateway to the admin service |
| `-metrics` | empty | expvar / pprof listen address |
| `-workers` | `0` | Broker worker count; `0` means `runtime.NumCPU()` |
| `-task-queue` | `4096` | Broker publish task queue size |
| `-enqueue-wait` | `200ms` | Maximum wait time when enqueueing publish tasks |
| `-outbound-queue` | `1024` | Per-client outbound queue size |

## Admin Flags

| Flag | Default | Description |
|---|---:|---|
| `-addr` | `:18080` | Admin HTTP listen address |
| `-config` | `admin/config.example.json` | Device and ACL config file |

## Data Model

The broker and admin service run as separate processes. The current `./admin` service is intentionally lightweight: it does not use a database and keeps devices and ACLs in a JSON config file.

Example `admin/config.example.json`:

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

A full admin system can replace `./admin` later as long as it keeps the same authentication and authorization API contract. The broker does not connect to any admin database. Authentication and authorization are resolved through the admin API, which allows multiple broker instances to scale horizontally behind the same admin service.

## Admin API

If `token` is configured in `admin/config.example.json`, all `/api/*` requests must include one of the following headers:

```http
Authorization: Bearer <token>
```

or:

```http
X-Admin-Token: <token>
```

Endpoints:

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Health check |
| `POST` | `/api/security/authenticate` | Device authentication used by the broker |
| `POST` | `/api/security/authorize` | Topic authorization used by the broker |

Authentication request:

```json
{"protocol":"mqtt-3.1.1","clientId":"demo-mqtt3","username":"demo-user","password":"demo-password"}
```

Authorization request:

```json
{"protocol":"mqtt-3.1.1","clientId":"demo-mqtt3","topic":"device/1/status","action":"publish"}
```

Response:

```json
{"allowed":true}
```

## Metrics

Start the broker with metrics enabled:

```bash
go run ./cmd/meta-broker -host tcp://0.0.0.0:1883 -metrics :18081
```

Metrics and pprof endpoints:

- `http://127.0.0.1:18081/debug/vars`
- `http://127.0.0.1:18081/debug/pprof/`

Core metrics are exposed under `meta_broker`:

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

`task_dropped` and `publish_rejected` should be monitored closely. For QoS1 / QoS2, the broker does not acknowledge a publish if the message cannot be enqueued, so clients do not incorrectly treat internally dropped work as successful delivery.

## Benchmark

Basic benchmark:

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

Benchmark with connection ramp-up:

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

Benchmark flags:

| Flag | Default | Description |
|---|---:|---|
| `-broker` | `tcp://127.0.0.1:1883` | Broker URI |
| `-n` | `100` | Number of clients |
| `-topics` | `1` | Number of topics |
| `-duration` | `30s` | Benchmark duration |
| `-connect-ramp` | `0` | Spread client connections over the given time window |
| `-connect-timeout` | `10s` | Per-client connection timeout |
| `-interval` | `2s` | Publish interval per client |
| `-qos` | `0` | Publish and subscribe QoS |
| `-payload` | `64` | Payload size in bytes |
| `-username` | `admin` | MQTT username |
| `-password` | `admin` | MQTT password |
| `-client-id` | empty | Fixed client ID; when `n > 1`, an index suffix is appended |

## Verification

```bash
go test ./...
go test -race ./...
```
