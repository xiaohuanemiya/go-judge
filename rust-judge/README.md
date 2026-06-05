# rust-judge

A **Rust rewrite** of [go-judge](https://github.com/criyle/go-judge) — a secure, sandboxed code execution service.

`rust-judge` exposes the **same REST API** as go-judge so it is a drop-in replacement.

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/run` | Execute one or more commands in the sandbox |
| `GET` | `/file` | List all cached files (`{fileId: name}`) |
| `POST` | `/file` | Upload a file; returns `fileId` |
| `GET` | `/file/:fileId` | Download a cached file |
| `DELETE` | `/file/:fileId` | Delete a cached file |
| `GET` | `/version` | Version information |
| `GET` | `/config` | Runtime configuration |

### POST /run

#### Request

```json
{
  "requestId": "optional-string",
  "cmd": [
    {
      "args": ["/bin/sh", "-c", "echo hello"],
      "env": ["PATH=/usr/bin:/bin"],
      "files": [
        null,
        {"name": "stdout", "max": 10485760},
        {"name": "stderr", "max": 10485760}
      ],
      "cpuLimit":    10000000000,
      "clockLimit":  30000000000,
      "memoryLimit": 268435456,
      "procLimit":   50,
      "copyIn": {
        "input.txt": {"content": "hello\n"}
      },
      "copyOut":       ["output.txt"],
      "copyOutCached": ["a.out"]
    }
  ]
}
```

All `*Limit` values are in **nanoseconds** (time) or **bytes** (memory/size).

#### Response

```json
[
  {
    "status":     "Accepted",
    "exitStatus": 0,
    "time":       976000,
    "memory":     2035712,
    "runTime":    1172158,
    "files": {
      "stdout": "hello\n",
      "stderr": ""
    },
    "fileIds": {
      "a.out": "uuid-of-cached-file"
    }
  }
]
```

Status values mirror go-judge: `Accepted`, `Nonzero Exit Status`, `Time Limit Exceeded`,
`Memory Limit Exceeded`, `Output Limit Exceeded`, `File Error`, `Signalled`, `Internal Error`, etc.

## Quick Start with Docker

```bash
# Build and start
cd rust-judge
docker-compose up -d

# Test
curl -s http://localhost:5050/version | python3 -m json.tool
curl -s -X POST http://localhost:5050/run \
  -H "Content-Type: application/json" \
  -d '{
    "cmd": [{
      "args": ["/bin/echo", "Hello, rust-judge!"],
      "env": ["PATH=/usr/bin:/bin"],
      "files": [null, {"name":"stdout","max":65536}, {"name":"stderr","max":65536}],
      "cpuLimit": 10000000000,
      "memoryLimit": 268435456
    }]
  }' | python3 -m json.tool
```

## Building Locally

```bash
cargo build --release
./target/release/rust-judge --http-addr 0.0.0.0:5050
```

## Configuration

All options can be set via CLI flags or environment variables:

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--http-addr` | `HTTP_ADDR` | `0.0.0.0:5050` | Listen address |
| `--parallelism` | `PARALLELISM` | # of CPUs | Concurrent executions |
| `--work-dir` | `WORK_DIR` | `/tmp/rust-judge` | Temp working directory |
| `--file-store-dir` | `FILE_STORE_DIR` | `$WORK_DIR/files` | File store directory |
| `--output-limit` | `OUTPUT_LIMIT` | `268435456` | Per-stream output limit (bytes) |
| `--copy-out-limit` | `COPY_OUT_LIMIT` | `67108864` | Copy-out size limit (bytes) |
| `--file-timeout` | `FILE_TIMEOUT` | `0` | File TTL in seconds (0 = no expiry) |
| `--auth-token` | `AUTH_TOKEN` | `` | Bearer token for auth |
| `--log-level` | `LOG_LEVEL` | `info` | Log level |

## Security Notes

- Process resource limits are enforced via `setrlimit(2)` (CPU time, memory, process count, file size).
- A clock-timeout thread sends `SIGKILL` to processes that exceed the wall-clock limit.
- Unlike go-judge's full namespace/seccomp sandbox, `rust-judge` relies on OS-level resource limits.
  For stricter isolation run the service inside Docker with `--privileged` (or use a seccomp profile).
