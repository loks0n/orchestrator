# Orchestrator Usage Guide

This guide covers how to interact with the orchestrator service as an API client.

## API Overview

Base URL: `http://localhost:8080` (default)

All endpoints require the `Authorization: Bearer <token>` header when `API_KEY_FILE` is configured.

## Endpoints

### Create Job

```
POST /v1/jobs
```

**Request Body:**

```json
{
  "id": "my-job-123",
  "meta": {
    "userId": "user-456",
    "requestId": "req-789"
  },
  "image": "alpine:latest",
  "command": "sh -c 'echo hello > /shared/output.txt'",
  "cpu": 1,
  "memory": 512,
  "environment": {
    "MY_VAR": "value"
  },
  "timeoutSeconds": 300,
  "inputs": [...],
  "outputs": [...],
  "callback": {...}
}
```

**Response:** `202 Accepted`

```json
{
  "id": "my-job-123",
  "status": "accepted"
}
```

### Get Job Status

```
GET /v1/jobs/{jobId}
```

**Response:**

```json
{
  "id": "my-job-123",
  "status": "completed",
  "exitCode": 0
}
```

Status values: `accepted`, `running`, `completed`, `failed`, `cancelled`

### List Jobs

```
GET /v1/jobs
```

**Response:**

```json
{
  "jobs": [
    {"id": "job-1", "status": "running"},
    {"id": "job-2", "status": "completed", "exitCode": 0}
  ]
}
```

### Cancel Job

```
DELETE /v1/jobs/{jobId}
```

**Response:** `204 No Content`

### Health Checks

```
GET /livez   # Liveness probe
GET /readyz  # Readiness probe (checks Docker connectivity)
```

## Inputs

Inputs are processed before the job container starts. Files are written to `/shared/` inside the container.

### Download Input

Download a file from a URL:

```json
{
  "id": "model-weights",
  "type": "download",
  "path": "models/weights.bin",
  "url": "https://example.com/weights.bin"
}
```

### File Input

Write content directly:

```json
{
  "id": "config",
  "type": "file",
  "path": "config.json",
  "content": "{\"key\": \"value\"}"
}
```

## Outputs

Outputs are processed after the job writes files to `/shared/`.

### Upload Output

Upload a file to a presigned URL:

```json
{
  "id": "result",
  "type": "upload",
  "path": "output.tar.gz",
  "url": "https://storage.example.com/presigned-upload-url"
}
```

### Event Output

Include file contents in the callback event:

```json
{
  "id": "metrics",
  "type": "event",
  "path": "metrics.json"
}
```

The file contents (parsed as JSON if valid, otherwise string) are included in the `orchestrator.job.output` event's `content` field.

## Callbacks

Receive lifecycle events via HTTP POST to your endpoint.

```json
{
  "callback": {
    "url": "https://your-service.example.com/webhook",
    "key": "your-hmac-secret",
    "events": ["orchestrator.job.start", "orchestrator.job.exit"]
  }
}
```

- `url` - Your webhook endpoint
- `key` - (Optional) HMAC-SHA256 signing key for verifying events
- `events` - (Optional) Filter which events to receive. Empty = all events.

### Event Types

| Type | Description |
|------|-------------|
| `orchestrator.job.start` | Job container started |
| `orchestrator.job.input` | Input processed (success/failed) |
| `orchestrator.job.log` | Log lines from stdout/stderr |
| `orchestrator.job.exit` | Job container exited |
| `orchestrator.job.output` | Output processed (success/failed) |

### CloudEvent Format

Events follow the [CloudEvents 1.0 specification](https://cloudevents.io/).

**Headers:**

```
Content-Type: application/cloudevents+json
Ce-Specversion: 1.0
Ce-Type: orchestrator.job.exit
Ce-Source: orchestrator/supervisor
Ce-Subject: my-job-123
Ce-Id: my-job-123-1234567890
Ce-Time: 2024-01-15T10:30:00Z
X-Signature-256: sha256=abc123...
```

**Body:**

```json
{
  "specversion": "1.0",
  "type": "orchestrator.job.exit",
  "source": "orchestrator/supervisor",
  "subject": "my-job-123",
  "id": "my-job-123-1234567890",
  "time": "2024-01-15T10:30:00.000Z",
  "datacontenttype": "application/json",
  "data": {
    "jobId": "my-job-123",
    "exitCode": 0,
    "meta": {
      "userId": "user-456",
      "requestId": "req-789"
    }
  }
}
```

### Event Data Schemas

**start:**
```json
{"jobId": "...", "meta": {...}}
```

**input:**
```json
{"jobId": "...", "inputId": "...", "status": "success|failed", "path": "...", "error": "...", "meta": {...}}
```

**log:**
```json
{"jobId": "...", "lines": ["line1", "line2"], "stream": "stdout|stderr", "meta": {...}}
```

**exit:**
```json
{"jobId": "...", "exitCode": 0, "error": "...", "meta": {...}}
```

**output:**
```json
{"jobId": "...", "outputId": "...", "outputType": "upload|event", "status": "success|failed", "content": ..., "error": "...", "meta": {...}}
```

### Verifying Signatures

When `callback.key` is set, events are signed with HMAC-SHA256. Verify the `X-Signature-256` header:

```go
func verifySignature(body []byte, signature, key string) bool {
    mac := hmac.New(sha256.New, []byte(key))
    mac.Write(body)
    expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
    return hmac.Equal([]byte(signature), []byte(expected))
}
```

```php
function verifySignature(string $body, string $signature, string $key): bool {
    $expected = 'sha256=' . hash_hmac('sha256', $body, $key);
    return hash_equals($expected, $signature);
}
```

## Complete Example

```json
{
  "id": "video-transcode-001",
  "meta": {
    "userId": "user-123",
    "projectId": "proj-456"
  },
  "image": "ffmpeg:latest",
  "command": "ffmpeg -i /shared/input.mp4 -c:v libx264 /shared/output.mp4",
  "cpu": 4,
  "memory": 4096,
  "timeoutSeconds": 3600,
  "inputs": [
    {
      "id": "source-video",
      "type": "download",
      "path": "input.mp4",
      "url": "https://storage.example.com/videos/source.mp4"
    }
  ],
  "outputs": [
    {
      "id": "transcoded-video",
      "type": "upload",
      "path": "output.mp4",
      "url": "https://storage.example.com/upload/output.mp4?signature=..."
    }
  ],
  "callback": {
    "url": "https://api.example.com/webhooks/jobs",
    "key": "whsec_abc123",
    "events": ["orchestrator.job.exit", "orchestrator.job.output"]
  }
}
```

## Error Responses

All errors return JSON:

```json
{
  "error": "Job not found"
}
```

| Status | Meaning |
|--------|---------|
| 400 | Invalid request |
| 401 | Missing/invalid API key |
| 404 | Job not found |
| 409 | Job already exists |
| 500 | Internal error |
